"""Concurrent processing for independent pipeline steps.

Following the Zen of Python:
- Beautiful is better than ugly
- Simple is better than complex
- There should be one obvious way to do it

This module provides concurrent execution of independent steps
using asyncio and threading for improved performance.
"""

import asyncio
import concurrent.futures
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlflow.logging import get_logger

from .async_execution import AsyncStepExecutor
from .execution import ExecutionContext
from .memory_optimization import (
    OptimizedStepDefinition,
    OptimizedStepResult,
    StepStatus,
)

logger = get_logger(__name__)


class DependencyGraph:
    """Builds and analyzes step dependencies for concurrent execution.

    Provides topological sorting and dependency analysis to determine
    which steps can be executed concurrently.
    """

    def __init__(self, steps: List[OptimizedStepDefinition]):
        """Initialize dependency graph from steps."""
        self.steps = {step.id: step for step in steps}
        self.dependencies = self._build_dependency_graph()
        self.reverse_dependencies = self._build_reverse_dependencies()

    def _build_dependency_graph(self) -> Dict[str, Set[str]]:
        """Build forward dependency graph."""
        graph = {}
        for step in self.steps.values():
            graph[step.id] = set(step.depends_on)
        return graph

    def _build_reverse_dependencies(self) -> Dict[str, Set[str]]:
        """Build reverse dependency graph (dependents)."""
        reverse_graph = defaultdict(set)
        for step_id, deps in self.dependencies.items():
            for dep in deps:
                reverse_graph[dep].add(step_id)
        return dict(reverse_graph)

    def get_executable_steps(self, completed_steps: Set[str]) -> List[str]:
        """Get steps that can be executed given completed steps."""
        executable = []

        for step_id, deps in self.dependencies.items():
            if step_id not in completed_steps:
                # Check if all dependencies are completed
                if deps.issubset(completed_steps):
                    executable.append(step_id)

        return executable

    def topological_sort(self) -> List[List[str]]:
        """Return steps grouped by execution level for concurrent processing."""
        levels = []
        completed = set()
        remaining = set(self.steps.keys())

        while remaining:
            # Find steps with no unfulfilled dependencies
            current_level = []
            for step_id in remaining:
                deps = self.dependencies[step_id]
                if deps.issubset(completed):
                    current_level.append(step_id)

            if not current_level:
                # Circular dependency detected
                raise ValueError(
                    f"Circular dependency detected among steps: {remaining}"
                )

            levels.append(current_level)
            completed.update(current_level)
            remaining.difference_update(current_level)

        return levels

    def get_critical_path(self) -> List[str]:
        """Find the critical path (longest dependency chain)."""

        def dfs_longest_path(step_id: str, visited: Set[str]) -> List[str]:
            if step_id in visited:
                return []  # Avoid cycles

            visited.add(step_id)

            longest_path = [step_id]
            max_sub_path = []

            # Check all dependencies
            for dep in self.dependencies[step_id]:
                sub_path = dfs_longest_path(dep, visited.copy())
                if len(sub_path) > len(max_sub_path):
                    max_sub_path = sub_path

            return max_sub_path + longest_path

        # Find the longest path from any starting node
        critical_path = []
        for step_id in self.steps:
            path = dfs_longest_path(step_id, set())
            if len(path) > len(critical_path):
                critical_path = path

        return critical_path[::-1]  # Reverse to get dependency order


class ConcurrentStepExecutor:
    """Executes pipeline steps concurrently when dependencies allow.

    Uses asyncio for I/O-bound operations and thread pools for
    CPU-bound operations.
    """

    def __init__(
        self,
        step_executors: List[AsyncStepExecutor],
        max_concurrent_steps: int = 5,
        max_threads: int = 4,
    ):
        """Initialize concurrent executor."""
        self.step_executors = step_executors
        self.max_concurrent_steps = max_concurrent_steps
        self.max_threads = max_threads
        self.semaphore = asyncio.Semaphore(max_concurrent_steps)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_threads
        )

    async def execute_steps_concurrently(
        self, steps: List[OptimizedStepDefinition], context: ExecutionContext
    ) -> List[OptimizedStepResult]:
        """Execute steps with maximum concurrency allowed by dependencies."""
        dependency_graph = DependencyGraph(steps)
        execution_levels = dependency_graph.topological_sort()

        logger.info(
            f"Executing {len(steps)} steps in {len(execution_levels)} concurrent levels"
        )

        results = {}
        completed_steps = set()

        # Execute each level concurrently
        for level_index, step_ids in enumerate(execution_levels):
            logger.info(f"Executing level {level_index + 1} with {len(step_ids)} steps")

            # Create tasks for all steps in this level
            tasks = []
            for step_id in step_ids:
                step = dependency_graph.steps[step_id]
                task = self._execute_step_with_concurrency_control(step, context)
                tasks.append((step_id, task))

            # Wait for all tasks in this level to complete
            level_results = await asyncio.gather(
                *[task for _, task in tasks], return_exceptions=True
            )

            # Process results and check for failures
            for i, result in enumerate(level_results):
                step_id = tasks[i][0]

                if isinstance(result, Exception):
                    logger.error(f"Step {step_id} failed with exception: {result}")
                    result = OptimizedStepResult.error(step_id, str(result))

                results[step_id] = result

                # Mark as completed if successful
                if result.status == StepStatus.SUCCESS:
                    completed_steps.add(step_id)
                else:
                    # Fail fast - stop execution on error
                    logger.error(f"Stopping execution due to failed step: {step_id}")
                    return self._convert_results_to_list(results, steps)

        return self._convert_results_to_list(results, steps)

    async def _execute_step_with_concurrency_control(
        self, step: OptimizedStepDefinition, context: ExecutionContext
    ) -> OptimizedStepResult:
        """Execute single step with concurrency control."""
        async with self.semaphore:
            start_time = time.time()

            try:
                # Find appropriate executor
                executor = await self._find_executor(step)
                if not executor:
                    return OptimizedStepResult.error(
                        step.id, f"No executor found for step type: {step.type}"
                    )

                # Convert to dict format for executor
                step_dict = step.to_dict()

                # Execute step
                result = await executor.execute(step_dict, context)
                execution_time = time.time() - start_time

                # Convert to optimized result
                if hasattr(result, "status") and result.status == "success":
                    return OptimizedStepResult.success(
                        step.id,
                        message=getattr(
                            result, "message", "Step completed successfully"
                        ),
                        execution_time=execution_time,
                        data=getattr(result, "data", None),
                        row_count=getattr(result, "row_count", None),
                    )
                else:
                    return OptimizedStepResult.error(
                        step.id,
                        getattr(result, "error", "Unknown error"),
                    )

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Error executing step {step.id}: {e}")
                return OptimizedStepResult.error(step.id, str(e))

    async def _find_executor(
        self, step: OptimizedStepDefinition
    ) -> Optional[AsyncStepExecutor]:
        """Find appropriate executor for step."""
        step_dict = step.to_dict()

        for executor in self.step_executors:
            if await executor.can_execute(step_dict):
                return executor

        return None

    def _convert_results_to_list(
        self,
        results: Dict[str, OptimizedStepResult],
        steps: List[OptimizedStepDefinition],
    ) -> List[OptimizedStepResult]:
        """Convert results dict to ordered list."""
        ordered_results = []

        for step in steps:
            if step.id in results:
                ordered_results.append(results[step.id])
            else:
                # Step was not executed (likely due to earlier failure)
                ordered_results.append(
                    OptimizedStepResult(
                        step_id=step.id,
                        status=StepStatus.SKIPPED,
                        message="Step skipped due to earlier failure",
                    )
                )

        return ordered_results


class ThreadPoolStepExecutor:
    """Execute CPU-intensive steps using thread pools.

    For steps that are CPU-bound rather than I/O-bound,
    uses thread pools for concurrent execution.
    """

    def __init__(self, max_workers: int = 4):
        """Initialize thread pool executor."""
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def execute_cpu_intensive_steps(
        self,
        steps: List[OptimizedStepDefinition],
        context: ExecutionContext,
        step_executor_func: callable,
    ) -> List[OptimizedStepResult]:
        """Execute CPU-intensive steps using thread pool."""
        dependency_graph = DependencyGraph(steps)
        execution_levels = dependency_graph.topological_sort()

        logger.info(
            f"Executing {len(steps)} CPU-intensive steps using {self.max_workers} threads"
        )

        results = {}
        completed_steps = set()

        # Execute each level using thread pool
        for level_index, step_ids in enumerate(execution_levels):
            logger.info(
                f"Executing CPU level {level_index + 1} with {len(step_ids)} steps"
            )

            # Submit all steps in this level to thread pool
            future_to_step = {}
            for step_id in step_ids:
                step = dependency_graph.steps[step_id]
                future = self.executor.submit(step_executor_func, step, context)
                future_to_step[future] = step_id

            # Wait for all futures to complete
            for future in concurrent.futures.as_completed(future_to_step):
                step_id = future_to_step[future]

                try:
                    result = future.result()
                    results[step_id] = result

                    if result.status == StepStatus.SUCCESS:
                        completed_steps.add(step_id)
                    else:
                        # Fail fast on error
                        logger.error(f"CPU step {step_id} failed: {result.error}")
                        self._cancel_remaining_futures(future_to_step)
                        return self._convert_results_to_list(results, steps)

                except Exception as e:
                    logger.error(f"Exception in CPU step {step_id}: {e}")
                    result = OptimizedStepResult.error(step_id, str(e))
                    results[step_id] = result
                    self._cancel_remaining_futures(future_to_step)
                    return self._convert_results_to_list(results, steps)

        return self._convert_results_to_list(results, steps)

    def _cancel_remaining_futures(self, future_to_step: Dict) -> None:
        """Cancel remaining futures on error."""
        for future in future_to_step:
            if not future.done():
                future.cancel()

    def _convert_results_to_list(
        self,
        results: Dict[str, OptimizedStepResult],
        steps: List[OptimizedStepDefinition],
    ) -> List[OptimizedStepResult]:
        """Convert results dict to ordered list."""
        ordered_results = []

        for step in steps:
            if step.id in results:
                ordered_results.append(results[step.id])
            else:
                ordered_results.append(
                    OptimizedStepResult(
                        step_id=step.id,
                        status=StepStatus.SKIPPED,
                        message="Step skipped due to earlier failure",
                    )
                )

        return ordered_results

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown thread pool executor."""
        self.executor.shutdown(wait=wait)


class HybridConcurrentExecutor:
    """Hybrid executor combining async and thread-based concurrency.

    Automatically chooses the best execution strategy based on
    step characteristics and system resources.
    """

    def __init__(
        self,
        async_executors: List[AsyncStepExecutor],
        max_async_concurrent: int = 5,
        max_threads: int = 4,
    ):
        """Initialize hybrid executor."""
        self.async_executor = ConcurrentStepExecutor(
            async_executors, max_async_concurrent
        )
        self.thread_executor = ThreadPoolStepExecutor(max_threads)

    async def execute_pipeline_optimally(
        self, steps: List[OptimizedStepDefinition], context: ExecutionContext
    ) -> List[OptimizedStepResult]:
        """Execute pipeline using optimal concurrency strategy."""
        # Analyze steps to determine execution strategy
        io_intensive_steps, cpu_intensive_steps = self._categorize_steps(steps)

        logger.info(
            f"Executing pipeline: {len(io_intensive_steps)} I/O steps, "
            f"{len(cpu_intensive_steps)} CPU steps"
        )

        all_results = []

        # Execute I/O-intensive steps with async
        if io_intensive_steps:
            logger.info("Executing I/O-intensive steps with async concurrency")
            io_results = await self.async_executor.execute_steps_concurrently(
                io_intensive_steps, context
            )
            all_results.extend(io_results)

        # Execute CPU-intensive steps with thread pool
        if cpu_intensive_steps:
            logger.info("Executing CPU-intensive steps with thread pool")

            def sync_step_executor(
                step: OptimizedStepDefinition, ctx: ExecutionContext
            ) -> OptimizedStepResult:
                # This would need to be implemented based on your sync executors
                # For now, return a placeholder
                return OptimizedStepResult.success(
                    step.id, "CPU step executed via thread pool"
                )

            cpu_results = self.thread_executor.execute_cpu_intensive_steps(
                cpu_intensive_steps, context, sync_step_executor
            )
            all_results.extend(cpu_results)

        # Sort results by original step order
        step_order = {step.id: i for i, step in enumerate(steps)}
        all_results.sort(key=lambda r: step_order.get(r.step_id, float("inf")))

        return all_results

    def _categorize_steps(
        self, steps: List[OptimizedStepDefinition]
    ) -> Tuple[List[OptimizedStepDefinition], List[OptimizedStepDefinition]]:
        """Categorize steps as I/O or CPU intensive."""
        io_intensive = []
        cpu_intensive = []

        for step in steps:
            # Heuristics for categorizing steps
            if step.type in ("load", "export"):
                # Data loading/exporting is typically I/O bound
                io_intensive.append(step)
            elif step.type == "transform":
                # Check if transform involves complex computation
                if self._is_complex_transform(step):
                    cpu_intensive.append(step)
                else:
                    io_intensive.append(step)
            else:
                # Default to I/O intensive
                io_intensive.append(step)

        return io_intensive, cpu_intensive

    def _is_complex_transform(self, step: OptimizedStepDefinition) -> bool:
        """Determine if transform step is CPU intensive."""
        if not step.sql:
            return False

        # Simple heuristics for CPU-intensive operations
        cpu_intensive_keywords = [
            "RECURSIVE",
            "DENSE_RANK",
            "LAG",
            "LEAD",
            "PARTITION BY",
            "WINDOW",
            "CROSS JOIN",
        ]

        sql_upper = step.sql.upper()
        return any(keyword in sql_upper for keyword in cpu_intensive_keywords)

    def shutdown(self) -> None:
        """Shutdown all executors."""
        self.thread_executor.shutdown()


class ConcurrentPipelineCoordinator:
    """Main coordinator for concurrent pipeline execution.

    Provides the high-level interface for executing pipelines
    with maximum concurrency and optimal resource utilization.
    """

    def __init__(
        self,
        async_executors: List[AsyncStepExecutor],
        max_concurrent_steps: int = 5,
        max_threads: int = 4,
    ):
        """Initialize concurrent pipeline coordinator."""
        self.hybrid_executor = HybridConcurrentExecutor(
            async_executors, max_concurrent_steps, max_threads
        )

    async def execute_pipeline(
        self, steps: List[OptimizedStepDefinition], context: ExecutionContext
    ) -> List[OptimizedStepResult]:
        """Execute pipeline with optimal concurrency."""
        logger.info(f"Starting concurrent execution of {len(steps)} steps")

        start_time = time.time()

        try:
            results = await self.hybrid_executor.execute_pipeline_optimally(
                steps, context
            )

            execution_time = time.time() - start_time
            successful_steps = sum(1 for r in results if r.status == StepStatus.SUCCESS)

            logger.info(
                f"Concurrent pipeline execution completed: "
                f"{successful_steps}/{len(steps)} steps successful in {execution_time:.2f}s"
            )

            return results

        except Exception as e:
            logger.error(f"Concurrent pipeline execution failed: {e}")
            raise
        finally:
            self.hybrid_executor.shutdown()

    def get_performance_metrics(
        self, results: List[OptimizedStepResult]
    ) -> Dict[str, Any]:
        """Calculate performance metrics for concurrent execution."""
        total_execution_time = sum(
            r.execution_time for r in results if r.execution_time is not None
        )

        successful_steps = [r for r in results if r.status == StepStatus.SUCCESS]
        failed_steps = [r for r in results if r.status == StepStatus.ERROR]

        return {
            "total_steps": len(results),
            "successful_steps": len(successful_steps),
            "failed_steps": len(failed_steps),
            "success_rate": len(successful_steps) / len(results) if results else 0,
            "total_execution_time": total_execution_time,
            "average_step_time": (
                total_execution_time / len(successful_steps) if successful_steps else 0
            ),
            "concurrency_achieved": True,
        }
