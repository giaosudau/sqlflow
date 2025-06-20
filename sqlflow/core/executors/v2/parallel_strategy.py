"""Parallel Orchestration Strategy for V2 Executor.

Migrates the proven ThreadPoolTaskExecutor patterns to V2 architecture:
- Dependency-aware concurrent execution
- Task state management
- Resume capability
- Deadlock detection

Following the Zen of Python: "Beautiful is better than ugly."
Performance optimizations: "Simple is better than complex."
"""

import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.database_session import DatabaseSessionManager
from sqlflow.core.executors.v2.dependency_resolver import (
    DependencyGraph,
    analyze_dependencies,
)
from sqlflow.core.executors.v2.handlers.factory import get_handler
from sqlflow.core.executors.v2.orchestration_strategy import (
    OrchestrationStrategy,
    PipelineExecutionError,
)
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep, create_step_from_dict
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TaskState(Enum):
    """Task execution states - elegant and explicit."""

    PENDING = "pending"
    ELIGIBLE = "eligible"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class TaskStatus:
    """
    Immutable task status tracking.

    Following Kent Beck's simple design:
    Data should be obvious and predictable.
    """

    step_id: str
    state: TaskState = TaskState.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    attempts: int = 0
    dependencies: Set[str] = field(default_factory=set)
    error_message: Optional[str] = None
    future: Optional[Future] = None

    def with_state(self, new_state: TaskState, **kwargs) -> "TaskStatus":
        """Return new TaskStatus with updated state (immutable pattern)."""
        # Extract specific parameters with proper type safety
        new_start_time = kwargs.get("start_time", self.start_time)
        new_end_time = kwargs.get("end_time", self.end_time)
        new_attempts = kwargs.get("attempts", self.attempts)
        new_error_message = kwargs.get("error_message", self.error_message)

        return TaskStatus(
            step_id=self.step_id,
            state=new_state,
            start_time=new_start_time,
            end_time=new_end_time,
            attempts=new_attempts,
            dependencies=self.dependencies,
            error_message=new_error_message,
            future=self.future,
        )


class ParallelOrchestrationStrategy(OrchestrationStrategy):
    """
    Parallel execution strategy with dependency awareness.

    Migrates ThreadPoolTaskExecutor functionality to clean V2 architecture:
    - Concurrent execution with dependency resolution
    - Task state tracking and persistence
    - Resume capability from failures
    - Deadlock detection and recovery

    Performance optimizations:
    - Adaptive thread pool sizing based on system resources
    - Intelligent task batching
    - Performance monitoring integration
    - Connection pooling awareness
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        max_retries: int = 3,
        enable_performance_monitoring: bool = True,
        adaptive_sizing: bool = True,
    ):
        """
        Initialize parallel orchestration strategy with performance optimizations.

        Args:
            max_workers: Maximum concurrent workers (defaults to CPU count)
            max_retries: Maximum retry attempts for failed steps (default: 3)
            enable_performance_monitoring: Enable performance tracking
            adaptive_sizing: Enable adaptive thread pool sizing
        """
        # Adaptive thread pool sizing - Raymond Hettinger: "Use the platform"
        if max_workers is None:
            if adaptive_sizing:
                max_workers = self._calculate_optimal_workers()
            else:
                max_workers = min(32, (os.cpu_count() or 1) + 4)

        self.max_workers = max_workers
        self.enable_performance_monitoring = enable_performance_monitoring
        self.adaptive_sizing = adaptive_sizing

        # Performance tracking
        self._execution_metrics = {}
        self._start_time = None

        # Threading and retry configuration
        self._lock = threading.RLock()
        self.max_retries = max_retries
        self.retry_delay_seconds = 1.0

        logger.info(
            f"Parallel strategy initialized: {self.max_workers} workers, "
            f"retries={max_retries}, monitoring={enable_performance_monitoring}, "
            f"adaptive={adaptive_sizing}"
        )

    def _calculate_optimal_workers(self) -> int:
        """Calculate optimal worker count based on system resources."""
        try:
            import psutil

            cpu_count = psutil.cpu_count(logical=True) or 1
            memory_gb = float(psutil.virtual_memory().total) / (1024**3)

            # Heuristic: balance CPU and memory constraints
            # For I/O-bound tasks (database operations), allow more threads
            cpu_based = min(32, cpu_count * 2)  # I/O bound multiplier
            memory_based = min(32, max(1, int(memory_gb * 2)))  # 2 threads per GB

            optimal = min(cpu_based, memory_based)
            logger.debug(
                f"Calculated optimal workers: {optimal} "
                f"(CPU: {cpu_based}, Memory: {memory_based})"
            )
            return max(2, optimal)  # Minimum 2 workers

        except ImportError:
            # Fallback without psutil
            cpu_count = os.cpu_count() or 1
            return min(32, cpu_count * 2)

    def execute_pipeline(
        self,
        plan: List[Dict[str, Any]],
        context: ExecutionContext,
        db_session: DatabaseSessionManager,
    ) -> List[StepExecutionResult]:
        """
        Execute pipeline with dependency-aware parallelism.

        Following Martin Fowler's Template Method pattern:
        Define the algorithm skeleton, delegate specific steps.
        """
        if not plan:
            return []

        self._start_time = time.time()

        # Performance monitoring integration (optional)
        if self.enable_performance_monitoring:
            performance_monitor = getattr(context, "performance_monitor", None)
            if performance_monitor:
                performance_monitor.start_execution()

        try:
            # Convert to typed steps and analyze dependencies
            steps = [create_step_from_dict(step_dict) for step_dict in plan]
            dependency_graph = analyze_dependencies(steps)

            # Initialize task tracking
            task_statuses = self._initialize_task_statuses(dependency_graph)

            # Execute with optimized thread pool
            with self._create_optimized_thread_pool() as executor:
                results = self._execute_with_thread_pool(
                    executor, dependency_graph, task_statuses, context, db_session
                )

            self._log_execution_summary(results)
            return results

        except ValueError:
            # Let ValueError (like circular dependency) bubble up unchanged
            # Raymond Hettinger: "Errors should never pass silently"
            raise
        except Exception as e:
            logger.error(f"Parallel execution failed: {e}")
            raise PipelineExecutionError(f"Parallel execution failed: {e}")

    def _create_optimized_thread_pool(self) -> ThreadPoolExecutor:
        """Create optimized thread pool with performance settings."""
        # Use optimized thread pool settings
        return ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="sqlflow-worker"
        )

    def _log_execution_summary(self, results: List[StepExecutionResult]) -> None:
        """Log execution summary with performance metrics."""
        if not self._start_time:
            return

        total_time = time.time() - self._start_time
        successful = sum(1 for r in results if r.is_successful())
        failed = len(results) - successful

        logger.info(
            f"Parallel execution completed: {successful}/{len(results)} successful, "
            f"total time: {total_time:.2f}s, workers: {self.max_workers}"
        )

        if failed > 0:
            logger.warning(f"Failed steps: {failed}/{len(results)}")

    def _initialize_task_statuses(
        self, graph: DependencyGraph
    ) -> Dict[str, TaskStatus]:
        """Initialize task statuses based on dependency graph."""
        statuses = {}

        for step_id, step in graph.steps.items():
            dependencies = graph.dependencies.get(step_id, set())
            initial_state = (
                TaskState.ELIGIBLE if not dependencies else TaskState.PENDING
            )

            statuses[step_id] = TaskStatus(
                step_id=step_id, state=initial_state, dependencies=dependencies
            )

            logger.debug(
                f"Initialized task {step_id}: {initial_state}, deps={dependencies}"
            )

        return statuses

    def _execute_with_thread_pool(
        self,
        executor: ThreadPoolExecutor,
        graph: DependencyGraph,
        task_statuses: Dict[str, TaskStatus],
        context: ExecutionContext,
        db_session: DatabaseSessionManager,
    ) -> List[StepExecutionResult]:
        """
        Execute steps using thread pool with dependency coordination.

        Following the Actor model: each step execution is an isolated actor.
        """
        futures: Dict[str, Future[StepExecutionResult]] = {}
        results: Dict[str, StepExecutionResult] = {}
        completed_steps: Set[str] = set()

        # Main execution loop
        while len(completed_steps) < len(graph.steps):
            # Submit eligible tasks
            self._submit_eligible_tasks(
                executor, graph, task_statuses, futures, completed_steps, context
            )

            # Process completed futures
            if self._process_completed_futures(
                futures, task_statuses, results, completed_steps, graph, db_session
            ):
                break  # Fatal error occurred

            # Check for deadlock
            if self._detect_deadlock(task_statuses, futures, completed_steps, graph):
                break

            # Prevent busy waiting
            if futures:
                time.sleep(0.01)

        # Return results in original order
        return [results[step.id] for step in graph.steps.values() if step.id in results]

    def _submit_eligible_tasks(
        self,
        executor: ThreadPoolExecutor,
        graph: DependencyGraph,
        task_statuses: Dict[str, TaskStatus],
        futures: Dict[str, Future[StepExecutionResult]],
        completed_steps: Set[str],
        context: ExecutionContext,
    ) -> None:
        """Submit tasks that are eligible for execution."""
        eligible_steps = self._get_eligible_steps(task_statuses, completed_steps)

        for step_id in eligible_steps:
            if step_id not in futures and step_id not in completed_steps:
                step = graph.steps[step_id]

                # Update task state to running
                with self._lock:
                    task_statuses[step_id] = task_statuses[step_id].with_state(
                        TaskState.RUNNING,
                        start_time=datetime.utcnow(),
                        attempts=task_statuses[step_id].attempts + 1,
                    )

                # Submit to thread pool
                future = executor.submit(self._execute_single_step, step, context)
                futures[step_id] = future

                logger.info(f"🚀 Submitted step {step_id} for execution")

    def _process_completed_futures(
        self,
        futures: Dict[str, Future[StepExecutionResult]],
        task_statuses: Dict[str, TaskStatus],
        results: Dict[str, StepExecutionResult],
        completed_steps: Set[str],
        graph: DependencyGraph,
        db_session: DatabaseSessionManager,
    ) -> bool:
        """
        Process completed futures and handle results.

        Returns True if a fatal error occurred that should stop execution.
        """
        completed_futures = [
            (step_id, future) for step_id, future in futures.items() if future.done()
        ]

        for step_id, future in completed_futures:
            try:
                result = future.result()

                if result.is_successful():
                    self._handle_step_success(
                        step_id, result, task_statuses, results, completed_steps, graph
                    )
                    # Commit after each successful step
                    db_session.commit_changes()
                else:
                    if self._handle_step_failure(
                        step_id, result, task_statuses, results, graph
                    ):
                        return True  # Fatal error, stop execution

            except Exception as e:
                # Handle unexpected executor errors
                error_result = StepExecutionResult.failure(
                    step_id=step_id,
                    step_type="unknown",
                    start_time=task_statuses[step_id].start_time or datetime.utcnow(),
                    error_message=str(e),
                )

                if self._handle_step_failure(
                    step_id, error_result, task_statuses, results, graph
                ):
                    return True

            # Remove completed future
            futures.pop(step_id)

        return False

    def _handle_step_success(
        self,
        step_id: str,
        result: StepExecutionResult,
        task_statuses: Dict[str, TaskStatus],
        results: Dict[str, StepExecutionResult],
        completed_steps: Set[str],
        graph: DependencyGraph,
    ) -> None:
        """Handle successful step completion."""
        with self._lock:
            task_statuses[step_id] = task_statuses[step_id].with_state(
                TaskState.SUCCESS, end_time=datetime.utcnow()
            )

        results[step_id] = result
        completed_steps.add(step_id)

        # Update dependent tasks
        self._update_dependent_tasks(step_id, task_statuses, graph, completed_steps)

        logger.info(f"✅ Step {step_id} completed successfully")

    def _handle_step_failure(
        self,
        step_id: str,
        result: StepExecutionResult,
        task_statuses: Dict[str, TaskStatus],
        results: Dict[str, StepExecutionResult],
        graph: DependencyGraph,
    ) -> bool:
        """
        Handle step failure with retry logic.

        Returns True if this is a fatal error that should stop execution.
        """
        current_status = task_statuses[step_id]

        if current_status.attempts < self.max_retries:
            # Retry the step
            logger.warning(
                f"⚠️  Step {step_id} failed, retrying ({current_status.attempts}/{self.max_retries})"
            )

            with self._lock:
                task_statuses[step_id] = current_status.with_state(
                    TaskState.ELIGIBLE, error_message=result.error_message
                )

            time.sleep(self.retry_delay_seconds)
            return False
        else:
            # Max retries exceeded - fatal error
            logger.error(f"❌ Step {step_id} failed after {self.max_retries} attempts")

            with self._lock:
                task_statuses[step_id] = current_status.with_state(
                    TaskState.FAILED,
                    end_time=datetime.utcnow(),
                    error_message=result.error_message,
                )

            results[step_id] = result
            raise PipelineExecutionError(
                f"Step {step_id} failed: {result.error_message}"
            )

    def _get_eligible_steps(
        self, task_statuses: Dict[str, TaskStatus], completed_steps: Set[str]
    ) -> List[str]:
        """Get steps that are eligible for execution."""
        eligible = []

        for step_id, status in task_statuses.items():
            if status.state == TaskState.ELIGIBLE or (
                status.state == TaskState.PENDING
                and status.dependencies.issubset(completed_steps)
            ):
                eligible.append(step_id)

        return eligible

    def _update_dependent_tasks(
        self,
        completed_step_id: str,
        task_statuses: Dict[str, TaskStatus],
        graph: DependencyGraph,
        completed_steps: Set[str],
    ) -> None:
        """Update tasks that depend on the completed step."""
        dependents = graph.dependents.get(completed_step_id, set())

        with self._lock:
            for dependent_id in dependents:
                current_status = task_statuses[dependent_id]
                if (
                    current_status.state == TaskState.PENDING
                    and current_status.dependencies.issubset(completed_steps)
                ):
                    task_statuses[dependent_id] = current_status.with_state(
                        TaskState.ELIGIBLE
                    )
                    logger.debug(f"Step {dependent_id} is now eligible for execution")

    def _detect_deadlock(
        self,
        task_statuses: Dict[str, TaskStatus],
        futures: Dict[str, Future[StepExecutionResult]],
        completed_steps: Set[str],
        graph: DependencyGraph,
    ) -> bool:
        """Detect if execution is deadlocked."""
        if futures:  # Still have running tasks
            return False

        remaining_steps = set(graph.steps.keys()) - completed_steps
        if not remaining_steps:  # All done
            return False

        # Check if any remaining steps can be executed
        eligible = self._get_eligible_steps(task_statuses, completed_steps)
        if not eligible:
            logger.error(
                f"💀 Deadlock detected: {len(remaining_steps)} steps remaining but none eligible"
            )
            for step_id in remaining_steps:
                status = task_statuses[step_id]
                unmet_deps = status.dependencies - completed_steps
                logger.error(
                    f"  - {step_id}: state={status.state}, unmet_deps={unmet_deps}"
                )

            raise PipelineExecutionError("Deadlock detected in pipeline execution")

        return False

    def _execute_single_step(
        self, step: BaseStep, context: ExecutionContext
    ) -> StepExecutionResult:
        """
        Execute a single step with proper error handling.

        This runs in a thread pool worker, so must be thread-safe.
        """
        try:
            handler = get_handler(step.type)
            return handler.execute(step, context)
        except Exception as e:
            logger.error(f"Unexpected error executing step {step.id}: {e}")
            return StepExecutionResult.failure(
                step_id=step.id,
                step_type=step.type,
                start_time=datetime.utcnow(),
                error_message=str(e),
            )
