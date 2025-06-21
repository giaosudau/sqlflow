"""Async execution support for V2 Executor.

Following the Zen of Python:
- Simple is better than complex
- There should be one obvious way to do it
- Beautiful is better than ugly

This module provides async/await support for I/O-bound operations
to improve performance for large datasets and concurrent processing.
"""

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Protocol

from sqlflow.logging import get_logger

from .execution import ExecutionContext, StepExecutionResult

logger = get_logger(__name__)


class AsyncStepExecutor(Protocol):
    """Protocol for async step executors."""

    async def can_execute(self, step: Dict[str, Any]) -> bool:
        """Check if this executor can handle the given step."""
        ...

    async def execute(
        self, step: Dict[str, Any], context: ExecutionContext
    ) -> StepExecutionResult:
        """Execute the step asynchronously."""
        ...


class AsyncDatabaseEngine(Protocol):
    """Protocol for async database operations."""

    async def execute_query(self, sql: str) -> Any:
        """Execute SQL query asynchronously."""
        ...

    async def load_data_async(self, data: AsyncGenerator, table: str) -> int:
        """Load data asynchronously using generators."""
        ...


class AsyncSequentialExecutionStrategy:
    """Async version of sequential execution strategy.

    Executes steps one after another but with async I/O operations
    for better performance on I/O-bound tasks.
    """

    def __init__(self, step_executors: List[AsyncStepExecutor]):
        """Initialize with async step executors."""
        self._step_executors = step_executors

    async def execute_steps(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepExecutionResult]:
        """Execute steps sequentially with async I/O."""
        results = []
        current_context = context

        for step in steps:
            step_id = step.get("id", f"step_{len(results)}")

            try:
                # Find appropriate async executor
                executor = await self._find_executor(step)
                if not executor:
                    result = StepExecutionResult.with_error(
                        step_id,
                        f"No async executor found for step type: {step.get('type', 'unknown')}",
                    )
                    results.append(result)
                    break  # Fail fast

                # Execute step asynchronously
                start_time = time.time()
                result = await executor.execute(step, current_context)
                execution_time = time.time() - start_time

                # Update execution time if not set
                if hasattr(result, "execution_time") and result.execution_time is None:
                    result = StepExecutionResult(
                        step_id=result.step_id,
                        status=result.status,
                        message=result.message,
                        error=result.error,
                        execution_time=execution_time,
                        data=result.data if hasattr(result, "data") else None,
                    )

                results.append(result)

                # Update context with new state
                if (
                    hasattr(result, "data")
                    and result.data
                    and result.status == "success"
                ):
                    if "source_definition" in result.data:
                        current_context = current_context.with_source_definition(
                            result.data["source_definition"]["name"],
                            result.data["source_definition"],
                        )

                # Stop on error (fail-fast)
                if result.status == "error":
                    logger.error(f"Step {step_id} failed: {result.error}")
                    break

            except Exception as e:
                logger.error(f"Unexpected error executing step {step_id}: {e}")
                result = StepExecutionResult.with_error(step_id, str(e))
                results.append(result)
                break

        return results

    async def _find_executor(self, step: Dict[str, Any]) -> Optional[AsyncStepExecutor]:
        """Find appropriate async executor for step."""
        for executor in self._step_executors:
            if await executor.can_execute(step):
                return executor
        return None


class AsyncConcurrentExecutionStrategy:
    """Execute independent steps concurrently using asyncio.

    This strategy analyzes step dependencies and executes independent
    steps concurrently for maximum performance.
    """

    def __init__(
        self, step_executors: List[AsyncStepExecutor], max_concurrency: int = 5
    ):
        """Initialize with async step executors and concurrency limit."""
        self._step_executors = step_executors
        self._max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def execute_steps(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepExecutionResult]:
        """Execute steps with maximum concurrency where dependencies allow."""
        # Build dependency graph
        dependency_graph = self._build_dependency_graph(steps)

        # Group steps by dependency level
        step_id_levels = self._topological_sort(dependency_graph)

        # Convert step IDs back to step objects
        step_map = {step.get("id", "unknown"): step for step in steps}
        step_levels = [
            [step_map[step_id] for step_id in level if step_id in step_map]
            for level in step_id_levels
        ]

        results = []
        current_context = context

        # Execute each level concurrently
        for level_steps in step_levels:
            if not level_steps:
                continue

            # Execute all steps in this level concurrently
            tasks = []
            for step in level_steps:
                task = self._execute_step_with_semaphore(step, current_context)
                tasks.append(task)

            # Wait for all steps in this level to complete
            level_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results and update context
            for i, result in enumerate(level_results):
                if isinstance(result, Exception):
                    step_id = level_steps[i].get("id", f"step_{len(results)}")
                    result = StepExecutionResult.with_error(step_id, str(result))

                results.append(result)

                # Update context with successful results
                if (
                    hasattr(result, "data")
                    and result.data
                    and result.status == "success"
                ):
                    if "source_definition" in result.data:
                        current_context = current_context.with_source_definition(
                            result.data["source_definition"]["name"],
                            result.data["source_definition"],
                        )

                # Check for failures (fail-fast per level)
                if hasattr(result, "status") and result.status == "error":
                    logger.error(f"Step failed in concurrent execution: {result.error}")
                    return results  # Return early on error

        return results

    async def _execute_step_with_semaphore(
        self, step: Dict[str, Any], context: ExecutionContext
    ) -> StepExecutionResult:
        """Execute step with concurrency control."""
        async with self._semaphore:
            step_id = step.get("id", "unknown")

            try:
                executor = await self._find_executor(step)
                if not executor:
                    return StepExecutionResult.with_error(
                        step_id,
                        f"No async executor found for step type: {step.get('type', 'unknown')}",
                    )

                start_time = time.time()
                result = await executor.execute(step, context)
                execution_time = time.time() - start_time

                # Update execution time
                if hasattr(result, "execution_time") and result.execution_time is None:
                    result = StepExecutionResult(
                        step_id=result.step_id,
                        status=result.status,
                        message=result.message,
                        error=result.error,
                        execution_time=execution_time,
                        data=result.data if hasattr(result, "data") else None,
                    )

                return result

            except Exception as e:
                logger.error(f"Error executing step {step_id}: {e}")
                return StepExecutionResult.with_error(step_id, str(e))

    async def _find_executor(self, step: Dict[str, Any]) -> Optional[AsyncStepExecutor]:
        """Find appropriate async executor for step."""
        for executor in self._step_executors:
            if await executor.can_execute(step):
                return executor
        return None

    def _build_dependency_graph(
        self, steps: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """Build dependency graph from steps."""
        graph = {}

        for step in steps:
            step_id = step.get("id", "unknown")
            dependencies = step.get("depends_on", [])
            graph[step_id] = dependencies

        return graph

    def _topological_sort(
        self, dependency_graph: Dict[str, List[str]]
    ) -> List[List[str]]:
        """Sort steps into dependency levels for concurrent execution."""
        # Simple implementation - group by dependency level
        # In practice, you'd want a more sophisticated topological sort

        # For now, return single level (sequential execution)
        # TODO: Implement proper topological sorting
        return [list(dependency_graph.keys())]


class AsyncDataStream:
    """Async generator for streaming data processing.

    Provides memory-efficient data processing using async generators
    to handle large datasets without loading everything into memory.
    """

    def __init__(self, data_source: Any, chunk_size: int = 1000):
        """Initialize with data source and chunk size."""
        self._data_source = data_source
        self._chunk_size = chunk_size

    async def stream_chunks(self) -> AsyncGenerator[List[Any], None]:
        """Stream data in chunks asynchronously."""
        # This is a placeholder - actual implementation would depend on data source type
        await asyncio.sleep(0)  # Yield control to event loop

        # Example: reading from a file or database
        chunk = []
        for item in self._data_source:
            chunk.append(item)

            if len(chunk) >= self._chunk_size:
                yield chunk
                chunk = []
                await asyncio.sleep(0)  # Yield control between chunks

        # Yield remaining items
        if chunk:
            yield chunk

    async def process_async(self, processor_func) -> AsyncGenerator[Any, None]:
        """Process data asynchronously with given function."""
        async for chunk in self.stream_chunks():
            # Process chunk asynchronously
            processed_chunk = await asyncio.get_event_loop().run_in_executor(
                None, processor_func, chunk
            )
            yield processed_chunk


class AsyncPipelineOrchestrator:
    """Main async orchestrator for pipeline execution.

    Coordinates async execution strategies and provides the main
    entry point for async pipeline processing.
    """

    def __init__(
        self, strategy: AsyncSequentialExecutionStrategy, max_concurrency: int = 5
    ):
        """Initialize with execution strategy."""
        self._strategy = strategy
        self._max_concurrency = max_concurrency

    async def execute_pipeline(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepExecutionResult]:
        """Execute pipeline asynchronously."""
        logger.info(f"Starting async pipeline execution with {len(steps)} steps")

        start_time = time.time()
        try:
            results = await self._strategy.execute_steps(steps, context)

            execution_time = time.time() - start_time
            logger.info(f"Async pipeline completed in {execution_time:.2f}s")

            return results

        except Exception as e:
            logger.error(f"Async pipeline execution failed: {e}")
            raise

    async def execute_with_streaming(
        self,
        steps: List[Dict[str, Any]],
        context: ExecutionContext,
        data_stream: AsyncDataStream,
    ) -> AsyncGenerator[StepExecutionResult, None]:
        """Execute pipeline with streaming data processing."""
        logger.info("Starting streaming async pipeline execution")

        # Process data in chunks asynchronously
        async for chunk in data_stream.stream_chunks():
            # Update context with current chunk
            chunk_context = context.with_data(chunk)

            # Execute steps for this chunk
            chunk_results = await self._strategy.execute_steps(steps, chunk_context)

            # Yield results as they're produced
            for result in chunk_results:
                yield result
