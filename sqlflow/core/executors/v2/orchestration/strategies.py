"""Execution strategies for different coordination patterns."""

import logging
import time
from typing import Any, Dict, List

from ..execution import ExecutionContext, StepExecutionResult
from ..protocols import StepExecutor

logger = logging.getLogger(__name__)


class SequentialExecutionStrategy:
    """Execute steps one after another.

    This is the default strategy that executes steps sequentially,
    stopping on first error (fail-fast behavior).
    """

    def __init__(self, step_executors: List[StepExecutor]):
        """Initialize with step executors."""
        self._step_executors = step_executors

    def execute_steps(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepExecutionResult]:
        """Execute steps sequentially."""
        results = []
        current_context = context

        for step in steps:
            step_id = step.get("id", f"step_{len(results)}")

            try:
                # Find appropriate executor
                executor = self._find_executor(step)
                if not executor:
                    result = StepExecutionResult.with_error(
                        step_id,
                        f"No executor found for step type: {step.get('type', 'unknown')}",
                    )
                    results.append(result)
                    break  # Fail fast

                # Execute step
                start_time = time.time()
                result = executor.execute(step, current_context)
                execution_time = time.time() - start_time

                # Create result with timing
                if hasattr(result, "execution_time") and result.execution_time is None:
                    # Update execution time if not set by executor
                    result = StepExecutionResult(
                        step_id=result.step_id,
                        status=result.status,
                        message=result.message,
                        error=result.error,
                        execution_time=execution_time,
                        data=result.data if hasattr(result, "data") else None,
                    )

                results.append(result)

                # Update context if step produced new state
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
                break  # Fail fast on unexpected errors

        return results

    def _find_executor(self, step: Dict[str, Any]) -> StepExecutor:
        """Find appropriate executor for step."""
        for executor in self._step_executors:
            if executor.can_execute(step):
                return executor
        return None


class ParallelExecutionStrategy:
    """Execute independent steps in parallel.

    This strategy analyzes step dependencies and executes
    independent steps concurrently for better performance.
    """

    def __init__(self, step_executors: List[StepExecutor], max_workers: int = 4):
        """Initialize with step executors and parallelism limit."""
        self._step_executors = step_executors
        self._max_workers = max_workers

    def execute_steps(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepExecutionResult]:
        """Execute steps with parallelism where possible."""
        # For now, fall back to sequential execution
        # TODO: Implement dependency analysis and parallel execution
        sequential_strategy = SequentialExecutionStrategy(self._step_executors)
        return sequential_strategy.execute_steps(steps, context)


class ContinueOnErrorStrategy:
    """Execute all steps even if some fail.

    This strategy continues execution even when steps fail,
    collecting all errors for comprehensive reporting.
    """

    def __init__(self, step_executors: List[StepExecutor]):
        """Initialize with step executors."""
        self._step_executors = step_executors

    def execute_steps(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepExecutionResult]:
        """Execute all steps, continuing on errors."""
        results = []
        current_context = context

        for step in steps:
            step_id = step.get("id", f"step_{len(results)}")

            try:
                # Find appropriate executor
                executor = self._find_executor(step)
                if not executor:
                    result = StepExecutionResult.with_error(
                        step_id,
                        f"No executor found for step type: {step.get('type', 'unknown')}",
                    )
                    results.append(result)
                    continue  # Continue with next step

                # Execute step
                start_time = time.time()
                result = executor.execute(step, current_context)
                execution_time = time.time() - start_time

                # Create result with timing
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

                # Update context if step produced new state
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

                # Log errors but continue
                if result.status == "error":
                    logger.error(f"Step {step_id} failed: {result.error}")

            except Exception as e:
                logger.error(f"Unexpected error executing step {step_id}: {e}")
                result = StepExecutionResult.with_error(step_id, str(e))
                results.append(result)
                # Continue with next step

        return results

    def _find_executor(self, step: Dict[str, Any]) -> StepExecutor:
        """Find appropriate executor for step."""
        for executor in self._step_executors:
            if executor.can_execute(step):
                return executor
        return None
