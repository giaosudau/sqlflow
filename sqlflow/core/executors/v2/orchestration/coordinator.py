"""Clean orchestration coordinator for V2 executor.

Following Raymond Hettinger's philosophy:
- "Simple is better than complex"
- "Flat is better than nested"
- "Readability counts"

This coordinator is the heart of the V2 executor, but it's designed
to be small, focused, and easy to understand. It orchestrates the
execution of pipeline steps without being a god class.

Key principles:
- Single responsibility: coordinate step execution
- Dependency injection: all dependencies passed in
- Immutable context: no side effects
- Clear error handling

Maximum complexity: < 150 lines as per the implementation plan.
"""

import logging
import time
from typing import Any, Dict, List, Optional

from sqlflow.core.executors.v2.execution.context import ExecutionContext
from sqlflow.core.executors.v2.protocols.core import Step, StepResult
from sqlflow.core.executors.v2.results.models import (
    ExecutionResult,
    create_execution_result,
)
from sqlflow.core.executors.v2.steps.definitions import create_step_from_dict
from sqlflow.core.executors.v2.steps.registry import StepExecutorRegistry

logger = logging.getLogger(__name__)


class ExecutionCoordinator:
    """Clean, focused orchestration coordinator.

    This class coordinates the execution of pipeline steps without
    becoming a god class. It follows the Single Responsibility Principle
    and uses dependency injection for all its needs.
    """

    def __init__(
        self,
        registry: Optional[StepExecutorRegistry] = None,
        **kwargs,
    ):
        """Initialize coordinator with step registry.

        Args:
            registry: Step executor registry for clean architecture
            **kwargs: Additional arguments for extensibility
        """
        if registry is None:
            # Create default registry with all standard executors
            from ..steps.registry import create_default_registry

            registry = create_default_registry()

        self.registry = registry
        self.result: Optional[ExecutionResult] = None
        self.context: Optional[ExecutionContext] = None

        logger.info("ExecutionCoordinator initialized")

    def execute(
        self,
        steps: List[Dict[str, Any]],
        context: ExecutionContext,
        fail_fast: bool = True,
    ) -> ExecutionResult:
        """Execute a complete pipeline.

        Simple, clean execution with fail-fast behavior.

        Args:
            steps: List of step dictionaries to execute
            context: Execution context
            fail_fast: Whether to stop execution on first failure
        """
        self.context = context
        if not steps:
            logger.info("Empty pipeline - nothing to execute")
            self.result = create_execution_result([])
            return self.result

        logger.info(f"Starting pipeline execution with {len(steps)} steps")

        # Convert dictionary steps to typed steps
        typed_steps = self._convert_steps(steps, context)

        # Execute each step
        step_results = []
        for step in typed_steps:
            step_result = self._execute_single_step(step, context)
            step_results.append(step_result)

            # Fail-fast: stop on first failure
            if not step_result.success and fail_fast:
                logger.error(f"Pipeline stopped due to step failure: {step.id}")
                break

        result = create_execution_result(
            step_results=step_results, variables=context.variables
        )
        self.result = result

        logger.info(
            f"Pipeline execution completed with status: {'success' if result.success else 'failed'}"
        )
        return result

    def _convert_steps(
        self, step_dicts: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[Step]:
        """Convert dictionary steps to typed steps with variable substitution."""
        typed_steps = []
        for step_dict in step_dicts:
            try:
                # Apply variable substitution to step configuration
                if context.variables:
                    from ..variables.substitution import substitute_in_step

                    step_dict = substitute_in_step(step_dict, context.variables)

                typed_step = create_step_from_dict(step_dict)
                typed_steps.append(typed_step)
            except Exception as e:
                step_id = step_dict.get("id", "unknown")
                logger.error(f"Failed to convert step {step_id}: {e}")
                raise ValueError(f"Invalid step definition for '{step_id}': {e}")
        return typed_steps

    def _execute_single_step(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute a single step with proper error handling."""
        logger.info(f"Executing step: {step.id}")

        # Start observability tracking
        if context.observability:
            context.observability.start_step(step.id)

        start_time = time.time()

        try:
            # Find and execute step
            executor = self.registry.find_executor(step)
            result = executor.execute(step, context)

            # Record success
            if context.observability:
                context.observability.end_step(
                    step.id, result.success, result.error_message
                )
                context.observability.record_rows_affected(
                    step.id, result.rows_affected
                )

            duration = (time.time() - start_time) * 1000
            logger.info(f"Step {step.id} completed in {duration:.1f}ms")

            return result

        except Exception as e:
            duration = (time.time() - start_time) * 1000
            error_msg = str(e)

            # Record failure
            if context.observability:
                context.observability.end_step(step.id, False, error_msg)

            logger.error(f"Step {step.id} failed after {duration:.1f}ms: {error_msg}")

            # Create error result
            from sqlflow.core.executors.v2.results.models import create_error_result

            return create_error_result(
                step_id=step.id, duration_ms=duration, error_message=error_msg
            )


def create_coordinator(registry: StepExecutorRegistry) -> ExecutionCoordinator:
    """Factory function for creating execution coordinators."""
    return ExecutionCoordinator(registry)
