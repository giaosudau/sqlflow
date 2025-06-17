"""Base StepHandler Interface and Observability Decorators.

This module defines the core contracts for step execution in the V2 Executor,
following the Strategy pattern and providing automatic observability integration.
"""

import functools
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, TypeVar

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep
from sqlflow.logging import get_logger

logger = get_logger(__name__)

# Type variable for methods that return StepExecutionResult
F = TypeVar("F", bound=Callable[..., StepExecutionResult])


class StepHandler(ABC):
    """
    Abstract base class for step execution strategies.

    Each step type (load, transform, export, etc.) implements this interface
    to provide its specific execution logic. The handler pattern allows for:
    - Clear separation of concerns
    - Easy testing of individual step types
    - Simple extension for new step types
    - Consistent observability across all handlers

    The handler should be stateless - all state is passed through the
    ExecutionContext to maintain thread safety and enable caching.
    """

    @abstractmethod
    def execute(self, step: BaseStep, context: ExecutionContext) -> StepExecutionResult:
        """
        Execute the logic for a given step.

        Args:
            step: The step to execute (strongly typed)
            context: Execution context containing shared services

        Returns:
            StepExecutionResult containing detailed execution information

        Raises:
            Exception: If step execution fails (will be caught by observability)
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement execute method"
        )

    def can_handle(self, step: BaseStep) -> bool:
        """
        Check if this handler can handle the given step type.

        Default implementation checks if step.type matches the handler's expected type.
        Override for more complex validation logic.

        Args:
            step: Step to validate

        Returns:
            True if this handler can execute the step
        """
        expected_type = getattr(self, "STEP_TYPE", None)
        if expected_type:
            return step.type == expected_type
        return True


def observed_execution(
    step_type: str,
    *,
    record_schema: bool = True,
    record_performance: bool = True,
    record_resources: bool = False,
) -> Callable[[F], F]:
    """
    Decorator that adds comprehensive observability to step handlers.

    This decorator automatically:
    - Records execution timing and status
    - Captures performance metrics
    - Handles exceptions with proper logging
    - Integrates with the ObservabilityManager
    - Provides failure recovery insights

    Args:
        step_type: Type of step being executed (for categorization)
        record_schema: Whether to capture schema information
        record_performance: Whether to record performance metrics
        record_resources: Whether to monitor resource usage

    Returns:
        Decorated function with observability integration

    Example:
        @observed_execution("load", record_schema=True)
        def execute(self, step: LoadStep, context: ExecutionContext) -> StepExecutionResult:
            # Step execution logic here
            return StepExecutionResult.success(...)
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(
            handler_self, step: BaseStep, context: ExecutionContext
        ) -> StepExecutionResult:
            start_time = datetime.utcnow()
            step_id = step.id

            # Record step start
            _record_step_start(context, step_id, step_type)

            try:
                # Execute the actual step logic
                result = func(handler_self, step, context)

                # Enhance and record success
                return _handle_successful_execution(
                    result,
                    step,
                    context,
                    start_time,
                    step_id,
                    step_type,
                    record_schema,
                    record_performance,
                    record_resources,
                )

            except Exception as e:
                # Handle failure
                return _handle_failed_execution(
                    e, step_id, step_type, context, start_time
                )

        return wrapper  # type: ignore

    return decorator


def _record_step_start(context: ExecutionContext, step_id: str, step_type: str) -> None:
    """Record step start with observability manager."""
    try:
        context.observability_manager.record_step_start(step_id, step_type)
        logger.debug(f"Started executing {step_type} step: {step_id}")
    except Exception as obs_error:
        logger.warning(f"Observability recording failed for step start: {obs_error}")


def _handle_successful_execution(
    result: StepExecutionResult,
    step: BaseStep,
    context: ExecutionContext,
    start_time: datetime,
    step_id: str,
    step_type: str,
    record_schema: bool,
    record_performance: bool,
    record_resources: bool,
) -> StepExecutionResult:
    """Handle successful step execution with observability."""
    # Enhance result with observability data
    enriched_result = _enrich_result_with_observability(
        result, step, start_time, record_schema, record_performance, record_resources
    )

    # Record success with observability manager
    try:
        context.observability_manager.record_step_success(
            step_id, enriched_result.to_observability_event()
        )
        logger.debug(f"Successfully completed {step_type} step: {step_id}")
    except Exception as obs_error:
        logger.warning(f"Observability recording failed for step success: {obs_error}")

    return enriched_result


def _handle_failed_execution(
    exception: Exception,
    step_id: str,
    step_type: str,
    context: ExecutionContext,
    start_time: datetime,
) -> StepExecutionResult:
    """Handle failed step execution with observability."""
    end_time = datetime.utcnow()
    execution_time_ms = (end_time - start_time).total_seconds() * 1000

    # Record failure with observability manager
    try:
        context.observability_manager.record_step_failure(
            step_id, step_type, str(exception), execution_time_ms
        )
    except Exception as obs_error:
        logger.warning(f"Observability recording failed for step failure: {obs_error}")

    # Create failure result
    failure_result = StepExecutionResult.failure(
        step_id=step_id,
        step_type=step_type,
        start_time=start_time,
        error_message=str(exception),
        end_time=end_time,
    )

    logger.error(f"Failed to execute {step_type} step {step_id}: {exception}")
    return failure_result


def _enrich_result_with_observability(
    result: StepExecutionResult,
    step: BaseStep,
    start_time: datetime,
    record_schema: bool,
    record_performance: bool,
    record_resources: bool,
) -> StepExecutionResult:
    """
    Enrich a StepExecutionResult with additional observability data.

    This function adds performance metrics, schema information, and other
    observability data to the base result returned by step handlers.

    Args:
        result: Original result from step handler
        step: The executed step
        start_time: When execution started
        record_schema: Whether to capture schema information
        record_performance: Whether to record performance metrics
        record_resources: Whether to monitor resource usage

    Returns:
        Enhanced StepExecutionResult with observability data
    """
    # The result already contains the basic observability data
    # This function can be extended to add more sophisticated metrics

    # Add step metadata to performance metrics
    if record_performance and hasattr(result, "performance_metrics"):
        performance_metrics = dict(result.performance_metrics)
        performance_metrics.update(
            {
                "step_criticality": step.criticality,
                "expected_duration_ms": step.expected_duration_ms,
                "actual_vs_expected_ratio": (
                    result.execution_duration_ms / step.expected_duration_ms
                    if step.expected_duration_ms and step.expected_duration_ms > 0
                    else None
                ),
            }
        )

        # Use dataclass replace if available, otherwise return original result
        try:
            from dataclasses import replace

            result = replace(result, performance_metrics=performance_metrics)
        except (ImportError, TypeError):
            # Fallback: modify the result's performance_metrics if it's mutable
            if hasattr(result, "performance_metrics") and hasattr(
                result.performance_metrics, "update"
            ):
                result.performance_metrics.update(performance_metrics)

    return result
