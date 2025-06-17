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

    def _handle_execution_error(
        self, step: BaseStep, start_time: datetime, exception: Exception
    ) -> StepExecutionResult:
        """
        Common error handling for all handlers - DRY principle.

        Args:
            step: The step that failed
            start_time: When execution started
            exception: The exception that occurred

        Returns:
            StepExecutionResult with failure details
        """
        from datetime import datetime

        operation_name = (
            self.__class__.__name__.replace("StepHandler", "")
            .replace("Handler", "")
            .lower()
        )
        error_message = f"{operation_name} operation failed: {str(exception)}"
        logger.error(f"{step.type.title()}Step {step.id} failed: {error_message}")

        return StepExecutionResult.failure(
            step_id=step.id,
            step_type=step.type,
            start_time=start_time,
            end_time=datetime.utcnow(),
            error_message=error_message,
            error_code=f"{step.type.upper()}_EXECUTION_ERROR",
        )


def observed_execution(step_type: str) -> Callable[[F], F]:
    """
    Decorator that adds observability to step handlers.

    Simplified design following "Simple is better than complex".
    All observability features are enabled by default.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(
            handler_self, step: BaseStep, context: ExecutionContext
        ) -> StepExecutionResult:
            start_time = datetime.utcnow()
            step_id = step.id

            # Start observability tracking
            _record_step_start(context, step_id, step_type)

            try:
                # Execute the actual step logic
                result = func(handler_self, step, context)

                # Record successful completion
                _record_step_success(context, step_id, result)

                return result

            except Exception as e:
                # Handle execution failure
                return _handle_step_failure(context, step_id, step_type, start_time, e)

        return wrapper  # type: ignore

    return decorator


def _record_step_start(context: ExecutionContext, step_id: str, step_type: str) -> None:
    """Record step start with graceful failure handling."""
    try:
        context.observability_manager.record_step_start(step_id, step_type)
        logger.debug(f"Started {step_type} step: {step_id}")
    except Exception as e:
        logger.warning(f"Observability recording failed: {e}")


def _record_step_success(
    context: ExecutionContext, step_id: str, result: StepExecutionResult
) -> None:
    """Record step success with graceful failure handling."""
    try:
        context.observability_manager.record_step_success(
            step_id, result.to_observability_event()
        )
        logger.debug(f"Completed {result.step_type} step: {step_id}")
    except Exception as e:
        logger.warning(f"Observability recording failed: {e}")


def _handle_step_failure(
    context: ExecutionContext,
    step_id: str,
    step_type: str,
    start_time: datetime,
    exception: Exception,
) -> StepExecutionResult:
    """Handle step execution failure with observability recording."""
    end_time = datetime.utcnow()
    execution_time_ms = (end_time - start_time).total_seconds() * 1000

    # Record failure (with graceful handling)
    try:
        context.observability_manager.record_step_failure(
            step_id, step_type, str(exception), execution_time_ms
        )
    except Exception:
        pass  # Ignore observability failures during error handling

    # Return structured failure result
    return StepExecutionResult.failure(
        step_id=step_id,
        step_type=step_type,
        start_time=start_time,
        end_time=end_time,
        error_message=str(exception),
        error_code=f"{step_type.upper()}_EXECUTION_ERROR",
    )
