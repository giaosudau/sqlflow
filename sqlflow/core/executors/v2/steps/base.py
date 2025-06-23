"""Base Step Executor providing common functionality.

Following the DRY principle, this base class provides common
functionality for all step executors, such as result creation
and error handling.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlflow.logging import get_logger

from ..exceptions import ExecutionError
from ..protocols.core import ExecutionContext, Step, StepExecutor
from ..results.models import StepResult, create_error_result, create_success_result

logger = get_logger(__name__)


@dataclass
class BaseStepExecutor(StepExecutor):
    """Base class for all step executors.

    Provides common functionality for creating results and handling errors.
    """

    def can_execute(self, step: Step) -> bool:
        """Check if this executor can handle the step."""
        raise NotImplementedError("Subclasses must implement can_execute")

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute the step with given context."""
        raise NotImplementedError("Subclasses must implement execute")

    def _create_result(
        self,
        step_id: str,
        status: str,
        message: str,
        execution_time: float,
        rows_affected: int = 0,
        table_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StepResult:
        """Create a successful step result."""
        full_metadata = metadata or {}
        if table_name:
            full_metadata["target_table"] = table_name

        if status == "success":
            return create_success_result(
                step_id=step_id,
                duration_ms=execution_time * 1000,
                rows_affected=rows_affected,
                metadata=full_metadata,
            )
        else:
            # Fallback for non-success status
            return create_error_result(
                step_id=step_id,
                duration_ms=execution_time * 1000,
                error_message=message,
                metadata=full_metadata,
            )

    def _handle_execution_error(
        self, step_id: str, error: Exception, execution_time: float
    ) -> StepResult:
        """Handle execution errors and create an error result."""
        if isinstance(error, ExecutionError):
            # If it's already an ExecutionError, just return it as a result
            return create_error_result(
                step_id=step_id,
                duration_ms=execution_time * 1000,
                error_message=str(error),
                metadata=error.context,
            )

        # Wrap other exceptions in ExecutionError
        err = ExecutionError(step_id=step_id, message=str(error), original_error=error)
        logger.error(f"Unhandled exception in step '{step_id}': {err}", exc_info=True)

        return create_error_result(
            step_id=step_id,
            duration_ms=execution_time * 1000,
            error_message=str(err),
            metadata=err.context,
        )
