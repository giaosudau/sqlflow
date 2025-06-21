"""Base step executor protocols and classes.

Provides clean protocols for step execution following SOLID principles:
- Single Responsibility Principle for each executor
- Dependency inversion through protocols
- Consistent error handling and result creation
"""

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

from ..protocols.core import ExecutionContext, Step, StepResult


class StepExecutor(Protocol):
    """Protocol for step executors with focused responsibilities.

    Each executor handles exactly one step type following Single Responsibility Principle.
    Uses protocols for dependency inversion and clean testing.
    """

    @abstractmethod
    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the given step."""
        ...

    @abstractmethod
    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute the step and return a result."""
        ...


@dataclass
class StepExecutionResult:
    """Immutable result from step execution."""

    step_id: str
    status: str
    message: Optional[str] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    rows_affected: Optional[int] = None
    table_name: Optional[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class BaseStepExecutor:
    """Base class providing common functionality for step executors.

    Following the DRY principle while maintaining single responsibility.
    """

    def __init__(self):
        """Initialize base executor."""

    def _create_result(
        self,
        step_id: str,
        status: str,
        message: Optional[str] = None,
        error: Optional[str] = None,
        execution_time: Optional[float] = None,
        **kwargs,
    ) -> StepExecutionResult:
        """Create a standardized step execution result."""
        return StepExecutionResult(
            step_id=step_id,
            status=status,
            message=message,
            error=error,
            execution_time=execution_time,
            **kwargs,
        )

    def _handle_execution_error(
        self, step_id: str, error: Exception, execution_time: Optional[float] = None
    ) -> StepExecutionResult:
        """Handle execution errors consistently."""
        return self._create_result(
            step_id=step_id,
            status="error",
            error=str(error),
            execution_time=execution_time,
            message=f"Step execution failed: {type(error).__name__}",
        )
