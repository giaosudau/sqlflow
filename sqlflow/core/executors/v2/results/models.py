"""Result models for V2 executor.

Following clean architecture principles:
- Immutable data structures using frozen dataclasses
- Clear separation between step results and execution results
- Rich information for observability and debugging
- Memory optimization with slots

These replace the old dictionary-based results with strongly typed,
immutable data structures that are easier to work with and test.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _create_empty_dict() -> Dict[str, Any]:
    """Factory function for creating empty metadata dict."""
    return {}


@dataclass(frozen=True, slots=True)
class StepResult:
    """Immutable step execution result.

    Contains all information about a single step execution,
    designed for comprehensive observability.
    """

    step_id: str
    success: bool
    duration_ms: float
    rows_affected: int = 0
    error_message: Optional[str] = None
    start_time: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=_create_empty_dict)

    def validate(self) -> None:
        """Validate result consistency."""
        if not self.success and not self.error_message:
            raise ValueError("Failed step must have error_message")
        if self.success and self.error_message:
            raise ValueError("Successful step cannot have error_message")


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Immutable pipeline execution result.

    Contains complete information about pipeline execution,
    including all step results and aggregate metrics.
    """

    success: bool
    step_results: Tuple[StepResult, ...]  # Immutable sequence
    total_duration_ms: float
    variables: Dict[str, Any] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_rows_affected(self) -> int:
        """Total rows affected across all steps."""
        return sum(result.rows_affected for result in self.step_results)

    @property
    def failed_steps(self) -> List[StepResult]:
        """List of failed step results."""
        return [result for result in self.step_results if not result.success]

    @property
    def successful_steps(self) -> List[StepResult]:
        """List of successful step results."""
        return [result for result in self.step_results if result.success]

    def __post_init__(self):
        """Validate execution result consistency."""
        # Allow empty step results for empty pipelines
        if self.step_results:
            # Execution is successful only if all steps succeeded
            all_successful = all(result.success for result in self.step_results)
            if self.success != all_successful:
                raise ValueError("ExecutionResult.success must match all step results")


def create_success_result(
    step_id: str,
    duration_ms: float,
    rows_affected: int = 0,
    metadata: Optional[Dict[str, Any]] = None,
) -> StepResult:
    """Factory function for successful step results.

    Following the factory pattern for common result creation scenarios.
    Makes the code more readable and reduces repetition.
    """
    return StepResult(
        step_id=step_id,
        success=True,
        duration_ms=duration_ms,
        rows_affected=rows_affected,
        metadata=metadata or {},
    )


def create_error_result(
    step_id: str,
    duration_ms: float,
    error_message: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> StepResult:
    """Factory function for error step results."""
    return StepResult(
        step_id=step_id,
        success=False,
        duration_ms=duration_ms,
        error_message=error_message,
        metadata=metadata or {},
    )


def create_execution_result(
    step_results: List[StepResult],
    variables: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    """Factory function for execution results.

    Automatically calculates success status and total duration
    from step results. Handles empty pipelines gracefully.
    """
    if not step_results:
        # Empty pipeline - return successful result with zero duration
        return ExecutionResult(
            success=True,
            step_results=tuple(),
            total_duration_ms=0.0,
            variables=variables or {},
            metadata=metadata or {},
        )

    total_duration = sum(result.duration_ms for result in step_results)
    all_successful = all(result.success for result in step_results)

    return ExecutionResult(
        success=all_successful,
        step_results=tuple(step_results),  # Convert to immutable tuple
        total_duration_ms=total_duration,
        variables=variables or {},
        metadata=metadata or {},
    )
