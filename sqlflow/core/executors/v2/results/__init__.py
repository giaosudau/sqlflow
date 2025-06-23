"""Results package for V2 executor.

Exports clean, immutable result models for step and pipeline execution.
"""

from .models import (
    ExecutionResult,
    StepResult,
    create_error_result,
    create_execution_result,
    create_success_result,
)

__all__ = [
    "StepResult",
    "ExecutionResult",
    "create_success_result",
    "create_error_result",
    "create_execution_result",
]
