"""V2 Executor Exceptions.

A simplified exception hierarchy for the V2 execution engine following the Zen of Python:
- "Simple is better than complex"
- "Flat is better than nested"
- "Errors should never pass silently"

This module provides the minimal set of exceptions needed for clear error handling.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional


class SQLFlowError(Exception):
    """Base exception for all SQLFlow V2 errors.

    Provides comprehensive error context and follows the principle
    that "Errors should never pass silently" from the Zen of Python.
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        suggested_actions: Optional[List[str]] = None,
        recoverable: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__.upper()
        self.context = context or {}
        self.suggested_actions = suggested_actions or []
        self.recoverable = recoverable
        self.timestamp = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for serialization."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "context": self.context,
            "suggested_actions": self.suggested_actions,
            "recoverable": self.recoverable,
            "timestamp": self.timestamp.isoformat(),
        }

    def __str__(self) -> str:
        """String representation including context."""
        base_message = self.message

        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            base_message += f" (Context: {context_str})"

        if self.suggested_actions:
            actions_str = "; ".join(self.suggested_actions)
            base_message += f" (Suggested actions: {actions_str})"

        return base_message


class SQLFlowWarning(UserWarning):
    """Base warning class for SQLFlow V2.

    Used for non-fatal issues that users should be aware of.
    """

    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        suggested_actions: Optional[List[str]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.suggested_actions = suggested_actions or []
        self.timestamp = datetime.utcnow()


class ExecutionError(SQLFlowError):
    """Error during pipeline or step execution.

    This is the primary exception type used throughout the V2 executor.
    For configuration errors, use standard ValueError or TypeError.
    For UDF errors, this exception will wrap the original error with context.
    """

    def __init__(
        self,
        message: str,
        step_id: Optional[str] = None,
        original_error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        exec_context = context or {}
        if step_id:
            exec_context["step_id"] = step_id
        if original_error:
            exec_context["original_error"] = str(original_error)
            exec_context["original_error_type"] = type(original_error).__name__

        super().__init__(
            message=message,
            context=exec_context,
            suggested_actions=["Review step configuration and logs"],
        )
        self.step_id = step_id
        self.original_error = original_error


__all__ = [
    "SQLFlowError",
    "SQLFlowWarning",
    "ExecutionError",
]
