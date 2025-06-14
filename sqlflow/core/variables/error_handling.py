"""Unified Error Handling Strategy for SQLFlow Variable Substitution.

This module provides consistent, configurable error handling for variable
substitution operations across all contexts and components.

Zen of Python: "Errors should never pass silently."
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional, Set

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ErrorStrategy(Enum):
    """Strategy for handling variable substitution errors.

    Provides configurable behavior for different use cases and environments.
    """

    FAIL_FAST = "fail_fast"  # Raise exception immediately on first error
    WARN_CONTINUE = "warn_continue"  # Log warning and continue with fallback
    IGNORE = "ignore"  # Silently ignore errors and use fallback
    COLLECT_REPORT = "collect_report"  # Collect all errors and report at end


@dataclass
class VariableError:
    """Represents a single variable substitution error with comprehensive metadata."""

    variable_name: str
    error_type: str
    error_message: str
    context: str
    original_text: str
    position: Optional[int] = None
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    suggested_fix: Optional[str] = None
    severity: str = "error"  # error, warning, info

    def __str__(self) -> str:
        """Create human-readable error description."""
        parts = [f"Variable '{self.variable_name}' {self.error_type}"]

        if self.position is not None:
            parts.append(f"at position {self.position}")

        if self.line_number is not None and self.column_number is not None:
            parts.append(f"(line {self.line_number}, column {self.column_number})")

        parts.append(f"in {self.context} context")

        if self.error_message:
            parts.append(f": {self.error_message}")

        return " ".join(parts)


@dataclass
class ErrorReport:
    """Comprehensive error report for variable substitution operations."""

    errors: List[VariableError] = field(default_factory=list)
    warnings: List[VariableError] = field(default_factory=list)
    success_count: int = 0
    total_variables: int = 0
    context: str = ""

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0

    @property
    def error_count(self) -> int:
        """Get total number of errors."""
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        """Get total number of warnings."""
        return len(self.warnings)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_variables == 0:
            return 100.0
        return (self.success_count / self.total_variables) * 100.0

    def add_error(self, error: VariableError) -> None:
        """Add an error to the report."""
        if error.severity == "error":
            self.errors.append(error)
        elif error.severity == "warning":
            self.warnings.append(error)

    def get_missing_variables(self) -> Set[str]:
        """Get set of all missing variable names."""
        missing = set()
        for error in self.errors + self.warnings:
            if error.error_type == "not found":
                missing.add(error.variable_name)
        return missing

    def get_error_summary(self) -> str:
        """Get a summary of all errors and warnings."""
        if not self.has_errors and not self.has_warnings:
            return f"All {self.total_variables} variables processed successfully"

        parts = []

        if self.has_errors:
            parts.append(f"{self.error_count} error(s)")

        if self.has_warnings:
            parts.append(f"{self.warning_count} warning(s)")

        summary = f"Variable substitution completed with {', '.join(parts)}"

        if self.total_variables > 0:
            summary += f" ({self.success_rate:.1f}% success rate)"

        return summary

    def format_detailed_report(self) -> str:
        """Format a detailed error report for logging or display."""
        lines = [
            "=" * 60,
            "Variable Substitution Error Report",
            "=" * 60,
            f"Context: {self.context}",
            f"Total Variables: {self.total_variables}",
            f"Successful: {self.success_count}",
            f"Errors: {self.error_count}",
            f"Warnings: {self.warning_count}",
            f"Success Rate: {self.success_rate:.1f}%",
            "",
        ]

        if self.has_errors:
            lines.extend(["ERRORS:", "-" * 40])
            for i, error in enumerate(self.errors, 1):
                lines.append(f"{i}. {error}")
                if error.suggested_fix:
                    lines.append(f"   Suggestion: {error.suggested_fix}")
            lines.append("")

        if self.has_warnings:
            lines.extend(["WARNINGS:", "-" * 40])
            for i, warning in enumerate(self.warnings, 1):
                lines.append(f"{i}. {warning}")
                if warning.suggested_fix:
                    lines.append(f"   Suggestion: {warning.suggested_fix}")
            lines.append("")

        lines.extend(["=" * 60, ""])

        return "\n".join(lines)


class VariableSubstitutionError(Exception):
    """Exception raised for variable substitution errors."""

    def __init__(self, message: str, error_report: Optional[ErrorReport] = None):
        """Initialize with message and optional error report."""
        super().__init__(message)
        self.error_report = error_report or ErrorReport()


class ErrorHandler:
    """Unified error handler for variable substitution operations.

    Provides consistent error handling behavior across all components with
    configurable strategies for different use cases.
    """

    def __init__(self, strategy: ErrorStrategy = ErrorStrategy.WARN_CONTINUE):
        """Initialize error handler with specified strategy.

        Args:
            strategy: Error handling strategy to use
        """
        self.strategy = strategy
        self.error_report = ErrorReport()
        self._suggestions = {
            "not found": "Check variable name spelling and ensure it's defined",
            "invalid format": "Verify variable syntax: ${variable_name} or ${variable_name|default}",
            "circular reference": "Remove circular references in variable definitions",
            "type error": "Ensure variable value is compatible with context requirements",
        }

    def handle_missing_variable(
        self,
        var_name: str,
        context: str,
        position: Optional[int] = None,
        line_number: Optional[int] = None,
        column_number: Optional[int] = None,
        original_text: str = "",
    ) -> str:
        """Handle missing variable according to configured strategy.

        Args:
            var_name: Name of the missing variable
            context: Context where variable was used (sql, text, ast, json)
            position: Character position in text
            line_number: Line number where variable appears
            column_number: Column number where variable appears
            original_text: Original text containing the variable

        Returns:
            Fallback value based on context and strategy

        Raises:
            VariableSubstitutionError: If strategy is FAIL_FAST
        """
        error = VariableError(
            variable_name=var_name,
            error_type="not found",
            error_message=f"Variable '{var_name}' is not defined",
            context=context,
            original_text=original_text,
            position=position,
            line_number=line_number,
            column_number=column_number,
            suggested_fix=self._suggestions.get("not found"),
            severity="error",
        )

        return self._handle_error(
            error, self._get_fallback_for_missing(var_name, context)
        )

    def handle_invalid_format(
        self,
        var_name: str,
        context: str,
        error_message: str,
        position: Optional[int] = None,
        line_number: Optional[int] = None,
        column_number: Optional[int] = None,
        original_text: str = "",
    ) -> str:
        """Handle invalid variable format according to configured strategy.

        Args:
            var_name: Name of the problematic variable
            context: Context where variable was used
            error_message: Specific error message
            position: Character position in text
            line_number: Line number where variable appears
            column_number: Column number where variable appears
            original_text: Original text containing the variable

        Returns:
            Fallback value based on context and strategy

        Raises:
            VariableSubstitutionError: If strategy is FAIL_FAST
        """
        error = VariableError(
            variable_name=var_name,
            error_type="invalid format",
            error_message=error_message,
            context=context,
            original_text=original_text,
            position=position,
            line_number=line_number,
            column_number=column_number,
            suggested_fix=self._suggestions.get("invalid format"),
            severity="error",
        )

        return self._handle_error(
            error, self._get_fallback_for_invalid(var_name, context)
        )

    def handle_type_error(
        self,
        var_name: str,
        value: Any,
        context: str,
        error_message: str,
        position: Optional[int] = None,
        line_number: Optional[int] = None,
        column_number: Optional[int] = None,
        original_text: str = "",
    ) -> str:
        """Handle type conversion error according to configured strategy.

        Args:
            var_name: Name of the variable with type error
            value: The problematic value
            context: Context where variable was used
            error_message: Specific error message
            position: Character position in text
            line_number: Line number where variable appears
            column_number: Column number where variable appears
            original_text: Original text containing the variable

        Returns:
            Fallback value based on context and strategy

        Raises:
            VariableSubstitutionError: If strategy is FAIL_FAST
        """
        error = VariableError(
            variable_name=var_name,
            error_type="type error",
            error_message=f"Cannot format value {repr(value)} for {context} context: {error_message}",
            context=context,
            original_text=original_text,
            position=position,
            line_number=line_number,
            column_number=column_number,
            suggested_fix=self._suggestions.get("type error"),
            severity="warning",  # Type errors are usually warnings
        )

        return self._handle_error(
            error, str(value)
        )  # Fallback to string representation

    def record_success(self, var_name: str) -> None:
        """Record a successful variable substitution."""
        self.error_report.success_count += 1

    def set_total_variables(self, count: int) -> None:
        """Set the total number of variables being processed."""
        self.error_report.total_variables = count

    def set_context(self, context: str) -> None:
        """Set the context for error reporting."""
        self.error_report.context = context

    def get_error_report(self) -> ErrorReport:
        """Get the current error report."""
        return self.error_report

    def finalize(self) -> None:
        """Finalize error handling and report results.

        Raises:
            VariableSubstitutionError: If strategy requires it and errors occurred
        """
        if (
            self.strategy == ErrorStrategy.COLLECT_REPORT
            and self.error_report.has_errors
        ):
            # Log detailed report
            logger.error(self.error_report.format_detailed_report())

            # Raise exception with all collected errors
            raise VariableSubstitutionError(
                self.error_report.get_error_summary(), self.error_report
            )
        elif self.error_report.has_errors or self.error_report.has_warnings:
            # Log summary for other strategies
            logger.info(self.error_report.get_error_summary())

    def _handle_error(self, error: VariableError, fallback: str) -> str:
        """Handle an error according to the configured strategy.

        Args:
            error: The error to handle
            fallback: Fallback value to return

        Returns:
            Fallback value or raises exception

        Raises:
            VariableSubstitutionError: If strategy is FAIL_FAST
        """
        # Always add to error report for statistics
        self.error_report.add_error(error)

        if self.strategy == ErrorStrategy.FAIL_FAST:
            logger.error(str(error))
            raise VariableSubstitutionError(str(error), self.error_report)

        elif self.strategy == ErrorStrategy.WARN_CONTINUE:
            if error.severity == "error":
                logger.warning(str(error))
            else:
                logger.debug(str(error))

        elif self.strategy == ErrorStrategy.IGNORE:
            logger.debug(str(error))

        elif self.strategy == ErrorStrategy.COLLECT_REPORT:
            # Just collect, will log/report at finalize()
            logger.debug(f"Collected error: {error}")

        return fallback

    def _get_fallback_for_missing(self, var_name: str, context: str) -> str:
        """Get appropriate fallback value for missing variable by context."""
        fallbacks = {
            "sql": "NULL",
            "text": f"${{{var_name}}}",  # Keep original placeholder
            "ast": "None",
            "json": "null",
        }
        return fallbacks.get(context, f"${{{var_name}}}")

    def _get_fallback_for_invalid(self, var_name: str, context: str) -> str:
        """Get appropriate fallback value for invalid variable by context."""
        fallbacks = {
            "sql": "NULL",
            "text": f"${{{var_name}}}",  # Keep original placeholder
            "ast": "None",
            "json": "null",
        }
        return fallbacks.get(context, f"${{{var_name}}}")


def create_error_handler(
    strategy: ErrorStrategy = ErrorStrategy.WARN_CONTINUE,
) -> ErrorHandler:
    """Create an error handler with the specified strategy.

    This is a convenience function for creating error handlers with
    commonly used configurations.

    Args:
        strategy: Error handling strategy to use

    Returns:
        Configured ErrorHandler instance
    """
    return ErrorHandler(strategy)


def handle_variable_error(
    error_type: str,
    var_name: str,
    context: str,
    error_message: str = "",
    strategy: ErrorStrategy = ErrorStrategy.WARN_CONTINUE,
    **kwargs,
) -> str:
    """Handle a variable error with specified strategy.

    This is a convenience function for one-off error handling.

    Args:
        error_type: Type of error (not found, invalid format, type error)
        var_name: Name of the problematic variable
        context: Context where error occurred
        error_message: Specific error message
        strategy: Error handling strategy to use
        **kwargs: Additional arguments for error metadata

    Returns:
        Fallback value based on context and strategy
    """
    handler = ErrorHandler(strategy)

    if error_type == "not found":
        return handler.handle_missing_variable(var_name, context, **kwargs)
    elif error_type == "invalid format":
        return handler.handle_invalid_format(var_name, context, error_message, **kwargs)
    elif error_type == "type error":
        value = kwargs.get("value")
        return handler.handle_type_error(
            var_name, value, context, error_message, **kwargs
        )
    else:
        # Generic error handling
        error = VariableError(
            variable_name=var_name,
            error_type=error_type,
            error_message=error_message,
            context=context,
            original_text=kwargs.get("original_text", ""),
            position=kwargs.get("position"),
            line_number=kwargs.get("line_number"),
            column_number=kwargs.get("column_number"),
        )
        return handler._handle_error(error, f"${{{var_name}}}")
