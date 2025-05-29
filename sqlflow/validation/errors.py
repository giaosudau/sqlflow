"""Validation error definitions for SQLFlow DSL."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ValidationError(Exception):
    """Immutable validation error with precise location.

    Provides clear, actionable error messages with precise location information
    and helpful suggestions for fixing issues.
    """

    message: str
    line: int
    column: int = 0
    error_type: str = "ValidationError"
    suggestions: List[str] = field(default_factory=list)
    help_url: Optional[str] = None

    def __str__(self) -> str:
        """Simple, clear error formatting for MVP.

        Returns:
            Formatted error string with suggestions and help URL if available
        """
        result = f"❌ {self.error_type} at line {self.line}"
        if self.column > 0:
            result += f", column {self.column}"
        result += f": {self.message}"

        if self.suggestions:
            result += "\n\n💡 Suggestions:"
            for suggestion in self.suggestions:
                result += f"\n  - {suggestion}"

        if self.help_url:
            result += f"\n\n📖 Help: {self.help_url}"

        return result

    @classmethod
    def from_parse_error(cls, parse_error: Exception) -> "ValidationError":
        """Convert parser errors to validation errors.

        Args:
            parse_error: Exception from parser with optional line/column info

        Returns:
            ValidationError instance with extracted position information
        """
        # Extract line/column info if available
        line = getattr(parse_error, "line", 1)
        column = getattr(parse_error, "column", 0)

        return cls(
            message=str(parse_error),
            line=line,
            column=column,
            error_type="Syntax Error",
        )


@dataclass
class AggregatedValidationError(Exception):
    """Exception that aggregates multiple validation errors.

    This exception is used when multiple validation errors are found in a pipeline,
    providing comprehensive feedback to users about all issues at once.
    """

    errors: List[ValidationError]

    def __post_init__(self):
        """Initialize the exception message after dataclass creation."""
        if not self.errors:
            super().__init__("No validation errors found")
        elif len(self.errors) == 1:
            super().__init__(str(self.errors[0]))
        else:
            # Create a summary message for multiple errors
            error_types = {}
            for error in self.errors:
                error_type = error.error_type
                error_types[error_type] = error_types.get(error_type, 0) + 1

            type_summary = ", ".join(
                [
                    f"{count} {error_type}(s)"
                    for error_type, count in error_types.items()
                ]
            )
            summary_msg = f"Pipeline validation failed with {len(self.errors)} error(s): {type_summary}"
            super().__init__(summary_msg)

    def _group_errors_by_type(self) -> dict:
        """Group validation errors by type and sort by line number.

        Returns:
            Dictionary mapping error types to sorted lists of errors
        """
        error_groups = {}
        for error in self.errors:
            error_type = error.error_type
            if error_type not in error_groups:
                error_groups[error_type] = []
            error_groups[error_type].append(error)

        # Sort errors within each group by line number
        for error_list in error_groups.values():
            error_list.sort(key=lambda e: (e.line, e.column))

        return error_groups

    def _format_error_line(self, error: ValidationError) -> str:
        """Format the main error line with location information.

        Args:
            error: ValidationError to format

        Returns:
            Formatted error line string
        """
        error_line = f"  Line {error.line}"
        if error.column > 0:
            error_line += f", Column {error.column}"
        error_line += f": {error.message}"
        return error_line

    def _format_error_suggestions(self, error: ValidationError) -> List[str]:
        """Format suggestion lines for an error.

        Args:
            error: ValidationError with suggestions

        Returns:
            List of formatted suggestion lines
        """
        suggestion_lines = []
        if error.suggestions:
            for suggestion in error.suggestions:
                suggestion_lines.append(f"    💡 {suggestion}")

        if error.help_url:
            suggestion_lines.append(f"    📖 Help: {error.help_url}")

        return suggestion_lines

    def _format_error_group(
        self, error_type: str, type_errors: List[ValidationError]
    ) -> List[str]:
        """Format a group of errors of the same type.

        Args:
            error_type: Type of errors in this group
            type_errors: List of errors of this type

        Returns:
            List of formatted lines for this error group
        """
        lines = []
        if len(type_errors) > 0:
            lines.append(f"📋 {error_type}s:")

            for error in type_errors:
                # Add main error line
                lines.append(self._format_error_line(error))

                # Add suggestions if available
                suggestion_lines = self._format_error_suggestions(error)
                lines.extend(suggestion_lines)

                lines.append("")  # Empty line between errors

        return lines

    def __str__(self) -> str:
        """Format all validation errors for display.

        Returns:
            Formatted string containing all validation errors
        """
        if not self.errors:
            return "No validation errors found"

        if len(self.errors) == 1:
            return str(self.errors[0])

        # Group errors by type
        error_groups = self._group_errors_by_type()

        # Build formatted output
        lines = []
        lines.append(f"❌ Pipeline validation failed with {len(self.errors)} error(s):")
        lines.append("")

        # Format each error group
        for error_type, type_errors in error_groups.items():
            group_lines = self._format_error_group(error_type, type_errors)
            lines.extend(group_lines)

        return "\n".join(lines).rstrip()

    @property
    def first_error(self) -> Optional[ValidationError]:
        """Get the first error for backward compatibility.

        Returns:
            The first ValidationError in the list, or None if empty
        """
        return self.errors[0] if self.errors else None
