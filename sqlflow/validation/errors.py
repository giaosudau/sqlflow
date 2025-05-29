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
