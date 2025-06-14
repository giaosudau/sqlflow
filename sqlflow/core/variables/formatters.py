"""Context-Specific Variable Formatters for SQLFlow.

This module provides dedicated formatters for different contexts where variables
are used. Each formatter has a single responsibility and handles the specific
requirements of its context.

Zen of Python: "Do one thing and do it well."
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class VariableFormatter(ABC):
    """Abstract base class for variable formatters.

    Each formatter is responsible for formatting variable values in a specific
    context (SQL, text, JSON, etc.) following the single responsibility principle.
    """

    @abstractmethod
    def format_value(self, value: Any, context: Optional[str] = None) -> str:
        """Format a variable value for the specific context.

        Args:
            value: The value to format
            context: Optional context information for formatting decisions

        Returns:
            Formatted string representation of the value
        """

    @abstractmethod
    def format_missing_variable(
        self, var_name: str, context: Optional[str] = None
    ) -> str:
        """Handle missing variable in the specific context.

        Args:
            var_name: Name of the missing variable
            context: Optional context information

        Returns:
            Formatted representation for missing variable
        """


class SQLFormatter(VariableFormatter):
    """Formats variables for SQL context with proper SQL type handling.

    Responsibilities:
    - SQL NULL handling for None values
    - Proper string quoting and escaping
    - Numeric type preservation
    - Boolean to SQL boolean conversion
    - SQL injection prevention through proper escaping
    """

    def format_value(self, value: Any, context: Optional[str] = None) -> str:
        """Format a value for SQL context with proper type handling."""
        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"

        if isinstance(value, (int, float)):
            return str(value)

        if isinstance(value, str):
            return self._format_string_for_sql(value)

        # For complex types, convert to JSON string and then format
        try:
            json_str = json.dumps(value)
            return self._format_string_for_sql(json_str)
        except (TypeError, ValueError):
            # Fallback to string representation
            return self._format_string_for_sql(str(value))

    def format_missing_variable(
        self, var_name: str, context: Optional[str] = None
    ) -> str:
        """Handle missing variable in SQL context."""
        logger.warning(f"Variable '{var_name}' not found in SQL context, using NULL")
        return "NULL"

    def _format_string_for_sql(self, value: str) -> str:
        """Format string value for SQL with proper escaping and quoting."""
        # Handle already quoted strings
        if self._is_already_quoted(value):
            return value

        # Handle special SQL values
        if self._is_sql_keyword_or_function(value):
            return value

        # Handle numeric strings
        if self._is_numeric_string(value):
            return value

        # Handle boolean strings (convert to uppercase)
        if value.lower() in ("true", "false"):
            return value.upper()

        # Handle SQL NULL
        if value.upper() == "NULL":
            return "NULL"

        # Standard string escaping and quoting
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    def _is_already_quoted(self, value: str) -> bool:
        """Check if string is already properly quoted."""
        return (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        )

    def _is_sql_keyword_or_function(self, value: str) -> bool:
        """Check if value is a SQL keyword or function that shouldn't be quoted."""
        sql_keywords = {
            "NULL",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "NOW()",
            "SYSDATE",
        }

        # Note: We don't include TRUE/FALSE here because we want to handle
        # the boolean string conversion explicitly in _format_string_for_sql

        # Check for function calls (contains parentheses)
        if "(" in value and value.endswith(")"):
            return True

        return value.upper() in sql_keywords

    def _is_numeric_string(self, value: str) -> bool:
        """Check if string represents a number."""
        try:
            float(value)
            return True
        except ValueError:
            return False


class TextFormatter(VariableFormatter):
    """Formats variables for plain text context.

    Responsibilities:
    - Simple string conversion for all types
    - Preserving readable format for humans
    - No special escaping or quoting
    """

    def format_value(self, value: Any, context: Optional[str] = None) -> str:
        """Format a value for plain text context."""
        if value is None:
            return ""

        return str(value)

    def format_missing_variable(
        self, var_name: str, context: Optional[str] = None
    ) -> str:
        """Handle missing variable in text context."""
        logger.warning(f"Variable '{var_name}' not found in text context")
        return f"${{{var_name}}}"  # Keep original placeholder


class ASTFormatter(VariableFormatter):
    """Formats variables for Python AST evaluation context.

    Responsibilities:
    - Python literal formatting
    - Proper string quoting for Python
    - Boolean/None conversion to Python literals
    - Safe evaluation format
    """

    def format_value(self, value: Any, context: Optional[str] = None) -> str:
        """Format a value for Python AST evaluation context."""
        if value is None:
            return "None"

        if isinstance(value, bool):
            return "True" if value else "False"

        if isinstance(value, (int, float)):
            return str(value)

        if isinstance(value, str):
            return self._format_string_for_python(value)

        # For complex types, use JSON representation
        try:
            return json.dumps(value)
        except (TypeError, ValueError):
            # Fallback to string representation
            return self._format_string_for_python(str(value))

    def format_missing_variable(
        self, var_name: str, context: Optional[str] = None
    ) -> str:
        """Handle missing variable in AST context."""
        logger.warning(f"Variable '{var_name}' not found in AST context, using None")
        return "None"

    def _format_string_for_python(self, value: str) -> str:
        """Format string for Python evaluation with proper escaping."""
        # Handle special Python values
        if value in ("None", "True", "False"):
            return value

        # Handle numeric strings
        if self._is_numeric_string(value):
            return value

        # Use repr() for proper Python string escaping
        # This handles all special characters including newlines, tabs, etc.
        return repr(value)

    def _is_numeric_string(self, value: str) -> bool:
        """Check if string represents a number."""
        try:
            float(value)
            return True
        except ValueError:
            return False


class JSONFormatter(VariableFormatter):
    """Formats variables for JSON context.

    Responsibilities:
    - JSON-compatible value formatting
    - Proper JSON escaping
    - Type preservation for JSON serialization
    - Unicode handling
    """

    def format_value(self, value: Any, context: Optional[str] = None) -> str:
        """Format a value for JSON context."""
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            # Fallback for non-serializable objects
            return json.dumps(str(value), ensure_ascii=False)

    def format_missing_variable(
        self, var_name: str, context: Optional[str] = None
    ) -> str:
        """Handle missing variable in JSON context."""
        logger.warning(f"Variable '{var_name}' not found in JSON context, using null")
        return "null"


class FormatterRegistry:
    """Registry for managing different variable formatters.

    Provides a centralized way to access formatters for different contexts
    and enables easy extension with new formatters.
    """

    def __init__(self):
        """Initialize the formatter registry with default formatters."""
        self._formatters = {
            "sql": SQLFormatter(),
            "text": TextFormatter(),
            "ast": ASTFormatter(),
            "json": JSONFormatter(),
        }

    def get_formatter(self, context: str) -> VariableFormatter:
        """Get formatter for the specified context.

        Args:
            context: The context name (sql, text, ast, json)

        Returns:
            Appropriate formatter for the context

        Raises:
            ValueError: If context is not supported
        """
        if context not in self._formatters:
            raise ValueError(f"Unsupported formatter context: {context}")

        return self._formatters[context]

    def register_formatter(self, context: str, formatter: VariableFormatter) -> None:
        """Register a new formatter for a context.

        Args:
            context: The context name
            formatter: The formatter instance
        """
        self._formatters[context] = formatter
        logger.debug(f"Registered formatter for context: {context}")

    def get_available_contexts(self) -> list[str]:
        """Get list of available formatter contexts.

        Returns:
            List of available context names
        """
        return list(self._formatters.keys())


# Global formatter registry instance
_formatter_registry: Optional[FormatterRegistry] = None


def get_formatter_registry() -> FormatterRegistry:
    """Get the global formatter registry instance.

    Returns:
        Global FormatterRegistry instance
    """
    global _formatter_registry
    if _formatter_registry is None:
        _formatter_registry = FormatterRegistry()
        logger.debug("Global formatter registry created")
    return _formatter_registry


def get_formatter(context: str) -> VariableFormatter:
    """Get formatter for the specified context.

    This is a convenience function that provides direct access to formatters
    without needing to interact with the registry directly.

    Args:
        context: The context name (sql, text, ast, json)

    Returns:
        Appropriate formatter for the context
    """
    return get_formatter_registry().get_formatter(context)


def format_value(value: Any, context: str) -> str:
    """Format a value for the specified context.

    This is a convenience function for one-off formatting operations.

    Args:
        value: The value to format
        context: The context name (sql, text, ast, json)

    Returns:
        Formatted string representation of the value
    """
    formatter = get_formatter(context)
    return formatter.format_value(value)


def format_missing_variable(var_name: str, context: str) -> str:
    """Format a missing variable for the specified context.

    Args:
        var_name: Name of the missing variable
        context: The context name (sql, text, ast, json)

    Returns:
        Formatted representation for missing variable
    """
    formatter = get_formatter(context)
    return formatter.format_missing_variable(var_name)
