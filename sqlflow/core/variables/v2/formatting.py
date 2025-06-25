"""Context-specific formatting for SQLFlow Variables V2

This module implements pure functions for formatting variable values in different
contexts (SQL, Text, AST, JSON) with proper type handling and security.

Following Zen of Python principles:
- Simple is better than complex
- Explicit is better than implicit
- Pure functions with no side effects
"""

import json
from typing import Any

from .types import VariableContext


def format_for_context(value: Any, context: str = "text") -> str:
    """Format value for specific context with V1 compatibility.

    Following Guido's preference: simple, readable if-elif chains.

    Args:
        value: The value to format
        context: Context type ('text', 'sql', 'ast', 'json')

    Returns:
        Formatted string representation of the value
    """
    if value is None:
        return _format_none_for_context(context)

    # Simple, readable dispatch - Guido's preference
    if context == VariableContext.SQL.value:
        return _format_for_sql(value)
    elif context == VariableContext.JSON.value:
        return _format_for_json(value)
    elif context == VariableContext.AST.value:
        return _format_for_ast(value)
    else:  # text context (default)
        return _format_for_text(value)


def _format_none_for_context(context: str) -> str:
    """Format None value based on context.

    Args:
        context: The formatting context

    Returns:
        Appropriate None representation for the context
    """
    if context == VariableContext.SQL.value:
        return "NULL"
    elif context == VariableContext.AST.value:
        return "None"
    elif context == VariableContext.JSON.value:
        return "null"
    else:  # text context
        return ""


def _format_for_text(value: Any) -> str:
    """Format value for plain text context.

    Args:
        value: Value to format

    Returns:
        String representation suitable for text
    """
    return str(value)


def _format_for_sql(value: Any) -> str:
    """Format value for SQL context with proper quoting and SQL injection prevention.

    Args:
        value: Value to format

    Returns:
        SQL-safe string representation
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        return _format_string_for_sql(value)
    else:
        # For complex types, convert to JSON string and then format
        try:
            json_str = json.dumps(value)
            return _format_string_for_sql(json_str)
        except (TypeError, ValueError):
            # Fallback to string representation
            return _format_string_for_sql(str(value))


def _format_for_ast(value: Any) -> str:
    """Format value for Python AST evaluation context.

    Args:
        value: Value to format

    Returns:
        Python literal representation
    """
    if isinstance(value, bool):
        return "True" if value else "False"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        return repr(value)  # Use repr() for proper Python string escaping
    else:
        # For complex types, use JSON representation or repr
        try:
            return json.dumps(value)
        except (TypeError, ValueError):
            return repr(value)


def _format_for_json(value: Any) -> str:
    """Format value for JSON context.

    Args:
        value: Value to format

    Returns:
        JSON string representation
    """
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        # Fallback for non-serializable objects
        return json.dumps(str(value), ensure_ascii=False)


def _format_string_for_sql(value: str) -> str:
    """Format string value for SQL with proper escaping and quoting.

    Args:
        value: String value to format

    Returns:
        SQL-safe quoted string
    """
    # Handle already quoted strings
    if _is_already_quoted(value):
        return value

    # Handle special SQL values
    if _is_sql_keyword_or_function(value):
        return value

    # Handle numeric strings
    if _is_numeric_string(value):
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


def _is_already_quoted(value: str) -> bool:
    """Check if string is already properly quoted.

    Args:
        value: String to check

    Returns:
        True if already quoted
    """
    if len(value) < 2:
        return False
    return (value.startswith("'") and value.endswith("'")) or (
        value.startswith('"') and value.endswith('"')
    )


def _is_sql_keyword_or_function(value: str) -> bool:
    """Check if value is a SQL keyword or function that shouldn't be quoted.

    Args:
        value: String to check

    Returns:
        True if it's a SQL keyword or function
    """
    sql_keywords = {
        "NULL",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "NOW()",
        "SYSDATE",
    }

    # Check for function calls (contains parentheses)
    if "(" in value and value.endswith(")"):
        return True

    return value.upper() in sql_keywords


def _is_numeric_string(value: str) -> bool:
    """Check if string represents a number.

    Args:
        value: String to check

    Returns:
        True if it represents a number
    """
    try:
        float(value)
        return True
    except ValueError:
        return False
