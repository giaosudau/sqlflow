"""Core variable substitution functions for SQLFlow V2

This module implements pure functions for variable substitution with support
for all V1 syntax patterns. Following Zen of Python principles:
- Simple is better than complex
- Explicit is better than implicit
- Pure functions with no side effects
"""

import re
from typing import Any, Dict, List

from .formatting import format_for_context
from .types import VariableInfo

# Unified regex pattern for all syntax forms
_VARIABLE_PATTERN = re.compile(r"\$\{([^}|]+)(?:\|([^}]+))?\}")
_SIMPLE_DOLLAR_PATTERN = re.compile(r"\$([a-zA-Z_][a-zA-Z0-9_]*)")


def substitute_variables(text: str, variables: Dict[str, Any]) -> str:
    """Pure function for variable substitution with all V1 syntax support.

    Supports:
    - ${variable}
    - ${variable|default}
    - ${variable|"quoted default"}
    - ${variable|'quoted default'}

    Args:
        text: Text containing variable placeholders
        variables: Dictionary of variable values

    Returns:
        Text with variables substituted
    """
    if not text:
        return text

    # Use variables or empty dict if None
    vars_dict = variables or {}

    def replace(match):
        var_name = match.group(1).strip()
        default = match.group(2).strip() if match.group(2) else None

        if var_name in vars_dict:
            return str(vars_dict[var_name])
        elif default is not None:
            return _clean_default_value(default)
        else:
            return match.group(0)  # Keep original if no replacement

    return re.sub(_VARIABLE_PATTERN, replace, text)


def substitute_simple_dollar(text: str, variables: Dict[str, Any]) -> str:
    """Handle simple $variable syntax with word boundary checking.

    Args:
        text: Text containing $variable placeholders
        variables: Dictionary of variable values

    Returns:
        Text with simple dollar variables substituted
    """
    if not text or not variables:
        return text

    def replace(match):
        var_name = match.group(1)
        if var_name in variables:
            return str(variables[var_name])
        else:
            return match.group(0)  # Keep original if no replacement

    return re.sub(_SIMPLE_DOLLAR_PATTERN, replace, text)


def substitute_in_dict(
    data: Dict[str, Any], variables: Dict[str, Any]
) -> Dict[str, Any]:
    """Substitute variables in a dictionary recursively.

    Args:
        data: Dictionary to process
        variables: Dictionary of variable values

    Returns:
        Dictionary with variables substituted
    """
    result = {}
    for key, value in data.items():
        # Substitute in key if it's a string
        new_key = substitute_variables(key, variables) if isinstance(key, str) else key
        # Substitute in value recursively
        new_value = substitute_any(value, variables)
        result[new_key] = new_value
    return result


def substitute_in_list(data: List[Any], variables: Dict[str, Any]) -> List[Any]:
    """Substitute variables in a list recursively.

    Args:
        data: List to process
        variables: Dictionary of variable values

    Returns:
        List with variables substituted
    """
    return [substitute_any(item, variables) for item in data]


def substitute_any(data: Any, variables: Dict[str, Any]) -> Any:
    """Substitute variables in any data structure.

    Args:
        data: Data structure to process
        variables: Dictionary of variable values

    Returns:
        Data structure with variables substituted
    """
    if isinstance(data, str):
        return substitute_variables(data, variables)
    elif isinstance(data, dict):
        return substitute_in_dict(data, variables)
    elif isinstance(data, list):
        return substitute_in_list(data, variables)
    else:
        return data


def find_variables(text: str) -> List[VariableInfo]:
    """Find all variables in text without substituting them.

    Args:
        text: Text to scan for variables

    Returns:
        List of VariableInfo objects for found variables
    """
    if not text:
        return []

    variables = []

    # Find ${var} and ${var|default} patterns
    for match in _VARIABLE_PATTERN.finditer(text):
        var_name = match.group(1).strip()
        default = match.group(2).strip() if match.group(2) else None

        variables.append(
            VariableInfo(
                name=var_name,
                default_value=_clean_default_value(default) if default else None,
                start_pos=match.start(),
                end_pos=match.end(),
                full_match=match.group(0),
            )
        )

    # Find $var patterns
    for match in _SIMPLE_DOLLAR_PATTERN.finditer(text):
        var_name = match.group(1)

        variables.append(
            VariableInfo(
                name=var_name,
                default_value=None,
                start_pos=match.start(),
                end_pos=match.end(),
                full_match=match.group(0),
            )
        )

    return variables


def _clean_default_value(default: str) -> str:
    """Clean default value by removing quotes if appropriate.

    Args:
        default: Raw default value from regex match

    Returns:
        Cleaned default value
    """
    if not default:
        return default

    # Remove outer quotes if present and it's a simple string
    if len(default) >= 2:
        if (default.startswith('"') and default.endswith('"')) or (
            default.startswith("'") and default.endswith("'")
        ):
            # Only remove quotes for simple strings, not complex expressions
            inner = default[1:-1]
            # Keep quotes for complex expressions like SQL lists
            if "," in inner and ('"' in inner or "'" in inner):
                return default
            return inner

    return default


def substitute_variables_for_sql(text: str, variables: Dict[str, Any]) -> str:
    """Substitute variables in text with SQL-appropriate formatting.

    This function substitutes variables and formats the values for SQL context,
    ensuring proper quoting of string values while leaving SQL keywords unquoted.

    Args:
        text: Text containing variable placeholders
        variables: Dictionary of variable values

    Returns:
        Text with variables substituted and formatted for SQL context
    """
    if not text:
        return text

    # Use variables or empty dict if None
    vars_dict = variables or {}

    def replace(match):
        var_name = match.group(1).strip()
        default = match.group(2).strip() if match.group(2) else None

        if var_name in vars_dict:
            # Format the variable value for SQL context
            value = vars_dict[var_name]
            return format_for_context(value, "sql")
        elif default is not None:
            # Format the default value for SQL context
            cleaned_default = _clean_default_value(default)
            return format_for_context(cleaned_default, "sql")
        else:
            return match.group(0)  # Keep original if no replacement

    return re.sub(_VARIABLE_PATTERN, replace, text)
