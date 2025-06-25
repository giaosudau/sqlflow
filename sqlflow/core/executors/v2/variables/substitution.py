"""Pure functions for variable substitution.

Following functional programming principles:
- Pure functions with no side effects
- Immutable data transformations
- Type safety with comprehensive hints
- Clear, composable functions

This module embodies the Zen of Python:
- Simple is better than complex
- Explicit is better than implicit
- Readability counts
"""

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Compile regex patterns once for performance
_DOLLAR_VAR_PATTERN = re.compile(r"\$([a-zA-Z_][a-zA-Z0-9_]*)")
_DOLLAR_BRACE_PATTERN = re.compile(r"\$\{([^}]+)\}")


def substitute_variables(text: str, variables: Dict[str, Any]) -> str:
    """Pure function to substitute variables in text.

    Supports $variable and ${variable} syntax.
    Optimized for performance with compiled regex patterns.

    Args:
        text: Template string with $variable or ${variable} placeholders
        variables: Dictionary of variable names and values

    Returns:
        String with variables substituted

    Examples:
        >>> substitute_variables("Hello $name", {"name": "World"})
        'Hello World'

        >>> substitute_variables("SELECT * FROM ${table}", {"table": "users"})
        'SELECT * FROM users'
    """
    if not isinstance(text, str):
        return str(text)

    if not variables:
        return text

    try:
        return _perform_substitution(text, variables)
    except Exception as e:
        logger.debug(f"Variable substitution failed for text '{text[:50]}...': {e}")
        return text


def _perform_substitution(text: str, variables: Dict[str, Any]) -> str:
    """Perform the actual variable substitution with optimizations."""
    # Quick optimization: if no $ or { characters, skip substitution
    if "$" not in text and "{" not in text:
        return text

    result = text

    # Handle ${variable} syntax first (most specific)
    result = _DOLLAR_BRACE_PATTERN.sub(
        lambda m: str(variables.get(m.group(1), m.group(0))), result
    )

    # Handle $variable syntax (simple dollar prefix)
    result = _DOLLAR_VAR_PATTERN.sub(
        lambda m: _replace_dollar_var(m, variables), result
    )

    return result


def _replace_dollar_var(match, variables: Dict[str, Any]) -> str:
    """Replace a dollar variable with proper word boundary checking."""
    var_name = match.group(1)
    if var_name in variables:
        return str(variables[var_name])
    return match.group(0)  # Keep original if not found


def substitute_in_dict(
    data: Dict[str, Any], variables: Dict[str, Any]
) -> Dict[str, Any]:
    """Pure function to recursively substitute variables in dictionary values.

    Args:
        data: Dictionary that may contain template strings
        variables: Variables to substitute

    Returns:
        New dictionary with variables substituted

    Examples:
        >>> substitute_in_dict(
        ...     {"query": "SELECT * FROM $table"}, {"table": "users"}
        ... )
        {'query': 'SELECT * FROM users'}
    """
    if not isinstance(data, dict) or not variables:
        return data

    result = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = substitute_variables(value, variables)
        elif isinstance(value, dict):
            result[key] = substitute_in_dict(value, variables)
        elif isinstance(value, list):
            result[key] = substitute_in_list(value, variables)
        else:
            result[key] = value

    return result


def substitute_in_list(data: List[Any], variables: Dict[str, Any]) -> List[Any]:
    """Pure function to substitute variables in list items.

    Args:
        data: List that may contain template strings
        variables: Variables to substitute

    Returns:
        New list with variables substituted
    """
    if not isinstance(data, list) or not variables:
        return data

    result = []
    for item in data:
        if isinstance(item, str):
            result.append(substitute_variables(item, variables))
        elif isinstance(item, dict):
            result.append(substitute_in_dict(item, variables))
        elif isinstance(item, list):
            result.append(substitute_in_list(item, variables))
        else:
            result.append(item)

    return result


def substitute_in_step(
    step: Dict[str, Any], variables: Dict[str, Any]
) -> Dict[str, Any]:
    """Pure function to substitute variables in a pipeline step.

    This is a specialized version of substitute_in_dict
    optimized for step processing.

    Args:
        step: Pipeline step configuration
        variables: Variables to substitute

    Returns:
        New step with variables substituted
    """
    if not variables:
        return step

    return substitute_in_dict(step, variables)


def merge_variables(*variable_dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Pure function to merge multiple variable dictionaries.

    Later dictionaries override earlier ones.

    Args:
        *variable_dicts: Variable dictionaries to merge

    Returns:
        Merged variables dictionary

    Examples:
        >>> merge_variables({"a": 1}, {"b": 2}, {"a": 3})
        {'a': 3, 'b': 2}
    """
    result = {}
    for var_dict in variable_dicts:
        if isinstance(var_dict, dict):
            result.update(var_dict)
    return result


def validate_variables(variables: Any) -> Dict[str, Any]:
    """Pure function to validate and normalize variables.

    Args:
        variables: Variables to validate (can be dict, None, etc.)

    Returns:
        Valid variables dictionary

    Raises:
        TypeError: If variables is not None or dict
    """
    if variables is None:
        return {}

    if not isinstance(variables, dict):
        raise TypeError(f"Variables must be dict or None, got {type(variables)}")

    # Ensure all keys are strings
    return {str(k): v for k, v in variables.items()}
