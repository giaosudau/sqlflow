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
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def substitute_variables(text: str, variables: Dict[str, Any]) -> str:
    """Pure function to substitute variables in text.

    Supports both ${variable} and {{variable}} syntax for compatibility.
    Optimized for performance with minimal regex usage.

    Args:
        text: Template string with $variable or {{variable}} placeholders
        variables: Dictionary of variable names and values

    Returns:
        String with variables substituted

    Examples:
        >>> substitute_variables("Hello $name", {"name": "World"})
        'Hello World'

        >>> substitute_variables("SELECT * FROM {{table}}", {"table": "users"})
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
    result = text

    # Quick optimization: if no $ or { characters, skip substitution
    if "$" not in result and "{" not in result:
        return result

    # Optimize: Only substitute variables that actually appear in the text
    # This avoids O(n*m) complexity when most variables aren't used
    relevant_variables = []
    for key, value in variables.items():
        # Check if any variable syntax appears in text
        if f"${key}" in result or f"${{{key}}}" in result or f"{{{{{key}}}}}" in result:
            relevant_variables.append((key, value))

    if not relevant_variables:
        return result

    # Sort only the relevant variables by length (longest first) to avoid partial matches
    relevant_variables.sort(key=lambda x: len(x[0]), reverse=True)

    # Handle all variable syntaxes efficiently
    for key, value in relevant_variables:
        result = _substitute_single_variable(result, key, value)

    return result


def _substitute_single_variable(text: str, key: str, value: Any) -> str:
    """Substitute a single variable in all supported syntax forms."""
    str_value = str(value)
    result = text

    # Handle {{variable}} syntax (Jinja2-style) - simple replace
    jinja_placeholder = f"{{{{{key}}}}}"
    if jinja_placeholder in result:
        result = result.replace(jinja_placeholder, str_value)

    # Handle ${variable} syntax (shell-style) - simple replace
    dollar_brace_placeholder = f"${{{key}}}"
    if dollar_brace_placeholder in result:
        result = result.replace(dollar_brace_placeholder, str_value)

    # Handle $variable syntax (simple dollar prefix)
    dollar_placeholder = f"${key}"
    if dollar_placeholder in result:
        result = _substitute_dollar_variable(result, key, str_value)

    return result


def _substitute_dollar_variable(text: str, key: str, str_value: str) -> str:
    """Substitute $variable syntax with boundary checking."""
    dollar_placeholder = f"${key}"

    if dollar_placeholder not in text:
        return text

    # Split on the placeholder and rejoin with value,
    # but be careful with boundaries
    parts = text.split(dollar_placeholder)
    if len(parts) <= 1:
        return text

    new_parts = [parts[0]]
    for i in range(1, len(parts)):
        # Check if this is a word boundary
        # (next char is not alphanumeric or _)
        if _is_word_boundary(parts[i]):
            new_parts.append(str_value + parts[i])
        else:
            new_parts.append(dollar_placeholder + parts[i])

    return "".join(new_parts)


def _is_word_boundary(text_after: str) -> bool:
    """Check if the text represents a word boundary."""
    return not text_after or (not text_after[0].isalnum() and text_after[0] != "_")


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
