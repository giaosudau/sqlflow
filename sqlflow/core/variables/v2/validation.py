"""Validation and error handling for SQLFlow Variables V2

This module implements pure functions for variable validation with clear,
actionable error messages. Following Zen of Python principles:
- Simple is better than complex
- Explicit is better than implicit
- Pure functions with no side effects
"""

import re
from typing import Any, Dict, List, Optional

from .types import ValidationResult, VariableInfo

# Compile patterns once for performance
_VARIABLE_PATTERN = re.compile(r"\$\{([^}|]+)(?:\|([^}]+))?\}")
_SIMPLE_DOLLAR_PATTERN = re.compile(r"\$([a-zA-Z_][a-zA-Z0-9_]*)")


def validate_variables(text: str, variables: Dict[str, Any]) -> List[str]:
    """Validate variables and return missing ones.

    Args:
        text: Text containing variable placeholders
        variables: Dictionary of available variables

    Returns:
        List of missing variable names (unique, no duplicates)
    """
    if not text:
        return []

    missing = []
    found_variables = find_variables(text)

    for var_info in found_variables:
        if var_info.name not in variables and var_info.default_value is None:
            missing.append(var_info.name)

    # Remove duplicates while preserving order
    seen = set()
    unique_missing = []
    for var in missing:
        if var not in seen:
            seen.add(var)
            unique_missing.append(var)

    return unique_missing


def validate_variables_with_details(
    text: str, variables: Dict[str, Any]
) -> ValidationResult:
    """Validate variables and return detailed validation result.

    Args:
        text: Text containing variable placeholders
        variables: Dictionary of available variables

    Returns:
        ValidationResult with detailed information
    """
    if not text:
        return ValidationResult(is_valid=True)

    missing_variables = []
    invalid_syntax = []
    suggestions = []

    found_variables = find_variables(text)

    for var_info in found_variables:
        # Check if variable is missing
        if var_info.name not in variables and var_info.default_value is None:
            missing_variables.append(var_info.name)

            # Provide suggestions for similar variable names
            similar = _find_similar_variables(var_info.name, list(variables.keys()))
            if similar:
                suggestions.append(
                    f"'{var_info.name}' not found. Did you mean: {', '.join(similar)}"
                )

        # Check for invalid syntax patterns
        if _has_invalid_syntax(var_info):
            invalid_syntax.append(var_info.full_match)

    is_valid = len(missing_variables) == 0 and len(invalid_syntax) == 0

    return ValidationResult(
        is_valid=is_valid,
        missing_variables=missing_variables,
        invalid_syntax=invalid_syntax,
        suggestions=suggestions,
    )


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


def validate_variable_name(var_name: str) -> bool:
    """Validate a single variable name.

    Args:
        var_name: Variable name to validate

    Returns:
        True if valid, False otherwise
    """
    if not var_name:
        return False

    # Check for valid characters
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", var_name):
        return False

    # Check for reserved words
    reserved_words = {
        "and",
        "as",
        "assert",
        "break",
        "class",
        "continue",
        "def",
        "del",
        "elif",
        "else",
        "except",
        "finally",
        "for",
        "from",
        "global",
        "if",
        "import",
        "in",
        "is",
        "lambda",
        "nonlocal",
        "not",
        "or",
        "pass",
        "raise",
        "return",
        "try",
        "while",
        "with",
        "yield",
    }

    return var_name.lower() not in reserved_words


def _clean_default_value(default: str) -> str:
    """Clean default value by removing quotes if appropriate.

    Args:
        default: Raw default value from regex match

    Returns:
        Cleaned default value
    """
    if not default:
        return default

    # Remove outer quotes if present
    if len(default) >= 2:
        if (default.startswith('"') and default.endswith('"')) or (
            default.startswith("'") and default.endswith("'")
        ):
            return default[1:-1]

    return default


def _find_similar_variables(
    target: str, available_vars: List[str], max_suggestions: int = 3
) -> List[str]:
    """Find similar variable names for suggestions.

    Args:
        target: Target variable name
        available_vars: List of available variable names
        max_suggestions: Maximum number of suggestions to return

    Returns:
        List of similar variable names
    """
    if not target or not available_vars:
        return []

    # Simple similarity check - Raymond's preference for simplicity
    similar = []
    target_lower = target.lower()

    for var in available_vars:
        var_lower = var.lower()

        # Exact prefix match
        if var_lower.startswith(target_lower):
            similar.append(var)
        # Contains the target
        elif target_lower in var_lower:
            similar.append(var)
        # Levenshtein-like check for typos
        elif _simple_similarity(target_lower, var_lower) > 0.7:
            similar.append(var)

    # Return unique suggestions, limited by max_suggestions
    unique_suggestions = list(dict.fromkeys(similar))  # Preserve order
    return unique_suggestions[:max_suggestions]


def _simple_similarity(str1: str, str2: str) -> float:
    """Calculate simple similarity between two strings.

    Args:
        str1: First string
        str2: Second string

    Returns:
        Similarity score between 0 and 1
    """
    if str1 == str2:
        return 1.0

    # Simple character-based similarity
    common_chars = set(str1) & set(str2)
    total_chars = set(str1) | set(str2)

    if not total_chars:
        return 0.0

    return len(common_chars) / len(total_chars)


def _has_invalid_syntax(var_info: VariableInfo) -> bool:
    """Check if variable has invalid syntax.

    Args:
        var_info: VariableInfo object to check

    Returns:
        True if syntax is invalid
    """
    # Check for empty variable names
    if not var_info.name.strip():
        return True

    # Check for invalid characters in variable name
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", var_info.name):
        return True

    return False


class VariableError(Exception):
    """Simple variable error with context.

    Following Raymond's recommendation: simple, focused exceptions.
    """

    def __init__(
        self,
        message: str,
        variable_name: Optional[str] = None,
        context: Optional[str] = None,
    ):
        super().__init__(message)
        self.variable_name = variable_name
        self.context = context

    def __str__(self) -> str:
        message = super().__str__()
        if self.variable_name:
            message += f" (variable: {self.variable_name})"
        if self.context:
            message += f" (context: {self.context})"
        return message
