"""Validation and error handling for SQLFlow Variables V2

This module implements simple, effective validation and error handling for
variable substitution operations.

Following Zen of Python principles:
- Simple is better than complex
- Errors should never pass silently
- Pure functions with no side effects
"""

import re
from typing import Any, Dict, List, Optional

from .substitution import find_variables
from .types import ValidationResult


class VariableError(Exception):
    """Simple variable error with context."""

    def __init__(
        self,
        message: str,
        variable_name: Optional[str] = None,
        context: Optional[str] = None,
    ):
        super().__init__(message)
        self.variable_name = variable_name
        self.context = context


def validate_variables(text: str, variables: Dict[str, Any]) -> List[str]:
    """Validate variables and return missing ones.

    Args:
        text: Text to validate for variable usage
        variables: Available variables

    Returns:
        List of missing variable names
    """
    if not text:
        return []

    missing = []
    found_vars = find_variables(text)

    for var_info in found_vars:
        var_name = var_info.name
        default = var_info.default_value

        # Variable is missing if it's not in variables and has no default
        if var_name not in variables and default is None:
            missing.append(var_name)

    # Remove duplicates while preserving order
    seen = set()
    unique_missing = []
    for var in missing:
        if var not in seen:
            seen.add(var)
            unique_missing.append(var)

    return unique_missing


def validate_comprehensive(text: str, variables: Dict[str, Any]) -> ValidationResult:
    """Comprehensive validation with detailed results.

    Args:
        text: Text to validate
        variables: Available variables

    Returns:
        ValidationResult with detailed validation information
    """
    if not text:
        return ValidationResult(
            is_valid=True, missing_variables=[], invalid_syntax=[], suggestions=[]
        )

    found_vars = find_variables(text)
    missing_vars = []
    invalid_syntax = []
    suggestions = []

    for var_info in found_vars:
        var_name = var_info.name
        default = var_info.default_value

        # Check for missing variables
        if var_name not in variables and default is None:
            missing_vars.append(var_name)

            # Suggest similar variable names
            similar = _find_similar_variables(var_name, list(variables.keys()))
            if similar:
                suggestions.append(
                    f"'{var_name}' not found. Did you mean: {', '.join(similar)}?"
                )

        # Check for invalid syntax
        if not _is_valid_variable_name(var_name):
            invalid_syntax.append(var_info.full_match)

    # Remove duplicates
    missing_vars = list(dict.fromkeys(missing_vars))
    invalid_syntax = list(dict.fromkeys(invalid_syntax))

    is_valid = len(missing_vars) == 0 and len(invalid_syntax) == 0

    return ValidationResult(
        is_valid=is_valid,
        missing_variables=missing_vars,
        invalid_syntax=invalid_syntax,
        suggestions=suggestions,
    )


def check_circular_references(variables: Dict[str, Any]) -> List[str]:
    """Check for circular references in variable definitions.

    Args:
        variables: Dictionary of variables to check

    Returns:
        List of variables involved in circular references
    """
    circular = []

    for var_name, value in variables.items():
        if isinstance(value, str):
            if _has_circular_reference(var_name, value, variables):
                circular.append(var_name)

    return circular


def validate_variable_name(name: str) -> bool:
    """Validate that a variable name follows proper conventions.

    Args:
        name: Variable name to validate

    Returns:
        True if valid, False otherwise
    """
    return _is_valid_variable_name(name)


def get_variable_usage_stats(text: str) -> Dict[str, int]:
    """Get statistics about variable usage in text.

    Args:
        text: Text to analyze

    Returns:
        Dictionary mapping variable names to usage counts
    """
    if not text:
        return {}

    found_vars = find_variables(text)
    usage_counts = {}

    for var_info in found_vars:
        var_name = var_info.name
        usage_counts[var_name] = usage_counts.get(var_name, 0) + 1

    return usage_counts


def _is_valid_variable_name(name: str) -> bool:
    """Check if a variable name is valid.

    Args:
        name: Variable name to validate

    Returns:
        True if valid, False otherwise
    """
    if not name:
        return False

    # Basic validation: starts with letter or underscore, contains only alphanumeric and underscore
    return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name))


def _find_similar_variables(target: str, available: List[str]) -> List[str]:
    """Find variables with similar names for suggestions.

    Args:
        target: Variable name to find similar ones for
        available: List of available variable names

    Returns:
        List of similar variable names (max 3)
    """
    similar = []
    target_lower = target.lower()

    for var in available:
        var_lower = var.lower()

        # Check for common patterns
        if (
            # Same length, different by 1-2 characters
            (len(target) == len(var) and _char_diff_count(target_lower, var_lower) <= 2)
            or
            # One is substring of the other
            target_lower in var_lower
            or var_lower in target_lower
            or
            # Similar prefix/suffix
            (
                len(target) > 3
                and (
                    target_lower[:3] == var_lower[:3]
                    or target_lower[-3:] == var_lower[-3:]
                )
            )
        ):
            similar.append(var)

    return similar[:3]  # Return max 3 suggestions


def _char_diff_count(str1: str, str2: str) -> int:
    """Count character differences between two strings of same length.

    Args:
        str1: First string
        str2: Second string

    Returns:
        Number of different characters
    """
    if len(str1) != len(str2):
        return len(str1) + len(str2)  # Return large number for different lengths

    return sum(c1 != c2 for c1, c2 in zip(str1, str2))


def _has_circular_reference(
    var_name: str,
    value: str,
    variables: Dict[str, Any],
    visited: Optional[set[str]] = None,
) -> bool:
    """Check if a variable has circular references.

    Args:
        var_name: Name of the variable being checked
        value: Value of the variable (string)
        variables: All variables dictionary
        visited: Set of already visited variables (for recursion)

    Returns:
        True if circular reference exists
    """
    if visited is None:
        visited = set()

    if var_name in visited:
        return True

    visited.add(var_name)

    # Find variables referenced in the value
    referenced_vars = find_variables(value)

    for var_info in referenced_vars:
        ref_name = var_info.name
        if ref_name in variables and isinstance(variables[ref_name], str):
            if _has_circular_reference(
                ref_name, variables[ref_name], variables, visited.copy()
            ):
                return True

    return False
