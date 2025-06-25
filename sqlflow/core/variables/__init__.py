"""SQLFlow Variables Module - V2 Implementation Only

Pure functional approach following Zen of Python principles.
Simple, explicit variable substitution without complex class hierarchies.
"""

from .v2 import (  # Core substitution functions; Priority resolution; Context formatting; Validation; Types
    ValidationResult,
    VariableContext,
    VariableError,
    find_variables,
    format_for_context,
    resolve_from_environment,
    resolve_variables,
    substitute_any,
    substitute_in_dict,
    substitute_in_list,
    substitute_simple_dollar,
    substitute_variables,
    substitute_variables_for_sql,
    validate_variables,
)

__all__ = [
    # Core substitution functions
    "substitute_variables",
    "substitute_variables_for_sql",
    "substitute_in_dict",
    "substitute_in_list",
    "substitute_simple_dollar",
    "substitute_any",
    "find_variables",
    # Priority resolution
    "resolve_variables",
    "resolve_from_environment",
    # Context formatting
    "format_for_context",
    # Validation
    "validate_variables",
    "VariableError",
    # Types
    "VariableContext",
    "ValidationResult",
]
