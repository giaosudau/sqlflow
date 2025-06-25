"""SQLFlow Variables Module V2 - Pure Functional Implementation

This module provides a clean, simple, and performant variable substitution system
following Zen of Python principles. All functions are pure with no side effects.

Following Raymond Hettinger and Guido van Rossum's recommendations:
- Simple is better than complex
- Explicit is better than implicit
- Use built-in features (lru_cache, frozen dataclasses)
- Clear, readable code over clever abstractions
"""

from .formatting import format_for_context
from .resolution import (
    get_variable_priority,
    merge_variable_sources,
    resolve_from_environment,
    resolve_variables,
    resolve_variables_legacy,
    resolve_with_sources,
)
from .substitution import (
    find_variables,
    substitute_any,
    substitute_in_dict,
    substitute_in_list,
    substitute_simple_dollar,
    substitute_variables,
    substitute_variables_for_sql,
)
from .types import (
    ValidationResult,
    VariableContext,
    VariableInfo,
    VariableSources,
)
from .validation import (
    VariableError,
    validate_variable_name,
    validate_variables,
    validate_variables_with_details,
)

__all__ = [
    # Core substitution functions
    "substitute_variables",
    "substitute_simple_dollar",
    "substitute_in_dict",
    "substitute_in_list",
    "substitute_any",
    "substitute_variables_for_sql",
    # Resolution functions
    "resolve_variables",
    "resolve_variables_legacy",
    "resolve_from_environment",
    "get_variable_priority",
    "resolve_with_sources",
    "merge_variable_sources",
    # Validation functions
    "validate_variables",
    "validate_variables_with_details",
    "validate_variable_name",
    # Formatting functions
    "format_for_context",
    # Utility functions
    "find_variables",
    # Types and exceptions
    "VariableContext",
    "VariableSources",
    "ValidationResult",
    "VariableInfo",
    "VariableError",
]
