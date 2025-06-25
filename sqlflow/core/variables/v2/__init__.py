"""SQLFlow Variables Module V2 - Simple, Functional, Performant

This module provides the V2 implementation of variable substitution following
Zen of Python principles:
- Simple is better than complex
- There should be one obvious way to do it
- Explicit is better than implicit

All V2 functions are pure functions with no side effects, making them
easy to test, understand, and maintain.
"""

from .formatting import (
    format_for_context,
)
from .resolution import (
    get_variable_priority,
    merge_variable_sources,
    resolve_from_environment,
    resolve_variables,
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
)
from .validation import (
    VariableError,
    check_circular_references,
    get_variable_usage_stats,
    validate_comprehensive,
    validate_variable_name,
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
    "merge_variable_sources",
    "get_variable_priority",
    "resolve_with_sources",
    # Context formatting
    "format_for_context",
    # Validation
    "validate_variables",
    "validate_comprehensive",
    "validate_variable_name",
    "check_circular_references",
    "get_variable_usage_stats",
    "VariableError",
    # Types
    "VariableContext",
    "ValidationResult",
]
