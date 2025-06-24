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
    resolve_from_environment,
    resolve_variables,
)
from .substitution import (
    substitute_in_dict,
    substitute_in_list,
    substitute_simple_dollar,
    substitute_variables,
)
from .types import (
    ValidationResult,
    VariableContext,
)
from .validation import (
    VariableError,
    validate_variables,
)

__all__ = [
    # Core substitution functions
    "substitute_variables",
    "substitute_in_dict",
    "substitute_in_list",
    "substitute_simple_dollar",
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
