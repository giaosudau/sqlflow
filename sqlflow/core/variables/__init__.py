"""SQLFlow Variables Module - V2 Implementation

Pure functional approach following Zen of Python principles.
All functions are pure with no side effects.

Following Raymond Hettinger and Guido van Rossum's recommendations:
- Simple is better than complex
- Explicit is better than implicit
- Use built-in features for performance
- Clear, readable APIs
"""

from .v2 import (
    ValidationResult,
    VariableContext,
    VariableError,
    VariableInfo,
    VariableSources,
    find_variables,
    format_for_context,
    get_variable_priority,
    merge_variable_sources,
    resolve_from_environment,
)
from .v2 import (
    resolve_variables as v2_resolve_variables,  # Core substitution functions; Resolution functions; Validation functions; Formatting functions; Utility functions; Types and exceptions
)
from .v2 import (
    resolve_variables_legacy,
    resolve_with_sources,
    substitute_any,
    substitute_in_dict,
    substitute_in_list,
    substitute_simple_dollar,
    substitute_variables,
    substitute_variables_for_sql,
    validate_variable_name,
    validate_variables,
    validate_variables_with_details,
)


# Backward compatibility aliases for existing code
def resolve_variables_backward_compat(
    cli_vars=None,
    profile_vars=None,
    set_vars=None,
    env_vars=None,
):
    """Backward compatibility function for existing code."""
    return resolve_variables_legacy(cli_vars, profile_vars, set_vars, env_vars)


# Legacy-compatible resolve_variables for backward compatibility
import warnings


def resolve_variables(*args, **kwargs):
    """Backward compatible resolve_variables function.
    Accepts either a VariableSources object (new API) or multiple dicts (legacy API).
    """
    if args and isinstance(args[0], VariableSources):
        return v2_resolve_variables(args[0])
    # If called with multiple dicts, use legacy
    if len(args) > 0 or kwargs:
        warnings.warn(
            "Using legacy resolve_variables signature. Please migrate to VariableSources.",
            DeprecationWarning,
        )
        return resolve_variables_legacy(*args, **kwargs)
    raise TypeError("Invalid arguments for resolve_variables")


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
    # Backward compatibility
    "resolve_variables_backward_compat",
]
