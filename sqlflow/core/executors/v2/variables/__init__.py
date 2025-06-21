"""Variables package for pure functional variable handling."""

from .substitution import (
    merge_variables,
    substitute_in_dict,
    substitute_in_list,
    substitute_in_step,
    substitute_variables,
    validate_variables,
)

__all__ = [
    "substitute_variables",
    "substitute_in_dict",
    "substitute_in_list",
    "substitute_in_step",
    "merge_variables",
    "validate_variables",
]
