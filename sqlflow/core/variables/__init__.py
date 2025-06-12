"""Unified variable management system for SQLFlow.

This package provides a single, consistent interface for variable substitution
across the entire codebase, eliminating duplication and ensuring Zen of Python
principles are followed.

Following the completion of Phase 4, this module now provides only the new
unified VariableManager system, having removed all legacy implementations.
"""

from .facade import (
    LegacyVariableSupport,
    substitute_variables_unified,
    validate_variables_unified,
)

# Import new unified system
from .manager import ValidationResult, VariableConfig, VariableManager
from .validator import VariableValidator

# Export only the new unified system (legacy classes removed in Phase 4)
__all__ = [
    # New unified system
    "VariableManager",
    "VariableConfig",
    "ValidationResult",
    "VariableValidator",
    # Migration facade (for transition compatibility)
    "LegacyVariableSupport",
    "substitute_variables_unified",
    "validate_variables_unified",
]
