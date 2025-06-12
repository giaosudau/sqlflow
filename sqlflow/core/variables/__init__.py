"""Unified variable management system for SQLFlow.

This package provides a single, consistent interface for variable substitution
across the entire codebase, eliminating duplication and ensuring Zen of Python
principles are followed.

This module maintains backward compatibility by exporting the existing
VariableContext and VariableSubstitutor classes, while introducing the new
unified VariableManager for future development.
"""

# Import legacy classes for backward compatibility
from .legacy import VariableContext, VariableSubstitutor

# Import new unified system
from .manager import ValidationResult, VariableConfig, VariableManager
from .validator import VariableValidator

# Export everything for backward compatibility and new functionality
__all__ = [
    # Legacy classes (backward compatibility)
    "VariableContext",
    "VariableSubstitutor",
    # New unified system
    "VariableManager",
    "VariableConfig",
    "ValidationResult",
    "VariableValidator",
]
