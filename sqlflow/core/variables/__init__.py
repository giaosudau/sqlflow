"""Unified Variable Management System for SQLFlow.

This module provides the VariableManager class as the single source of truth
for variable substitution across the entire codebase.

Following Zen of Python principles:
- Simple is better than complex
- There should be one obvious way to do it
- Explicit is better than implicit
"""

from .manager import (
    VariableConfig,
    VariableManager,
    substitute_variables,
    validate_variables,
)
from .validator import VariableValidator

__all__ = [
    "VariableManager",
    "VariableConfig",
    "VariableValidator",
    "substitute_variables",
    "validate_variables",
]
