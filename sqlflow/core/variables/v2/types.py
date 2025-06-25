"""Type definitions for SQLFlow Variables V2

This module contains all type definitions, enums, and dataclasses used
by the V2 variables implementation.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class VariableContext(Enum):
    """Enumeration of variable formatting contexts."""

    TEXT = "text"
    SQL = "sql"
    AST = "ast"
    JSON = "json"


@dataclass(frozen=True)
class VariableSources:
    """Immutable variable sources with clear priority order.

    Following Raymond Hettinger's recommendation: use frozen dataclasses
    for better performance and clearer intent.
    """

    cli: Dict[str, Any] = field(default_factory=dict)
    profile: Dict[str, Any] = field(default_factory=dict)
    set: Dict[str, Any] = field(default_factory=dict)
    env: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of variable validation."""

    is_valid: bool
    missing_variables: List[str] = field(default_factory=list)
    invalid_syntax: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)


@dataclass
class VariableInfo:
    """Information about a variable found in text."""

    name: str
    default_value: Optional[str]
    start_pos: int
    end_pos: int
    full_match: str
