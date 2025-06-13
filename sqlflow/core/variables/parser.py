"""Unified variable parser for SQLFlow.

This module provides the single source of truth for variable parsing across
the entire codebase, eliminating duplication and ensuring consistent behavior.

Following Zen of Python principles:
- Simple is better than complex
- There should be one obvious way to do it
- Explicit is better than implicit
"""

import re
from dataclasses import dataclass
from typing import List, NamedTuple, Optional, Tuple

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class VariableExpression(NamedTuple):
    """Parsed variable expression."""

    variable_name: str
    default_value: Optional[str]
    original_match: str
    span: Tuple[int, int]


@dataclass
class ParseResult:
    """Result of variable parsing."""

    expressions: List[VariableExpression]
    has_variables: bool


class StandardVariableParser:
    """Single source of truth for variable parsing.

    Zen of Python: There should be one obvious way to do it.
    """

    # THE definitive pattern for the entire system
    VARIABLE_PATTERN = re.compile(r"\$\{([^}|]+)(?:\|([^}]+))?\}")

    @classmethod
    def parse_expression(cls, match: re.Match) -> VariableExpression:
        """Parse a single variable expression."""
        var_name = match.group(1).strip()
        default = match.group(2).strip() if match.group(2) else None

        # Handle quoted defaults consistently
        if default and cls._is_quoted(default):
            default = default[1:-1]  # Remove quotes

        return VariableExpression(
            variable_name=var_name,
            default_value=default,
            original_match=match.group(0),
            span=match.span(),
        )

    @classmethod
    def find_variables(cls, text: str) -> ParseResult:
        """Find all variables in text."""
        if not text:
            return ParseResult(expressions=[], has_variables=False)

        matches = cls.VARIABLE_PATTERN.finditer(text)
        expressions = []

        for match in matches:
            expr = cls.parse_expression(match)
            # Only include expressions with non-empty variable names
            if expr.variable_name.strip():
                expressions.append(expr)

        return ParseResult(expressions=expressions, has_variables=len(expressions) > 0)

    @classmethod
    def _is_quoted(cls, value: str) -> bool:
        """Check if value is quoted."""
        return (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        )
