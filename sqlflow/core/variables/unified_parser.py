"""Unified Variable Parser for SQLFlow.

This module provides the single source of truth for all variable parsing in SQLFlow.
Eliminates regex pattern duplication and provides comprehensive parsing capabilities.

Zen of Python: "There should be one obvious way to do it."
"""

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class VariableExpression:
    """Represents a parsed variable expression with comprehensive metadata."""

    variable_name: str
    default_value: Optional[str]
    original_match: str
    span: Tuple[int, int]
    line_number: int = 1
    column_number: int = 1


@dataclass
class ParseResult:
    """Result of variable parsing with performance metrics and comprehensive information."""

    expressions: List[VariableExpression]
    has_variables: bool
    parse_time_ms: float
    unique_variables: Set[str]
    total_variable_count: int


class UnifiedVariableParser:
    """Single source of truth for all variable parsing in SQLFlow.

    This class eliminates all regex pattern duplication across the system and provides
    the definitive implementation for variable parsing.

    Supports patterns:
    - ${var}
    - ${var|default}
    - ${var|"quoted default"}
    - ${var|'quoted default'}

    Performance features:
    - Compiled regex pattern for maximum efficiency
    - Optional caching for repeated parsing operations
    - Comprehensive error metadata with line/column information
    """

    # THE definitive pattern for the entire SQLFlow system
    # This is the single source of truth that replaces all other patterns
    _VARIABLE_PATTERN = re.compile(
        r"\$\{([^}|]+)(?:\|([^}]+))?\}", re.MULTILINE | re.DOTALL
    )

    def __init__(self):
        """Initialize parser with caching capability."""
        self._parse_cache: Dict[str, ParseResult] = {}

    @classmethod
    def get_pattern(cls) -> re.Pattern:
        """Get the unified regex pattern for the entire system.

        This method provides access to the single definitive pattern that should
        be used throughout SQLFlow, eliminating all pattern duplication.

        Returns:
            Compiled regex pattern for variable matching
        """
        return cls._VARIABLE_PATTERN

    def parse(self, text: str, use_cache: bool = True) -> ParseResult:
        """Parse text for variable expressions with comprehensive analysis.

        Args:
            text: Text to parse for variables
            use_cache: Whether to use parsing cache for performance optimization

        Returns:
            ParseResult with comprehensive parsing information including:
            - All variable expressions found
            - Performance metrics
            - Unique variable count
            - Line/column information for error reporting

        Raises:
            TypeError: If text is None
        """
        if text is None:
            raise TypeError("Cannot parse None as text")

        if use_cache and text in self._parse_cache:
            return self._parse_cache[text]

        start_time = time.perf_counter()

        if not text:
            result = ParseResult(
                expressions=[],
                has_variables=False,
                parse_time_ms=0.0,
                unique_variables=set(),
                total_variable_count=0,
            )
            if use_cache:
                self._parse_cache[text] = result
            return result

        expressions = []
        unique_vars = set()

        # Calculate line starts for accurate line/column reporting
        line_starts = self._calculate_line_starts(text)

        for match in self._VARIABLE_PATTERN.finditer(text):
            expr = self._parse_expression(match, line_starts)
            if expr.variable_name.strip():  # Only include non-empty variable names
                expressions.append(expr)
                unique_vars.add(expr.variable_name)

        parse_time = (time.perf_counter() - start_time) * 1000

        result = ParseResult(
            expressions=expressions,
            has_variables=len(expressions) > 0,
            parse_time_ms=parse_time,
            unique_variables=unique_vars,
            total_variable_count=len(expressions),
        )

        if use_cache:
            self._parse_cache[text] = result

        logger.debug(
            f"Parsed {len(expressions)} variables in {parse_time:.2f}ms: {list(unique_vars)}"
        )

        return result

    def _calculate_line_starts(self, text: str) -> List[int]:
        """Calculate starting positions of each line for accurate error reporting."""
        lines = text.split("\n")
        line_starts = [0]
        for line in lines[:-1]:
            line_starts.append(line_starts[-1] + len(line) + 1)
        return line_starts

    def _parse_expression(
        self, match: re.Match, line_starts: List[int]
    ) -> VariableExpression:
        """Parse a single variable expression with comprehensive metadata."""
        var_name = match.group(1).strip()
        default = match.group(2).strip() if match.group(2) else None

        # Process default value intelligently
        if default:
            default = self._process_default_value(default)

        # Calculate line and column for comprehensive error reporting
        start_pos = match.start()
        line_num = self._find_line_number(start_pos, line_starts)
        col_num = start_pos - line_starts[line_num - 1] + 1

        return VariableExpression(
            variable_name=var_name,
            default_value=default,
            original_match=match.group(0),
            span=match.span(),
            line_number=line_num,
            column_number=col_num,
        )

    def _process_default_value(self, default: str) -> str:
        """Process default value with intelligent quote handling."""
        # Handle quoted defaults intelligently
        if self._is_quoted(default) and self._is_simple_string_value(default):
            # Only strip quotes from simple string values, not complex expressions
            return default[1:-1]
        return default

    def _is_quoted(self, value: str) -> bool:
        """Check if value is properly quoted."""
        return (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        )

    def _is_simple_string_value(self, value: str) -> bool:
        """Check if quoted value is a simple string (not a complex expression)."""
        if not self._is_quoted(value):
            return False

        inner_value = value[1:-1]

        # Check for comma-separated values (like 'active','pending')
        if "','" in value:
            return False

        # Consider it complex if it contains SQL-like patterns or special characters
        complex_patterns = [
            r"\bSELECT\b",
            r"\bFROM\b",
            r"\bWHERE\b",  # SQL keywords
            r"\$\{",
            r"\}",  # Nested variables
            r"\|\|",  # SQL concatenation
            r"\bNULL\b",
            r"\bTRUE\b",
            r"\bFALSE\b",  # SQL literals
        ]

        for pattern in complex_patterns:
            if re.search(pattern, inner_value, re.IGNORECASE):
                return False

        return True

    def _find_line_number(self, position: int, line_starts: List[int]) -> int:
        """Find line number for a given character position."""
        for i, start_pos in enumerate(line_starts):
            if i == len(line_starts) - 1 or position < line_starts[i + 1]:
                return i + 1
        return len(line_starts)

    def clear_cache(self) -> None:
        """Clear the parsing cache for memory management."""
        self._parse_cache.clear()
        logger.debug("Unified parser cache cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for performance monitoring."""
        return {
            "cache_size": len(self._parse_cache),
            "cache_keys": list(self._parse_cache.keys())[:5],  # Show first 5 keys
        }


# Global singleton instance for system-wide use
_parser_instance: Optional[UnifiedVariableParser] = None


def get_unified_parser() -> UnifiedVariableParser:
    """Get the global unified parser instance.

    This provides a singleton pattern for the unified parser to ensure
    consistent behavior and shared caching across the entire system.

    Returns:
        Global UnifiedVariableParser instance
    """
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = UnifiedVariableParser()
        logger.debug("Global unified parser instance created")
    return _parser_instance


def get_unified_pattern() -> re.Pattern:
    """Get the unified regex pattern for the entire system.

    This is a convenience function that provides direct access to the
    unified pattern without needing to create a parser instance.

    Returns:
        The single definitive regex pattern for SQLFlow variable matching
    """
    return UnifiedVariableParser.get_pattern()
