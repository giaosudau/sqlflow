"""Unified Variable Substitution Engine for SQLFlow.

This module provides the VariableSubstitutionEngine as the single unified interface
for all variable substitution operations across the SQLFlow system.

Phase 2 Architectural Cleanup: This eliminates the need for individual components
to implement their own variable substitution logic by providing a unified engine
with context-specific formatters.

Zen of Python:
- Simple is better than complex
- There should be one obvious way to do it
- Explicit is better than implicit
"""

import re
from typing import Any, Dict, Optional, Protocol

from sqlflow.core.variables.unified_parser import get_unified_parser
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class VariableFormatter(Protocol):
    """Protocol for context-specific variable formatters."""

    def format_value(self, value: Any, context: Dict[str, Any]) -> str:
        """Format a value for the specific context."""
        ...


class TextFormatter:
    """Plain text formatter for general use."""

    def format_value(self, value: Any, context: Dict[str, Any]) -> str:
        """Format value as plain text."""
        return str(value) if value is not None else ""


class SQLFormatter:
    """SQL formatter for database queries."""

    def format_value(self, value: Any, context: Dict[str, Any]) -> str:
        """Format value for SQL context with proper quoting."""
        if value is None:
            return "NULL"

        # Check if we're inside quotes to avoid double-quoting
        inside_quotes = context.get("inside_quotes", False)
        if inside_quotes:
            return str(value)

        # SQL formatting logic
        if isinstance(value, str):
            # Check if this looks like a complex SQL expression that should not be quoted
            if self._is_complex_sql_expression(value):
                return value

            # Check if this is a boolean-like string that should be unquoted
            if self._is_boolean_like_string(value):
                return value.lower()

            # Escape single quotes in the string
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return f"'{str(value)}'"

    def _is_complex_sql_expression(self, value: str) -> bool:
        """Check if a string looks like a complex SQL expression that shouldn't be quoted."""
        # Check for SQL list patterns like 'item1','item2'
        if "','" in value:
            return True

        # Check for SQL keywords or operators
        sql_patterns = [
            r"\bSELECT\b",
            r"\bFROM\b",
            r"\bWHERE\b",
            r"\bJOIN\b",
            r"\bUNION\b",
            r"\bGROUP BY\b",
            r"\bORDER BY\b",
            r"\bNULL\b",
            r"\bCASE\b",
            r"\bWHEN\b",
            r"\bTHEN\b",
            r"\bELSE\b",
            r"\bEND\b",
        ]

        for pattern in sql_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True

        return False

    def _is_boolean_like_string(self, value: str) -> bool:
        """Check if a string represents a boolean value that should be unquoted in SQL."""
        return value.lower() in ("true", "false")


class ASTFormatter:
    """AST formatter for Python AST evaluation."""

    def format_value(self, value: Any, context: Dict[str, Any]) -> str:
        """Format value for Python AST evaluation."""
        if value is None:
            return "None"

        # Check if we're inside quotes to avoid double-quoting
        inside_quotes = context.get("inside_quotes", False)

        # For AST evaluation, handle string values
        if isinstance(value, str):
            # Check if this looks like a complex SQL expression that should not be quoted
            if self._is_complex_sql_expression(value):
                return value

            # Check if this is a boolean-like string that should be converted to Python boolean
            if self._is_boolean_like_string(value):
                return "True" if value.lower() == "true" else "False"

            # Check if this is a numeric value that should not be quoted
            if self._is_numeric_string(value):
                return value

            if inside_quotes:
                # If already inside quotes, just return the value with escaping
                return value.replace("'", "\\'")
            else:
                # If not inside quotes, add quotes and escape
                escaped_value = value.replace("'", "\\'")
                return f"'{escaped_value}'"
        elif isinstance(value, bool):
            return "True" if value else "False"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            if inside_quotes:
                return str(value)
            else:
                return f"'{str(value)}'"

    def _is_complex_sql_expression(self, value: str) -> bool:
        """Check if a string looks like a complex SQL expression that shouldn't be quoted."""
        # Check for SQL list patterns like 'item1','item2'
        if "','" in value:
            return True

        # Check for SQL keywords or operators
        sql_patterns = [
            r"\bSELECT\b",
            r"\bFROM\b",
            r"\bWHERE\b",
            r"\bJOIN\b",
            r"\bUNION\b",
            r"\bGROUP BY\b",
            r"\bORDER BY\b",
            r"\bNULL\b",
            r"\bCASE\b",
            r"\bWHEN\b",
            r"\bTHEN\b",
            r"\bELSE\b",
            r"\bEND\b",
        ]

        for pattern in sql_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True

        return False

    def _is_boolean_like_string(self, value: str) -> bool:
        """Check if a string represents a boolean value that should be converted."""
        return value.lower() in ("true", "false")

    def _is_numeric_string(self, value: str) -> bool:
        """Check if a string represents a numeric value that should not be quoted."""
        try:
            # Try to convert to int or float
            if "." in value:
                float(value)
            else:
                int(value)
            return True
        except ValueError:
            return False


class VariableSubstitutionEngine:
    """Unified variable substitution engine for all SQLFlow components.

    This engine provides a single interface for variable substitution with
    context-specific formatting. It eliminates the need for individual components
    to implement their own variable substitution logic.

    Phase 2 Architectural Cleanup: This is the unified interface that all components
    will use, replacing individual component implementations.
    """

    def __init__(self, variables: Optional[Dict[str, Any]] = None):
        """Initialize the substitution engine.

        Args:
            variables: Dictionary of variable values
        """
        self.variables = variables or {}
        self.parser = get_unified_parser()

        # Context-specific formatters
        self.formatters = {
            "text": TextFormatter(),
            "sql": SQLFormatter(),
            "ast": ASTFormatter(),
        }

        logger.debug(
            f"VariableSubstitutionEngine initialized with {len(self.variables)} variables"
        )

    def substitute(
        self, template: str, context: str = "text", context_detection: bool = True
    ) -> str:
        """Substitute variables in a template string.

        Args:
            template: Template string with variable placeholders
            context: Context type ('text', 'sql', 'ast')
            context_detection: Whether to detect context automatically

        Returns:
            Template with variables substituted
        """
        if not template:
            return template

        parse_result = self.parser.parse(template)

        if not parse_result.has_variables:
            return template

        logger.debug(
            f"Substituting {len(parse_result.expressions)} variables in {context} context"
        )

        # Get appropriate formatter
        formatter = self.formatters.get(context, self.formatters["text"])

        new_parts = []
        last_end = 0

        for expr in parse_result.expressions:
            # Append text between variables
            new_parts.append(template[last_end : expr.span[0]])

            # Determine context for this specific variable
            var_context = self._build_context(template, expr, context_detection)

            # Get variable value
            if expr.variable_name in self.variables:
                value = self.variables[expr.variable_name]
                formatted_value = formatter.format_value(value, var_context)
            elif expr.default_value is not None:
                formatted_value = formatter.format_value(
                    expr.default_value, var_context
                )
            else:
                # Handle missing variables based on context
                if context == "sql":
                    formatted_value = "NULL"
                elif context == "ast":
                    formatted_value = "None"
                else:
                    logger.warning(
                        f"Variable '{expr.variable_name}' not found and no default provided"
                    )
                    new_parts.append(expr.original_match)
                    last_end = expr.span[1]
                    continue

            new_parts.append(formatted_value)
            last_end = expr.span[1]

        # Append remaining text
        new_parts.append(template[last_end:])

        result = "".join(new_parts)
        logger.debug(f"Variable substitution completed, result length: {len(result)}")

        return result

    def _build_context(
        self, template: str, expr, context_detection: bool
    ) -> Dict[str, Any]:
        """Build context information for variable formatting."""
        context = {}

        if context_detection:
            # Detect if variable is inside quotes
            context["inside_quotes"] = self._is_inside_quotes(
                template, expr.span[0], expr.span[1]
            )

        return context

    def _is_inside_quotes(self, template: str, start_pos: int, end_pos: int) -> bool:
        """Check if a variable is inside quoted string context."""
        # Simplified quote detection for better maintainability
        window_size = 100
        search_start = max(0, start_pos - window_size)

        window = template[search_start:start_pos]

        # Count quotes before the variable (simple approach)
        single_quotes = window.count("'")
        double_quotes = window.count('"')

        # If odd number of quotes, we're likely inside quotes
        return (single_quotes % 2 == 1) or (double_quotes % 2 == 1)

    def register_variable(self, name: str, value: Any) -> None:
        """Register a variable with the engine."""
        self.variables[name] = value
        logger.debug(
            f"Registered variable '{name}' with value type: {type(value).__name__}"
        )

    def register_variables(self, variables: Dict[str, Any]) -> None:
        """Register multiple variables with the engine."""
        self.variables.update(variables)
        logger.debug(f"Registered {len(variables)} variables")

    def has_variable(self, name: str) -> bool:
        """Check if a variable is registered."""
        return name in self.variables

    def get_variable(self, name: str) -> Any:
        """Get a variable value."""
        return self.variables.get(name)

    def clear_cache(self) -> None:
        """Clear the parser cache."""
        self.parser.clear_cache()
        logger.debug("Parser cache cleared")

    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics."""
        return {
            "variable_count": len(self.variables),
            "parser_stats": self.parser.get_cache_stats(),
        }


# Global engine instance for system-wide use
_engine_instance: Optional[VariableSubstitutionEngine] = None


def get_substitution_engine(
    variables: Optional[Dict[str, Any]] = None,
) -> VariableSubstitutionEngine:
    """Get the global substitution engine instance.

    Args:
        variables: Variables to register (only used for first call)

    Returns:
        Global VariableSubstitutionEngine instance
    """
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = VariableSubstitutionEngine(variables)
        logger.debug("Global VariableSubstitutionEngine instance created")
    elif variables:
        _engine_instance.register_variables(variables)
    return _engine_instance


def reset_substitution_engine() -> None:
    """Reset the global substitution engine instance (for testing)."""
    global _engine_instance
    _engine_instance = None
    logger.debug("Global VariableSubstitutionEngine instance reset")
