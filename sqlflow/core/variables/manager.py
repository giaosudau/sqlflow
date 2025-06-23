"""Variable management for SQLFlow.

This module provides the VariableManager class as the single source of truth
for variable substitution across the entire codebase.

Following Zen of Python principles:
- Simple is better than complex
- There should be one obvious way to do it
- Don't repeat yourself
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of variable validation."""

    is_valid: bool
    missing_variables: List[str] = field(default_factory=list)
    invalid_defaults: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    context_locations: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class VariableConfig:
    """Configuration for variable management with priority ordering.

    Priority order (highest to lowest):
    1. CLI variables (--vars)
    2. Profile variables (from profiles/*.yml)
    3. SET variables (from SET statements in pipeline)
    4. Environment variables (from os.environ)
    """

    cli_variables: Dict[str, Any] = field(default_factory=dict)
    profile_variables: Dict[str, Any] = field(default_factory=dict)
    set_variables: Dict[str, Any] = field(default_factory=dict)
    env_variables: Dict[str, Any] = field(default_factory=dict)

    def resolve_priority(self) -> Dict[str, Any]:
        """Resolve variables according to priority order.

        Returns:
            Dictionary with variables resolved by priority
        """
        result = {}
        # Apply in priority order (lowest to highest)
        result.update(self.env_variables or {})
        result.update(self.set_variables or {})
        result.update(self.profile_variables or {})
        result.update(self.cli_variables or {})
        return result


class VariableManager:
    """Variable manager with priority-based resolution and efficient substitution.

    Manages variable resolution from multiple sources with defined priority order.
    Provides context-aware variable substitution with comprehensive error handling.

    Priority order (highest to lowest):
    1. CLI variables (--vars command line arguments)
    2. Profile variables (profile.yml files)
    3. SET variables (SET statements in pipelines)
    4. Environment variables (os.environ)
    """

    # Cache for compiled regex patterns to improve performance
    _REGEX_CACHE = {}

    def __init__(self, config: Optional[VariableConfig] = None):
        """Initialize VariableManager with configuration.

        Args:
            config: Variable configuration with sources and priorities

        Raises:
            TypeError: If config is not VariableConfig instance when provided
        """
        if config is not None and not isinstance(config, VariableConfig):
            raise TypeError("config must be a VariableConfig instance")

        self._config = config or VariableConfig()
        self._resolved_cache = None  # Cache for resolved variables

        # Initialize error handler with default strategy
        from sqlflow.core.variables.error_handling import ErrorHandler, ErrorStrategy

        self._error_handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)
        self._error_handler.set_total_variables(self._count_total_variables())
        self._error_handler.set_context("VariableManager")

        # Performance optimization: pre-calculate total count for logging
        total_vars = self._count_total_variables()
        if total_vars > 0:
            logger.debug(f"VariableManager initialized with {total_vars} variables")
        else:
            logger.debug("VariableManager initialized with no variables")

    def _count_total_variables(self) -> int:
        """Count total variables across all sources for logging."""
        return (
            len(self._config.cli_variables or {})
            + len(self._config.profile_variables or {})
            + len(self._config.set_variables or {})
            + len(self._config.env_variables or {})
        )

    def _get_resolved_variables(self) -> Dict[str, Any]:
        """Get resolved variables with lazy initialization and caching.

        Returns:
            Dictionary of all variables resolved according to priority
        """
        if self._resolved_cache is None:
            resolved_from_config = self._config.resolve_priority()
            # Add environment variables automatically (lowest priority)
            env_vars = dict(os.environ)
            # Environment variables have lowest priority
            combined = {}
            combined.update(env_vars)
            combined.update(resolved_from_config)
            self._resolved_cache = combined

            logger.debug(
                f"Cached resolved variables: {len(self._resolved_cache)} total"
            )

        return self._resolved_cache

    def get_resolved_variables(self) -> Dict[str, Any]:
        """Get all resolved variables.

        Public method for accessing resolved variables.
        Used by planner and other components that need the full variable set.

        Returns:
            Dictionary of all resolved variables
        """
        return self._get_resolved_variables()

    def substitute(self, data: Any) -> Any:
        """Substitute variables in any data structure.

        This method handles strings, dictionaries, lists, and nested structures.
        It performs pure variable substitution without any formatting.

        Args:
            data: The data structure to perform variable substitution on

        Returns:
            The data structure with variables substituted
        """
        if data is None:
            return data

        resolved_vars = self._get_resolved_variables()

        if isinstance(data, str):
            return self._substitute_string(data, resolved_vars)
        elif isinstance(data, dict):
            return self._substitute_dict(data, resolved_vars)
        elif isinstance(data, list):
            return self._substitute_list(data, resolved_vars)
        else:
            # For other types (int, float, bool, etc.), return as-is
            return data

    def _substitute_string(self, text: str, variables: Dict[str, Any]) -> str:
        """Substitute variables in a string.

        Phase 2 Architectural Cleanup: Now uses the unified VariableSubstitutionEngine
        with text context, while preserving VariableManager's specific context detection.

        Args:
            text: String with variable placeholders
            variables: Dictionary of variable values

        Returns:
            String with variables substituted
        """
        from sqlflow.core.variables.unified_parser import get_unified_parser

        # Quick check if there are variables to substitute
        parser = get_unified_parser()
        parse_result = parser.parse(text)

        if not parse_result.has_variables:
            return text

        # Use the unified engine with text context, but apply our specific formatting
        new_parts = []
        last_end = 0

        for expr in parse_result.expressions:
            # Append the text between the last match and this one
            new_parts.append(text[last_end : expr.span[0]])

            if expr.variable_name in variables:
                value = variables[expr.variable_name]
                # Keep our specialized context detection for backward compatibility
                formatted_value = self._format_value_for_context(
                    value, text, expr.span[0], expr.span[1]
                )
            elif expr.default_value is not None:
                # Keep our specialized context detection for backward compatibility
                formatted_value = self._format_value_for_context(
                    expr.default_value, text, expr.span[0], expr.span[1]
                )
            else:
                # Variable not found and no default - reduce logging overhead for performance
                # Only log in debug mode to avoid hot path logging overhead
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Variable '{expr.variable_name}' not found and no default provided"
                    )
                new_parts.append(expr.original_match)
                last_end = expr.span[1]
                continue

            # Append the substituted value
            new_parts.append(formatted_value)
            last_end = expr.span[1]

        # Append the rest of the string after the last match
        new_parts.append(text[last_end:])

        return "".join(new_parts)

    def _format_value_for_context(
        self, value: Any, text: str, start_pos: int, end_pos: int
    ) -> str:
        """Format a value appropriately based on the surrounding context.

        This method detects conditional contexts and applies appropriate formatting
        to ensure valid syntax in conditional expressions.

        Args:
            value: The value to format
            text: The full text containing the variable
            start_pos: Start position of the variable in the text
            end_pos: End position of the variable in the text

        Returns:
            Appropriately formatted value
        """
        # Convert value to string first
        str_value = str(value) if value is not None else ""

        # Performance optimization: Use cached regex patterns
        if "if_statement" not in self._REGEX_CACHE:
            import re

            self._REGEX_CACHE["if_statement"] = re.compile(r"\bIF\b.*$", re.IGNORECASE)
            self._REGEX_CACHE["conditional_comparison"] = re.compile(r"\s*(==|!=)\s")

        # Only apply special formatting for conditional expressions in IF statements
        # Check for very specific IF statement conditional patterns that need quoting
        context_before = text[:start_pos].strip()
        context_after = text[end_pos:].strip()

        # Check if we're in an IF statement conditional (not SQL WHERE clause)
        is_if_statement = self._REGEX_CACHE["if_statement"].search(context_before)

        # Look for conditional comparison operators only in IF context
        is_conditional_comparison = is_if_statement and self._REGEX_CACHE[
            "conditional_comparison"
        ].match(context_after)

        # Only quote if we're in an IF conditional comparison context AND it's a string value
        if is_conditional_comparison and isinstance(value, str):
            # Check if it's already quoted
            if (str_value.startswith('"') and str_value.endswith('"')) or (
                str_value.startswith("'") and str_value.endswith("'")
            ):
                return str_value

            # Check if it's a number or boolean
            if str_value.lower() in ("true", "false") or str_value.isdigit():
                return str_value

            # For string identifiers in conditional context, add quotes
            return f"'{str_value}'"

        return str_value

    def _substitute_dict(
        self, data: Dict[str, Any], variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Substitute variables in a dictionary recursively."""
        result = {}
        for key, value in data.items():
            # Substitute in both key and value
            new_key = self.substitute(key) if isinstance(key, str) else key
            new_value = self.substitute(value)
            result[new_key] = new_value
        return result

    def _substitute_list(self, data: List[Any], variables: Dict[str, Any]) -> List[Any]:
        """Substitute variables in a list recursively."""
        return [self.substitute(item) for item in data]

    def has_variable(self, variable_name: str) -> bool:
        """Check if a variable exists in any of the variable sources.

        Args:
            variable_name: Name of the variable to check

        Returns:
            True if the variable exists in any source, False otherwise
        """
        resolved_vars = self._get_resolved_variables()
        return variable_name in resolved_vars

    def validate(self, content: str) -> ValidationResult:
        """Validate variable usage in content.

        Args:
            content: Content to validate for variable references

        Returns:
            ValidationResult with validation details
        """
        from sqlflow.core.variables.parser import StandardVariableParser

        missing_variables = []
        resolved_vars = self._get_resolved_variables()

        parse_result = StandardVariableParser.find_variables(content)
        for expr in parse_result.expressions:
            if expr.variable_name not in resolved_vars and expr.default_value is None:
                missing_variables.append(expr.variable_name)

        return ValidationResult(
            is_valid=len(missing_variables) == 0,
            missing_variables=missing_variables,
        )

    # ========================================
    # FACTORY METHODS - ZEN OF PYTHON SOLUTION
    # ========================================

    @classmethod
    def from_cli_variables(cls, variables: Dict[str, Any]) -> "VariableManager":
        """Create VariableManager from CLI variables only.

        ZEN OF PYTHON: Simple is better than complex.
        One-line factory method replaces 3-line boilerplate.

        Args:
            variables: CLI variables dictionary

        Returns:
            Configured VariableManager instance
        """
        config = VariableConfig(cli_variables=variables)
        return cls(config)

    @classmethod
    def from_profile_variables(cls, variables: Dict[str, Any]) -> "VariableManager":
        """Create VariableManager from profile variables only.

        Args:
            variables: Profile variables dictionary

        Returns:
            Configured VariableManager instance
        """
        config = VariableConfig(profile_variables=variables)
        return cls(config)

    @classmethod
    def from_mixed_sources(
        cls,
        cli_variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
        set_variables: Optional[Dict[str, Any]] = None,
        env_variables: Optional[Dict[str, Any]] = None,
    ) -> "VariableManager":
        """Create VariableManager from multiple variable sources.

        Args:
            cli_variables: CLI variables (highest priority)
            profile_variables: Profile variables
            set_variables: SET statement variables
            env_variables: Environment variables (lowest priority)

        Returns:
            Configured VariableManager instance
        """
        config = VariableConfig(
            cli_variables=cli_variables or {},
            profile_variables=profile_variables or {},
            set_variables=set_variables or {},
            env_variables=env_variables or {},
        )
        return cls(config)


# ========================================
# UTILITY FUNCTIONS - ZEN OF PYTHON SOLUTION
# ========================================


def substitute_variables(data: Any, variables: Dict[str, Any]) -> Any:
    """Utility function for simple variable substitution.

    ZEN OF PYTHON: Simple is better than complex.
    One-line utility replaces 3-line boilerplate everywhere.

    Args:
        data: Data to substitute variables in
        variables: Variables dictionary

    Returns:
        Data with variables substituted
    """
    return VariableManager.from_cli_variables(variables).substitute(data)


def validate_variables(content: str, variables: Dict[str, Any]) -> ValidationResult:
    """Utility function for simple variable validation.

    Args:
        content: Content to validate
        variables: Available variables

    Returns:
        ValidationResult
    """
    return VariableManager.from_cli_variables(variables).validate(content)
