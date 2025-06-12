"""Unified Variable Management System for SQLFlow.

This module provides the VariableManager class, which serves as the single
source of truth for variable substitution across the entire codebase.
It follows Zen of Python principles: Simple, Readable, Explicit.

The VariableManager provides its own robust implementation for
variable substitution with clear priority ordering and validation.
"""

import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of variable validation with detailed context.

    Following Zen of Python: Simple is better than complex.
    All validation information in one clear structure.
    """

    is_valid: bool
    missing_variables: List[str]
    invalid_defaults: List[str]
    warnings: List[str]
    context_locations: Dict[str, List[str]]


@dataclass
class VariableConfig:
    """Configuration for variable resolution with clear priority order.

    Following Zen of Python: Explicit is better than implicit.
    Priority order is clearly documented and enforced.

    Priority Order (highest to lowest):
    1. CLI variables (cli_variables)
    2. Profile variables (profile_variables)
    3. SET variables (set_variables)
    4. Environment variables (env_variables)
    """

    cli_variables: Optional[Dict[str, Any]] = None
    profile_variables: Optional[Dict[str, Any]] = None
    set_variables: Optional[Dict[str, Any]] = None
    env_variables: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Initialize empty dictionaries if None provided."""
        self.cli_variables = self.cli_variables or {}
        self.profile_variables = self.profile_variables or {}
        self.set_variables = self.set_variables or {}
        self.env_variables = self.env_variables or {}

    def resolve_priority(self) -> Dict[str, Any]:
        """Resolve variables according to priority order.

        Following Zen of Python: There should be one obvious way to do it.
        Single method that implements the priority resolution clearly.

        Returns:
            Dictionary with variables resolved according to priority
        """
        result = {}
        # Apply in reverse priority order, so higher priority overwrites
        result.update(self.env_variables)
        result.update(self.set_variables)
        result.update(self.profile_variables)
        result.update(self.cli_variables)

        logger.debug(f"Resolved {len(result)} variables with priority order")
        return result


class IVariableManager(ABC):
    """Interface for variable management.

    Following Zen of Python: Simple is better than complex.
    Clean interface with minimal, focused methods.
    """

    @abstractmethod
    def substitute(self, data: Any) -> Any:
        """Substitute variables in any data structure.

        Args:
            data: String, dict, list, or any data containing variables

        Returns:
            Data with variables substituted
        """

    @abstractmethod
    def validate(self, content: str) -> ValidationResult:
        """Validate variable usage in content.

        Args:
            content: Content to validate for variable usage

        Returns:
            ValidationResult with detailed information
        """


class VariableManager(IVariableManager):
    """Unified variable manager for SQLFlow.

    Following Zen of Python principles:
    - Simple is better than complex
    - Readability counts
    - There should be one obvious way to do it

    This class provides a single, consistent interface for all variable
    operations with its own implementation, no longer depending on legacy systems.
    """

    # Compile regex patterns once for better performance
    VARIABLE_PATTERN = re.compile(r"\$\{([^}]+)\}")
    VARIABLE_WITH_DEFAULT_PATTERN = re.compile(r"\$\{([^|}]+)\|([^}]+)\}")

    def __init__(self, config: Optional[VariableConfig] = None):
        """Initialize the variable manager with configuration.

        Args:
            config: Variable configuration with priority-ordered variables
        """
        self._config = config or VariableConfig()
        self._resolved_variables = None  # Lazy initialization

        logger.debug(
            f"VariableManager initialized with {self._count_total_variables()} variables"
        )

    def _get_resolved_variables(self) -> Dict[str, Any]:
        """Get resolved variables with lazy initialization.

        Returns:
            Dictionary of all variables resolved according to priority
        """
        if self._resolved_variables is None:
            self._resolved_variables = self._config.resolve_priority()
            # Add environment variables automatically
            env_vars = dict(os.environ)
            # Environment variables have lowest priority
            combined = {}
            combined.update(env_vars)
            combined.update(self._resolved_variables)
            self._resolved_variables = combined

        return self._resolved_variables

    def substitute(self, data: Any) -> Any:
        """Substitute variables in any data structure.

        Following Zen of Python: Simple is better than complex.
        Single method handles all data types transparently.

        Args:
            data: String, dict, list, or any data containing variables

        Returns:
            Data with variables substituted
        """
        if data is None:
            return None

        logger.debug(f"Substituting variables in {type(data).__name__}")

        if isinstance(data, str):
            result = self._substitute_string(data)
        elif isinstance(data, dict):
            result = self._substitute_dict(data)
        elif isinstance(data, list):
            result = self._substitute_list(data)
        else:
            result = data

        logger.debug("Variable substitution completed")
        return result

    def _substitute_string(self, text: str) -> str:
        """Substitute variables in a string.

        Args:
            text: String containing variable references

        Returns:
            String with variables substituted
        """
        variables = self._get_resolved_variables()

        def replace_variable(match: re.Match) -> str:
            var_expr = match.group(1)

            # Handle expressions with defaults: ${var|default}
            if "|" in var_expr:
                var_name, default_value = var_expr.split("|", 1)
                var_name = var_name.strip()
                default_value = default_value.strip()

                # Remove quotes from default value if present
                default_value = self._unquote_value(default_value)

                # Check variables
                if var_name in variables:
                    value = variables[var_name]
                    logger.debug(f"Using variable ${{{var_name}}} = '{value}'")
                    return str(value)
                else:
                    logger.debug(
                        f"Using default value '{default_value}' for variable ${{{var_name}}}"
                    )
                    return default_value
            else:
                # Simple variable reference: ${var}
                var_name = var_expr.strip()
                if var_name in variables:
                    value = variables[var_name]
                    logger.debug(f"Using variable ${{{var_name}}} = '{value}'")
                    return str(value)
                else:
                    logger.warning(
                        f"Variable '{var_name}' not found and no default provided"
                    )
                    return match.group(0)  # Keep original text

        result = self.VARIABLE_PATTERN.sub(replace_variable, text)
        return result

    def _substitute_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute variables in a dictionary."""
        return {key: self.substitute(value) for key, value in data_dict.items()}

    def _substitute_list(self, data_list: List[Any]) -> List[Any]:
        """Substitute variables in a list."""
        return [self.substitute(item) for item in data_list]

    def _unquote_value(self, value: str) -> str:
        """Remove quotes from a value if present."""
        if len(value) >= 2:
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                return value[1:-1]
        return value

    def validate(self, content: str) -> ValidationResult:
        """Validate variable usage in content.

        Following Zen of Python: Errors should never pass silently.
        Comprehensive validation with detailed error information.

        Args:
            content: Content to validate for variable usage

        Returns:
            ValidationResult with detailed validation information
        """
        if not content:
            return ValidationResult(
                is_valid=True,
                missing_variables=[],
                invalid_defaults=[],
                warnings=[],
                context_locations={},
            )

        logger.debug(f"Validating variable usage in content ({len(content)} chars)")

        missing = self._validate_required_variables(content)

        # Create validation result
        result = ValidationResult(
            is_valid=len(missing) == 0,
            missing_variables=missing,
            invalid_defaults=[],  # TODO: Implement invalid defaults detection
            warnings=[],  # TODO: Implement warnings
            context_locations={},  # TODO: Implement context locations
        )

        logger.debug(f"Validation completed: {'PASS' if result.is_valid else 'FAIL'}")
        if not result.is_valid:
            logger.debug(f"Missing variables: {', '.join(result.missing_variables)}")

        return result

    def _validate_required_variables(self, content: str) -> List[str]:
        """Find missing required variables in content.

        Args:
            content: Content to check for missing variables

        Returns:
            List of missing variable names
        """
        variables = self._get_resolved_variables()
        missing = []

        for match in self.VARIABLE_PATTERN.finditer(content):
            var_expr = match.group(1)

            # Handle expressions with defaults: ${var|default}
            if "|" in var_expr:
                var_name, _ = var_expr.split("|", 1)
                var_name = var_name.strip()
                # Variables with defaults are not considered missing
            else:
                # Simple variable reference: ${var}
                var_name = var_expr.strip()
                if var_name not in variables:
                    if var_name not in missing:
                        missing.append(var_name)

        return missing

    def _count_total_variables(self) -> int:
        """Count total variables across all sources."""
        total = (
            len(self._config.cli_variables)
            + len(self._config.profile_variables)
            + len(self._config.set_variables)
            + len(self._config.env_variables)
        )
        return total

    # Additional utility methods for debugging and introspection

    def get_configuration(self) -> VariableConfig:
        """Get the current variable configuration.

        Returns:
            Copy of the current VariableConfig
        """
        return self._config

    def get_resolved_variables(self) -> Dict[str, Any]:
        """Get all resolved variables in priority order.

        Returns:
            Dictionary of all variables resolved according to priority
        """
        return self._get_resolved_variables().copy()

    def has_variable(self, name: str) -> bool:
        """Check if a variable is available.

        Args:
            name: Variable name to check

        Returns:
            True if variable is available, False otherwise
        """
        variables = self._get_resolved_variables()
        return name in variables
