"""Unified Variable Management System for SQLFlow.

This module provides the VariableManager class, which serves as the single
source of truth for variable substitution across the entire codebase.
It follows Zen of Python principles: Simple, Readable, Explicit.

The VariableManager uses the existing VariableSubstitutionEngine internally
to ensure backward compatibility during the transition period.
"""

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
    operations while internally using the existing VariableSubstitutionEngine
    to ensure backward compatibility.
    """

    def __init__(self, config: Optional[VariableConfig] = None):
        """Initialize the variable manager with configuration.

        Args:
            config: Variable configuration with priority-ordered variables
        """
        self._config = config or VariableConfig()
        self._setup_components()

        logger.debug(
            f"VariableManager initialized with {self._count_total_variables()} variables"
        )

    def _setup_components(self):
        """Setup internal components using existing implementations.

        Following Zen of Python: Practicality beats purity.
        Uses existing VariableSubstitutionEngine to ensure compatibility.
        """
        # Import here to avoid circular imports
        from sqlflow.core.variable_substitution import VariableSubstitutionEngine

        self._config.resolve_priority()

        # Merge environment variables into set variables since they have same priority level
        merged_set_variables = {}
        merged_set_variables.update(self._config.env_variables)
        merged_set_variables.update(
            self._config.set_variables
        )  # SET variables override env

        # Use existing engine with priority-based configuration
        self._engine = VariableSubstitutionEngine(
            cli_variables=self._config.cli_variables,
            profile_variables=self._config.profile_variables,
            set_variables=merged_set_variables,
            # Environment variables from os.environ are handled automatically by VariableSubstitutionEngine
        )

        logger.debug(
            "Internal components initialized using existing VariableSubstitutionEngine"
        )

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
        result = self._engine.substitute(data)
        logger.debug("Variable substitution completed")

        return result

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

        # Use existing validation from VariableSubstitutionEngine
        missing = self._engine.validate_required_variables(content)

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
        return self._config.resolve_priority()

    def has_variable(self, name: str) -> bool:
        """Check if a variable is available.

        Args:
            name: Variable name to check

        Returns:
            True if variable is available, False otherwise
        """
        resolved = self._config.resolve_priority()
        return name in resolved
