"""Migration facade for variable substitution system.

This facade provides new variable substitution functionality alongside existing APIs
without modifying any existing code. This enables gradual migration and testing
while maintaining 100% backward compatibility.

Following Zen of Python:
- Explicit is better than implicit
- Simple is better than complex
- Flat is better than nested
- Sparse is better than dense
"""

from typing import Any, Dict, List, Optional

from .manager import VariableConfig, VariableManager
from .validator import VariableValidator


class LegacyVariableSupport:
    """Facade for backward compatibility - doesn't modify existing code.

    This class provides new variable substitution methods that work alongside
    existing functionality, enabling gradual migration testing without breaking
    existing APIs.

    All methods in this class are static to ensure no state dependencies and
    maintain compatibility with existing function-based interfaces.
    """

    @staticmethod
    def substitute_variables_new(data: Any, variables: Dict[str, Any]) -> Any:
        """Pure variable substitution using the unified VariableManager.

        Following Zen of Python: Simple is better than complex.
        This method provides PURE variable substitution without any formatting.
        Each consuming component (DuckDB engine, ConditionEvaluator, etc.)
        applies its own context-specific formatting on top.

        DESIGN PRINCIPLE: Separation of Concerns
        - VariableManager: Pure variable substitution (raw values)
        - DuckDBEngine: Adds SQL formatting (quotes strings)
        - ConditionEvaluator: Adds AST formatting (quotes identifiers)

        Args:
            data: The data structure to perform variable substitution on
            variables: Dictionary of variable name->value mappings

        Returns:
            The data structure with variables substituted (raw values, no formatting)

        Example:
            >>> result = LegacyVariableSupport.substitute_variables_new(
            ...     "Hello ${name}", {"name": "Alice"}
            ... )
            >>> assert result == "Hello Alice"
        """
        config = VariableConfig(cli_variables=variables)
        manager = VariableManager(config)
        return manager.substitute(data)

    @staticmethod
    def validate_variables_new(content: str, variables: Dict[str, Any]) -> List[str]:
        """New validation available alongside existing validation logic.

        This method provides enhanced validation using the new VariableValidator
        while maintaining the same interface as existing validation functions.

        Args:
            content: The content to validate for variable references
            variables: Dictionary of available variables

        Returns:
            List of missing variable names (empty list if all variables present)

        Example:
            >>> missing = LegacyVariableSupport.validate_variables_new(
            ...     "Hello ${name} and ${missing}", {"name": "Alice"}
            ... )
            >>> assert missing == ["missing"]
        """
        config = VariableConfig(cli_variables=variables)
        validator = VariableValidator(variables)
        result = validator.validate(content, config)
        return result.missing_variables

    @staticmethod
    def substitute_with_priority_new(
        data: Any,
        cli_variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
        set_variables: Optional[Dict[str, Any]] = None,
        env_variables: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """New priority-based substitution not available in existing system.

        This method demonstrates the enhanced capabilities of the new system
        by supporting explicit priority-based variable resolution.

        Priority order (highest to lowest):
        1. CLI variables
        2. Profile variables
        3. SET variables
        4. Environment variables

        Args:
            data: The data structure to perform variable substitution on
            cli_variables: Variables from command line arguments
            profile_variables: Variables from profile configuration
            set_variables: Variables from SET statements
            env_variables: Variables from environment

        Returns:
            The data structure with variables substituted using priority resolution

        Example:
            >>> result = LegacyVariableSupport.substitute_with_priority_new(
            ...     "Environment: ${env}",
            ...     cli_variables={"env": "prod"},
            ...     profile_variables={"env": "dev"}
            ... )
            >>> assert result == "Environment: prod"  # CLI takes priority
        """
        config = VariableConfig(
            cli_variables=cli_variables or {},
            profile_variables=profile_variables or {},
            set_variables=set_variables or {},
            env_variables=env_variables or {},
        )
        manager = VariableManager(config)
        return manager.substitute(data)

    @staticmethod
    def validate_with_context_new(
        content: str, variables: Dict[str, Any], context_name: str = "unknown"
    ) -> Dict[str, Any]:
        """Enhanced validation with context information - new capability.

        This method provides enhanced validation capabilities not available
        in the existing system, including context location tracking and
        detailed validation results.

        Args:
            content: The content to validate
            variables: Available variables
            context_name: Name of the context for better error reporting

        Returns:
            Dictionary with validation results including:
            - is_valid: bool
            - missing_variables: List[str]
            - warnings: List[str]
            - context_locations: Dict[str, List[str]]

        Example:
            >>> result = LegacyVariableSupport.validate_with_context_new(
            ...     "SELECT * FROM ${table} WHERE id = ${missing}",
            ...     {"table": "users"},
            ...     "query.sql"
            ... )
            >>> assert not result["is_valid"]
            >>> assert "missing" in result["missing_variables"]
        """
        config = VariableConfig(cli_variables=variables)
        validator = VariableValidator(variables)
        result = validator.validate(content, config)

        # Enhanced context information (new capability)
        context_locations = {}
        if not result.is_valid:
            for var in result.missing_variables:
                context_locations[var] = [f"Context: {context_name}"]

        return {
            "is_valid": result.is_valid,
            "missing_variables": result.missing_variables,
            "invalid_defaults": result.invalid_defaults,
            "warnings": result.warnings,
            "context_locations": context_locations,
        }

    @staticmethod
    def extract_variables_new(content: str) -> List[str]:
        """Extract all variable references from content - enhanced capability.

        This method provides variable extraction capabilities using the new
        validator's optimized regex patterns.

        Args:
            content: The content to extract variables from

        Returns:
            List of unique variable names found in content

        Example:
            >>> variables = LegacyVariableSupport.extract_variables_new(
            ...     "Hello ${name}, you are ${age} years old, ${name}!"
            ... )
            >>> assert set(variables) == {"name", "age"}
        """
        validator = VariableValidator({})
        return validator.extract_variables(content)

    @staticmethod
    def get_migration_info() -> Dict[str, str]:
        """Get information about the migration facade for debugging/monitoring.

        Returns:
            Dictionary with migration facade information
        """
        return {
            "facade_version": "1.0.0",
            "task": "1.3",
            "strategy": "PURE_ADDITION",
            "compatibility": "100%",
            "status": "ADDITIVE_ONLY",
        }


# Convenience functions that mirror existing function-based interfaces
def substitute_variables_unified(data: Any, variables: Dict[str, Any]) -> Any:
    """Convenience function that mirrors existing substitute_variables() interface.

    This function provides the same interface as existing code but uses the new
    unified system internally. Can be used as a drop-in replacement for testing.
    """
    return LegacyVariableSupport.substitute_variables_new(data, variables)


def validate_variables_unified(content: str, variables: Dict[str, Any]) -> List[str]:
    """Convenience function that mirrors existing validation interface.

    This function provides the same interface as existing validation code but
    uses the new unified validator internally.
    """
    return LegacyVariableSupport.validate_variables_new(content, variables)
