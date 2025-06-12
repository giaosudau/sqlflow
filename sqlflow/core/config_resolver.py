"""Configuration resolution and merging for SQLFlow profile-based connectors.

This module provides configuration resolution with priority-based merging
and variable substitution for connector configurations.
"""

import copy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlflow.core.profiles import ProfileManager
from sqlflow.core.variables import substitute_variables
from sqlflow.core.variables.manager import VariableConfig, VariableManager
from sqlflow.logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@dataclass
class ResolutionConfig:
    """Configuration for config resolution to simplify parameter passing."""

    profile_name: str
    options: Dict[str, Any] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)
    defaults: Dict[str, Any] = field(default_factory=dict)
    environment: Optional[str] = None


class ConfigurationResolver:
    """Handles configuration merging and variable substitution.

    Merges configuration from multiple sources with priority:
    1. OPTIONS (highest priority - runtime overrides)
    2. Profile PARAMS (medium priority - profile defaults)
    3. Default Values (lowest priority - connector defaults)
    """

    def __init__(
        self,
        profile_manager: ProfileManager,
        variable_manager: Optional[VariableManager] = None,
    ):
        """Initialize ConfigurationResolver.

        Args:
            profile_manager: ProfileManager instance for profile access
            variable_manager: VariableManager for variable substitution
        """
        self.profile_manager = profile_manager
        self.variable_manager = variable_manager or VariableManager(VariableConfig())

        logger.debug("Initialized ConfigurationResolver")

    def resolve_config(
        self,
        profile_name: str,
        options: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        defaults: Optional[Dict[str, Any]] = None,
        environment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resolve configuration by merging profile config with options override.

        Priority order (highest to lowest):
        1. OPTIONS - runtime overrides
        2. Profile PARAMS - profile configuration
        3. Default Values - connector defaults

        Args:
            profile_name: Name of the connector profile to use
            options: Runtime options that override profile settings
            variables: Variables for substitution
            defaults: Default configuration values
            environment: Environment name (uses ProfileManager default if None)

        Returns:
            Merged and resolved configuration dictionary

        Raises:
            ValueError: If profile or connector not found
        """
        # Create configuration object for cleaner parameter handling
        config = ResolutionConfig(
            profile_name=profile_name,
            options=options or {},
            variables=variables or {},
            defaults=defaults or {},
            environment=environment,
        )

        return self._resolve_with_config(config)

    def resolve_with_config(self, config: ResolutionConfig) -> Dict[str, Any]:
        """Resolve configuration using ResolutionConfig object.

        Args:
            config: ResolutionConfig with all resolution parameters

        Returns:
            Merged and resolved configuration dictionary

        Raises:
            ValueError: If profile or connector not found
        """
        return self._resolve_with_config(config)

    def _resolve_with_config(self, config: ResolutionConfig) -> Dict[str, Any]:
        """Internal method to resolve configuration with ResolutionConfig."""
        logger.debug(f"Resolving configuration for profile '{config.profile_name}'")

        # 1. Get connector configuration from raw profile (no substitution yet)
        try:
            connector_profile = self.profile_manager.get_connector_profile_raw(
                config.profile_name, config.environment
            )
        except ValueError as e:
            logger.error(
                f"Failed to get connector profile '{config.profile_name}': {e}"
            )
            raise

        # 2. Get fully resolved profile variables (includes nested variable resolution)
        resolved_profile_variables = self.profile_manager.get_variables(
            config.environment
        )

        # 3. Merge profile variables with provided variables (runtime variables override)
        merged_variables = {
            **resolved_profile_variables,
            **config.variables,
        }  # variables override profile vars

        # 4. Build configuration with priority merging
        # Start with defaults (lowest priority)
        resolved_config = copy.deepcopy(config.defaults)

        # Add connector type information
        resolved_config["type"] = connector_profile.connector_type

        # Apply profile params (medium priority)
        profile_params = copy.deepcopy(connector_profile.params)
        self._deep_merge(resolved_config, profile_params)

        # Apply options (highest priority)
        self._deep_merge(resolved_config, config.options)

        logger.debug(f"Pre-substitution config: {resolved_config}")

        # 5. Apply variable substitution to the final configuration
        # ZEN OF PYTHON: Simple is better than complex
        # Use utility function - one obvious way to do it
        resolved_config = substitute_variables(resolved_config, merged_variables)

        logger.debug(
            f"Final resolved config for '{config.profile_name}': {resolved_config}"
        )
        return resolved_config

    def substitute_variables(
        self, config: Dict[str, Any], variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply variable substitution to configuration values.

        Args:
            config: Configuration dictionary to process
            variables: Variables available for substitution

        Returns:
            Configuration with variables substituted
        """
        logger.debug(f"Applying variable substitution with {len(variables)} variables")

        # Use the variable manager to substitute variables
        substituted_config = self.variable_manager.substitute(config)

        logger.debug("Variable substitution completed")
        return substituted_config

    def get_connector_defaults(self, connector_type: str) -> Dict[str, Any]:
        """Get default configuration for a connector type.

        This method provides a centralized place to define connector defaults.
        Can be extended to load defaults from configuration files or registry.

        Args:
            connector_type: Type of connector (e.g., 'csv', 'postgres', 's3')

        Returns:
            Dictionary of default configuration values
        """
        # Default configurations for common connector types
        connector_defaults = {
            "csv": {
                "has_header": True,
                "delimiter": ",",
                "encoding": "utf-8",
                "quote_char": '"',
                "escape_char": "\\",
            },
            "postgres": {
                "port": 5432,
                "sslmode": "prefer",
                "connect_timeout": 30,
                "application_name": "sqlflow",
            },
            "s3": {"region": "us-east-1", "use_ssl": True, "signature_version": "s3v4"},
            "rest": {"timeout": 30, "verify_ssl": True, "max_retries": 3},
        }

        defaults = connector_defaults.get(connector_type, {})
        logger.debug(
            f"Retrieved defaults for connector type '{connector_type}': {defaults}"
        )
        return defaults

    def resolve_with_defaults(
        self,
        profile_name: str,
        options: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        environment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resolve configuration including automatic connector defaults.

        This is a convenience method that automatically retrieves connector defaults
        based on the connector type from the profile.

        Args:
            profile_name: Name of the connector profile to use
            options: Runtime options that override profile settings
            variables: Variables for substitution
            environment: Environment name (uses ProfileManager default if None)

        Returns:
            Fully resolved configuration with defaults

        Raises:
            ValueError: If profile or connector not found
        """
        # Get connector type to determine defaults
        connector_profile = self.profile_manager.get_connector_profile(
            profile_name, environment
        )
        defaults = self.get_connector_defaults(connector_profile.connector_type)

        return self.resolve_config(
            profile_name=profile_name,
            options=options,
            variables=variables,
            defaults=defaults,
            environment=environment,
        )

    def validate_resolved_config(
        self, config: Dict[str, Any], required_fields: Optional[list] = None
    ) -> list:
        """Validate resolved configuration for completeness.

        Args:
            config: Resolved configuration to validate
            required_fields: List of required field names

        Returns:
            List of validation errors (empty if valid)
        """
        required_fields = required_fields or []
        errors = []

        for field_name in required_fields:
            if field_name not in config:
                errors.append(
                    f"Required field '{field_name}' missing from configuration"
                )
            elif config[field_name] is None:
                errors.append(f"Required field '{field_name}' cannot be null")
            elif isinstance(config[field_name], str) and not config[field_name].strip():
                errors.append(f"Required field '{field_name}' cannot be empty")

        logger.debug(f"Configuration validation: {len(errors)} errors found")
        return errors

    def get_resolution_info(
        self,
        profile_name: str,
        options: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        environment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get detailed information about configuration resolution.

        Useful for debugging and understanding how configuration was resolved.

        Args:
            profile_name: Name of the connector profile
            options: Runtime options
            variables: Variables for substitution
            environment: Environment name

        Returns:
            Dictionary with resolution details
        """
        options = options or {}
        variables = variables or {}

        # Get components
        connector_profile = self.profile_manager.get_connector_profile(
            profile_name, environment
        )
        defaults = self.get_connector_defaults(connector_profile.connector_type)
        profile_variables = self.profile_manager.get_variables(environment)

        return {
            "profile_name": profile_name,
            "connector_type": connector_profile.connector_type,
            "environment": environment or self.profile_manager.environment,
            "defaults": defaults,
            "profile_params": connector_profile.params,
            "options_override": options,
            "profile_variables": profile_variables,
            "runtime_variables": variables,
            "merged_variables": {**profile_variables, **variables},
        }

    def _deep_merge(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """Deep merge source dictionary into target dictionary.

        Recursively merges nested dictionaries instead of replacing them.

        Args:
            target: Target dictionary to merge into (modified in place)
            source: Source dictionary to merge from
        """
        for key, value in source.items():
            if (
                key in target
                and isinstance(target[key], dict)
                and isinstance(value, dict)
            ):
                # Both are dicts, merge recursively
                self._deep_merge(target[key], value)
            else:
                # Replace value (non-dict or key doesn't exist in target)
                target[key] = copy.deepcopy(value)
