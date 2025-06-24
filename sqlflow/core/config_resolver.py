"""Configuration Resolution for SQLFlow

This module handles configuration merging and variable substitution using
V2 functional approach for better performance and simplicity.
"""

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlflow.core.profiles import ProfileManager
from sqlflow.core.variables import substitute_any
from sqlflow.logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@dataclass(frozen=True)
class ResolvedConfig:
    """Immutable resolved configuration.

    Raymond Hettinger: Single source of truth with clear precedence.
    """

    variables: Dict[str, Any]
    connection_params: Dict[str, Any]
    execution_options: Dict[str, Any]

    @classmethod
    def from_sources(
        cls,
        *,
        profile: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        step_options: Optional[Dict[str, Any]] = None,
    ):
        """Single method to resolve all configuration sources.

        Raymond Hettinger: Clear precedence - variables override profile,
        step_options override both.
        """
        # Merge all sources with clear precedence
        merged_vars = {}
        if profile:
            merged_vars.update(profile.get("variables", {}))
        if variables:
            merged_vars.update(variables)

        # Connection params from profile
        connection_params = profile.get("connections", {}) if profile else {}

        # Execution options from step
        execution_options = step_options or {}

        return cls(
            variables=merged_vars,
            connection_params=connection_params,
            execution_options=execution_options,
        )


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

    Raymond Hettinger: Single source of truth for configuration resolution.
    Merges configuration from multiple sources with priority:
    1. OPTIONS (highest priority - runtime overrides)
    2. Profile PARAMS (medium priority - profile defaults)
    3. Default Values (lowest priority - connector defaults)
    """

    def __init__(
        self,
        profile_manager: ProfileManager,
    ):
        """Initialize ConfigurationResolver.

        Args:
            profile_manager: ProfileManager instance for profile access
        """
        self.profile_manager = profile_manager

        # Performance optimization: cache resolved configurations
        self._resolution_cache = {}

        logger.debug("Initialized ConfigurationResolver with V2 variables")

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

        # Simple cache key using JSON-serializable approach
        cache_key = self._build_cache_key(config)

        if cache_key in self._resolution_cache:
            logger.debug(f"Cache hit for profile '{config.profile_name}'")
            return self._resolution_cache[cache_key].copy()

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
        resolved_config = dict(config.defaults)  # Start with defaults (lowest priority)

        # Add connector type information
        resolved_config["type"] = connector_profile.connector_type

        # Apply profile params (medium priority)
        self._merge_config(resolved_config, connector_profile.params)

        # Apply options (highest priority)
        self._merge_config(resolved_config, config.options)

        logger.debug(f"Pre-substitution config: {resolved_config}")

        # 5. Apply variable substitution to the final configuration
        resolved_config = substitute_any(resolved_config, merged_variables)

        # Cache the result
        self._resolution_cache[cache_key] = resolved_config.copy()

        logger.debug(
            f"Final resolved config for '{config.profile_name}': {resolved_config}"
        )
        return resolved_config

    def _build_cache_key(self, config: ResolutionConfig) -> str:
        """Build a hashable cache key using string representation."""
        # Create a simple, deterministic cache key
        key_dict = {
            "profile": config.profile_name,
            "env": config.environment,
            "options": config.options,
            "variables": config.variables,
            "defaults": config.defaults,
        }

        # Use JSON for deterministic string representation
        return json.dumps(key_dict, sort_keys=True, default=str)

    def _merge_config(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """Simple recursive merge - Zen of Python: Simple is better than complex."""
        for key, value in source.items():
            if (
                key in target
                and isinstance(target[key], dict)
                and isinstance(value, dict)
            ):
                self._merge_config(target[key], value)
            else:
                target[key] = value

    def substitute_variables(
        self, config: Dict[str, Any], variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Substitute variables in configuration using V2 functions.

        Args:
            config: Configuration dictionary with potential variable placeholders
            variables: Variables to substitute

        Returns:
            Configuration with variables substituted
        """
        return substitute_any(config, variables)

    # Connector defaults - simplified approach
    # Zen of Python: "There should be one obvious way to do it"
    _CONNECTOR_DEFAULTS = {
        "csv": {
            "delimiter": ",",
            "encoding": "utf-8",
            "has_header": True,
            "quote_char": '"',
            "escape_char": "\\",
        },
        "json": {"encoding": "utf-8"},
        "parquet": {},
        "postgres": {"port": 5432, "timeout": 30, "sslmode": "prefer"},
        "mysql": {"port": 3306, "timeout": 30},
        "s3": {"region": "us-east-1", "use_ssl": True},
    }

    def get_connector_defaults(self, connector_type: str) -> Dict[str, Any]:
        """Get default configuration for a connector type.

        Raymond Hettinger: Simple dictionary lookup - one obvious way to do it.

        Args:
            connector_type: Type of connector

        Returns:
            Default configuration dictionary
        """
        return self._CONNECTOR_DEFAULTS.get(connector_type, {}).copy()

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

    def clear_cache(self) -> None:
        """Clear the resolution cache.

        Useful for testing or when profiles have been updated.
        """
        self._resolution_cache.clear()
        logger.debug("Configuration resolution cache cleared")

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics for monitoring.

        Returns:
            Dictionary with cache size and other statistics
        """
        return {"cache_size": len(self._resolution_cache)}

    def validate_resolved_config(
        self, config: Dict[str, Any], required_fields: Optional[List[str]] = None
    ) -> List[str]:
        """Validate resolved configuration and return list of errors.

        Zen of Python: "Explicit is better than implicit"

        Args:
            config: Resolved configuration to validate
            required_fields: List of required field names

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        required_fields = required_fields or []

        # Check for required fields
        for field_name in required_fields:
            if field_name not in config:
                errors.append(f"Missing required field: {field_name}")
            elif config[field_name] is None:
                errors.append(f"Field '{field_name}' cannot be null")
            elif config[field_name] == "":
                errors.append(f"Field '{field_name}' cannot be empty")

        # Only check empty/None values for required fields
        # Zen: "Explicit is better than implicit" - don't make assumptions

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
