"""Profile-based connector configuration management for SQLFlow.

This module provides centralized profile configuration management with caching,
validation, and variable substitution for connector configurations.
"""

import copy
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml

from sqlflow.core.variables import substitute_variables
from sqlflow.core.variables.manager import VariableConfig, VariableManager
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConnectorProfile:
    """Represents a connector configuration from a profile."""

    name: str
    connector_type: str
    params: Dict[str, Any]

    @classmethod
    def from_dict(cls, name: str, config: Dict[str, Any]) -> "ConnectorProfile":
        """Create ConnectorProfile from dictionary configuration.

        Args:
            name: Name of the connector profile
            config: Configuration dictionary containing 'type' and 'params'

        Returns:
            ConnectorProfile instance

        Raises:
            ValueError: If required fields are missing
        """
        if not isinstance(config, dict):
            raise ValueError(f"Connector '{name}' configuration must be a dictionary")

        connector_type = config.get("type")
        if not connector_type:
            raise ValueError(f"Connector '{name}' missing required 'type' field")

        params = config.get("params", {})
        if not isinstance(params, dict):
            raise ValueError(f"Connector '{name}' params must be a dictionary")

        return cls(name=name, connector_type=connector_type, params=params)


@dataclass
class ValidationResult:
    """Result of profile validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str]

    def __bool__(self) -> bool:
        """Returns True if validation passed."""
        return self.is_valid


class VariableResolver:
    """Handles iterative variable resolution with clear convergence strategy."""

    def __init__(self, max_iterations: int = 10):
        """Initialize variable resolver.

        Args:
            max_iterations: Maximum number of resolution iterations to prevent infinite loops
        """
        self.max_iterations = max_iterations

    def resolve_variables(self, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve variables with iterative substitution until convergence.

        Args:
            variables: Dictionary of variables that may reference each other

        Returns:
            Dictionary with all variable references resolved
        """
        if not variables:
            return {}

        resolved = copy.deepcopy(variables)

        for iteration in range(self.max_iterations):
            logger.debug(f"Variable resolution iteration {iteration + 1}")
            previous = copy.deepcopy(resolved)

            # Apply one round of substitution
            resolved = self._substitute_pass(resolved)

            # Check for convergence
            if self._has_converged(previous, resolved):
                logger.debug(f"Variables converged after {iteration + 1} iterations")
                return resolved

        logger.warning(
            f"Variable resolution did not converge after {self.max_iterations} iterations"
        )
        return resolved

    def _substitute_pass(self, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Perform one pass of variable substitution."""
        # ZEN OF PYTHON: Simple is better than complex
        # Use utility function - one obvious way to do it
        return substitute_variables(variables, variables)

    def _has_converged(self, previous: Dict[str, Any], current: Dict[str, Any]) -> bool:
        """Check if variable resolution has converged (no more changes)."""
        return previous == current


class ProfileManager:
    """Centralized profile configuration management with caching and validation."""

    def __init__(self, profile_dir: str, environment: str = "dev"):
        """Initialize ProfileManager.

        Args:
            profile_dir: Directory containing profile YAML files
            environment: Environment name (default: 'dev')
        """
        self.profile_dir = profile_dir
        self.environment = environment
        self._profile_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self._variable_manager = VariableManager(VariableConfig())

        logger.debug(
            f"Initialized ProfileManager for environment '{environment}' in '{profile_dir}'"
        )

    def load_profile(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """Load environment-specific profile with caching.

        Args:
            environment: Environment name to load (uses instance default if None)

        Returns:
            Profile configuration dictionary

        Raises:
            FileNotFoundError: If profile file doesn't exist
            ValueError: If profile YAML is invalid
        """
        env_name = environment or self.environment
        profile_path = os.path.join(self.profile_dir, f"{env_name}.yml")

        # Check cache first
        if self._is_cache_valid(profile_path):
            logger.debug(f"Using cached profile for environment '{env_name}'")
            return self._profile_cache[profile_path]

        # Load from file
        logger.debug(f"Loading profile from '{profile_path}'")

        if not os.path.exists(profile_path):
            raise FileNotFoundError(f"Profile file not found: {profile_path}")

        try:
            with open(profile_path, "r", encoding="utf-8") as f:
                profile_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in profile '{profile_path}': {e}")

        if profile_data is None:
            profile_data = {}

        # Apply variable substitution to the loaded profile
        profile_data = self._apply_variable_substitution(profile_data)

        # Validate profile structure
        validation_result = self.validate_profile(profile_path, profile_data)
        if not validation_result.is_valid:
            error_msg = (
                f"Profile validation failed for '{profile_path}':\n"
                + "\n".join(f"  - {error}" for error in validation_result.errors)
            )
            raise ValueError(error_msg)

        # Log warnings
        for warning in validation_result.warnings:
            logger.warning(f"Profile '{profile_path}': {warning}")

        # Cache the loaded and substituted profile
        self._profile_cache[profile_path] = profile_data
        self._cache_timestamps[profile_path] = time.time()

        logger.debug(
            f"Successfully loaded and cached profile for environment '{env_name}'"
        )
        return profile_data

    def load_profile_raw(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """Load environment-specific profile without variable substitution.

        Args:
            environment: Environment name to load (uses instance default if None)

        Returns:
            Raw profile configuration dictionary without variable substitution

        Raises:
            FileNotFoundError: If profile file doesn't exist
            ValueError: If profile YAML is invalid
        """
        env_name = environment or self.environment
        profile_path = os.path.join(self.profile_dir, f"{env_name}.yml")

        logger.debug(f"Loading raw profile from '{profile_path}' (no substitution)")

        if not os.path.exists(profile_path):
            raise FileNotFoundError(f"Profile file not found: {profile_path}")

        try:
            with open(profile_path, "r", encoding="utf-8") as f:
                profile_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in profile '{profile_path}': {e}")

        if profile_data is None:
            profile_data = {}

        logger.debug(f"Successfully loaded raw profile for environment '{env_name}'")
        return profile_data

    def _apply_variable_substitution(
        self, profile_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply variable substitution to profile data.

        This method substitutes variables in the profile using an iterative approach:
        1. First pass: Substitute environment variables in the variables section
        2. Iterative passes: Substitute profile variables until no more changes occur
        3. Final pass: Apply substitution to the entire profile data

        Args:
            profile_data: Raw profile data from YAML

        Returns:
            Profile data with variables substituted
        """
        logger.debug("Applying variable substitution to profile data")

        # Make a deep copy to avoid mutating the original
        substituted_data = copy.deepcopy(profile_data)

        # ZEN OF PYTHON: Simple is better than complex
        # Use utility function - one obvious way to do it

        # First pass: Substitute environment variables in the variables section
        if "variables" in substituted_data:
            logger.debug(
                "Substituting environment variables in profile variables section"
            )
            substituted_data["variables"] = substitute_variables(
                substituted_data["variables"], dict(os.environ)
            )

        # Iterative resolution of profile variables that reference each other
        if "variables" in substituted_data:
            logger.debug("Resolving profile variable interdependencies")
            variables = substituted_data["variables"]

            # Create variable resolver (this is lightweight)
            variable_resolver = VariableResolver()
            variables = variable_resolver.resolve_variables(variables)

            substituted_data["variables"] = variables

        # Final pass: Apply substitution to entire profile data using utility function
        profile_variables = substituted_data.get("variables", {})
        logger.debug(
            f"Using resolved profile variables for substitution: {profile_variables}"
        )

        # ZEN OF PYTHON: Simple is better than complex
        # Use utility function - one obvious way to do it
        substituted_data = substitute_variables(substituted_data, profile_variables)

        logger.debug("Variable substitution completed")
        return substituted_data

    def get_connector_profile(
        self, connector_name: str, environment: Optional[str] = None
    ) -> ConnectorProfile:
        """Get specific connector configuration from profile.

        Args:
            connector_name: Name of the connector in the profile
            environment: Environment name (uses instance default if None)

        Returns:
            ConnectorProfile instance with variables substituted

        Raises:
            ValueError: If connector not found or invalid configuration
        """
        profile = self.load_profile(environment)

        connectors = profile.get("connectors", {})
        if connector_name not in connectors:
            available = list(connectors.keys())
            raise ValueError(
                f"Connector '{connector_name}' not found in profile. "
                f"Available connectors: {available}"
            )

        connector_config = connectors[connector_name]
        return ConnectorProfile.from_dict(connector_name, connector_config)

    def get_connector_profile_raw(
        self, connector_name: str, environment: Optional[str] = None
    ) -> ConnectorProfile:
        """Get specific connector configuration from raw profile (no variable substitution).

        Args:
            connector_name: Name of the connector in the profile
            environment: Environment name (uses instance default if None)

        Returns:
            ConnectorProfile instance without variable substitution

        Raises:
            ValueError: If connector not found or invalid configuration
        """
        profile = self.load_profile_raw(environment)

        connectors = profile.get("connectors", {})
        if connector_name not in connectors:
            available = list(connectors.keys())
            raise ValueError(
                f"Connector '{connector_name}' not found in profile. "
                f"Available connectors: {available}"
            )

        connector_config = connectors[connector_name]
        return ConnectorProfile.from_dict(connector_name, connector_config)

    def get_variables_raw(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """Get variables from the raw profile without substitution applied.

        Args:
            environment: Environment name (uses instance default if None)

        Returns:
            Dictionary of raw profile variables without substitution applied
        """
        try:
            profile = self.load_profile_raw(environment)
            return profile.get("variables", {})
        except (FileNotFoundError, ValueError):
            return {}

    def validate_profile(
        self, profile_path: str, profile_data: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate profile YAML structure and required fields.

        Args:
            profile_path: Path to profile file (for error reporting)
            profile_data: Profile data to validate (loads from file if None)

        Returns:
            ValidationResult with validation status and messages
        """
        if profile_data is None:
            profile_data = self._load_profile_data(profile_path)
            if isinstance(profile_data, ValidationResult):
                return profile_data  # Return error result

        if profile_data is None:
            profile_data = {}

        errors = []
        warnings = []

        # Validate top-level structure
        if not isinstance(profile_data, dict):
            errors.append("Profile must be a YAML dictionary")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Validate each section
        self._validate_version(profile_data, warnings)
        self._validate_variables_section(profile_data, errors)
        self._validate_connectors_section(profile_data, errors)
        self._validate_engines_section(profile_data, errors)

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def _load_profile_data(self, profile_path: str):
        """Load profile data from file, returning ValidationResult on error."""
        try:
            with open(profile_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except (OSError, yaml.YAMLError) as e:
            return ValidationResult(
                is_valid=False, errors=[f"Failed to load profile: {e}"], warnings=[]
            )

    def _validate_version(
        self, profile_data: Dict[str, Any], warnings: List[str]
    ) -> None:
        """Validate profile version."""
        if "version" in profile_data:
            version = profile_data["version"]
            if version != "1.0":
                warnings.append(f"Unknown profile version '{version}', expected '1.0'")

    def _validate_variables_section(
        self, profile_data: Dict[str, Any], errors: List[str]
    ) -> None:
        """Validate variables section."""
        if "variables" in profile_data:
            variables = profile_data["variables"]
            if not isinstance(variables, dict):
                errors.append("'variables' section must be a dictionary")

    def _validate_connectors_section(
        self, profile_data: Dict[str, Any], errors: List[str]
    ) -> None:
        """Validate connectors section."""
        if "connectors" in profile_data:
            connectors = profile_data["connectors"]
            if not isinstance(connectors, dict):
                errors.append("'connectors' section must be a dictionary")
            else:
                # Validate each connector
                for conn_name, conn_config in connectors.items():
                    try:
                        ConnectorProfile.from_dict(conn_name, conn_config)
                    except ValueError as e:
                        errors.append(str(e))

    def _validate_engines_section(
        self, profile_data: Dict[str, Any], errors: List[str]
    ) -> None:
        """Validate engines section."""
        if "engines" in profile_data:
            engines = profile_data["engines"]
            if not isinstance(engines, dict):
                errors.append("'engines' section must be a dictionary")
            else:
                self._validate_duckdb_engine(engines, errors)

    def _validate_duckdb_engine(
        self, engines: Dict[str, Any], errors: List[str]
    ) -> None:
        """Validate DuckDB engine configuration."""
        if "duckdb" in engines:
            duckdb_config = engines["duckdb"]
            if not isinstance(duckdb_config, dict):
                errors.append("'engines.duckdb' must be a dictionary")
            else:
                mode = duckdb_config.get("mode", "memory")
                if mode not in ["memory", "persistent"]:
                    errors.append(
                        f"Invalid DuckDB mode '{mode}'. Must be 'memory' or 'persistent'"
                    )

    def list_connectors(self, environment: Optional[str] = None) -> List[str]:
        """List available connector names in the profile.

        Args:
            environment: Environment name (uses instance default if None)

        Returns:
            List of connector names
        """
        try:
            profile = self.load_profile(environment)
            connectors = profile.get("connectors", {})
            return list(connectors.keys())
        except (FileNotFoundError, ValueError):
            return []

    def get_variables(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """Get variables from the profile with substitution applied.

        Args:
            environment: Environment name (uses instance default if None)

        Returns:
            Dictionary of profile variables with substitution applied
        """
        try:
            profile = self.load_profile(environment)
            return profile.get("variables", {})
        except (FileNotFoundError, ValueError):
            return {}

    def _is_cache_valid(self, profile_path: str) -> bool:
        """Check if cached profile is still valid.

        Args:
            profile_path: Path to profile file

        Returns:
            True if cache is valid, False otherwise
        """
        if profile_path not in self._profile_cache:
            return False

        if profile_path not in self._cache_timestamps:
            return False

        # Check if file has been modified since caching
        try:
            file_mtime = os.path.getmtime(profile_path)
            cache_time = self._cache_timestamps[profile_path]
            return file_mtime <= cache_time
        except OSError:
            # File doesn't exist anymore, cache is invalid
            return False

    def clear_cache(self) -> None:
        """Clear the profile cache."""
        self._profile_cache.clear()
        self._cache_timestamps.clear()
        logger.debug("Profile cache cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring.

        Returns:
            Dictionary with cache statistics
        """
        return {
            "cached_profiles": len(self._profile_cache),
            "cache_keys": list(self._profile_cache.keys()),
            "cache_size_bytes": sum(
                len(str(profile)) for profile in self._profile_cache.values()
            ),
        }
