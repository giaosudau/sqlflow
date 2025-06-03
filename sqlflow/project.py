"""Project management for SQLFlow."""

import os
from typing import Any, Dict, Optional

import yaml

from sqlflow.logging import configure_logging, get_logger

logger = get_logger(__name__)


class Project:
    """Manages SQLFlow project structure and configuration using profiles only."""

    def __init__(self, project_dir: str, profile_name: str = "dev"):
        """Initialize a Project instance using a profile.

        Args:
        ----
            project_dir: Path to the project directory
            profile_name: Name of the profile to load (default: 'dev')

        """
        self.project_dir = project_dir
        self.profile_name = profile_name
        self.profile = self._load_profile(profile_name)

        # Configure logging based on profile settings
        self._configure_logging_from_profile()

        logger.debug(f"Loaded profile: {profile_name}")

    def _load_profile(self, profile_name: str) -> Dict[str, Any]:
        """Load profile configuration from profiles directory.

        Args:
        ----
            profile_name: Name of the profile
        Returns:
            Dict containing profile configuration

        """
        profiles_dir = os.path.join(self.project_dir, "profiles")
        profile_path = os.path.join(profiles_dir, f"{profile_name}.yml")
        logger.debug(f"Loading profile from: {profile_path}")
        if not os.path.exists(profile_path):
            logger.warning(f"Profile not found at {profile_path}")
            return {}
        with open(profile_path, "r") as f:
            profile = yaml.safe_load(f)
            logger.debug(f"Loaded profile configuration: {profile}")

            # Validate profile structure
            validated_profile = self._validate_profile_structure(
                profile, profile_name, profile_path
            )
            return validated_profile or {}

    def _validate_profile_structure(
        self, profile: Dict[str, Any], profile_name: str, profile_path: str
    ) -> Dict[str, Any]:
        """Validate profile structure and provide helpful error messages.

        Args:
        ----
            profile: The loaded profile configuration
            profile_name: Name of the profile being loaded
            profile_path: Path to the profile file

        Returns:
        -------
            Validated and normalized profile configuration

        Raises:
        ------
            ValueError: If profile structure is invalid
        """
        if not profile:
            return {}

        # Handle deprecated nested format (profile_name as wrapper)
        profile = self._handle_deprecated_nested_format(
            profile, profile_name, profile_path
        )

        # Check if this is a connector-only profile (legacy format for connect tests)
        if self._is_connector_only_profile(profile):
            logger.debug(
                f"Profile '{profile_name}' appears to be connector-only format"
            )
            return profile

        # Validate full SQLFlow profile structure
        errors = self._validate_sqlflow_profile(profile, profile_path)

        if errors:
            self._raise_validation_error(errors, profile_path)

        logger.debug(f"Profile '{profile_name}' validation passed")
        return profile

    def _handle_deprecated_nested_format(
        self, profile: Dict[str, Any], profile_name: str, profile_path: str
    ) -> Dict[str, Any]:
        """Handle deprecated nested profile format."""
        if profile_name in profile:
            logger.warning(
                f"Profile '{profile_name}' uses deprecated nested format. "
                f"Please update {profile_path} to remove the '{profile_name}:' wrapper."
            )
            # Auto-extract the nested profile for backward compatibility
            nested_profile = profile[profile_name]
            logger.debug(f"Extracted nested profile configuration: {nested_profile}")
            return nested_profile
        return profile

    def _is_connector_only_profile(self, profile: Dict[str, Any]) -> bool:
        """Check if this is a legacy connector-only profile."""
        # If it has engines section, it's a full SQLFlow profile
        if "engines" in profile:
            return False

        # If it has any SQLFlow-specific top-level keys, it's a SQLFlow profile
        sqlflow_keys = {
            "log_level",
            "log_file",
            "module_log_levels",
            "variables",
            "connectors",
        }
        if any(key in profile for key in sqlflow_keys):
            return False

        # If all top-level keys are string names (potential connector names),
        # treat as connector-only profile even if some are malformed
        if not profile:
            return False

        # Consider it a connector profile if all keys are strings and values are dicts
        # (even if they're missing the 'type' field - let the connector validation handle that)
        return all(
            isinstance(key, str) and isinstance(value, dict)
            for key, value in profile.items()
        )

    def _validate_sqlflow_profile(
        self, profile: Dict[str, Any], profile_path: str
    ) -> list:
        """Validate full SQLFlow profile structure."""
        errors = []

        # Check for engines section
        if "engines" not in profile:
            errors.append("Missing required 'engines' section")
            return errors  # Can't validate further without engines section

        engines = profile["engines"]
        if not isinstance(engines, dict):
            errors.append("'engines' section must be a dictionary")
            return errors

        # Validate DuckDB engine if present
        if "duckdb" in engines:
            errors.extend(self._validate_duckdb_config(engines["duckdb"], profile_path))

        # Validate variables section if present
        if "variables" in profile:
            variables = profile["variables"]
            if not isinstance(variables, dict):
                errors.append("'variables' section must be a dictionary")

        return errors

    def _validate_duckdb_config(self, duckdb_config: Any, profile_path: str) -> list:
        """Validate DuckDB configuration."""
        errors = []

        if not isinstance(duckdb_config, dict):
            errors.append("'engines.duckdb' must be a dictionary")
            return errors

        # Validate DuckDB mode
        mode = duckdb_config.get("mode", "memory")
        if mode not in ["memory", "persistent"]:
            errors.append(
                f"Invalid DuckDB mode '{mode}'. Must be 'memory' or 'persistent'"
            )

        # Handle deprecated database_path field
        if "database_path" in duckdb_config:
            logger.warning(
                f"Profile uses deprecated 'database_path' field. "
                f"Please rename it to 'path' in {profile_path}"
            )
            # Auto-fix for backward compatibility
            if "path" not in duckdb_config:
                duckdb_config["path"] = duckdb_config["database_path"]
                del duckdb_config["database_path"]
                logger.debug("Auto-renamed 'database_path' to 'path'")

        # Validate persistent mode requirements
        if mode == "persistent":
            if "path" not in duckdb_config:
                errors.append(
                    f"DuckDB persistent mode requires 'path' field. "
                    f"Add 'path: \"path/to/database.duckdb\"' to engines.duckdb in {profile_path}"
                )
            elif not duckdb_config["path"]:
                errors.append(
                    f"DuckDB persistent mode 'path' field cannot be empty. "
                    f"Specify a valid database file path in {profile_path}"
                )

        return errors

    def _raise_validation_error(self, errors: list, profile_path: str) -> None:
        """Raise a formatted validation error."""
        error_msg = f"Invalid profile structure in '{profile_path}':\n"
        for i, error in enumerate(errors, 1):
            error_msg += f"  {i}. {error}\n"
        error_msg += "\nProfile format should be:\n"
        error_msg += "engines:\n"
        error_msg += "  duckdb:\n"
        error_msg += "    mode: memory|persistent\n"
        error_msg += '    path: "path/to/db.duckdb"  # Required for persistent mode\n'
        error_msg += "variables:  # Optional\n"
        error_msg += "  key: value\n"
        error_msg += "\nSee documentation for complete profile specification."
        raise ValueError(error_msg)

    def _configure_logging_from_profile(self) -> None:
        """Configure logging based on profile settings."""
        # Get log level from profile
        log_level_str = self.profile.get("log_level", "info").lower()

        # Convert string level to boolean flags for configure_logging
        verbose = log_level_str == "debug"
        quiet = log_level_str in ["warning", "error", "critical"]

        # Configure logging with these settings
        configure_logging(verbose=verbose, quiet=quiet)

        # Now manually set module-specific log levels if specified
        if "module_log_levels" in self.profile:
            import logging

            module_levels = self.profile["module_log_levels"]
            for module_name, level_str in module_levels.items():
                # Convert string level to logging level constant
                level = getattr(logging, level_str.upper(), logging.INFO)
                module_logger = logging.getLogger(module_name)
                module_logger.setLevel(level)

        logger.debug(f"Configured logging from profile with level: {log_level_str}")

    def get_pipeline_path(self, pipeline_name: str) -> str:
        """Get the full path to a pipeline file.

        Args:
        ----
            pipeline_name: Name of the pipeline
        Returns:
            Full path to the pipeline file

        """
        pipelines_dir = self.profile.get("paths", {}).get("pipelines", "pipelines")
        return os.path.join(self.project_dir, pipelines_dir, f"{pipeline_name}.sf")

    def get_profile(self) -> Dict[str, Any]:
        """Get the loaded profile configuration.

        Returns
        -------
            Dict containing profile configuration

        """
        return self.profile

    def get_path(self, path_type: str) -> Optional[str]:
        """Get a path from the profile configuration.

        Args:
        ----
            path_type: Type of path to get (e.g. 'pipelines', 'models', etc.)

        Returns:
        -------
            Path if found, None otherwise

        """
        return self.profile.get("paths", {}).get(path_type)

    @staticmethod
    def init(project_dir: str, project_name: str) -> "Project":
        """Initialize a new SQLFlow project.

        Args:
        ----
            project_dir: Directory to create the project in
            project_name: Name of the project

        Returns:
        -------
            New Project instance

        """
        logger.debug(
            f"Initializing new project at {project_dir} with name {project_name}"
        )
        # Only create directories for implemented features
        os.makedirs(os.path.join(project_dir, "pipelines"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "profiles"), exist_ok=True)
        # Create data directory for input files
        os.makedirs(os.path.join(project_dir, "data"), exist_ok=True)

        # Only create a default profile, not sqlflow.yml
        default_profile = {
            "engines": {
                "duckdb": {
                    # Default to memory mode for quick development
                    # Options:
                    # - "memory": Uses in-memory database that doesn't persist
                    # - "persistent": Saves data to disk at the path specified below
                    "mode": "memory",
                    # Memory limit for DuckDB (applies to both modes)
                    "memory_limit": "2GB",
                    # Path for persistent database file
                    # This is required when mode is "persistent"
                    # Recommended to use an absolute path to avoid confusion
                    # Example:
                    # "path": "/absolute/path/to/project/data/sqlflow.duckdb"
                    # Or relative path:
                    # "path": "data/sqlflow.duckdb"
                }
            },
            # Add default logging configuration
            "log_level": "info",
            "module_log_levels": {
                "sqlflow.core.engines": "info",
                "sqlflow.connectors": "info",
            },
            # Add more default keys as needed
        }

        # Create a profile with persistent mode example (commented out)
        persistent_profile = {
            "engines": {
                "duckdb": {
                    # Use persistent mode that saves to disk
                    "mode": "persistent",
                    # Path for the database file (required for persistent mode)
                    "path": "data/sqlflow_prod.duckdb",
                    # Memory limit for DuckDB
                    "memory_limit": "4GB",
                }
            },
            # Production logging configuration
            "log_level": "warning",
            "module_log_levels": {
                "sqlflow.core.engines": "info",
                "sqlflow.connectors": "info",
            },
        }

        # Create profiles directory
        profiles_dir = os.path.join(project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        # Create data directory for database files
        data_dir = os.path.join(project_dir, "data")
        os.makedirs(data_dir, exist_ok=True)

        # Write dev profile
        dev_profile_path = os.path.join(profiles_dir, "dev.yml")
        logger.debug(f"Writing initial dev profile to {dev_profile_path}")
        with open(dev_profile_path, "w") as f:
            yaml.dump(default_profile, f, default_flow_style=False)

        # Write prod profile
        prod_profile_path = os.path.join(profiles_dir, "prod.yml")
        logger.debug(f"Writing initial prod profile to {prod_profile_path}")
        with open(prod_profile_path, "w") as f:
            yaml.dump(persistent_profile, f, default_flow_style=False)

        # Add README explaining profiles
        readme_path = os.path.join(profiles_dir, "README.md")
        with open(readme_path, "w") as f:
            f.write(
                """# SQLFlow Profiles

This directory contains profile configuration files for different environments.

## Available Profiles

- `dev.yml`: Development profile with in-memory database (fast, no persistence)
- `prod.yml`: Production profile with persistent database (slower, but data persists)

## DuckDB Persistence

SQLFlow uses DuckDB as its execution engine. There are two modes:

1. **Memory Mode** (`mode: memory`):
   - Faster execution
   - Data is lost when SQLFlow exits
   - Good for development and testing

2. **Persistent Mode** (`mode: persistent`):
   - Data is saved to disk at the specified path
   - Data persists between SQLFlow runs
   - Required for production use

To use persistent mode, make sure to set:
```yaml
engines:
  duckdb:
    mode: persistent
    path: path/to/database.duckdb  # Required for persistent mode
```

The `path` can be absolute or relative to the project directory.
"""
            )

        logger.debug("Project initialization complete.")
        return Project(project_dir)

    def get_config(self) -> Dict[str, Any]:
        """Get the project configuration.

        Returns
        -------
            Dict containing project configuration

        """
        return self.profile
