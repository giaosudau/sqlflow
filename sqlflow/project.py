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
            return profile or {}

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
            pipeline_name: Name of the pipeline
        Returns:
            Full path to the pipeline file
        """
        pipelines_dir = self.profile.get("paths", {}).get("pipelines", "pipelines")
        return os.path.join(self.project_dir, pipelines_dir, f"{pipeline_name}.sf")

    def get_profile(self) -> Dict[str, Any]:
        """Get the loaded profile configuration.
        Returns:
            Dict containing profile configuration
        """
        return self.profile

    def get_path(self, path_type: str) -> Optional[str]:
        """Get a path from the profile configuration.

        Args:
            path_type: Type of path to get (e.g. 'pipelines', 'models', etc.)
        Returns:
            Path if found, None otherwise
        """
        return self.profile.get("paths", {}).get(path_type)

    @staticmethod
    def init(project_dir: str, project_name: str) -> "Project":
        """Initialize a new SQLFlow project.

        Args:
            project_dir: Directory to create the project in
            project_name: Name of the project

        Returns:
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

        Returns:
            Dict containing project configuration
        """
        return self.profile
