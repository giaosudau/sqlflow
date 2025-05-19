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

        logger.info(f"Loaded profile: {profile_name}")

    def _load_profile(self, profile_name: str) -> Dict[str, Any]:
        """Load profile configuration from profiles directory.

        Args:
            profile_name: Name of the profile
        Returns:
            Dict containing profile configuration
        """
        profiles_dir = os.path.join(self.project_dir, "profiles")
        profile_path = os.path.join(profiles_dir, f"{profile_name}.yml")
        logger.info(f"Loading profile from: {profile_path}")
        if not os.path.exists(profile_path):
            logger.warning(f"Profile not found at {profile_path}")
            return {}
        with open(profile_path, "r") as f:
            profile = yaml.safe_load(f)
            logger.info(f"Loaded profile configuration: {profile}")
            return profile or {}

    def _configure_logging_from_profile(self) -> None:
        """Configure logging based on profile settings."""
        # Build logging configuration from profile
        log_config = {}

        # Extract log level if present
        if "log_level" in self.profile:
            log_config["log_level"] = self.profile["log_level"]

        # Extract module-specific log levels if present
        if "module_log_levels" in self.profile:
            log_config["module_log_levels"] = self.profile["module_log_levels"]

        # Configure logging with these settings
        if log_config:
            configure_logging(config=log_config)
            logger.debug(f"Configured logging from profile with: {log_config}")

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
        os.makedirs(os.path.join(project_dir, "pipelines"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "models"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "macros"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "connectors"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "profiles"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "tests"), exist_ok=True)

        # Only create a default profile, not sqlflow.yml
        default_profile = {
            "engines": {
                "duckdb": {
                    "mode": "memory",
                    "memory_limit": "2GB",
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
        profiles_dir = os.path.join(project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        profile_path = os.path.join(profiles_dir, "dev.yml")
        logger.debug(f"Writing initial dev profile to {profile_path}")
        with open(profile_path, "w") as f:
            yaml.dump(default_profile, f, default_flow_style=False)

        logger.debug("Project initialization complete.")
        return Project(project_dir)

    def get_config(self) -> Dict[str, Any]:
        """Get the project configuration.

        Returns:
            Dict containing project configuration
        """
        return self.profile
