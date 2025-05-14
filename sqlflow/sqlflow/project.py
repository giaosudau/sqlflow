"""Project management for SQLFlow."""

import os
import yaml
from typing import Dict, Any, Optional


class Project:
    """Manages SQLFlow project structure and configuration."""

    def __init__(self, project_dir: str):
        """Initialize a Project instance.

        Args:
            project_dir: Path to the project directory
        """
        self.project_dir = project_dir
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load project configuration from sqlflow.yml.

        Returns:
            Dict containing project configuration
        """
        config_path = os.path.join(self.project_dir, "sqlflow.yml")
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                return yaml.safe_load(f) or {}
        return {}

    def get_pipeline_path(self, pipeline_name: str) -> str:
        """Get the full path to a pipeline file.

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            Full path to the pipeline file
        """
        pipelines_dir = self.config.get("paths", {}).get("pipelines", "pipelines")
        return os.path.join(self.project_dir, pipelines_dir, f"{pipeline_name}.sf")

    def get_profile(self, profile_name: Optional[str] = None) -> Dict[str, Any]:
        """Get a profile configuration.

        Args:
            profile_name: Name of the profile, or None for default

        Returns:
            Dict containing profile configuration
        """
        if profile_name is None:
            profile_name = self.config.get("default_profile", "default")

        profiles_dir = self.config.get("paths", {}).get("profiles", "profiles")
        profile_path = os.path.join(
            self.project_dir, profiles_dir, f"{profile_name}.yml"
        )

        if os.path.exists(profile_path):
            with open(profile_path, "r") as f:
                return yaml.safe_load(f) or {}
        return {}

    @staticmethod
    def init(project_dir: str, project_name: str) -> "Project":
        """Initialize a new SQLFlow project.

        Args:
            project_dir: Directory to create the project in
            project_name: Name of the project

        Returns:
            New Project instance
        """
        os.makedirs(os.path.join(project_dir, "pipelines"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "models"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "macros"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "connectors"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "profiles"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "tests"), exist_ok=True)

        config = {
            "name": project_name,
            "version": "0.1.0",
            "default_profile": "default",
            "paths": {
                "pipelines": "pipelines",
                "models": "models",
                "macros": "macros",
                "connectors": "connectors",
                "profiles": "profiles",
            },
        }

        with open(os.path.join(project_dir, "sqlflow.yml"), "w") as f:
            yaml.dump(config, f, default_flow_style=False)

        default_profile = {
            "target": "duckdb",
            "outputs": {
                "duckdb": {
                    "type": "duckdb",
                    "path": "target/default.db",
                }
            },
        }

        os.makedirs(os.path.join(project_dir, "profiles"), exist_ok=True)
        with open(os.path.join(project_dir, "profiles", "default.yml"), "w") as f:
            yaml.dump(default_profile, f, default_flow_style=False)

        return Project(project_dir)
