"""Project management for SQLFlow."""

import logging
import os
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class Project:
    """Manages SQLFlow project structure and configuration."""

    def __init__(self, project_dir: str):
        """Initialize a Project instance.

        Args:
            project_dir: Path to the project directory
        """
        print(f"DEBUG: Project initialized with project_dir: {project_dir}")
        self.project_dir = project_dir
        self.config = self._load_config()
        self.profile = self._load_profile()

    def _load_config(self) -> Dict[str, Any]:
        """Load project configuration from sqlflow.yml.

        Returns:
            Dict containing project configuration
        """
        config_path = os.path.join(self.project_dir, "sqlflow.yml")
        print(f"DEBUG: Attempting to load project configuration from: {config_path}")
        logger.info(f"Loading project configuration from: {config_path}")
        if not os.path.exists(config_path):
            print(f"DEBUG: No sqlflow.yml found at {config_path}")
            logger.warning(f"No sqlflow.yml found at {config_path}")
            return {}

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            print(f"DEBUG: Loaded project configuration: {config}")
            logger.info(f"Loaded project configuration: {config}")
            return config or {}

    def _load_profile(self) -> Dict[str, Any]:
        """Load profile configuration.

        Returns:
            Dict containing profile configuration
        """
        profile_name = self.config.get("default_profile", "default")
        profiles_dir = os.path.join(
            self.project_dir,
            self.config.get("paths", {}).get("profiles", "profiles"),
        )
        profile_path = os.path.join(profiles_dir, f"{profile_name}.yml")

        print(f"DEBUG: Attempting to load profile from: {profile_path}")
        logger.info(f"Loading profile from: {profile_path}")
        if not os.path.exists(profile_path):
            print(f"DEBUG: No profile found at {profile_path}, using project config")
            logger.warning(f"No profile found at {profile_path}, using project config")
            return self.config

        with open(profile_path, "r") as f:
            profile = yaml.safe_load(f)
            print(f"DEBUG: Loaded profile configuration: {profile}")
            logger.info(f"Loaded profile configuration: {profile}")
            return profile or {}

    def get_pipeline_path(self, pipeline_name: str) -> str:
        """Get the full path to a pipeline file.

        Args:
            pipeline_name: Name of the pipeline (with or without .sf extension)

        Returns:
            Full path to the pipeline file
        """
        pipelines_dir = self.config.get("paths", {}).get("pipelines", "pipelines")

        if os.path.isabs(pipeline_name):
            if os.path.exists(pipeline_name):
                return pipeline_name
            if not pipeline_name.endswith(".sf"):
                full_path = f"{pipeline_name}.sf"
                if os.path.exists(full_path):
                    return full_path

        if pipeline_name.endswith(".sf"):
            return os.path.join(self.project_dir, pipelines_dir, pipeline_name)

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

        print(f"DEBUG: Getting profile {profile_name} from path: {profile_path}")
        if os.path.exists(profile_path):
            with open(profile_path, "r") as f:
                profile_data = yaml.safe_load(f)
                print(f"DEBUG: Retrieved profile data: {profile_data}")
                return profile_data or {}
        print(f"DEBUG: Profile file not found at {profile_path}")
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
        print(
            f"DEBUG: Initializing new project at {project_dir} with name {project_name}"
        )
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

        config_path = os.path.join(project_dir, "sqlflow.yml")
        print(f"DEBUG: Writing initial sqlflow.yml to {config_path}")
        with open(config_path, "w") as f:
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

        profiles_dir = os.path.join(project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        profile_path = os.path.join(profiles_dir, "default.yml")
        print(f"DEBUG: Writing initial default profile to {profile_path}")
        with open(profile_path, "w") as f:
            yaml.dump(default_profile, f, default_flow_style=False)

        print("DEBUG: Project initialization complete.")
        return Project(project_dir)

    def get_config(self) -> Dict[str, Any]:
        """Get the project configuration.

        Returns:
            Dict containing project configuration
        """
        return self.config

    def get_path(self, path_type: str) -> Optional[str]:
        """Get a path from the project configuration.

        Args:
            path_type: Type of path to get (e.g. 'pipelines', 'models', etc.)

        Returns:
            Path if found, None otherwise
        """
        return self.config.get("paths", {}).get(path_type)
