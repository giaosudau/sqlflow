"""Simple factory functions for CLI dependencies.

Raymond Hettinger: Replace complex protocols with simple cached functions.
Functions are first-class citizens in Python.
"""

import os
from functools import lru_cache
from typing import Any, Dict, Optional

from sqlflow.cli.errors import ProfileNotFoundError, ProjectNotFoundError
from sqlflow.core.executors import get_executor
from sqlflow.core.planner_main import Planner
from sqlflow.logging import get_logger
from sqlflow.project import Project

logger = get_logger(__name__)


# Raymond Hettinger: Simple cached factory functions
@lru_cache(maxsize=None)
def get_planner() -> Planner:
    """Get planner instance - cached for performance.

    Raymond Hettinger: Built-in, tested, optimized.
    """
    return Planner()


def load_project_for_command(
    profile_name: Optional[str] = None,
) -> Optional[Project]:
    """Load project configuration for CLI commands.

    Args:
        profile_name: Optional profile name to load (defaults to 'dev')

    Returns:
        Project: Loaded project configuration or None if not found

    Raises:
        ProfileNotFoundError: If profile cannot be found
        ProjectNotFoundError: If no SQLFlow project found
    """
    try:
        current_dir = os.getcwd()
        profile = profile_name or "dev"

        # Check if we're in a SQLFlow project
        profiles_dir = os.path.join(current_dir, "profiles")
        if not os.path.exists(profiles_dir):
            raise ProjectNotFoundError(current_dir)

        # Try to load the project with specified profile
        try:
            project = Project(current_dir, profile_name=profile)
            logger.debug(f"Loaded project with profile '{profile}' from {current_dir}")
            return project
        except FileNotFoundError:
            # Profile not found
            raise ProfileNotFoundError(profile, [profiles_dir])

    except (ProfileNotFoundError, ProjectNotFoundError):
        # Re-raise CLI-specific errors
        raise
    except Exception as e:
        logger.error(f"Failed to load project: {e}")
        raise ProjectNotFoundError(current_dir)


def create_executor_for_command(
    profile_name: Optional[str] = None, variables: Optional[Dict[str, Any]] = None
) -> Any:
    """Create executor instance for CLI commands.

    Uses V2 executor system when available with automatic fallback to V1.
    This provides seamless transition to the new architecture while maintaining
    complete backward compatibility.

    Args:
        profile_name: Optional profile name for configuration
        variables: Optional variables for execution context

    Returns:
        Any: Configured executor instance

    Raises:
        ProfileNotFoundError: If profile cannot be found
        ProjectNotFoundError: If no SQLFlow project found
    """
    try:
        # Load project to get configuration
        project = load_project_for_command(profile_name)
        if not project:
            raise ProjectNotFoundError(os.getcwd())

        # Raymond Hettinger: Simple, direct approach
        executor = get_executor(
            project_dir=project.project_dir,
            profile_name=profile_name or "dev",
        )
        logger.debug(f"Created executor for profile '{profile_name or 'dev'}'")
        return executor

    except Exception as e:
        logger.error(f"Failed to create executor: {e}")
        raise


def get_available_pipelines(project: Optional[Project] = None) -> list:
    """Get list of available pipelines in the project.

    Args:
        project: Optional project instance (will load current if not provided)

    Returns:
        list: List of available pipeline names

    Raises:
        ProjectNotFoundError: If no SQLFlow project found
    """
    try:
        if not project:
            project = load_project_for_command()
            if not project:
                return []

        pipelines_dir = os.path.join(project.project_dir, "pipelines")
        if not os.path.exists(pipelines_dir):
            return []

        # Find all .sf files
        pipelines = [f[:-3] for f in os.listdir(pipelines_dir) if f.endswith(".sf")]

        return sorted(pipelines)

    except Exception as e:
        logger.debug(f"Failed to get available pipelines: {e}")
        return []


def get_available_profiles(project_dir: Optional[str] = None) -> list:
    """Get list of available profiles in the project.

    Args:
        project_dir: Optional project directory (defaults to current)

    Returns:
        list: List of available profile names
    """
    try:
        current_dir = project_dir or os.getcwd()
        profiles_dir = os.path.join(current_dir, "profiles")

        if not os.path.exists(profiles_dir):
            return []

        # Find all .yml and .yaml files
        profiles = [
            f.rsplit(".", 1)[0]
            for f in os.listdir(profiles_dir)
            if f.endswith((".yml", ".yaml"))
        ]

        return sorted(profiles)

    except Exception as e:
        logger.debug(f"Failed to get available profiles: {e}")
        return []
