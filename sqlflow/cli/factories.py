"""Simple factory functions for CLI dependencies.

This module implements the simple factory pattern from Task 2.3, replacing
complex DI with type-safe factory functions that are easier to understand
and maintain.
"""

import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Protocol

from sqlflow.cli.errors import ProfileNotFoundError, ProjectNotFoundError
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner_main import Planner
from sqlflow.logging import get_logger
from sqlflow.project import Project

# V2 Executor integration - graceful handling when V2 is not available
try:
    from sqlflow.core.executors import get_executor

    V2_AVAILABLE = True
except ImportError:
    V2_AVAILABLE = False

logger = get_logger(__name__)


# Type-safe protocols for dependency injection
class ProjectProtocol(Protocol):
    """Protocol for project objects used in CLI operations."""

    def get_pipeline_path(self, pipeline_name: str) -> str:
        """Get the path to a pipeline file."""
        ...

    def get_path(self, path_type: str) -> Optional[str]:
        """Get a configured path by type."""
        ...

    @property
    def project_dir(self) -> str:
        """Get the project directory."""
        ...

    @property
    def profile(self) -> Dict[str, Any]:
        """Get the profile configuration."""
        ...


class ExecutorProtocol(Protocol):
    """Protocol for executor objects used in CLI operations."""

    def execute(
        self,
        plan: List[Dict[str, Any]],
        variables: Optional[Dict[str, Any]] = None,
        dependency_resolver: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Execute a list of operations."""
        ...


class PlannerProtocol(Protocol):
    """Protocol for planner objects used in CLI operations."""

    def create_plan(
        self,
        pipeline: Any,
        variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Create an execution plan from a pipeline."""
        ...


@lru_cache(maxsize=None)
def get_planner() -> PlannerProtocol:
    """Get planner instance - cached for performance.

    Returns:
        PlannerProtocol: Planner instance for creating execution plans
    """
    return Planner()


def load_project_for_command(
    profile_name: Optional[str] = None,
) -> Optional[ProjectProtocol]:
    """Load project configuration for CLI commands.

    Args:
        profile_name: Optional profile name to load (defaults to 'dev')

    Returns:
        ProjectProtocol: Loaded project configuration or None if not found

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
            # Profile not found, list available profiles for helpful error
            available_profiles = []
            if os.path.exists(profiles_dir):
                available_profiles = [
                    f[:-4]
                    for f in os.listdir(profiles_dir)
                    if f.endswith((".yml", ".yaml"))
                ]

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
        ExecutorProtocol: Configured executor instance (V2 if available, V1 fallback)

    Raises:
        ProfileNotFoundError: If profile cannot be found
        ProjectNotFoundError: If no SQLFlow project found
    """
    try:
        # Load project to get configuration
        project = load_project_for_command(profile_name)
        if not project:
            raise ProjectNotFoundError(os.getcwd())

        # Create executor with V2/V1 automatic selection
        if V2_AVAILABLE:
            try:
                # Use V2 executor system with feature flag integration
                executor = get_executor(
                    project_dir=project.project_dir,
                    profile_name=profile_name or "dev",
                )
                logger.debug(
                    f"Created V2 executor for profile '{profile_name or 'dev'}'"
                )
                return executor
            except Exception as e:
                logger.warning(f"V2 executor creation failed, falling back to V1: {e}")
                # Fall through to V1 creation

        # Create V1 executor (fallback or when V2 not available)
        executor = LocalExecutor(
            project_dir=project.project_dir,
            profile_name=profile_name or "dev",
        )

        logger.debug(f"Created V1 executor for profile '{profile_name or 'dev'}'")
        return executor

    except Exception as e:
        logger.error(f"Failed to create executor: {e}")
        raise


def get_available_pipelines(project: Optional[ProjectProtocol] = None) -> list:
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
