"""Simple factory functions with type safety for SQLFlow CLI.

Following the technical design principle of replacing complex DI with simple,
type-safe factory functions that are easy to understand and test.
"""

import os
from functools import lru_cache
from typing import Any, Dict, Optional, Protocol

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner_main import Planner
from sqlflow.logging import get_logger
from sqlflow.project import Project

logger = get_logger(__name__)


# Type-safe protocols for factory functions
class ProjectProtocol(Protocol):
    """Protocol for project objects."""

    def get_profile(self) -> Dict[str, Any]:
        """Get the profile configuration."""
        ...
    
    def get_pipeline_path(self, pipeline_name: str) -> str:
        """Get the path to a pipeline file."""
        ...

    @property
    def profile(self) -> Dict[str, Any]:
        """Get the profile configuration."""
        ...

    @property
    def project_dir(self) -> str:
        """Get the project directory."""
        ...


class ExecutorProtocol(Protocol):
    """Protocol for executor objects."""

    def execute(
        self, plan: list[Dict[str, Any]], variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a pipeline plan."""
        ...

    @property
    def variables(self) -> Dict[str, Any]:
        """Get executor variables."""
        ...


class PlannerProtocol(Protocol):
    """Protocol for planner objects."""

    def create_plan(
        self,
        pipeline,
        variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
    ) -> list[Dict[str, Any]]:
        """Create an execution plan."""
        ...


# Simple factory functions with caching for performance
@lru_cache(maxsize=32)
def get_project(
    project_dir: Optional[str] = None, profile_name: str = "dev"
) -> Optional[ProjectProtocol]:
    """Get project instance with auto-discovery and caching.

    Args:
        project_dir: Directory path for the project, None for auto-discovery
        profile_name: Name of the profile to load

    Returns:
        Project instance or None if not found

    Raises:
        ValueError: If profile validation fails
    """
    # Auto-discover project directory if not provided
    if not project_dir:
        current_dir = os.getcwd()
        if os.path.exists(os.path.join(current_dir, "profiles")):
            project_dir = current_dir
            logger.debug(f"Auto-discovered project directory: {project_dir}")
        else:
            logger.debug("No project directory found for auto-discovery")
            return None

    try:
        project = Project(project_dir, profile_name)
        logger.debug(
            f"Created project with profile '{profile_name}' from {project_dir}"
        )
        return project
    except ValueError:
        # Re-raise profile validation errors
        raise
    except Exception as e:
        logger.debug(f"Could not create project from {project_dir}: {e}")
        return None


def create_executor(
    profile_name: str = "dev",
    project_dir: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> ExecutorProtocol:
    """Create configured executor with simple parameters.

    Args:
        profile_name: Name of the profile to use
        project_dir: Directory path for the project, None for auto-discovery
        variables: Additional variables to set on the executor

    Returns:
        Configured LocalExecutor instance

    Raises:
        ValueError: If profile validation fails
    """
    # Create executor with auto-discovery
    executor = LocalExecutor(project_dir=project_dir, profile_name=profile_name)

    # Set additional variables if provided
    if variables:
        executor.variables.update(variables)
        logger.debug(f"Updated executor variables: {list(variables.keys())}")

    logger.debug(f"Created executor with profile '{profile_name}'")
    return executor


@lru_cache(maxsize=8)
def get_planner() -> PlannerProtocol:
    """Get planner instance with caching for performance.

    Returns:
        Planner instance with default configuration
    """
    planner = Planner()
    logger.debug("Created default planner instance")
    return planner


def create_planner_with_config(config=None) -> PlannerProtocol:
    """Create planner with optional configuration.

    Args:
        config: Optional planner configuration

    Returns:
        Configured planner instance
    """
    if config:
        planner = Planner(config=config)
        logger.debug("Created planner with custom configuration")
    else:
        planner = get_planner()  # Use cached default
        logger.debug("Created planner with default configuration")

    return planner


# Convenience functions for common CLI patterns
def load_project_for_command(
    profile_name: Optional[str] = None,
) -> Optional[ProjectProtocol]:
    """Load project for CLI command with sensible defaults.

    Args:
        profile_name: Optional profile name, defaults to "dev"

    Returns:
        Project instance or None if not found

    This function implements the common CLI pattern of:
    1. Use provided profile name or default to "dev"
    2. Auto-discover project directory from current working directory
    3. Handle errors gracefully for CLI display
    """
    try:
        return get_project(profile_name=profile_name or "dev")
    except ValueError:
        # Profile validation errors should be re-raised for CLI display
        raise
    except Exception as e:
        logger.debug(f"Unexpected error loading project: {e}")
        return None


def create_executor_for_command(
    profile_name: Optional[str] = None, variables: Optional[Dict[str, Any]] = None
) -> ExecutorProtocol:
    """Create executor for CLI command with sensible defaults.

    Args:
        profile_name: Optional profile name, defaults to "dev"
        variables: Optional variables to set

    Returns:
        Configured executor instance

    This function implements the common CLI pattern of:
    1. Use provided profile name or default to "dev"
    2. Auto-discover project directory
    3. Set additional variables if provided
    4. Handle UDF discovery automatically
    """
    return create_executor(
        profile_name=profile_name or "dev",
        project_dir=None,  # Auto-discover
        variables=variables,
    )


# Clear cache function for testing
def clear_factory_cache() -> None:
    """Clear all factory caches.

    This is useful for testing to ensure fresh instances.
    """
    get_project.cache_clear()
    get_planner.cache_clear()
    logger.debug("Cleared factory caches")
