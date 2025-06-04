"""Environment variable utilities for SQLFlow.

This module provides utilities for loading environment variables from .env files
in SQLFlow project directories, following the standard .env file conventions.
"""

import os
from pathlib import Path
from typing import Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


def find_project_root(start_path: Optional[str] = None) -> Optional[Path]:
    """Find the SQLFlow project root by looking for profiles directory.

    Args:
    ----
        start_path: Path to start searching from (defaults to current directory)

    Returns:
    -------
        Path to the project root, or None if not found
    """
    if start_path is None:
        start_path = os.getcwd()

    current = Path(start_path).resolve()

    # Search up the directory tree for a profiles directory
    for parent in [current, *current.parents]:
        profiles_dir = parent / "profiles"
        if profiles_dir.exists() and profiles_dir.is_dir():
            logger.debug(f"Found SQLFlow project root at: {parent}")
            return parent

    logger.debug("No SQLFlow project root found")
    return None


def load_dotenv_file(project_root: Path) -> bool:
    """Load .env file from the project root if it exists.

    Args:
    ----
        project_root: Path to the project root directory

    Returns:
    -------
        True if .env file was loaded, False otherwise
    """
    try:
        from dotenv import load_dotenv

        env_file = project_root / ".env"

        if env_file.exists():
            # Load the .env file and override existing environment variables
            loaded = load_dotenv(env_file, override=True)
            if loaded:
                logger.debug(f"Loaded environment variables from: {env_file}")
                return True
            else:
                logger.debug(f"Failed to load .env file: {env_file}")
                return False
        else:
            logger.debug(f"No .env file found at: {env_file}")
            return False

    except ImportError:
        logger.warning("python-dotenv not available, cannot load .env files")
        return False
    except Exception as e:
        logger.warning(f"Error loading .env file: {e}")
        return False


def setup_environment(start_path: Optional[str] = None) -> bool:
    """Setup environment by loading .env file from SQLFlow project root.

    This function:
    1. Finds the SQLFlow project root (directory containing 'profiles' folder)
    2. Loads .env file from the project root if it exists
    3. Makes environment variables available to the entire process

    Args:
    ----
        start_path: Path to start searching from (defaults to current directory)

    Returns:
    -------
        True if .env file was found and loaded, False otherwise
    """
    project_root = find_project_root(start_path)

    if project_root is None:
        logger.debug("No SQLFlow project found, skipping .env file loading")
        return False

    return load_dotenv_file(project_root)


def get_env_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """Get an environment variable with optional default.

    Args:
    ----
        name: Environment variable name
        default: Default value if variable is not set

    Returns:
    -------
        Environment variable value or default
    """
    return os.environ.get(name, default)


def list_env_vars(prefix: Optional[str] = None) -> dict:
    """List environment variables, optionally filtered by prefix.

    Args:
    ----
        prefix: Optional prefix to filter variables (e.g., "SQLFLOW_")

    Returns:
    -------
        Dictionary of environment variables
    """
    if prefix:
        return {k: v for k, v in os.environ.items() if k.startswith(prefix)}
    return dict(os.environ)
