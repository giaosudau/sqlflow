"""
Executor Factory Module

This module provides a factory function for creating executor instances.

V2 Goal: get_executor() returns ExecutionCoordinator (V2) ONLY

Key principles:
- Single implementation: Only V2 ExecutionCoordinator
- No backward compatibility: Clean, simple API
- Fail fast: Clear error messages if V2 not available

Following Raymond Hettinger's Zen of Python:
- "There should be one-- and preferably only one --obvious way to do it"
- "Simple is better than complex"
- "Explicit is better than implicit"
"""

import logging
from typing import Any

from .base_executor import BaseExecutor

logger = logging.getLogger(__name__)

__all__ = ["get_executor", "BaseExecutor"]


def get_executor(**kwargs) -> Any:
    """Create and return executor instance.

    Returns V2 ExecutionCoordinator ONLY - clean, simple, predictable.

    Args:
        **kwargs: Executor configuration parameters

    Returns:
        ExecutionCoordinator: V2 ExecutionCoordinator instance

    Raises:
        ImportError: If V2 executor is not available
    """
    logger.debug("Creating V2 ExecutionCoordinator (only option)")
    try:
        from .v2 import ExecutionCoordinator

        return ExecutionCoordinator(**kwargs)
    except ImportError as e:
        logger.error(f"V2 ExecutionCoordinator not available: {e}")
        raise ImportError(
            f"V2 ExecutionCoordinator required but not available: {e}. "
            "Please check your installation."
        ) from e
