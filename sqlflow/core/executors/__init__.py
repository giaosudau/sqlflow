"""Simple SQLFlow Executors - V2 Only Implementation.

Following the Zen of Python:
- Simple is better than complex
- There should be one obvious way to do it
- Explicit is better than implicit

V2 Goal: get_executor() returns LocalOrchestrator (V2) ONLY
Clean Architecture: Single responsibility, dependency inversion
"""

import os

from sqlflow.logging import get_logger

# Base executor
from .base_executor import BaseExecutor

# V2 Executors ONLY
# Phase 6: Monitoring integration
from .monitoring import get_monitor
from .thread_pool_executor import ThreadPoolTaskExecutor

logger = get_logger(__name__)


def get_executor(**kwargs) -> BaseExecutor:
    """
    Get the default executor for SQLFlow operations.

    Returns V2 LocalOrchestrator ONLY - clean, simple, predictable.
    Following the Zen of Python: "There should be one obvious way to do it."

    Args:
        profile_name: Profile name for configuration (e.g., "dev", "prod")
        project_dir: Project directory path for configuration
        **kwargs: Additional executor configuration options

    Returns:
        BaseExecutor: V2 LocalOrchestrator instance
    """
    logger.debug("Creating V2 LocalOrchestrator (only option)")
    try:
        from .v2.orchestrator import LocalOrchestrator

        return LocalOrchestrator(**kwargs)
    except ImportError as e:
        logger.error(f"V2 LocalOrchestrator not available: {e}")
        raise ImportError(
            f"V2 LocalOrchestrator required but not available: {e}. "
            "Ensure V2 implementation is properly installed."
        )


# V2-only exports
__all__ = [
    "BaseExecutor",
    "ThreadPoolTaskExecutor",
    "get_executor",
    "get_monitor",
]


# Phase 6: Initialize monitoring
logger.info("🚀 SQLFlow Executors: V2 Default, V1 Rollback, Monitoring Enabled")
