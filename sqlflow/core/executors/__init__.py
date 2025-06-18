"""Enhanced SQLFlow Executors with V2 Production Integration.

Following the Zen of Python:
- Simple is better than complex
- There should be one obvious way to do it
- Explicit is better than implicit

This module provides the main entry point for executor selection with
V2 integration.
"""

from sqlflow.logging import get_logger

# V1 Executors
from .base_executor import BaseExecutor
from .local_executor import LocalExecutor
from .thread_pool_executor import ThreadPoolTaskExecutor

# V2 Production Integration
try:
    from .v2.compatibility_bridge import (
        V1V2CompatibilityBridge,
        create_executor,
    )
    from .v2.feature_flags import (
        disable_v2_for_testing,
        enable_v2_for_testing,
        should_use_v2_executor,
    )

    V2_AVAILABLE = True
except ImportError:
    V2_AVAILABLE = False

    def should_use_v2_executor() -> bool:
        """Fallback function when V2 is not available."""
        return False


logger = get_logger(__name__)


# Main executor factory function - "There should be one obvious way to do it"
def get_executor(**kwargs) -> BaseExecutor:
    """
    Get the appropriate executor based on configuration and feature flags.

    This is the main entry point for all executor creation in SQLFlow.

    Args:
        **kwargs: Executor configuration options

    Returns:
        BaseExecutor: Configured executor instance

    Environment Variables:
        SQLFLOW_ENVIRONMENT: development/staging/production
        SQLFLOW_V2_ENABLED: true/false (production only)
        SQLFLOW_V2_ROLLOUT_PERCENTAGE: 0-100 (gradual rollout)
    """
    if V2_AVAILABLE:
        logger.debug("V2 available, using compatibility bridge")
        return create_executor(**kwargs)
    else:
        logger.debug("V2 not available, using V1 LocalExecutor")
        return LocalExecutor(**kwargs)


# Legacy compatibility functions
def get_local_executor(**kwargs) -> LocalExecutor:
    """Get V1 LocalExecutor directly (for compatibility)."""
    return LocalExecutor(**kwargs)


def get_thread_pool_executor(**kwargs) -> ThreadPoolTaskExecutor:
    """Get ThreadPoolTaskExecutor (for compatibility)."""
    return ThreadPoolTaskExecutor(**kwargs)


# V2 specific functions
def get_v2_executor(**kwargs) -> BaseExecutor:
    """Get V2 executor directly (for testing and comparison)."""
    if V2_AVAILABLE:
        from .v2.orchestrator import LocalOrchestrator

        return LocalOrchestrator(**kwargs)
    else:
        logger.warning("V2 executor requested but not available, using V1")
        return LocalExecutor(**kwargs)


def get_compatibility_bridge(**kwargs) -> BaseExecutor:
    """Get V1/V2 compatibility bridge directly."""
    if V2_AVAILABLE:
        return V1V2CompatibilityBridge(**kwargs)
    else:
        logger.warning("Compatibility bridge requested but V2 not available, using V1")
        return LocalExecutor(**kwargs)


def is_v2_execution_enabled() -> bool:
    """Check if V2 execution is enabled for current environment."""
    if V2_AVAILABLE:
        return should_use_v2_executor()
    else:
        return False


# Testing utilities
def enable_v2_for_testing_if_available():
    """Enable V2 for testing if available."""
    if V2_AVAILABLE:
        enable_v2_for_testing()
        logger.info("V2 enabled for testing")
    else:
        logger.warning("Cannot enable V2 for testing - V2 not available")


def disable_v2_for_testing_if_available():
    """Disable V2 for testing if available."""
    if V2_AVAILABLE:
        disable_v2_for_testing()
        logger.info("V2 disabled for testing")


# Module exports - explicit is better than implicit
__all__ = [
    # Main factory function
    "get_executor",
    # Base classes
    "BaseExecutor",
    # V1 Executors
    "LocalExecutor",
    "ThreadPoolTaskExecutor",
    "get_local_executor",
    "get_thread_pool_executor",
    # V2 Integration (if available)
    "get_v2_executor",
    "get_compatibility_bridge",
    "is_v2_execution_enabled",
    # Testing utilities
    "enable_v2_for_testing_if_available",
    "disable_v2_for_testing_if_available",
]

# Add V2-specific exports if available
if V2_AVAILABLE:
    __all__.extend(
        [
            "V1V2CompatibilityBridge",
            "should_use_v2_executor",
        ]
    )


# Initialize logging for production integration
logger.info(f"SQLFlow Executors initialized - V2 Available: {V2_AVAILABLE}")
if V2_AVAILABLE:
    logger.info("🚀 V2 Production Integration Active")
else:
    logger.info("🔄 V1 Executor Mode")
