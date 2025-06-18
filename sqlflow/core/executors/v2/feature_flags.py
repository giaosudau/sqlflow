"""Feature flag system for V2 Executor production integration.

Following the Zen of Python:
- Simple is better than complex
- Explicit is better than implicit
- Readability counts

This module provides a clean, testable feature flag system for gradual V2 rollout.
"""

import hashlib
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class FeatureFlag(Enum):
    """Enumeration of all V2 feature flags.

    Explicit enumeration following 'Explicit is better than implicit'.
    """

    # Core V2 features
    V2_ORCHESTRATOR_ENABLED = "v2.orchestrator.enabled"
    V2_HANDLERS_ENABLED = "v2.handlers.enabled"
    V2_OBSERVABILITY_ENABLED = "v2.observability.enabled"

    # Advanced V2 features
    V2_PARALLEL_EXECUTION = "v2.parallel.execution.enabled"
    V2_STATE_MANAGEMENT = "v2.state.management.enabled"
    V2_PERFORMANCE_MONITORING = "v2.performance.monitoring.enabled"

    # Production features
    V2_GRADUAL_ROLLOUT = "v2.gradual.rollout.enabled"
    V2_AUTOMATIC_FALLBACK = "v2.automatic.fallback.enabled"
    V2_DETAILED_LOGGING = "v2.detailed.logging.enabled"


@dataclass(frozen=True)
class FeatureFlagConfig:
    """Configuration for feature flags with explicit defaults.

    Following 'Simple is better than complex' - straightforward configuration.
    """

    enabled_flags: Set[FeatureFlag] = field(default_factory=set)
    rollout_percentage: float = 0.0  # 0-100, percentage of executions using V2
    environment: str = "development"  # development, staging, production

    def is_enabled(self, flag: FeatureFlag) -> bool:
        """Check if a feature flag is enabled."""
        return flag in self.enabled_flags

    def with_flag_enabled(self, flag: FeatureFlag) -> "FeatureFlagConfig":
        """Return new config with flag enabled."""
        new_flags = self.enabled_flags.copy()
        new_flags.add(flag)
        return FeatureFlagConfig(
            enabled_flags=new_flags,
            rollout_percentage=self.rollout_percentage,
            environment=self.environment,
        )

    def with_flag_disabled(self, flag: FeatureFlag) -> "FeatureFlagConfig":
        """Return new config with flag disabled."""
        new_flags = self.enabled_flags.copy()
        new_flags.discard(flag)
        return FeatureFlagConfig(
            enabled_flags=new_flags,
            rollout_percentage=self.rollout_percentage,
            environment=self.environment,
        )

    def with_rollout_percentage(self, percentage: float) -> "FeatureFlagConfig":
        """Return new config with updated rollout percentage."""
        return FeatureFlagConfig(
            enabled_flags=self.enabled_flags.copy(),
            rollout_percentage=percentage,
            environment=self.environment,
        )


class FeatureFlagManager:
    """Simple, explicit feature flag manager.

    Following the principle 'There should be one obvious way to do it'.
    """

    def __init__(self, config: Optional[FeatureFlagConfig] = None):
        self._config = config or self._create_default_config()
        logger.info(
            f"Feature flags initialized for {self._config.environment} environment"
        )

    def _create_default_config(self) -> FeatureFlagConfig:
        """Create default configuration from environment variables.

        Environment variables provide explicit configuration.
        """
        # Read environment variables
        environment = os.getenv("SQLFLOW_ENVIRONMENT", "development")

        # Initialize empty config
        config = FeatureFlagConfig(environment=environment)

        # Default flags based on environment
        if environment == "development":
            config = config.with_flag_enabled(FeatureFlag.V2_OBSERVABILITY_ENABLED)
            config = config.with_flag_enabled(FeatureFlag.V2_DETAILED_LOGGING)
            config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
            config = config.with_flag_enabled(FeatureFlag.V2_HANDLERS_ENABLED)
            config = config.with_rollout_percentage(100.0)  # Full V2 in development
        elif environment == "staging":
            config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
            config = config.with_flag_enabled(FeatureFlag.V2_HANDLERS_ENABLED)
            config = config.with_flag_enabled(FeatureFlag.V2_OBSERVABILITY_ENABLED)
            config = config.with_rollout_percentage(50.0)  # 50% rollout in staging
        elif environment == "production":
            try:
                rollout_pct = float(os.getenv("SQLFLOW_V2_ROLLOUT_PERCENTAGE", "0.0"))
                config = config.with_rollout_percentage(rollout_pct)
            except ValueError:
                logger.warning(
                    "Invalid SQLFLOW_V2_ROLLOUT_PERCENTAGE value, using default 0.0"
                )
                config = config.with_rollout_percentage(0.0)

            # Production flags from environment
            if os.getenv("SQLFLOW_V2_ENABLED", "false").lower() == "true":
                config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
                config = config.with_flag_enabled(FeatureFlag.V2_HANDLERS_ENABLED)
                config = config.with_flag_enabled(FeatureFlag.V2_OBSERVABILITY_ENABLED)
                config = config.with_flag_enabled(FeatureFlag.V2_AUTOMATIC_FALLBACK)

        return config

    def is_enabled(self, flag: FeatureFlag) -> bool:
        """Check if a feature flag is enabled."""
        return self._config.is_enabled(flag)

    def should_use_v2_executor(self, execution_id: Optional[str] = None) -> bool:
        """Determine if V2 executor should be used for this execution.

        Uses rollout percentage for gradual deployment.
        """
        if not self.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED):
            return False

        # If gradual rollout is disabled, use V2 for all executions
        if not self.is_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT):
            return True

        # Use execution_id hash for consistent rollout behavior
        if execution_id:
            # Use MD5 for deterministic hashing
            hash_obj = hashlib.md5(execution_id.encode())
            hash_value = int(hash_obj.hexdigest()[:8], 16) % 100
            logger.debug(
                f"Execution ID {execution_id} hashed to {hash_value} (rollout: {self._config.rollout_percentage}%)"
            )
            return hash_value < self._config.rollout_percentage

        # Fallback to simple percentage
        import random

        return random.random() * 100 < self._config.rollout_percentage

    def get_config(self) -> FeatureFlagConfig:
        """Get current configuration."""
        return self._config

    def update_config(self, config: FeatureFlagConfig) -> None:
        """Update configuration (for testing and dynamic updates)."""
        self._config = config
        logger.info(
            f"Feature flag configuration updated: {len(config.enabled_flags)} flags enabled"
        )


# Global feature flag manager instance
# Following 'There should be one obvious way to do it'
_feature_flag_manager: Optional[FeatureFlagManager] = None


def get_feature_flag_manager() -> FeatureFlagManager:
    """Get the global feature flag manager.

    Lazy initialization following Python conventions.
    """
    global _feature_flag_manager
    if _feature_flag_manager is None:
        _feature_flag_manager = FeatureFlagManager()
    return _feature_flag_manager


def is_v2_enabled(flag: FeatureFlag) -> bool:
    """Convenience function to check if a V2 feature is enabled.

    Following 'Simple is better than complex'.
    """
    return get_feature_flag_manager().is_enabled(flag)


def should_use_v2_executor(execution_id: Optional[str] = None) -> bool:
    """Convenience function to determine V2 executor usage.

    This is the main function used throughout the codebase.
    """
    return get_feature_flag_manager().should_use_v2_executor(execution_id)


# Development and testing utilities
def enable_v2_for_testing() -> None:
    """Enable all V2 features for testing purposes."""
    config = FeatureFlagConfig(environment="testing")
    for flag in FeatureFlag:
        config = config.with_flag_enabled(flag)
    config = config.with_rollout_percentage(100.0)

    get_feature_flag_manager().update_config(config)
    logger.info("Enabled all V2 features for testing")


def disable_v2_for_testing() -> None:
    """Disable all V2 features for testing purposes."""
    config = FeatureFlagConfig(environment="testing", rollout_percentage=0.0)
    get_feature_flag_manager().update_config(config)
    logger.info("Disabled all V2 features for testing")
