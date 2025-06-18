"""Tests for V2 Feature Flag Management.

Following testing standards:
- Test behavior, not implementation
- Use real implementations where possible
- Clear, descriptive test names
- Test both positive and error scenarios
"""

import os
from unittest.mock import patch

from sqlflow.core.executors.v2.feature_flags import (
    FeatureFlag,
    FeatureFlagConfig,
    FeatureFlagManager,
    get_feature_flag_manager,
    should_use_v2_executor,
)


class TestFeatureFlagConfig:
    """Test feature flag configuration behavior."""

    def test_creates_config_with_defaults(self):
        """Should create configuration with sensible defaults."""
        config = FeatureFlagConfig()

        assert len(config.enabled_flags) == 0
        assert config.rollout_percentage == 0.0
        assert config.environment == "development"
        assert not config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    def test_enables_individual_flags_correctly(self):
        """Should enable individual feature flags correctly."""
        config = FeatureFlagConfig()

        updated_config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        assert updated_config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        assert not updated_config.is_enabled(FeatureFlag.V2_HANDLERS_ENABLED)

        # Original config should be unchanged (immutable)
        assert not config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    def test_disables_flags_correctly(self):
        """Should disable feature flags correctly."""
        config = FeatureFlagConfig()
        config_with_flag = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        assert config_with_flag.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        config_without_flag = config_with_flag.with_flag_disabled(
            FeatureFlag.V2_ORCHESTRATOR_ENABLED
        )

        assert not config_without_flag.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    def test_disabling_nonexistent_flag_is_safe(self):
        """Disabling a flag that wasn't enabled should be safe."""
        config = FeatureFlagConfig()

        # Should not raise exception
        updated_config = config.with_flag_disabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        assert not updated_config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    def test_updates_rollout_percentage(self):
        """Should update rollout percentage correctly."""
        config = FeatureFlagConfig()

        updated_config = config.with_rollout_percentage(75.0)

        assert updated_config.rollout_percentage == 75.0
        # Original should be unchanged
        assert config.rollout_percentage == 0.0

    def test_preserves_other_settings_when_updating(self):
        """Should preserve other settings when updating one field."""
        config = FeatureFlagConfig(environment="production")
        config_with_flag = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        config_with_rollout = config_with_flag.with_rollout_percentage(50.0)

        # All settings should be preserved
        assert config_with_rollout.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        assert config_with_rollout.rollout_percentage == 50.0
        assert config_with_rollout.environment == "production"


class TestFeatureFlagManager:
    """Test feature flag manager functionality."""

    def test_initializes_with_default_config(self):
        """Should initialize with default configuration."""
        manager = FeatureFlagManager()
        config = manager.get_config()

        assert config.environment == "development"
        assert config.rollout_percentage == 100.0  # Default for development

    def test_initializes_with_custom_config(self):
        """Should initialize with provided configuration."""
        custom_config = FeatureFlagConfig(environment="testing")
        custom_config = custom_config.with_rollout_percentage(25.0)

        manager = FeatureFlagManager(custom_config)
        config = manager.get_config()

        assert config.environment == "testing"
        assert config.rollout_percentage == 25.0

    def test_checks_individual_flags_correctly(self):
        """Manager should check individual flags correctly."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        manager = FeatureFlagManager(config)

        assert manager.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        assert not manager.is_enabled(FeatureFlag.V2_HANDLERS_ENABLED)

    def test_updates_configuration_dynamically(self):
        """Should allow dynamic configuration updates."""
        # Start with a clean config (not development defaults)
        initial_config = FeatureFlagConfig(environment="testing")
        manager = FeatureFlagManager(initial_config)

        # Initially no flags enabled
        assert not manager.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        # Update configuration
        new_config = FeatureFlagConfig()
        new_config = new_config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        manager.update_config(new_config)

        # Should now be enabled
        assert manager.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    def test_determines_v2_executor_usage_when_enabled_without_rollout(self):
        """Should use V2 executor when enabled and gradual rollout is disabled."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        # Don't enable gradual rollout flag

        manager = FeatureFlagManager(config)

        # Should always use V2 when rollout is disabled
        assert manager.should_use_v2_executor("test_execution_1")
        assert manager.should_use_v2_executor("test_execution_2")

    def test_determines_v2_executor_usage_with_consistent_rollout(self):
        """Should use consistent rollout behavior based on execution ID."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        config = config.with_flag_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT)
        config = config.with_rollout_percentage(50.0)

        manager = FeatureFlagManager(config)

        # Same execution ID should always give same result
        result1 = manager.should_use_v2_executor("consistent_test")
        result2 = manager.should_use_v2_executor("consistent_test")

        assert result1 == result2

    def test_updates_configuration_correctly(self):
        """Should update configuration correctly."""
        manager = FeatureFlagManager()
        new_config = FeatureFlagConfig()
        new_config = new_config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

        manager.update_config(new_config)

        assert manager.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        assert manager.get_config().is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)


class TestEnvironmentConfiguration:
    """Test environment-based configuration."""

    @patch.dict(os.environ, {"SQLFLOW_ENVIRONMENT": "development"})
    def test_development_environment_defaults(self):
        """Development environment should have appropriate defaults."""
        manager = FeatureFlagManager()
        config = manager.get_config()

        assert config.environment == "development"
        assert config.rollout_percentage == 100.0
        assert config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    @patch.dict(
        os.environ,
        {
            "SQLFLOW_ENVIRONMENT": "production",
            "SQLFLOW_V2_ENABLED": "true",
            "SQLFLOW_V2_ROLLOUT_PERCENTAGE": "25.0",
        },
    )
    def test_production_environment_configuration(self):
        """Production environment should respect environment variables."""
        manager = FeatureFlagManager()
        config = manager.get_config()

        assert config.environment == "production"
        assert config.rollout_percentage == 25.0
        assert config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    @patch.dict(
        os.environ, {"SQLFLOW_ENVIRONMENT": "production", "SQLFLOW_V2_ENABLED": "false"}
    )
    def test_production_environment_v2_disabled(self):
        """Production environment should respect V2 disabled flag."""
        manager = FeatureFlagManager()
        config = manager.get_config()

        assert config.environment == "production"
        assert config.rollout_percentage == 0.0
        assert not config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)


class TestFeatureFlagRolloutBehavior:
    """Test rollout percentage behavior."""

    def test_rollout_percentage_affects_executor_selection(self):
        """Rollout percentage should affect executor selection probability."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        config = config.with_flag_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT)
        config = config.with_rollout_percentage(50.0)

        manager = FeatureFlagManager(config)

        # Test multiple execution IDs
        results = []
        for i in range(20):
            result = manager.should_use_v2_executor(f"test_execution_{i}")
            results.append(result)

        # Should have mix of True/False (not all True or all False)
        true_count = sum(results)
        assert 0 < true_count < 20

    def test_full_rollout_always_uses_v2(self):
        """100% rollout should always use V2 executor."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        config = config.with_flag_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT)
        config = config.with_rollout_percentage(100.0)

        manager = FeatureFlagManager(config)

        # Should always return True
        for i in range(10):
            assert manager.should_use_v2_executor(f"test_execution_{i}")

    def test_zero_rollout_never_uses_v2(self):
        """0% rollout should never use V2 executor."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        config = config.with_flag_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT)
        config = config.with_rollout_percentage(0.0)

        manager = FeatureFlagManager(config)

        # Should always return False
        for i in range(10):
            assert not manager.should_use_v2_executor(f"test_execution_{i}")

    def test_consistent_execution_id_behavior(self):
        """Same execution ID should always give same rollout decision."""
        config = FeatureFlagConfig()
        config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
        config = config.with_flag_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT)
        config = config.with_rollout_percentage(50.0)

        manager = FeatureFlagManager(config)

        execution_id = "consistent_test_execution"

        # Multiple calls with same ID should give same result
        results = [manager.should_use_v2_executor(execution_id) for _ in range(5)]

        # All results should be the same
        assert all(r == results[0] for r in results)


class TestGlobalFeatureFlagManager:
    """Test global feature flag manager functionality."""

    def test_gets_global_manager_instance(self):
        """Should get global manager instance."""
        manager1 = get_feature_flag_manager()
        manager2 = get_feature_flag_manager()

        # Should be the same instance
        assert manager1 is manager2

    def test_global_should_use_v2_executor(self):
        """Global function should work correctly."""
        # This will use the global manager
        result = should_use_v2_executor("test_execution")

        # Should return boolean
        assert isinstance(result, bool)
