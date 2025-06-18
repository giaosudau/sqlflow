"""Unit tests for V2 feature flags functionality."""

import os
from unittest.mock import patch

from sqlflow.core.executors.v2.feature_flags import (
    FeatureFlag,
    FeatureFlagConfig,
    FeatureFlagManager,
    disable_v2_for_testing,
    enable_v2_for_testing,
    should_use_v2_executor,
)


def test_feature_flag_config_basic_operations():
    """Test basic feature flag configuration operations."""
    config = FeatureFlagConfig()

    # Initially no flags enabled
    assert not config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    assert config.rollout_percentage == 0.0

    # Enable a flag
    updated_config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    assert updated_config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    # Original config should be unchanged (immutable)
    assert not config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    # Disable a flag
    disabled_config = updated_config.with_flag_disabled(
        FeatureFlag.V2_ORCHESTRATOR_ENABLED
    )
    assert not disabled_config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    # Update rollout percentage
    rollout_config = config.with_rollout_percentage(50.0)
    assert rollout_config.rollout_percentage == 50.0
    assert config.rollout_percentage == 0.0  # Original unchanged


def test_feature_flag_manager_initialization():
    """Test feature flag manager initialization."""
    # Test with default config
    manager = FeatureFlagManager()
    assert manager.get_config().environment == "development"

    # Test with custom config
    custom_config = FeatureFlagConfig(environment="testing")
    custom_config = custom_config.with_rollout_percentage(25.0)

    manager = FeatureFlagManager(custom_config)
    assert manager.get_config().environment == "testing"
    assert manager.get_config().rollout_percentage == 25.0


def test_v2_executor_selection_with_rollout_percentage():
    """Test V2 executor selection respects rollout percentage."""
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

    # Should have a mix of True/False results
    true_count = sum(results)
    assert 0 < true_count < 20


def test_environment_based_configuration():
    """Test environment-based feature flag configuration."""
    # Test development environment
    with patch.dict(os.environ, {"SQLFLOW_ENVIRONMENT": "development"}):
        manager = FeatureFlagManager()
        config = manager.get_config()

        assert config.environment == "development"
        assert config.rollout_percentage == 100.0
        assert config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    # Test production environment
    with patch.dict(
        os.environ,
        {
            "SQLFLOW_ENVIRONMENT": "production",
            "SQLFLOW_V2_ENABLED": "true",
            "SQLFLOW_V2_ROLLOUT_PERCENTAGE": "25.0",
        },
    ):
        manager = FeatureFlagManager()
        config = manager.get_config()

        assert config.environment == "production"
        assert config.rollout_percentage == 25.0
        assert config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)


def test_consistent_rollout_behavior():
    """Test that rollout behavior is consistent for same execution ID."""
    config = FeatureFlagConfig()
    config = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    config = config.with_flag_enabled(FeatureFlag.V2_GRADUAL_ROLLOUT)
    config = config.with_rollout_percentage(50.0)

    manager = FeatureFlagManager(config)

    execution_id = "consistent_test"

    # Multiple calls with same ID should give same result
    results = [manager.should_use_v2_executor(execution_id) for _ in range(5)]

    # All results should be the same
    assert all(r == results[0] for r in results)


def test_global_should_use_v2_executor():
    """Test global should_use_v2_executor function."""
    # Test that function returns boolean
    result = should_use_v2_executor("test_execution")
    assert isinstance(result, bool)

    # Test with None execution_id
    result = should_use_v2_executor(None)
    assert isinstance(result, bool)


def test_feature_flag_config_immutability():
    """Test that FeatureFlagConfig is properly immutable."""
    config = FeatureFlagConfig(environment="test")

    # Test that with_* methods return new instances
    config_with_flag = config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    config_with_rollout = config.with_rollout_percentage(75.0)

    # Original should be unchanged
    assert config.environment == "test"
    assert config.rollout_percentage == 0.0
    assert not config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)

    # New configs should have changes
    assert config_with_flag.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    assert config_with_rollout.rollout_percentage == 75.0


def test_feature_flag_manager_config_updates():
    """Test that feature flag manager allows config updates."""
    manager = FeatureFlagManager()

    # Initially should not have V2 orchestrator enabled (except in development)
    manager.get_config()

    # Update config
    new_config = FeatureFlagConfig(environment="testing")
    new_config = new_config.with_flag_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    new_config = new_config.with_rollout_percentage(100.0)

    manager.update_config(new_config)

    # Verify update
    updated_config = manager.get_config()
    assert updated_config.environment == "testing"
    assert updated_config.is_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED)
    assert updated_config.rollout_percentage == 100.0


def test_testing_utilities():
    """Test V2 testing enable/disable utilities."""
    # Enable V2 for testing
    enable_v2_for_testing()
    assert should_use_v2_executor()

    # Disable V2 for testing
    disable_v2_for_testing()
    assert not should_use_v2_executor()
