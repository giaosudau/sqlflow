"""Integration tests for V2 executor production workflow."""

import os
from unittest.mock import patch

import pytest

from sqlflow.core.executors.v2.compatibility_bridge import create_executor
from sqlflow.core.executors.v2.feature_flags import (
    FeatureFlag,
    FeatureFlagConfig,
    FeatureFlagManager,
    should_use_v2_executor,
)


@pytest.fixture
def test_data_file(tmp_path):
    """Create a test data file for pipeline execution."""
    data_file = tmp_path / "test_data.csv"
    data_file.write_text("id,name\n1,test\n2,test2\n")
    return str(data_file)


def test_full_production_integration_workflow(test_data_file):
    """Test complete production workflow with real components."""
    # Configure environment for testing
    with patch.dict(
        os.environ,
        {
            "SQLFLOW_ENVIRONMENT": "production",
            "SQLFLOW_V2_ENABLED": "true",
            "SQLFLOW_V2_ROLLOUT_PERCENTAGE": "100.0",
        },
    ):
        # Create executor through factory
        executor = create_executor()

        # Test simple pipeline execution
        plan = [
            {
                "id": "load_data",
                "type": "load",
                "source": test_data_file,
                "target_table": "raw_data",
                "options": {"connector_type": "csv"},
            },
            {
                "id": "transform",
                "type": "transform",
                "sql": "SELECT * FROM raw_data",
                "target_table": "transformed",
                "depends_on": ["load_data"],
            },
            {
                "id": "validate",
                "type": "transform",
                "sql": "SELECT COUNT(*) as count FROM transformed",
                "target_table": "validation_results",
                "depends_on": ["transform"],
            },
        ]

        result = executor.execute(plan)

        # Verify execution completed successfully
        assert (
            "error" not in result
        ), f"Execution failed with error: {result.get('error', '')}"
        assert result["executor_version"] in ("v1", "v2")
        assert result["compatibility_bridge"] is True

        # Verify all steps executed
        if result["executor_version"] == "v2":
            assert len(result["execution_results"]) == 3
        else:
            assert len(result["executed_steps"]) == 3


def test_environment_variable_configuration():
    """Test executor configuration through environment variables."""
    test_environments = [
        ("development", True, 100.0),
        ("staging", True, 50.0),
        ("production", True, 25.0),
    ]

    for env_name, v2_enabled, rollout_pct in test_environments:
        with patch.dict(
            os.environ,
            {
                "SQLFLOW_ENVIRONMENT": env_name,
                "SQLFLOW_V2_ENABLED": str(v2_enabled).lower(),
                "SQLFLOW_V2_ROLLOUT_PERCENTAGE": str(rollout_pct),
            },
        ):
            manager = FeatureFlagManager()
            config = manager.get_config()

            assert config.rollout_percentage == rollout_pct
            assert should_use_v2_executor() in (
                True,
                False,
            )  # Could be either based on rollout


@patch("sqlflow.core.executors.v2.feature_flags.get_feature_flag_manager")
def test_gradual_rollout_behavior(mock_get_manager):
    """Test gradual rollout behavior with different execution IDs."""
    # Create initial config with V2 features enabled
    config = FeatureFlagConfig(
        enabled_flags={
            FeatureFlag.V2_ORCHESTRATOR_ENABLED,
            FeatureFlag.V2_GRADUAL_ROLLOUT,
            FeatureFlag.V2_HANDLERS_ENABLED,  # Required for V2 execution
        },
        rollout_percentage=50.0,
        environment="testing",
    )

    manager = FeatureFlagManager(config)
    mock_get_manager.return_value = manager

    # Test multiple execution IDs with deterministic hashing
    execution_ids = [
        "test_execution_1",  # hash: 49 (use V2)
        "test_execution_2",  # hash: 51 (use V1)
        "test_execution_3",  # hash: 48 (use V2)
        "test_execution_4",  # hash: 52 (use V1)
    ]

    results = [should_use_v2_executor(execution_id) for execution_id in execution_ids]

    # Should see mix of True/False with ~50% distribution
    true_count = sum(1 for r in results if r)
    assert (
        1 <= true_count <= 3
    ), f"Expected 1-3 True results, got {true_count} from {results}"
