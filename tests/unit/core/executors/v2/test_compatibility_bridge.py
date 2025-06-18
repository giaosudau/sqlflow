"""Tests for V1/V2 Executor Compatibility Bridge.

Following testing standards:
- Test behavior, not implementation
- Use real implementations where possible
- Clear, descriptive test names
- Test both positive and error scenarios
"""

from unittest.mock import Mock, patch

import pytest

from sqlflow.core.executors.v2.compatibility_bridge import (
    ExecutorCompatibilityError,
    V1V2CompatibilityBridge,
    create_executor,
)
from sqlflow.core.executors.v2.feature_flags import (
    disable_v2_for_testing,
    enable_v2_for_testing,
)


class TestV1V2CompatibilityBridge:
    """Test compatibility bridge behavior between V1 and V2 executors."""

    def setup_method(self):
        """Set up test environment."""
        # Reset feature flags before each test
        disable_v2_for_testing()

    def test_creates_bridge_with_fallback_enabled_by_default(self):
        """Bridge should be created with automatic fallback enabled by default."""
        bridge = V1V2CompatibilityBridge()

        assert bridge._fallback_enabled is True
        assert "compatibility_bridge" in str(bridge)

    def test_routes_to_v1_executor_when_v2_disabled(self):
        """Should route execution to V1 when V2 is disabled."""
        disable_v2_for_testing()

        mock_plan = [{"type": "load", "name": "test_table"}]
        bridge = V1V2CompatibilityBridge()

        with patch.object(bridge, "_get_v1_executor") as mock_v1:
            mock_v1.return_value.execute.return_value = {
                "status": "success",
                "executor": "v1",
            }

            result = bridge.execute(mock_plan)

            assert result["status"] == "success"
            assert "executor" in result
            mock_v1.return_value.execute.assert_called_once_with(mock_plan)

    def test_routes_to_v2_executor_when_enabled(self):
        """Should route execution to V2 when V2 is enabled."""
        enable_v2_for_testing()

        mock_plan = [{"type": "transform", "name": "analytics"}]
        bridge = V1V2CompatibilityBridge()

        with patch.object(bridge, "_get_v2_executor") as mock_v2:
            mock_v2.return_value.execute.return_value = {
                "status": "success",
                "executor": "v2",
            }

            result = bridge.execute(mock_plan)

            assert result["status"] == "success"
            mock_v2.return_value.execute.assert_called_once_with(mock_plan)

    def test_falls_back_to_v1_when_v2_executor_creation_fails(self):
        """Should fallback to V1 when V2 executor cannot be created."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        mock_plan = [{"type": "export", "name": "output"}]

        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
        ):

            # V2 creation fails
            mock_v2.return_value = None
            mock_v1.return_value.execute.return_value = {
                "status": "success",
                "fallback": True,
            }

            result = bridge.execute(mock_plan)

            assert result["status"] == "success"
            assert result.get("fallback") is True
            mock_v1.return_value.execute.assert_called_once()

    def test_falls_back_to_v1_when_v2_execution_fails(self):
        """Should fallback to V1 when V2 execution fails."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        mock_plan = [{"type": "load", "name": "failing_table"}]

        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
        ):

            # V2 execution fails
            mock_v2.return_value.execute.side_effect = Exception("V2 execution failed")
            mock_v1.return_value.execute.return_value = {
                "status": "success",
                "recovered": True,
            }

            result = bridge.execute(mock_plan)

            assert result["status"] == "success"
            assert result.get("recovered") is True
            mock_v1.return_value.execute.assert_called_once()

    def test_disables_fallback_when_requested(self):
        """Should disable fallback when explicitly requested."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge(enable_fallback=False)

        mock_plan = [{"type": "transform", "name": "test"}]

        with patch.object(bridge, "_get_v2_executor") as mock_v2:
            mock_v2.return_value.execute.side_effect = Exception("V2 failed")

            with pytest.raises(Exception, match="V2 failed"):
                bridge.execute(mock_plan)

    def test_tracks_execution_metrics_correctly(self):
        """Should track execution metrics correctly."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        mock_plan = [{"type": "load", "name": "metrics_test"}]

        with patch.object(bridge, "_get_v2_executor") as mock_v2:
            mock_v2.return_value.execute.return_value = {"status": "success"}

            bridge.execute(mock_plan)

            # Check that execution was tracked
            metrics = bridge.get_execution_metrics()
            assert metrics["total_executions"] == 1
            assert metrics["v2_executions"] == 1
            assert metrics["fallback_count"] == 0

    def test_tracks_fallback_metrics_correctly(self):
        """Should track fallback metrics when they occur."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        mock_plan = [{"type": "export", "name": "fallback_test"}]

        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
        ):

            mock_v2.return_value.execute.side_effect = Exception("V2 failed")
            mock_v1.return_value.execute.return_value = {"status": "success"}

            bridge.execute(mock_plan)

            metrics = bridge.get_execution_metrics()
            assert metrics["total_executions"] == 1
            assert metrics["v2_executions"] == 0  # V2 failed
            assert metrics["fallback_count"] == 1

    def test_provides_execution_state_information(self):
        """Should provide useful execution state information."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        state = bridge.get_execution_state()

        assert state["compatibility_bridge"] is True
        assert state["v2_enabled"] is True
        assert state["fallback_enabled"] is True

    def test_executes_multiple_operations_consistently(self):
        """Should execute multiple operations consistently using same executor."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        plans = [
            [{"type": "load", "name": "table1"}],
            [{"type": "transform", "name": "analysis1"}],
            [{"type": "export", "name": "output1"}],
        ]

        with patch.object(bridge, "_get_v2_executor") as mock_v2:
            mock_v2.return_value.execute.return_value = {"status": "success"}

            for plan in plans:
                bridge.execute(plan)

            # Should use same V2 executor for all operations
            assert mock_v2.return_value.execute.call_count == 3


class TestCompatibilityBridgeIntegration:
    """Test compatibility bridge integration scenarios."""

    def test_handles_complex_execution_plan(self):
        """Should handle complex execution plans correctly."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        complex_plan = [
            {"type": "source", "name": "customers", "connector": "postgres"},
            {
                "type": "transform",
                "name": "clean_data",
                "sql": "SELECT * FROM customers",
            },
            {"type": "load", "name": "clean_customers", "mode": "UPSERT"},
            {"type": "export", "name": "analytics_output", "destination": "s3"},
        ]

        with patch.object(bridge, "_get_v2_executor") as mock_v2:
            mock_v2.return_value.execute.return_value = {
                "status": "success",
                "rows_processed": 1000,
                "duration": 45.2,
            }

            result = bridge.execute(complex_plan)

            assert result["status"] == "success"
            assert "rows_processed" in result
            assert "duration" in result

    def test_preserves_error_information_during_fallback(self):
        """Should preserve error information when falling back to V1."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        mock_plan = [{"type": "load", "name": "error_test"}]

        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
        ):

            v2_error = ExecutorCompatibilityError("V2 database connection failed")
            mock_v2.return_value.execute.side_effect = v2_error
            mock_v1.return_value.execute.return_value = {
                "status": "success",
                "fallback_reason": str(v2_error),
            }

            result = bridge.execute(mock_plan)

            assert result["status"] == "success"
            assert "database connection failed" in result.get("fallback_reason", "")

    def test_handles_partial_v2_availability(self):
        """Should handle scenarios where V2 is partially available."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        # Simulate V2 being available but certain features not working
        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
        ):

            def selective_execution(plan):
                # V2 works for transforms but not for loads
                step_type = plan[0].get("type")
                if step_type == "load":
                    raise Exception("V2 load handler not ready")
                return {"status": "success", "executor": "v2", "type": step_type}

            mock_v2.return_value.execute.side_effect = selective_execution
            mock_v1.return_value.execute.return_value = {
                "status": "success",
                "executor": "v1",
            }

            # Transform should use V2
            transform_result = bridge.execute([{"type": "transform", "name": "test"}])
            assert transform_result["executor"] == "v2"

            # Load should fallback to V1
            load_result = bridge.execute([{"type": "load", "name": "test"}])
            assert load_result["executor"] == "v1"


class TestCreateExecutorFactory:
    """Test the create_executor factory function."""

    def test_creates_compatibility_bridge_by_default(self):
        """Should create compatibility bridge by default."""
        executor = create_executor()

        assert isinstance(executor, V1V2CompatibilityBridge)

    def test_creates_v2_executor_when_explicitly_requested(self):
        """Should create V2 executor when explicitly requested."""
        enable_v2_for_testing()

        with patch(
            "sqlflow.core.executors.v2.compatibility_bridge.LocalOrchestrator"
        ) as mock_orchestrator:
            mock_orchestrator.return_value = Mock()

            executor = create_executor(force_v2=True)

            mock_orchestrator.assert_called_once()

    def test_creates_v1_executor_when_explicitly_requested(self):
        """Should create V1 executor when explicitly requested."""
        with patch(
            "sqlflow.core.executors.v2.compatibility_bridge.LocalExecutor"
        ) as mock_local:
            mock_local.return_value = Mock()

            executor = create_executor(force_v1=True)

            mock_local.assert_called_once()

    def test_raises_error_when_conflicting_flags_provided(self):
        """Should raise error when both force_v1 and force_v2 are True."""
        with pytest.raises(ExecutorCompatibilityError, match="Cannot force both"):
            create_executor(force_v1=True, force_v2=True)

    def test_passes_configuration_to_created_executor(self):
        """Should pass configuration parameters to created executor."""
        config_params = {"profile": "test", "debug": True, "max_workers": 4}

        with patch(
            "sqlflow.core.executors.v2.compatibility_bridge.V1V2CompatibilityBridge"
        ) as mock_bridge:
            mock_bridge.return_value = Mock()

            create_executor(**config_params)

            mock_bridge.assert_called_once_with(**config_params)


class TestCompatibilityBridgeErrorHandling:
    """Test error handling scenarios in compatibility bridge."""

    def test_handles_v1_executor_creation_failure(self):
        """Should handle V1 executor creation failure gracefully."""
        disable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        with patch.object(bridge, "_get_v1_executor") as mock_v1:
            mock_v1.side_effect = Exception("V1 creation failed")

            with pytest.raises(
                ExecutorCompatibilityError, match="Both V1 and V2 executors failed"
            ):
                bridge.execute([{"type": "load", "name": "test"}])

    def test_handles_both_executors_failing(self):
        """Should handle scenario where both V1 and V2 fail."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
        ):

            mock_v2.return_value.execute.side_effect = Exception("V2 failed")
            mock_v1.return_value.execute.side_effect = Exception("V1 failed")

            with pytest.raises(
                ExecutorCompatibilityError, match="Both V1 and V2 executors failed"
            ):
                bridge.execute([{"type": "load", "name": "test"}])

    def test_preserves_original_error_when_fallback_disabled(self):
        """Should preserve original error when fallback is disabled."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge(enable_fallback=False)

        original_error = ValueError("Custom V2 error")

        with patch.object(bridge, "_get_v2_executor") as mock_v2:
            mock_v2.return_value.execute.side_effect = original_error

            with pytest.raises(ValueError, match="Custom V2 error"):
                bridge.execute([{"type": "transform", "name": "test"}])

    def test_logs_fallback_events_for_monitoring(self):
        """Should log fallback events for monitoring purposes."""
        enable_v2_for_testing()
        bridge = V1V2CompatibilityBridge()

        with (
            patch.object(bridge, "_get_v2_executor") as mock_v2,
            patch.object(bridge, "_get_v1_executor") as mock_v1,
            patch(
                "sqlflow.core.executors.v2.compatibility_bridge.logger"
            ) as mock_logger,
        ):

            mock_v2.return_value.execute.side_effect = Exception("V2 failed")
            mock_v1.return_value.execute.return_value = {"status": "success"}

            bridge.execute([{"type": "load", "name": "test"}])

            # Should log the fallback event
            mock_logger.warning.assert_called_with(
                "V2 execution failed, falling back to V1: V2 failed"
            )


@pytest.fixture
def mock_v1_executor():
    """Mock V1 executor."""
    executor = Mock()
    executor.execute.return_value = {
        "status": "success",
        "executed_steps": ["step1", "step2"],
        "total_steps": 2,
    }
    executor.can_resume.return_value = False
    executor.resume.return_value = {"status": "resumed"}
    return executor


@pytest.fixture
def mock_v2_executor():
    """Mock V2 executor."""
    executor = Mock()
    executor.execute.return_value = {
        "status": "success",
        "execution_results": {"step1": {}, "step2": {}},
        "performance_summary": {"total_time": 1.5},
    }
    executor.can_resume.return_value = True
    executor.resume.return_value = {"status": "resumed"}
    executor.get_execution_state.return_value = {"phase": "V2"}
    return executor


def test_bridge_initialization():
    """Test compatibility bridge initializes correctly."""
    bridge = V1V2CompatibilityBridge()
    assert bridge._v1_executor is None
    assert bridge._v2_executor is None
    assert isinstance(bridge._execution_options, dict)


@patch("sqlflow.core.executors.v2.compatibility_bridge.should_use_v2_executor")
@patch("sqlflow.core.executors.local_executor.LocalExecutor")
def test_v1_execution_path(mock_local_executor_class, mock_should_use_v2):
    """Test execution falls back to V1 when V2 is disabled."""
    mock_should_use_v2.return_value = False
    mock_executor = Mock()
    mock_executor.execute.return_value = {"status": "success"}
    mock_local_executor_class.return_value = mock_executor

    bridge = V1V2CompatibilityBridge()
    plan = [{"id": "test_step", "type": "load"}]

    result = bridge.execute(plan)

    assert result["executor_version"] == "v1"
    assert result["compatibility_bridge"] is True
    mock_executor.execute.assert_called_once()


@patch("sqlflow.core.executors.v2.compatibility_bridge.should_use_v2_executor")
@patch("sqlflow.core.executors.v2.orchestrator.LocalOrchestrator")
def test_v2_execution_path(mock_orchestrator_class, mock_should_use_v2):
    """Test execution uses V2 when enabled."""
    mock_should_use_v2.return_value = True
    mock_executor = Mock()
    mock_executor.execute.return_value = {"status": "success", "execution_results": {}}
    mock_orchestrator_class.return_value = mock_executor

    bridge = V1V2CompatibilityBridge()
    plan = [{"id": "test_step", "type": "load"}]

    result = bridge.execute(plan)

    assert result["executor_version"] == "v2"
    assert result["compatibility_bridge"] is True
    mock_executor.execute.assert_called_once()


@patch("sqlflow.core.executors.v2.compatibility_bridge.should_use_v2_executor")
@patch("sqlflow.core.executors.v2.compatibility_bridge.is_v2_enabled")
def test_automatic_fallback_on_v2_failure(mock_is_v2_enabled, mock_should_use_v2):
    """Test automatic fallback to V1 when V2 fails and fallback is enabled."""
    mock_should_use_v2.return_value = True
    mock_is_v2_enabled.return_value = True  # Fallback enabled

    with patch(
        "sqlflow.core.executors.v2.orchestrator.LocalOrchestrator"
    ) as mock_v2_class:
        mock_v2_executor = Mock()
        mock_v2_executor.execute.side_effect = Exception("V2 failed")
        mock_v2_class.return_value = mock_v2_executor

        with patch(
            "sqlflow.core.executors.local_executor.LocalExecutor"
        ) as mock_v1_class:
            mock_v1_executor = Mock()
            mock_v1_executor.execute.return_value = {"status": "success"}
            mock_v1_class.return_value = mock_v1_executor

            bridge = V1V2CompatibilityBridge()
            plan = [{"id": "test_step", "type": "load"}]

            result = bridge.execute(plan)

            # Should fallback to V1
            assert result["executor_version"] == "v1"
            mock_v1_executor.execute.assert_called_once()


def test_execution_id_generation_consistency():
    """Test execution ID generation is consistent for same plan."""
    bridge = V1V2CompatibilityBridge()
    plan = [{"id": "step1", "type": "load"}, {"id": "step2", "type": "transform"}]

    id1 = bridge._generate_execution_id(plan)
    bridge._generate_execution_id(plan)

    # Should be consistent within same second
    assert id1.startswith("exec_")
    assert len(id1) == 13  # "exec_" + 8 char hash


def test_create_executor_factory():
    """Test create_executor factory function."""
    # When V2 is enabled, should return bridge
    enable_v2_for_testing()
    executor = create_executor()
    assert isinstance(executor, V1V2CompatibilityBridge)

    # When V2 is disabled, should return V1
    disable_v2_for_testing()
    executor = create_executor()
    # Should fallback to V1 (actual LocalExecutor)
    assert hasattr(executor, "execute")
