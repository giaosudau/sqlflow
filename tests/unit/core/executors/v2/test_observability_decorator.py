"""Tests for observability decorator behavior in V2 handlers.

This test suite validates the @observed_execution decorator that provides
automatic observability, performance monitoring, and error handling.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.results import StepExecutionResult


class MockStep:
    """Mock step for testing."""

    def __init__(self, step_id="test_step_123", step_type="test"):
        self.id = step_id
        self.type = step_type
        # Add required attributes for observability
        self.criticality = "normal"
        self.expected_duration_ms = 1000.0
        self.retry_policy = None


class MockContext:
    """Mock execution context."""

    def __init__(self):
        # Create mock observability manager
        self.observability_manager = Mock()
        self.observability_manager.record_step_start = Mock()
        self.observability_manager.record_step_success = Mock()
        self.observability_manager.record_step_failure = Mock()


class TestStepHandler(StepHandler):
    """Test handler with observability decorator."""

    @observed_execution("test")
    def execute(self, step, context):
        """Simple test execution that succeeds."""
        return StepExecutionResult.success(
            step_id=step.id, step_type=step.type, start_time=datetime.utcnow()
        )


class FailingStepHandler(StepHandler):
    """Test handler that always fails."""

    @observed_execution("test")
    def execute(self, step, context):
        """Test execution that always fails."""
        raise ValueError("Intentional test failure")


class CustomTypeStepHandler(StepHandler):
    """Test handler with custom step type."""

    @observed_execution("custom_type")
    def execute(self, step, context):
        """Test execution with custom step type."""
        return StepExecutionResult.success(
            step_id=step.id, step_type="custom_type", start_time=datetime.utcnow()
        )


class TestObservabilityDecorator:
    """Tests for the @observed_execution decorator."""

    @pytest.fixture
    def mock_step(self):
        """Create a mock step for testing."""
        return MockStep()

    @pytest.fixture
    def mock_context(self):
        """Create a mock context for testing."""
        return MockContext()

    def test_successful_execution_observability(self, mock_step, mock_context):
        """Test that successful execution is properly observed."""
        handler = TestStepHandler()

        result = handler.execute(mock_step, mock_context)

        assert result.is_successful()
        assert result.step_id == "test_step_123"
        assert result.step_type == "test"

        # Verify observability manager was called
        mock_context.observability_manager.record_step_start.assert_called_once()

    def test_failed_execution_observability(self, mock_step, mock_context):
        """Test that failed execution is properly observed."""
        handler = FailingStepHandler()

        result = handler.execute(mock_step, mock_context)

        assert not result.is_successful()
        assert result.error_message == "Intentional test failure"

        # Verify observability manager methods were called
        mock_context.observability_manager.record_step_start.assert_called_once()

    def test_step_type_recorded_correctly(self, mock_step, mock_context):
        """Test that step type from decorator is recorded correctly."""
        handler = CustomTypeStepHandler()

        result = handler.execute(mock_step, mock_context)

        assert result.is_successful()
        assert result.step_type == "custom_type"

    def test_step_id_recorded_correctly(self, mock_context):
        """Test that step ID is recorded correctly."""
        handler = TestStepHandler()
        step = MockStep(step_id="custom_step_456")

        result = handler.execute(step, mock_context)

        assert result.is_successful()
        assert result.step_id == "custom_step_456"

    def test_execution_timing_captured(self, mock_step, mock_context):
        """Test that execution timing is captured."""
        handler = TestStepHandler()

        # Mock datetime to control timing
        fixed_time = datetime(2023, 1, 1, 12, 0)
        with patch("sqlflow.core.executors.v2.handlers.base.datetime") as mock_datetime:
            mock_datetime.utcnow.return_value = fixed_time

            result = handler.execute(mock_step, mock_context)

            assert result.is_successful()
            assert isinstance(result.execution_duration_ms, (int, float))
            assert result.execution_duration_ms >= 0

    def test_error_message_preservation(self):
        """Test that original error messages are preserved."""
        handler = FailingStepHandler()
        step = MockStep()
        context = MockContext()

        result = handler.execute(step, context)

        assert not result.is_successful()
        assert result.error_message == "Intentional test failure"

    def test_observability_data_structure(self):
        """Test that observability data has proper structure."""
        handler = TestStepHandler()
        step = MockStep()
        context = MockContext()

        result = handler.execute(step, context)

        # Verify basic structure
        assert result.is_successful()
        assert isinstance(result.observability_data, dict)
        assert isinstance(result.performance_metrics, dict)
        assert isinstance(result.data_lineage, dict)

    def test_performance_metrics_structure(self):
        """Test that performance metrics have expected structure."""
        handler = TestStepHandler()
        step = MockStep()
        context = MockContext()

        result = handler.execute(step, context)

        assert result.is_successful()
        # Basic timing should be captured
        assert result.execution_duration_ms >= 0

    def test_observability_manager_failure_handling(self, mock_step, mock_context):
        """Test that observability manager failures don't break execution."""
        # Make observability manager fail
        mock_context.observability_manager.record_step_start.side_effect = Exception(
            "Observability error"
        )

        handler = TestStepHandler()

        # Execution should still succeed despite observability failure
        result = handler.execute(mock_step, mock_context)

        assert result.is_successful()
        assert result.step_id == "test_step_123"

    def test_decorator_parameters(self):
        """Test that decorator parameters are handled correctly."""

        @observed_execution("custom", record_schema=False, record_performance=False)
        def custom_handler(self, step, context):
            return StepExecutionResult.success(
                step_id=step.id, step_type="custom", start_time=datetime.utcnow()
            )

        handler = TestStepHandler()
        handler.execute = custom_handler.__get__(handler, TestStepHandler)
        step = MockStep()
        context = MockContext()

        result = handler.execute(step, context)

        assert result.is_successful()
        assert result.step_type == "custom"

    def test_result_enrichment(self, mock_step, mock_context):
        """Test that results are enriched with observability data."""
        handler = TestStepHandler()

        result = handler.execute(mock_step, mock_context)

        assert result.is_successful()
        # Check that enrichment occurred
        assert hasattr(result, "performance_metrics")
        assert hasattr(result, "observability_data")

        # Performance metrics should include step metadata
        if result.performance_metrics:
            # The enrichment might add step criticality info
            assert isinstance(result.performance_metrics, dict)

    def test_exception_types_preserved(self, mock_step, mock_context):
        """Test that different exception types are handled correctly."""

        class CustomStepHandler(StepHandler):
            @observed_execution("test")
            def execute(self, step, context):
                raise RuntimeError("Custom runtime error")

        handler = CustomStepHandler()
        result = handler.execute(mock_step, mock_context)

        assert not result.is_successful()
        assert "Custom runtime error" in result.error_message
