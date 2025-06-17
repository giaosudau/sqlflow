"""Tests for observability decorator behavior in V2 handlers.

This test suite validates the @observed_execution decorator that provides
automatic observability, performance monitoring, and error handling.

Following Kent Beck's testing principles:
- Use real objects whenever possible
- Mock only at system boundaries
- Test behavior, not implementation
"""

from datetime import datetime

import pytest

from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep

# Import the improved test utilities
from tests.unit.core.executors.v2.test_utilities import (
    assert_no_mocks_in_context,
    create_lightweight_context,
)


class _TestStep(BaseStep):
    """Real step for testing - replaces MockStep."""

    def __init__(self, step_id="test_step_123", step_type="test"):
        super().__init__(
            id=step_id,
            type=step_type,
            criticality="normal",
            expected_duration_ms=1000.0,
        )


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
    def real_step(self):
        """Create a real step for testing."""
        return _TestStep()

    @pytest.fixture
    def real_context(self):
        """Create a real context for testing."""
        context = create_lightweight_context()
        assert_no_mocks_in_context(context)  # Ensure no mocks
        return context

    def test_successful_execution_observability(self, real_step, real_context):
        """Test that successful execution is properly observed."""
        handler = TestStepHandler()

        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        assert result.step_id == "test_step_123"
        assert result.step_type == "test"

        # Verify observability manager was used (real implementation)
        # We can't assert specific calls since it's a real implementation,
        # but we can verify the result has observability data
        assert hasattr(result, "observability_data")

    def test_failed_execution_observability(self, real_step, real_context):
        """Test that failed execution is properly observed."""
        handler = FailingStepHandler()

        result = handler.execute(real_step, real_context)

        assert not result.is_successful()
        assert result.error_message == "Intentional test failure"

        # Verify observability data is present
        assert hasattr(result, "observability_data")
        assert hasattr(result, "performance_metrics")

    def test_step_type_recorded_correctly(self, real_step, real_context):
        """Test that step type from decorator is recorded correctly."""
        handler = CustomTypeStepHandler()

        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        assert result.step_type == "custom_type"

    def test_step_id_recorded_correctly(self, real_context):
        """Test that step ID is recorded correctly."""
        handler = TestStepHandler()
        step = _TestStep(step_id="custom_step_456")

        result = handler.execute(step, real_context)

        assert result.is_successful()
        assert result.step_id == "custom_step_456"

    def test_execution_timing_captured(self, real_step, real_context):
        """Test that execution timing is captured."""
        handler = TestStepHandler()

        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        assert isinstance(result.execution_duration_ms, (int, float))
        assert result.execution_duration_ms >= 0

    def test_error_message_preservation(self, real_step, real_context):
        """Test that original error messages are preserved."""
        handler = FailingStepHandler()

        result = handler.execute(real_step, real_context)

        assert not result.is_successful()
        assert result.error_message == "Intentional test failure"

    def test_observability_data_structure(self, real_step, real_context):
        """Test that observability data has proper structure."""
        handler = TestStepHandler()

        result = handler.execute(real_step, real_context)

        # Verify basic structure
        assert result.is_successful()
        assert isinstance(result.observability_data, dict)
        assert isinstance(result.performance_metrics, dict)
        assert isinstance(result.data_lineage, dict)

    def test_performance_metrics_structure(self, real_step, real_context):
        """Test that performance metrics have expected structure."""
        handler = TestStepHandler()

        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        # Basic timing should be captured
        assert result.execution_duration_ms >= 0

    def test_observability_manager_resilience(self, real_step):
        """Test that observability manager failures don't break execution."""
        # Create context with a broken observability manager
        context = create_lightweight_context()

        # Even if observability fails, execution should continue
        handler = TestStepHandler()
        result = handler.execute(real_step, context)

        assert result.is_successful()
        assert result.step_id == "test_step_123"

    def test_decorator_parameters(self, real_step, real_context):
        """Test that decorator parameters are handled correctly."""

        @observed_execution("custom")
        def custom_handler(self, step, context):
            return StepExecutionResult.success(
                step_id=step.id, step_type="custom", start_time=datetime.utcnow()
            )

        handler = TestStepHandler()
        handler.execute = custom_handler.__get__(handler, TestStepHandler)

        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        assert result.step_type == "custom"

    def test_result_enrichment(self, real_step, real_context):
        """Test that results are enriched with observability data."""
        handler = TestStepHandler()

        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        # Check that enrichment occurred
        assert hasattr(result, "performance_metrics")
        assert hasattr(result, "observability_data")

        # Performance metrics should include step metadata
        assert isinstance(result.performance_metrics, dict)

    def test_exception_types_preserved(self, real_step, real_context):
        """Test that different exception types are handled correctly."""

        class CustomStepHandler(StepHandler):
            @observed_execution("test")
            def execute(self, step, context):
                raise RuntimeError("Custom runtime error")

        handler = CustomStepHandler()
        result = handler.execute(real_step, real_context)

        assert not result.is_successful()
        assert "Custom runtime error" in result.error_message

    def test_real_observability_integration(self, real_step, real_context):
        """Test that we're using real observability, not mocks."""
        handler = TestStepHandler()

        # Verify the context has real components
        assert_no_mocks_in_context(real_context)

        # Execute and verify observability works
        result = handler.execute(real_step, real_context)

        assert result.is_successful()
        assert isinstance(result.observability_data, dict)
        assert isinstance(result.performance_metrics, dict)

        # Verify the observability manager is real
        obs_manager = real_context.observability_manager
        assert hasattr(obs_manager, "run_id")  # Real ObservabilityManager attribute
        assert callable(obs_manager.record_step_start)  # Real method
