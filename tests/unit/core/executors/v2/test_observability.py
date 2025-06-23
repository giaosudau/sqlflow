"""Tests for Simple Observability Module.

Following Raymond Hettinger's philosophy:
- "Simple is better than complex"
- "Readability counts"
- Testing the actual simple implementation we created.
"""

import time

import pytest

from sqlflow.core.executors.v2.observability.metrics import (
    SimpleObservabilityManager,
    StepMetrics,
    create_observability_manager,
)


class TestStepMetrics:
    """Test the StepMetrics dataclass."""

    def test_step_metrics_initialization(self):
        """Test StepMetrics initializes with correct values."""
        start_time = time.time()
        metrics = StepMetrics(
            step_id="test_step",
            start_time=start_time,
            rows_affected=100,
            metadata={"source": "test"},
        )

        assert metrics.step_id == "test_step"
        assert metrics.start_time == start_time
        assert metrics.end_time is None
        assert metrics.success is True
        assert metrics.error_message is None
        assert metrics.rows_affected == 100
        assert metrics.metadata == {"source": "test"}
        assert not metrics.is_complete

    def test_step_metrics_duration_incomplete(self):
        """Test duration calculation for incomplete step."""
        start_time = time.time()
        metrics = StepMetrics(step_id="test", start_time=start_time)

        # Duration should be calculated from current time
        duration = metrics.duration_ms
        assert duration > 0
        assert duration < 1000  # Should be less than 1 second

    def test_step_metrics_duration_complete(self):
        """Test duration calculation for completed step."""
        start_time = time.time()
        end_time = start_time + 0.1  # 100ms later

        metrics = StepMetrics(step_id="test", start_time=start_time, end_time=end_time)

        assert metrics.is_complete
        assert abs(metrics.duration_ms - 100) < 10  # Should be approximately 100ms

    def test_step_metrics_with_error(self):
        """Test StepMetrics with error information."""
        metrics = StepMetrics(
            step_id="failed_step",
            start_time=time.time(),
            end_time=time.time(),
            success=False,
            error_message="Test error",
        )

        assert not metrics.success
        assert metrics.error_message == "Test error"
        assert metrics.is_complete


class TestSimpleObservabilityManager:
    """Test the SimpleObservabilityManager."""

    def test_manager_initialization(self):
        """Test manager initializes correctly."""
        manager = SimpleObservabilityManager("test_execution")

        assert manager.execution_id == "test_execution"
        assert not manager.has_failures
        assert manager.total_duration_ms > 0

    def test_manager_default_execution_id(self):
        """Test manager creates default execution ID."""
        manager = SimpleObservabilityManager()

        assert manager.execution_id is not None
        assert isinstance(manager.execution_id, str)

    def test_start_and_end_step(self):
        """Test basic step tracking."""
        manager = SimpleObservabilityManager("test")

        # Start step
        manager.start_step("load_data")
        metrics = manager.get_metrics()
        assert metrics["total_steps"] == 1
        assert metrics["completed_steps"] == 0

        # End step successfully
        manager.end_step("load_data", success=True)
        metrics = manager.get_metrics()
        assert metrics["completed_steps"] == 1
        assert metrics["successful_steps"] == 1
        assert metrics["failed_steps"] == 0

    def test_step_failure(self):
        """Test tracking failed steps."""
        manager = SimpleObservabilityManager("test")

        manager.start_step("failing_step")
        manager.end_step("failing_step", success=False, error="Test error")

        metrics = manager.get_metrics()
        assert metrics["failed_steps"] == 1
        assert metrics["successful_steps"] == 0
        assert manager.has_failures

        failed_steps = manager.get_failed_steps()
        assert len(failed_steps) == 1
        assert failed_steps[0].error_message == "Test error"

    def test_record_rows_affected(self):
        """Test recording rows affected by steps."""
        manager = SimpleObservabilityManager("test")

        manager.start_step("load_data")
        manager.record_rows_affected("load_data", 1000)
        manager.end_step("load_data", success=True)

        metrics = manager.get_metrics()
        assert metrics["total_rows_affected"] == 1000

        step_details = metrics["step_details"]["load_data"]
        assert step_details["rows_affected"] == 1000

    def test_add_step_metadata(self):
        """Test adding metadata to steps."""
        manager = SimpleObservabilityManager("test")

        manager.start_step("transform_data")
        manager.add_step_metadata(
            "transform_data", {"table": "customers", "operation": "join"}
        )
        manager.end_step("transform_data", success=True)

        metrics = manager.get_metrics()
        step_details = metrics["step_details"]["transform_data"]
        assert step_details["metadata"]["table"] == "customers"
        assert step_details["metadata"]["operation"] == "join"

    def test_measure_step_context_manager_success(self):
        """Test measure_step context manager for successful execution."""
        manager = SimpleObservabilityManager("test")

        with manager.measure_step("context_step"):
            time.sleep(0.01)  # Small delay to measure

        metrics = manager.get_metrics()
        assert metrics["completed_steps"] == 1
        assert metrics["successful_steps"] == 1

        step_details = metrics["step_details"]["context_step"]
        assert step_details["success"]
        assert step_details["duration_ms"] > 0

    def test_measure_step_context_manager_failure(self):
        """Test measure_step context manager for failed execution."""
        manager = SimpleObservabilityManager("test")

        with pytest.raises(ValueError):
            with manager.measure_step("failing_context_step"):
                raise ValueError("Test exception")

        metrics = manager.get_metrics()
        assert metrics["completed_steps"] == 1
        assert metrics["failed_steps"] == 1

        failed_steps = manager.get_failed_steps()
        assert len(failed_steps) == 1
        assert "Test exception" in failed_steps[0].error_message

    def test_step_retry_handling(self):
        """Test handling of step retries."""
        manager = SimpleObservabilityManager("test")

        # Start step
        manager.start_step("retry_step")

        # Start same step again (retry)
        manager.start_step("retry_step")
        manager.end_step("retry_step", success=True)

        metrics = manager.get_metrics()
        # Should have one completed step (the retry)
        assert metrics["completed_steps"] == 1
        assert metrics["successful_steps"] == 1

    def test_end_step_without_start(self):
        """Test ending a step that wasn't started."""
        manager = SimpleObservabilityManager("test")

        manager.end_step("unknown_step", success=False, error="Not started")

        metrics = manager.get_metrics()
        assert metrics["total_steps"] == 1
        assert metrics["failed_steps"] == 1

        step_details = metrics["step_details"]["unknown_step"]
        # When error is provided, it's used; when None, default message is used
        assert step_details["error_message"] == "Not started"

    def test_end_step_without_start_no_error(self):
        """Test ending a step that wasn't started with no error message."""
        manager = SimpleObservabilityManager("test")

        manager.end_step("unknown_step", success=False)

        metrics = manager.get_metrics()
        step_details = metrics["step_details"]["unknown_step"]
        assert "not properly started" in step_details["error_message"].lower()

    def test_get_successful_and_failed_steps(self):
        """Test filtering successful and failed steps."""
        manager = SimpleObservabilityManager("test")

        # Add successful step
        manager.start_step("success_step")
        manager.end_step("success_step", success=True)

        # Add failed step
        manager.start_step("fail_step")
        manager.end_step("fail_step", success=False, error="Failed")

        successful = manager.get_successful_steps()
        failed = manager.get_failed_steps()

        assert len(successful) == 1
        assert len(failed) == 1
        assert successful[0].step_id == "success_step"
        assert failed[0].step_id == "fail_step"

    def test_manager_string_representation(self):
        """Test the string representation of the manager."""
        manager = SimpleObservabilityManager("test_exec")

        manager.start_step("step1")
        manager.end_step("step1", success=True)

        str_repr = str(manager)
        assert "test_exec" in str_repr
        assert "1 steps completed" in str_repr
        assert "1 successful" in str_repr
        assert "0 failed" in str_repr


class TestCreateObservabilityManager:
    """Test the factory function."""

    def test_factory_with_execution_id(self):
        """Test factory function with execution ID."""
        manager = create_observability_manager("custom_id")

        assert isinstance(manager, SimpleObservabilityManager)
        assert manager.execution_id == "custom_id"

    def test_factory_without_execution_id(self):
        """Test factory function without execution ID."""
        manager = create_observability_manager()

        assert isinstance(manager, SimpleObservabilityManager)
        assert manager.execution_id is not None


class TestObservabilityIntegration:
    """Integration tests for the observability system."""

    def test_complete_pipeline_tracking(self):
        """Test tracking a complete pipeline execution."""
        manager = SimpleObservabilityManager("pipeline_test")

        # Simulate a complete pipeline
        steps = [
            ("extract", True, 1000),
            ("transform", True, 800),
            ("load", False, 0),  # Failed step
        ]

        for step_name, success, rows in steps:
            manager.start_step(step_name)
            if rows > 0:
                manager.record_rows_affected(step_name, rows)
            manager.add_step_metadata(step_name, {"pipeline_stage": step_name})
            manager.end_step(
                step_name,
                success=success,
                error=None if success else f"{step_name} failed",
            )

        # Verify final metrics
        metrics = manager.get_metrics()
        assert metrics["total_steps"] == 3
        assert metrics["completed_steps"] == 3
        assert metrics["successful_steps"] == 2
        assert metrics["failed_steps"] == 1
        assert metrics["total_rows_affected"] == 1800
        assert manager.has_failures

        # Verify step details
        assert metrics["step_details"]["extract"]["success"]
        assert metrics["step_details"]["transform"]["success"]
        assert not metrics["step_details"]["load"]["success"]
        assert "load failed" in metrics["step_details"]["load"]["error_message"]

    def test_concurrent_step_tracking(self):
        """Test that manager handles concurrent-like operations."""
        manager = SimpleObservabilityManager("concurrent_test")

        # Simulate overlapping step tracking
        manager.start_step("step1")
        manager.start_step("step2")

        manager.end_step("step2", success=True)
        manager.end_step("step1", success=True)

        metrics = manager.get_metrics()
        assert metrics["completed_steps"] == 2
        assert metrics["successful_steps"] == 2
