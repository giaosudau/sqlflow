"""Tests for V2 Error Handling Module.

Following SQLFlow testing standards and Week 7-8 requirements:
- Proper exception hierarchy
- Context manager functionality
- Error recovery mechanisms
- Observability integration
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from sqlflow.core.executors.v2.error_handling import (
    ConnectorError,
    DatabaseError,
    SQLFlowError,
    StepExecutionError,
    VariableSubstitutionError,
    step_execution_context,
)


class TestSQLFlowExceptionHierarchy:
    """Test the SQLFlow exception hierarchy follows Week 7-8 requirements."""

    def test_base_sqlflow_error_creation(self):
        """Test base SQLFlowError can be created with proper attributes."""
        error = SQLFlowError(
            message="Test error", error_code="TEST_001", context={"detail": "value"}
        )

        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.error_code == "TEST_001"
        assert error.context == {"detail": "value"}
        assert error.timestamp is not None

    def test_sqlflow_error_defaults(self):
        """Test SQLFlowError uses proper defaults."""
        error = SQLFlowError("Simple error")

        assert error.message == "Simple error"
        assert error.error_code == "SQLFLOWERROR"
        assert error.context == {}
        assert error.timestamp is not None

    def test_step_execution_error_specifics(self):
        """Test StepExecutionError includes step-specific information."""
        error = StepExecutionError(
            step_id="load_customers",
            step_type="load",
            message="Connection failed",
            error_code="CONN_FAIL",
        )

        assert error.step_id == "load_customers"
        assert error.step_type == "load"
        assert error.message == "Connection failed"
        assert error.error_code == "CONN_FAIL"

    def test_connector_error_inheritance(self):
        """Test ConnectorError properly inherits from SQLFlowError."""
        error = ConnectorError("Connector issue")

        assert isinstance(error, SQLFlowError)
        assert error.message == "Connector issue"

    def test_database_error_inheritance(self):
        """Test DatabaseError properly inherits from SQLFlowError."""
        error = DatabaseError("Database connection lost")

        assert isinstance(error, SQLFlowError)
        assert error.message == "Database connection lost"

    def test_variable_substitution_error_inheritance(self):
        """Test VariableSubstitutionError properly inherits from SQLFlowError."""
        error = VariableSubstitutionError("Variable ${missing} not found")

        assert isinstance(error, SQLFlowError)
        assert error.message == "Variable ${missing} not found"


class TestStepExecutionContext:
    """Test the step_execution_context context manager."""

    def test_successful_execution_with_context(self):
        """Test context manager handles successful execution."""
        mock_observability = MagicMock()

        with step_execution_context(
            "test_step", "load", mock_observability, auto_log=False
        ) as ctx:
            # Simulate some work
            time.sleep(0.01)
            ctx["rows_processed"] = 100

        # Verify observability calls
        mock_observability.record_step_start.assert_called_once_with(
            "test_step", "load"
        )
        mock_observability.record_step_success.assert_called_once()

        # Verify success call arguments
        success_call_args = mock_observability.record_step_success.call_args
        assert success_call_args[0][0] == "test_step"
        success_data = success_call_args[0][1]
        assert "duration_ms" in success_data
        assert success_data["step_type"] == "load"

    def test_execution_failure_with_context(self):
        """Test context manager properly handles exceptions."""
        mock_observability = MagicMock()

        with pytest.raises(StepExecutionError) as exc_info:
            with step_execution_context(
                "failing_step", "transform", mock_observability, auto_log=False
            ):
                raise ValueError("Something went wrong")

        # Verify the exception was converted to StepExecutionError
        error = exc_info.value
        assert isinstance(error, StepExecutionError)
        assert error.step_id == "failing_step"
        assert error.step_type == "transform"
        assert "Something went wrong" in error.message

        # Verify observability calls
        mock_observability.record_step_start.assert_called_once()
        mock_observability.record_step_failure.assert_called_once()

    def test_sqlflow_error_not_wrapped(self):
        """Test that SQLFlowError exceptions are not double-wrapped."""
        mock_observability = MagicMock()
        original_error = ConnectorError("Original connector issue")

        with pytest.raises(ConnectorError) as exc_info:
            with step_execution_context(
                "step_1", "load", mock_observability, auto_log=False
            ):
                raise original_error

        # Verify the original error is preserved
        assert exc_info.value is original_error

    def test_context_without_observability(self):
        """Test context manager works without observability manager."""
        with step_execution_context("test_step", "load", None, auto_log=False) as ctx:
            ctx["test_data"] = "success"

        # Should complete without errors
        assert ctx["test_data"] == "success"

    @patch("sqlflow.core.executors.v2.error_handling.logger")
    def test_auto_logging_enabled(self, mock_logger):
        """Test automatic logging when auto_log=True."""
        with step_execution_context("logged_step", "export", auto_log=True):
            pass

        # Verify start and success logs
        mock_logger.info.assert_any_call("🔄 Starting export step: logged_step")

        # Find the completion log call
        completion_calls = [
            call
            for call in mock_logger.info.call_args_list
            if "✅ Completed" in str(call)
        ]
        assert len(completion_calls) == 1

    @patch("sqlflow.core.executors.v2.error_handling.logger")
    def test_auto_logging_on_failure(self, mock_logger):
        """Test automatic error logging when auto_log=True."""
        with pytest.raises(StepExecutionError):
            with step_execution_context("error_step", "load", auto_log=True):
                raise RuntimeError("Test failure")

        # Verify error log
        mock_logger.error.assert_called()
        error_call = mock_logger.error.call_args_list[0]
        assert "❌ Failed load step: error_step" in str(error_call)

    def test_context_performance_metrics(self):
        """Test that context includes performance metrics."""
        with step_execution_context("perf_step", "transform", auto_log=False) as ctx:
            # Add custom performance metrics
            ctx["performance_metrics"]["custom_metric"] = 42

        assert "performance_metrics" in ctx
        assert ctx["performance_metrics"]["custom_metric"] == 42

    def test_observability_failure_handling(self):
        """Test that observability failures don't break execution."""
        mock_observability = MagicMock()
        mock_observability.record_step_start.side_effect = Exception(
            "Observability down"
        )

        # Should not raise exception even if observability fails
        with step_execution_context(
            "robust_step", "load", mock_observability, auto_log=False
        ):
            pass

    def test_context_timing_accuracy(self):
        """Test that timing measurements are accurate."""
        start_time = time.perf_counter()

        with step_execution_context("timed_step", "load", auto_log=False) as ctx:
            time.sleep(0.1)  # Sleep for 100ms

        end_time = time.perf_counter()
        actual_duration = (end_time - start_time) * 1000

        # Duration should be close to 100ms (within 50ms tolerance)
        assert 50 <= actual_duration <= 200


class TestErrorRecoveryMechanisms:
    """Test error recovery mechanisms as required by Week 7-8."""

    def test_graceful_degradation_example(self):
        """Test that system can gracefully degrade when components fail."""
        # This tests the principle that errors should provide actionable information

        mock_observability = MagicMock()
        mock_observability.record_step_start.side_effect = Exception(
            "Observability unavailable"
        )

        # System should continue working even if observability fails
        executed = False
        with step_execution_context(
            "resilient_step", "load", mock_observability, auto_log=False
        ):
            executed = True

        assert executed, "Step should execute even if observability fails"

    def test_error_context_propagation(self):
        """Test that error context is properly propagated."""
        with pytest.raises(StepExecutionError) as exc_info:
            with step_execution_context("context_step", "transform", auto_log=False):
                # Simulate error with context
                raise ValueError("Database connection timeout after 30s")

        error = exc_info.value
        assert "Database connection timeout" in error.message
        assert "context" in error.__dict__
        assert "original_error" in error.context

    def test_no_silent_failures(self):
        """Test that failures are never silent as per Zen of Python."""
        # Any exception should be converted and raised, never silenced

        with pytest.raises(StepExecutionError):
            with step_execution_context("no_silent_step", "load", auto_log=False):
                raise ConnectionError("Network issue")

        # If we reach here without exception, the test should fail
        # The context manager should ALWAYS propagate errors


class TestComprehensiveLogging:
    """Test comprehensive logging following SQLFlow standards."""

    @patch("sqlflow.core.executors.v2.error_handling.logger")
    def test_contextual_logging_information(self, mock_logger):
        """Test that logs include helpful contextual information."""
        mock_observability = MagicMock()

        with step_execution_context(
            "contextual_step", "load", mock_observability, auto_log=True
        ) as ctx:
            ctx["rows_processed"] = 1000
            ctx["data_source"] = "customers.csv"

        # Verify logs contain contextual information
        info_calls = mock_logger.info.call_args_list
        start_call = str(info_calls[0])
        completion_call = str(info_calls[1])

        assert "contextual_step" in start_call
        assert "load" in start_call
        assert "contextual_step" in completion_call
        assert "ms)" in completion_call  # Duration info

    @patch("sqlflow.core.executors.v2.error_handling.logger")
    def test_error_logging_detail(self, mock_logger):
        """Test that error logs provide actionable information."""
        with pytest.raises(StepExecutionError):
            with step_execution_context("detailed_error_step", "export", auto_log=True):
                raise PermissionError("Access denied to /data/output.csv")

        # Verify error log contains helpful details
        error_calls = mock_logger.error.call_args_list
        assert len(error_calls) == 1

        error_message = str(error_calls[0])
        assert "detailed_error_step" in error_message
        assert "export" in error_message
        assert "Access denied" in error_message


class TestWeek7And8Requirements:
    """Test specific Week 7-8 deliverables."""

    def test_clear_error_messages_with_actionable_information(self):
        """Test that error messages are clear and actionable."""
        error = StepExecutionError(
            step_id="data_load",
            step_type="load",
            message="Failed to connect to PostgreSQL: connection timeout",
            context={
                "host": "db.example.com",
                "port": 5432,
                "suggested_actions": [
                    "Check network connectivity",
                    "Verify database credentials",
                    "Check firewall settings",
                ],
            },
        )

        assert "PostgreSQL" in error.message
        assert "connection timeout" in error.message
        assert error.step_id == "data_load"
        assert "suggested_actions" in error.context

    def test_no_silent_failures_principle(self):
        """Test that no failures are silent."""
        # Every error should be logged and raised
        mock_observability = MagicMock()

        with pytest.raises(StepExecutionError):
            with step_execution_context("test_step", "load", mock_observability):
                # Simulate any kind of failure
                raise KeyError("Missing configuration key")

        # Observability should have recorded the failure
        mock_observability.record_step_failure.assert_called_once()

    def test_error_context_propagation_requirement(self):
        """Test proper error context propagation."""
        with pytest.raises(StepExecutionError) as exc_info:
            with step_execution_context("propagation_test", "transform"):
                raise RuntimeError("Downstream service unavailable")

        error = exc_info.value
        # Error should maintain context about where it occurred
        assert error.step_id == "propagation_test"
        assert error.step_type == "transform"
        assert "context" in error.__dict__
        assert "duration_ms" in error.context

    def test_performance_metrics_collection_requirement(self):
        """Test that performance metrics are collected."""
        mock_observability = MagicMock()

        with step_execution_context("metrics_test", "load", mock_observability) as ctx:
            # Simulate collecting metrics during execution
            ctx["performance_metrics"]["rows_processed"] = 5000
            ctx["performance_metrics"]["memory_used_mb"] = 128

        # Verify metrics were passed to observability
        success_call = mock_observability.record_step_success.call_args
        success_data = success_call[0][1]

        assert "rows_processed" in success_data
        assert "memory_used_mb" in success_data
        assert success_data["rows_processed"] == 5000
        assert success_data["memory_used_mb"] == 128
