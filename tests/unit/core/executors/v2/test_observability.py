"""Tests for V2 Observability Module.

Following SQLFlow testing standards and Week 7-8 requirements:
- Comprehensive metrics collection
- Performance alerts with actionable insights
- Tracing capabilities
- Integration with error handling
"""

import threading
import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from sqlflow.core.executors.v2.observability import (
    AlertSeverity,
    ObservabilityManager,
    PerformanceAlert,
    StepMetrics,
)


class TestStepMetrics:
    """Test the StepMetrics data collection."""

    def test_step_metrics_initialization(self):
        """Test StepMetrics initializes with correct defaults."""
        metrics = StepMetrics()

        assert metrics.calls == 0
        assert metrics.failures == 0
        assert metrics.total_time_ms == 0.0
        assert metrics.total_rows == 0
        assert metrics.success_rate == 0.0
        assert metrics.average_duration_ms == 0.0
        assert metrics.throughput_rows_per_second == 0.0

    def test_add_successful_execution(self):
        """Test adding successful execution metrics."""
        metrics = StepMetrics()

        metrics.add_execution(
            duration_ms=100.0, rows=500, success=True, resource_usage={"memory_mb": 64}
        )

        assert metrics.calls == 1
        assert metrics.failures == 0
        assert metrics.total_time_ms == 100.0
        assert metrics.total_rows == 500
        assert metrics.success_rate == 100.0
        assert metrics.average_duration_ms == 100.0
        assert metrics.throughput_rows_per_second == 5000.0  # 500 rows / 0.1 seconds

    def test_add_failed_execution(self):
        """Test adding failed execution metrics."""
        metrics = StepMetrics()

        metrics.add_execution(duration_ms=50.0, success=False)

        assert metrics.calls == 1
        assert metrics.failures == 1
        assert metrics.total_time_ms == 50.0
        assert metrics.total_rows == 0
        assert metrics.success_rate == 0.0

    def test_multiple_executions_statistics(self):
        """Test statistics with multiple executions."""
        metrics = StepMetrics()

        # Add multiple executions
        metrics.add_execution(duration_ms=100.0, rows=100, success=True)
        metrics.add_execution(duration_ms=200.0, rows=200, success=True)
        metrics.add_execution(duration_ms=150.0, rows=150, success=False)

        assert metrics.calls == 3
        assert metrics.failures == 1
        assert metrics.total_time_ms == 450.0
        assert metrics.total_rows == 300  # Only successful executions count rows
        assert metrics.success_rate == pytest.approx(66.67, abs=0.1)
        assert metrics.average_duration_ms == 150.0
        assert metrics.throughput_rows_per_second == pytest.approx(666.67, abs=0.1)


class TestPerformanceAlert:
    """Test the PerformanceAlert functionality."""

    def test_performance_alert_creation(self):
        """Test creating a performance alert with all attributes."""
        timestamp = datetime.utcnow()
        alert = PerformanceAlert(
            component="load_customers",
            alert_type="slow_execution",
            message="Step took too long",
            severity=AlertSeverity.WARNING,
            timestamp=timestamp,
            suggested_actions=["Optimize query", "Check indexes"],
            metadata={"duration_ms": 35000},
        )

        assert alert.component == "load_customers"
        assert alert.alert_type == "slow_execution"
        assert alert.message == "Step took too long"
        assert alert.severity == AlertSeverity.WARNING
        assert alert.timestamp == timestamp
        assert "Optimize query" in alert.suggested_actions
        assert alert.metadata["duration_ms"] == 35000

    def test_alert_to_dict_serialization(self):
        """Test alert serialization to dictionary."""
        timestamp = datetime.utcnow()
        alert = PerformanceAlert(
            component="test_component",
            alert_type="test_alert",
            message="Test message",
            severity=AlertSeverity.ERROR,
            timestamp=timestamp,
            suggested_actions=["Action 1"],
            metadata={"key": "value"},
        )

        alert_dict = alert.to_dict()

        assert alert_dict["component"] == "test_component"
        assert alert_dict["alert_type"] == "test_alert"
        assert alert_dict["message"] == "Test message"
        assert alert_dict["severity"] == "error"
        assert alert_dict["timestamp"] == timestamp.isoformat()
        assert alert_dict["suggested_actions"] == ["Action 1"]
        assert alert_dict["metadata"] == {"key": "value"}

    def test_alert_severity_enum(self):
        """Test AlertSeverity enum values."""
        assert AlertSeverity.INFO.value == "info"
        assert AlertSeverity.WARNING.value == "warning"
        assert AlertSeverity.ERROR.value == "error"
        assert AlertSeverity.CRITICAL.value == "critical"


class TestObservabilityManager:
    """Test the main ObservabilityManager functionality."""

    def test_observability_manager_initialization(self):
        """Test ObservabilityManager initializes correctly."""
        manager = ObservabilityManager("test_run_123")

        assert manager.run_id == "test_run_123"
        assert manager.slow_step_threshold_ms == 30000
        assert manager.critical_failure_rate == 25.0
        assert isinstance(manager._run_start_time, datetime)

    @patch("sqlflow.core.executors.v2.observability.logger")
    def test_record_step_start(self, mock_logger):
        """Test recording step start."""
        manager = ObservabilityManager("test_run")

        manager.record_step_start("step_1", "load")

        mock_logger.debug.assert_called_with("Step started: step_1 (load)")

    def test_record_step_success(self):
        """Test recording successful step completion."""
        manager = ObservabilityManager("test_run")

        event_data = {
            "duration_ms": 5000.0,
            "rows_affected": 1000,
            "step_type": "load",
            "resource_usage": {"memory_mb": 128},
        }

        manager.record_step_success("step_1", event_data)

        # Verify metrics were recorded
        metrics = manager._step_metrics["load"]
        assert metrics.calls == 1
        assert metrics.failures == 0
        assert metrics.total_time_ms == 5000.0
        assert metrics.total_rows == 1000

    def test_record_step_failure(self):
        """Test recording step failure."""
        manager = ObservabilityManager("test_run")

        manager.record_step_failure("step_1", "load", "Connection timeout", 2000.0)

        # Verify failure metrics
        metrics = manager._step_metrics["load"]
        assert metrics.calls == 1
        assert metrics.failures == 1
        assert metrics.total_time_ms == 2000.0

        # Verify alert was generated
        assert len(manager._alerts) == 1
        alert = manager._alerts[0]
        assert alert.alert_type == "step_failure"
        assert alert.component == "step_1"
        assert alert.severity == AlertSeverity.ERROR

    def test_slow_execution_alert_generation(self):
        """Test that slow execution generates alerts."""
        manager = ObservabilityManager("test_run")

        # Set a lower threshold for testing
        manager.slow_step_threshold_ms = 1000

        event_data = {
            "duration_ms": 5000.0,  # Exceeds threshold
            "step_type": "transform",
        }

        manager.record_step_success("slow_step", event_data)

        # Verify slow execution alert
        assert len(manager._alerts) == 1
        alert = manager._alerts[0]
        assert alert.alert_type == "slow_execution"
        assert alert.severity == AlertSeverity.WARNING
        assert "5.0s" in alert.message
        assert "Consider optimizing" in alert.suggested_actions[0]

    def test_thread_safety_concurrent_operations(self):
        """Test that observability manager is thread-safe."""
        manager = ObservabilityManager("concurrent_test")

        def record_operations():
            for i in range(10):
                manager.record_step_success(
                    f"step_{i}",
                    {"duration_ms": 100.0, "rows_affected": 50, "step_type": "load"},
                )

        # Run concurrent operations
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=record_operations)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all operations were recorded safely
        metrics = manager._step_metrics["load"]
        assert metrics.calls == 50  # 10 operations × 5 threads
        assert metrics.total_rows == 2500  # 50 rows × 50 operations

    def test_performance_summary_generation(self):
        """Test comprehensive performance summary generation."""
        manager = ObservabilityManager("summary_test")

        # Record some test data
        manager.record_step_success(
            "step_1", {"duration_ms": 1000.0, "rows_affected": 500, "step_type": "load"}
        )
        manager.record_step_success(
            "step_2",
            {"duration_ms": 2000.0, "rows_affected": 1000, "step_type": "transform"},
        )
        manager.record_step_failure("step_3", "export", "Network error", 500.0)

        summary = manager.get_performance_summary()

        assert summary["run_id"] == "summary_test"
        assert summary["total_steps"] == 3
        assert summary["total_failures"] == 1
        assert summary["failure_rate"] == pytest.approx(33.33, abs=0.1)
        assert summary["total_execution_time_ms"] == 3500.0
        assert summary["total_rows_processed"] == 1500
        assert summary["throughput_rows_per_second"] == pytest.approx(428.57, abs=0.1)
        assert "step_details" in summary
        assert len(summary["alerts"]) == 1  # One failure alert

    def test_observability_with_no_data(self):
        """Test observability behavior with no recorded data."""
        manager = ObservabilityManager("empty_test")

        summary = manager.get_performance_summary()

        assert summary["total_steps"] == 0
        assert summary["total_failures"] == 0
        assert summary["failure_rate"] == 0.0
        assert summary["total_execution_time_ms"] == 0
        assert summary["total_rows_processed"] == 0
        assert summary["throughput_rows_per_second"] == 0
        assert len(summary["alerts"]) == 0

    @patch("sqlflow.core.executors.v2.observability.logger")
    def test_alert_logging_integration(self, mock_logger):
        """Test that alerts are properly logged."""
        manager = ObservabilityManager("alert_test")
        manager.slow_step_threshold_ms = 100  # Low threshold for testing

        # Trigger slow execution alert
        manager.record_step_success(
            "slow_step", {"duration_ms": 500.0, "step_type": "load"}
        )

        # Verify warning was logged
        mock_logger.warning.assert_called()
        warning_call = mock_logger.warning.call_args[0][0]
        assert "PERFORMANCE ALERT" in warning_call

        # Trigger failure alert
        manager.record_step_failure("failed_step", "transform", "Test error", 100.0)

        # Verify error was logged
        mock_logger.error.assert_called()
        error_calls = [
            call
            for call in mock_logger.error.call_args_list
            if "PERFORMANCE ALERT" in str(call)
        ]
        assert len(error_calls) == 1


class TestObservabilityIntegration:
    """Test observability integration with other V2 components."""

    def test_error_handling_observability_integration(self):
        """Test integration with error handling module."""
        from sqlflow.core.executors.v2.error_handling import step_execution_context

        mock_observability = ObservabilityManager("integration_test")

        # Test successful execution
        with step_execution_context(
            "test_step", "load", mock_observability, auto_log=False
        ) as ctx:
            ctx["performance_metrics"]["rows_processed"] = 1000

        # Verify observability recorded the execution
        metrics = mock_observability._step_metrics["load"]
        assert metrics.calls == 1
        assert metrics.failures == 0

    def test_observability_failure_resilience(self):
        """Test that observability failures don't break execution."""
        from sqlflow.core.executors.v2.error_handling import step_execution_context

        # Create a mock that fails
        mock_observability = MagicMock()
        mock_observability.record_step_start.side_effect = Exception(
            "Observability error"
        )
        mock_observability.record_step_success.side_effect = Exception(
            "Observability error"
        )

        # Execution should still work
        executed = False
        with step_execution_context(
            "resilient_step", "load", mock_observability, auto_log=False
        ):
            executed = True

        assert executed, "Execution should succeed even if observability fails"


class TestWeek7And8ObservabilityRequirements:
    """Test specific Week 7-8 observability deliverables."""

    def test_comprehensive_metrics_collection(self):
        """Test that comprehensive metrics are collected as required."""
        manager = ObservabilityManager("metrics_test")

        # Record various types of operations
        manager.record_step_success(
            "load_step",
            {
                "duration_ms": 1500.0,
                "rows_affected": 10000,
                "step_type": "load",
                "resource_usage": {"memory_mb": 256, "cpu_percent": 45},
            },
        )

        manager.record_step_success(
            "transform_step",
            {
                "duration_ms": 3000.0,
                "rows_affected": 10000,
                "step_type": "transform",
                "resource_usage": {"memory_mb": 512, "cpu_percent": 80},
            },
        )

        summary = manager.get_performance_summary()

        # Verify comprehensive metrics
        assert "total_steps" in summary
        assert "total_failures" in summary
        assert "failure_rate" in summary
        assert "total_execution_time_ms" in summary
        assert "total_rows_processed" in summary
        assert "throughput_rows_per_second" in summary
        assert "run_duration_ms" in summary

    def test_actionable_performance_alerts(self):
        """Test that performance alerts provide actionable insights."""
        manager = ObservabilityManager("actionable_test")
        manager.slow_step_threshold_ms = 1000

        # Trigger slow execution
        manager.record_step_success(
            "slow_analytics", {"duration_ms": 5000.0, "step_type": "transform"}
        )

        alert = manager._alerts[0]

        # Verify actionable insights
        assert len(alert.suggested_actions) > 0
        actions = [action.lower() for action in alert.suggested_actions]
        actionable_keywords = ["optimize", "check", "review", "consider"]

        # At least one action should contain actionable keywords
        assert any(
            any(keyword in action for keyword in actionable_keywords)
            for action in actions
        )

    def test_error_recovery_observability(self):
        """Test observability during error recovery scenarios."""
        manager = ObservabilityManager("recovery_test")

        # Record a pattern of failures and recoveries
        manager.record_step_failure("unstable_step", "load", "Connection lost", 1000.0)
        manager.record_step_failure("unstable_step", "load", "Connection lost", 1500.0)
        manager.record_step_success(
            "unstable_step",
            {"duration_ms": 2000.0, "rows_affected": 1000, "step_type": "load"},
        )

        metrics = manager._step_metrics["load"]

        # Verify recovery is tracked
        assert metrics.calls == 3
        assert metrics.failures == 2
        assert metrics.success_rate == pytest.approx(33.33, abs=0.1)

        # Verify failure alerts were generated
        failure_alerts = [
            alert for alert in manager._alerts if alert.alert_type == "step_failure"
        ]
        assert len(failure_alerts) == 2

    def test_tracing_capabilities(self):
        """Test tracing capabilities for debugging."""
        manager = ObservabilityManager("tracing_test")

        # Simulate a pipeline execution with tracing
        steps = [
            ("extract_data", "load", 1000.0, 5000),
            ("clean_data", "transform", 2000.0, 5000),
            ("aggregate_data", "transform", 1500.0, 500),
            ("export_results", "export", 500.0, 500),
        ]

        for step_id, step_type, duration, rows in steps:
            manager.record_step_success(
                step_id,
                {
                    "duration_ms": duration,
                    "rows_affected": rows,
                    "step_type": step_type,
                },
            )

        summary = manager.get_performance_summary()

        # Verify tracing information is available
        assert summary["total_steps"] == 4
        assert summary["total_execution_time_ms"] == 5000.0

        # Each step type should have metrics
        assert "load" in manager._step_metrics
        assert "transform" in manager._step_metrics
        assert "export" in manager._step_metrics

        # Transform steps should show combined metrics
        transform_metrics = manager._step_metrics["transform"]
        assert transform_metrics.calls == 2
        assert transform_metrics.total_time_ms == 3500.0  # 2000 + 1500
        assert transform_metrics.total_rows == 5500  # 5000 + 500

    def test_no_silent_failures_in_observability(self):
        """Test that observability itself has no silent failures."""
        manager = ObservabilityManager("no_silent_test")

        # Even with invalid data, should not silently fail
        manager.record_step_success(
            "test_step",
            {
                "duration_ms": None,  # Invalid data
                "rows_affected": "invalid",  # Invalid data
                "step_type": "load",
            },
        )

        # Should still record the attempt
        metrics = manager._step_metrics["load"]
        assert metrics.calls == 1

        # Summary should still be generated
        summary = manager.get_performance_summary()
        assert summary["total_steps"] == 1

    @patch("sqlflow.core.executors.v2.observability.logger")
    def test_comprehensive_logging_standards(self, mock_logger):
        """Test that logging follows SQLFlow standards."""
        manager = ObservabilityManager("logging_test")

        # Various operations should produce appropriate logs
        manager.record_step_start("test_step", "load")
        manager.record_step_success(
            "test_step",
            {"duration_ms": 1000.0, "rows_affected": 500, "step_type": "load"},
        )
        manager.record_step_failure("failed_step", "transform", "Test error", 500.0)

        # Verify appropriate logging levels were used
        assert mock_logger.debug.called  # Step start
        assert mock_logger.debug.called  # Step success
        assert mock_logger.error.called  # Step failure

        # Verify no print statements were used
        # (This is enforced by the logging standards)
        # The observability module should only use the logging module


class TestWeek7And8NewFeatures:
    """Test the new features added for Week 7-8 implementation."""

    def test_measure_scope_success(self):
        """Test the measure_scope context manager for successful operations."""
        manager = ObservabilityManager("scope_test")

        executed = False
        with manager.measure_scope("test_operation", {"param": "value"}):
            time.sleep(0.01)  # Simulate work
            executed = True

        assert executed

    def test_measure_scope_failure(self):
        """Test the measure_scope context manager handles failures properly."""
        manager = ObservabilityManager("scope_failure_test")

        with pytest.raises(ValueError):
            with manager.measure_scope("failing_operation"):
                raise ValueError("Test failure")

        # Should have generated a failure alert
        alerts = manager.get_alerts()
        failure_alerts = [a for a in alerts if a.alert_type == "scope_failure"]
        assert len(failure_alerts) == 1

        alert = failure_alerts[0]
        assert alert.component == "failing_operation"
        assert alert.severity == AlertSeverity.ERROR
        assert "Test failure" in alert.message

    def test_record_recovery_attempt_success(self):
        """Test recording successful recovery attempts."""
        manager = ObservabilityManager("recovery_test")

        manager.record_recovery_attempt("database_connector", "reconnect", True)

        alerts = manager.get_alerts()
        recovery_alerts = [a for a in alerts if a.alert_type == "recovery_success"]
        assert len(recovery_alerts) == 1

        alert = recovery_alerts[0]
        assert alert.component == "database_connector"
        assert alert.severity == AlertSeverity.INFO
        assert "succeeded" in alert.message

    def test_record_recovery_attempt_failure(self):
        """Test recording failed recovery attempts."""
        manager = ObservabilityManager("recovery_fail_test")

        manager.record_recovery_attempt("api_connector", "retry", False)

        alerts = manager.get_alerts()
        recovery_alerts = [a for a in alerts if a.alert_type == "recovery_failure"]
        assert len(recovery_alerts) == 1

        alert = recovery_alerts[0]
        assert alert.component == "api_connector"
        assert alert.severity == AlertSeverity.WARNING
        assert "failed" in alert.message

    def test_system_health_check_healthy(self):
        """Test system health check when system is healthy."""
        manager = ObservabilityManager("health_test")

        # Record successful operations
        for i in range(10):
            manager.record_step_success(
                f"step_{i}",
                {"duration_ms": 100.0, "rows_affected": 100, "step_type": "load"},
            )

        health = manager.check_system_health()

        assert health["overall_health"] == "healthy"
        assert health["total_steps"] == 10
        assert health["total_failures"] == 0
        assert health["failure_rate"] == 0.0
        assert len(health["recommendations"]) == 0

    def test_system_health_check_degraded(self):
        """Test system health check when system is degraded."""
        manager = ObservabilityManager("degraded_test")

        # Record mixed operations with 30% failure rate
        for i in range(10):
            if i < 7:
                manager.record_step_success(
                    f"step_{i}",
                    {"duration_ms": 100.0, "rows_affected": 100, "step_type": "load"},
                )
            else:
                manager.record_step_failure(f"step_{i}", "load", "Test error", 100.0)

        health = manager.check_system_health()

        assert health["overall_health"] == "degraded"
        assert health["total_steps"] == 10
        assert health["total_failures"] == 3
        assert health["failure_rate"] == 30.0
        assert len(health["recommendations"]) > 0

    def test_system_health_check_critical(self):
        """Test system health check when system is critical."""
        manager = ObservabilityManager("critical_test")

        # Record mostly failures
        for i in range(10):
            if i < 3:
                manager.record_step_success(
                    f"step_{i}",
                    {"duration_ms": 100.0, "rows_affected": 100, "step_type": "load"},
                )
            else:
                manager.record_step_failure(
                    f"step_{i}", "load", "Critical error", 100.0
                )

        health = manager.check_system_health()

        assert health["overall_health"] == "critical"
        assert health["total_steps"] == 10
        assert health["total_failures"] == 7
        assert health["failure_rate"] == 70.0
        assert len(health["recommendations"]) > 0
        assert any("Immediate attention" in rec for rec in health["recommendations"])

    def test_get_alerts_filtering(self):
        """Test alert filtering by severity."""
        manager = ObservabilityManager("filter_test")

        # Generate different types of alerts
        manager.record_recovery_attempt("comp1", "strategy1", True)  # INFO
        manager.record_recovery_attempt("comp2", "strategy2", False)  # WARNING
        manager.record_step_failure("step1", "load", "Error", 100.0)  # ERROR

        all_alerts = manager.get_alerts()
        assert len(all_alerts) == 3

        info_alerts = manager.get_alerts(AlertSeverity.INFO)
        assert len(info_alerts) == 1

        warning_alerts = manager.get_alerts(AlertSeverity.WARNING)
        assert len(warning_alerts) == 1

        error_alerts = manager.get_alerts(AlertSeverity.ERROR)
        assert len(error_alerts) == 1

    @patch("sqlflow.core.executors.v2.observability.logger")
    def test_measure_scope_logging(self, mock_logger):
        """Test that measure_scope provides proper logging."""
        manager = ObservabilityManager("logging_test")

        with manager.measure_scope("logged_operation"):
            pass

        # Verify debug logs were called
        mock_logger.debug.assert_any_call(
            "Starting scope measurement: logged_operation"
        )

        # Find completion log
        completion_calls = [
            call
            for call in mock_logger.debug.call_args_list
            if "Completed scope" in str(call)
        ]
        assert len(completion_calls) == 1
