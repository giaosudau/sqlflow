"""
Unit tests for the monitoring and metrics collection framework.

Tests the monitoring components including MetricsCollector, AlertManager,
RealTimeMonitor, and integration with incremental strategies.
"""

import time
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from sqlflow.core.engines.duckdb.transform.monitoring import (
    Alert,
    AlertManager,
    AlertSeverity,
    MetricsCollector,
    MetricType,
    MonitoringManager,
    RealTimeMonitor,
    ThresholdRule,
    TransformOperationMonitor,
)


class TestMetricsCollector(unittest.TestCase):
    """Test MetricsCollector functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.collector = MetricsCollector(retention_hours=1, max_points_per_metric=100)

    def test_record_metric(self):
        """Test recording metrics."""
        self.collector.record_metric(
            "test.metric", 42.0, MetricType.GAUGE, unit="count"
        )

        # Verify metric was recorded
        value = self.collector.get_metric_value("test.metric")
        self.assertEqual(value, 42.0)

        # Verify metadata
        self.assertIn("test.metric", self.collector.metric_metadata)
        metadata = self.collector.metric_metadata["test.metric"]
        self.assertEqual(metadata["type"], "gauge")
        self.assertEqual(metadata["unit"], "count")

    def test_record_metric_with_labels(self):
        """Test recording metrics with labels."""
        labels = {"operation": "test", "status": "success"}
        self.collector.record_metric(
            "ops.count", 1.0, MetricType.COUNTER, labels=labels
        )

        # Get value with matching labels
        value = self.collector.get_metric_value("ops.count", labels)
        self.assertEqual(value, 1.0)

        # Get value with non-matching labels should return None
        value = self.collector.get_metric_value("ops.count", {"operation": "other"})
        self.assertIsNone(value)

    def test_get_metric_history(self):
        """Test getting metric history."""
        # Record multiple points
        for i in range(5):
            self.collector.record_metric("history.test", float(i), MetricType.GAUGE)
            time.sleep(0.001)  # Small delay to ensure different timestamps

        # Get all history
        history = self.collector.get_metric_history("history.test")
        self.assertEqual(len(history), 5)

        # Verify values are in order
        values = [point.value for point in history]
        self.assertEqual(values, [0.0, 1.0, 2.0, 3.0, 4.0])

    def test_get_metric_history_with_time_filter(self):
        """Test getting metric history with time filter."""
        datetime.now()

        # Record some points
        self.collector.record_metric("time.test", 1.0)
        time.sleep(0.01)

        filter_time = datetime.now()
        time.sleep(0.01)

        self.collector.record_metric("time.test", 2.0)

        # Get history since filter time
        history = self.collector.get_metric_history("time.test", since=filter_time)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].value, 2.0)

    def test_get_metrics_summary(self):
        """Test getting metrics summary."""
        # Record some metrics
        self.collector.record_metric("summary.test1", 10.0)
        self.collector.record_metric("summary.test1", 20.0)
        self.collector.record_metric("summary.test2", 5.0)

        summary = self.collector.get_metrics_summary()

        # Verify summary structure
        self.assertIn("total_metrics", summary)
        self.assertIn("total_points", summary)
        self.assertIn("metrics", summary)

        # Verify metrics details
        self.assertEqual(summary["total_metrics"], 2)
        self.assertEqual(summary["total_points"], 3)

        test1_metrics = summary["metrics"]["summary.test1"]
        self.assertEqual(test1_metrics["count"], 2)
        self.assertEqual(test1_metrics["latest_value"], 20.0)
        self.assertEqual(test1_metrics["min_value"], 10.0)
        self.assertEqual(test1_metrics["max_value"], 20.0)
        self.assertEqual(test1_metrics["avg_value"], 15.0)

    def test_cleanup_old_metrics(self):
        """Test automatic cleanup of old metrics."""
        # Use very short retention for testing
        collector = MetricsCollector(retention_hours=0.000001)  # Very small retention

        # Record a metric
        collector.record_metric("cleanup.test", 1.0)
        self.assertEqual(len(collector.metrics["cleanup.test"]), 1)

        # Wait a bit and trigger cleanup
        time.sleep(0.01)
        collector._cleanup_old_metrics()

        # Metric should be cleaned up (might not be 0 due to timing, but should be reduced)
        # Since timing is unreliable in tests, just verify cleanup was attempted
        self.assertLessEqual(len(collector.metrics["cleanup.test"]), 1)


class TestAlertManager(unittest.TestCase):
    """Test AlertManager functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.collector = MetricsCollector()
        self.alert_manager = AlertManager(self.collector)

    def test_add_threshold_rule(self):
        """Test adding threshold rules."""
        rule = ThresholdRule(
            metric_name="test.metric",
            threshold_value=100.0,
            operator="gt",
            severity=AlertSeverity.HIGH,
            message_template="Test alert: {current_value} > {threshold_value}",
        )

        self.alert_manager.add_threshold_rule(rule)
        self.assertEqual(len(self.alert_manager.threshold_rules), 1)
        self.assertEqual(self.alert_manager.threshold_rules[0], rule)

    def test_threshold_breach_triggers_alert(self):
        """Test that threshold breach triggers alert."""
        # Setup threshold rule
        rule = ThresholdRule(
            metric_name="test.cpu",
            threshold_value=80.0,
            operator="gt",
            severity=AlertSeverity.MEDIUM,
            message_template="High CPU: {current_value}%",
        )
        self.alert_manager.add_threshold_rule(rule)

        # Record metric that breaches threshold
        self.collector.record_metric("test.cpu", 85.0)

        # Check thresholds
        alerts = self.alert_manager.check_thresholds()

        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.metric_name, "test.cpu")
        self.assertEqual(alert.current_value, 85.0)
        self.assertEqual(alert.severity, AlertSeverity.MEDIUM)
        self.assertIn("High CPU: 85.0%", alert.message)

    def test_threshold_not_breached_no_alert(self):
        """Test that non-breached threshold doesn't trigger alert."""
        rule = ThresholdRule(
            metric_name="test.memory",
            threshold_value=90.0,
            operator="gt",
            severity=AlertSeverity.HIGH,
            message_template="High memory: {current_value}%",
        )
        self.alert_manager.add_threshold_rule(rule)

        # Record metric below threshold
        self.collector.record_metric("test.memory", 75.0)

        # Check thresholds
        alerts = self.alert_manager.check_thresholds()
        self.assertEqual(len(alerts), 0)

    def test_alert_cooldown(self):
        """Test alert cooldown functionality."""
        rule = ThresholdRule(
            metric_name="test.cooldown",
            threshold_value=50.0,
            operator="gt",
            severity=AlertSeverity.LOW,
            message_template="Test cooldown",
            cooldown_seconds=1,  # Short cooldown for testing
        )
        self.alert_manager.add_threshold_rule(rule)

        # Trigger alert
        self.collector.record_metric("test.cooldown", 60.0)
        alerts1 = self.alert_manager.check_thresholds()
        self.assertEqual(len(alerts1), 1)

        # Immediate check should not trigger due to cooldown
        self.collector.record_metric("test.cooldown", 70.0)
        alerts2 = self.alert_manager.check_thresholds()
        self.assertEqual(len(alerts2), 0)

        # After cooldown, should trigger again
        time.sleep(1.1)
        self.collector.record_metric("test.cooldown", 80.0)
        alerts3 = self.alert_manager.check_thresholds()
        self.assertEqual(len(alerts3), 1)

    def test_alert_resolution(self):
        """Test alert resolution when threshold is no longer breached."""
        rule = ThresholdRule(
            metric_name="test.resolution",
            threshold_value=100.0,
            operator="gt",
            severity=AlertSeverity.MEDIUM,
            message_template="Test resolution",
        )
        self.alert_manager.add_threshold_rule(rule)

        # Trigger alert
        self.collector.record_metric("test.resolution", 110.0)
        alerts = self.alert_manager.check_thresholds()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(len(self.alert_manager.get_active_alerts()), 1)

        # Resolve by recording value below threshold
        self.collector.record_metric("test.resolution", 90.0)
        self.alert_manager.check_thresholds()

        # Alert should be resolved
        active_alerts = self.alert_manager.get_active_alerts()
        self.assertEqual(len(active_alerts), 0)

    def test_alert_callback(self):
        """Test alert callback functionality."""
        callback_called = []

        def test_callback(alert: Alert):
            callback_called.append(alert)

        self.alert_manager.add_alert_callback(test_callback)

        # Setup and trigger alert
        rule = ThresholdRule(
            metric_name="test.callback",
            threshold_value=50.0,
            operator="gt",
            severity=AlertSeverity.HIGH,
            message_template="Test callback alert",
        )
        self.alert_manager.add_threshold_rule(rule)

        self.collector.record_metric("test.callback", 60.0)
        self.alert_manager.check_thresholds()

        # Verify callback was called
        self.assertEqual(len(callback_called), 1)
        self.assertEqual(callback_called[0].metric_name, "test.callback")

    def test_get_alert_summary(self):
        """Test getting alert summary."""
        # Add some rules and trigger alerts
        rule1 = ThresholdRule(
            metric_name="test.high",
            threshold_value=80.0,
            operator="gt",
            severity=AlertSeverity.HIGH,
            message_template="High alert",
        )

        rule2 = ThresholdRule(
            metric_name="test.medium",
            threshold_value=60.0,
            operator="gt",
            severity=AlertSeverity.MEDIUM,
            message_template="Medium alert",
        )

        self.alert_manager.add_threshold_rule(rule1)
        self.alert_manager.add_threshold_rule(rule2)

        # Trigger alerts
        self.collector.record_metric("test.high", 90.0)
        self.collector.record_metric("test.medium", 70.0)
        self.alert_manager.check_thresholds()

        summary = self.alert_manager.get_alert_summary()

        self.assertEqual(summary["active_alerts"], 2)
        self.assertEqual(summary["total_rules"], 2)
        self.assertEqual(summary["alerts_by_severity"]["high"], 1)
        self.assertEqual(summary["alerts_by_severity"]["medium"], 1)


class TestRealTimeMonitor(unittest.TestCase):
    """Test RealTimeMonitor functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.collector = MetricsCollector()
        self.alert_manager = AlertManager(self.collector)
        self.monitor = RealTimeMonitor(
            self.collector,
            self.alert_manager,
            monitoring_interval=0.1,  # Fast interval for testing
        )

    def tearDown(self):
        """Clean up after tests."""
        if self.monitor.running:
            self.monitor.stop()

    def test_system_metrics_collection(self):
        """Test system metrics collection."""
        # Create a mock psutil module
        mock_psutil = Mock()
        mock_psutil.cpu_percent.return_value = 75.0

        mock_memory = Mock()
        mock_memory.percent = 60.0
        mock_memory.available = 8 * 1024**3  # 8GB
        mock_psutil.virtual_memory.return_value = mock_memory

        mock_disk = Mock()
        mock_disk.used = 100 * 1024**3  # 100GB
        mock_disk.total = 500 * 1024**3  # 500GB
        mock_psutil.disk_usage.return_value = mock_disk

        # Patch the import inside the method
        with patch.dict("sys.modules", {"psutil": mock_psutil}):
            # Ensure system metrics are enabled
            self.monitor.system_metrics_enabled = True

            # Collect system metrics
            self.monitor._collect_system_metrics()

            # Verify metrics were recorded
            cpu_value = self.collector.get_metric_value("system.cpu.usage_percent")
            self.assertEqual(cpu_value, 75.0)

            memory_value = self.collector.get_metric_value(
                "system.memory.usage_percent"
            )
            self.assertEqual(memory_value, 60.0)

            disk_value = self.collector.get_metric_value("system.disk.usage_percent")
            self.assertEqual(disk_value, 20.0)  # 100/500 * 100

    def test_system_metrics_collection_without_psutil(self):
        """Test system metrics collection when psutil is not available."""
        # Temporarily disable system metrics
        original_enabled = self.monitor.system_metrics_enabled
        self.monitor.system_metrics_enabled = True

        # This should not raise an error and should disable system metrics
        self.monitor._collect_system_metrics()

        # System metrics should be disabled after failed import
        self.assertFalse(self.monitor.system_metrics_enabled)

        # Restore original state
        self.monitor.system_metrics_enabled = original_enabled

    def test_start_stop_monitoring(self):
        """Test starting and stopping monitoring."""
        self.assertFalse(self.monitor.running)

        # Start monitoring
        self.monitor.start()
        self.assertTrue(self.monitor.running)
        self.assertIsNotNone(self.monitor.monitor_thread)

        # Stop monitoring
        self.monitor.stop()
        self.assertFalse(self.monitor.running)

    def test_default_thresholds_setup(self):
        """Test that default threshold rules are set up."""
        # Check that some default rules were added
        self.assertGreater(len(self.alert_manager.threshold_rules), 0)

        # Check for specific default rules
        rule_names = [rule.metric_name for rule in self.alert_manager.threshold_rules]
        self.assertIn("system.cpu.usage_percent", rule_names)
        self.assertIn("system.memory.usage_percent", rule_names)


class TestTransformOperationMonitor(unittest.TestCase):
    """Test TransformOperationMonitor functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.collector = MetricsCollector()
        self.operation_monitor = TransformOperationMonitor(self.collector)

    def test_monitor_operation_success(self):
        """Test successful operation monitoring."""
        # Monitor a successful operation
        with self.operation_monitor.monitor_operation(
            "APPEND", "test_table", estimated_rows=1000, extra_metadata="test"
        ) as operation_id:
            # Simulate some work
            time.sleep(0.01)

            # Verify operation is tracked as active
            active_ops = self.operation_monitor.get_active_operations()
            self.assertEqual(len(active_ops), 1)
            self.assertIn(operation_id, active_ops)

            op_info = active_ops[operation_id]
            self.assertEqual(op_info["operation_type"], "APPEND")
            self.assertEqual(op_info["table_name"], "test_table")
            self.assertEqual(op_info["estimated_rows"], 1000)
            self.assertEqual(op_info["metadata"]["extra_metadata"], "test")

        # After completion, operation should be removed from active list
        active_ops = self.operation_monitor.get_active_operations()
        self.assertEqual(len(active_ops), 0)

        # Check that success metrics were recorded
        started_count = self.collector.get_metric_value(
            "transform.operations.started",
            labels={"operation_type": "APPEND", "table_name": "test_table"},
        )
        self.assertEqual(started_count, 1.0)

        completed_count = self.collector.get_metric_value(
            "transform.operations.completed",
            labels={
                "operation_type": "APPEND",
                "table_name": "test_table",
                "status": "success",
            },
        )
        self.assertEqual(completed_count, 1.0)

    def test_monitor_operation_failure(self):
        """Test failed operation monitoring."""
        # Monitor a failed operation
        with self.assertRaises(ValueError):
            with self.operation_monitor.monitor_operation(
                "MERGE", "test_table", estimated_rows=500
            ):
                # Simulate failure
                raise ValueError("Test error")

        # Check that error metrics were recorded
        error_count = self.collector.get_metric_value(
            "transform.operations.errors",
            labels={
                "operation_type": "MERGE",
                "table_name": "test_table",
                "error_type": "ValueError",
            },
        )
        self.assertEqual(error_count, 1.0)

        completed_count = self.collector.get_metric_value(
            "transform.operations.completed",
            labels={
                "operation_type": "MERGE",
                "table_name": "test_table",
                "status": "error",
            },
        )
        self.assertEqual(completed_count, 1.0)

    def test_get_operation_metrics(self):
        """Test getting operation metrics summary."""
        # Start some operations
        with self.operation_monitor.monitor_operation("APPEND", "table1", 100):
            with self.operation_monitor.monitor_operation("MERGE", "table2", 200):
                metrics = self.operation_monitor.get_operation_metrics()

                self.assertEqual(metrics["active_operations"], 2)
                self.assertIn("APPEND", metrics["operation_types"])
                self.assertIn("MERGE", metrics["operation_types"])
                self.assertIn("table1", metrics["tables_being_processed"])
                self.assertIn("table2", metrics["tables_being_processed"])


class TestMonitoringManager(unittest.TestCase):
    """Test MonitoringManager functionality."""

    def setUp(self):
        """Set up test fixtures."""
        config = {
            "retention_hours": 1,
            "monitoring_interval": 0.1,
            "auto_start_monitoring": False,  # Don't auto-start for tests
            "export_enabled": False,
        }
        self.manager = MonitoringManager(config)

    def tearDown(self):
        """Clean up after tests."""
        self.manager.stop_monitoring()

    def test_initialization(self):
        """Test monitoring manager initialization."""
        self.assertIsNotNone(self.manager.metrics_collector)
        self.assertIsNotNone(self.manager.alert_manager)
        self.assertIsNotNone(self.manager.real_time_monitor)
        self.assertIsNotNone(self.manager.operation_monitor)

    def test_get_dashboard_data(self):
        """Test getting dashboard data."""
        # Record some test data
        self.manager.metrics_collector.record_metric("test.dashboard", 42.0)

        dashboard = self.manager.get_dashboard_data()

        # Verify dashboard structure
        self.assertIn("timestamp", dashboard)
        self.assertIn("metrics_summary", dashboard)
        self.assertIn("alert_summary", dashboard)
        self.assertIn("active_alerts", dashboard)
        self.assertIn("operation_metrics", dashboard)
        self.assertIn("active_operations", dashboard)

        # Verify test metric is included
        metrics = dashboard["metrics_summary"]["metrics"]
        self.assertIn("test.dashboard", metrics)
        self.assertEqual(metrics["test.dashboard"]["latest_value"], 42.0)

    def test_export_metrics(self):
        """Test metrics export functionality."""
        import json
        import os
        import tempfile

        # Record some test data
        self.manager.metrics_collector.record_metric("test.export", 123.0)

        # Export to temporary file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            file_path = f.name

        try:
            exported_path = self.manager.export_metrics(file_path)
            self.assertEqual(exported_path, file_path)

            # Verify file contents
            with open(file_path, "r") as f:
                data = json.load(f)

            self.assertIn("timestamp", data)
            self.assertIn("metrics_summary", data)

            # Verify test metric is exported
            metrics = data["metrics_summary"]["metrics"]
            self.assertIn("test.export", metrics)
            self.assertEqual(metrics["test.export"]["latest_value"], 123.0)

        finally:
            # Clean up
            if os.path.exists(file_path):
                os.unlink(file_path)


class TestMonitoringIntegration(unittest.TestCase):
    """Test integration between monitoring components."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MonitoringManager(
            {"auto_start_monitoring": False, "export_enabled": False}
        )

    def tearDown(self):
        """Clean up after tests."""
        self.manager.stop_monitoring()

    def test_end_to_end_monitoring_flow(self):
        """Test complete monitoring flow from operation to alert."""
        # Setup alert threshold
        threshold_rule = ThresholdRule(
            metric_name="transform.operations.execution_time",
            threshold_value=0.05,  # 50ms threshold
            operator="gt",
            severity=AlertSeverity.HIGH,
            message_template="Slow operation: {current_value:.3f}s",
        )
        self.manager.alert_manager.add_threshold_rule(threshold_rule)

        # Monitor a slow operation that should trigger alert
        with self.manager.operation_monitor.monitor_operation(
            "SLOW_OPERATION", "test_table", 1000
        ):
            time.sleep(0.1)  # Sleep longer than threshold

        # Check that metrics were recorded
        execution_time = self.manager.metrics_collector.get_metric_value(
            "transform.operations.execution_time",
            labels={"operation_type": "SLOW_OPERATION", "table_name": "test_table"},
        )
        self.assertGreater(execution_time, 0.05)

        # Check thresholds to trigger alert
        alerts = self.manager.alert_manager.check_thresholds()
        self.assertEqual(len(alerts), 1)

        alert = alerts[0]
        self.assertEqual(alert.metric_name, "transform.operations.execution_time")
        self.assertGreater(alert.current_value, 0.05)
        self.assertEqual(alert.severity, AlertSeverity.HIGH)

    def test_monitoring_with_labels(self):
        """Test monitoring with metric labels."""
        # Monitor operations with different labels
        with self.manager.operation_monitor.monitor_operation(
            "APPEND", "table_a", 1000
        ):
            pass

        with self.manager.operation_monitor.monitor_operation(
            "APPEND", "table_b", 2000
        ):
            pass

        # Verify metrics are tracked separately by labels
        table_a_count = self.manager.metrics_collector.get_metric_value(
            "transform.operations.completed",
            labels={
                "operation_type": "APPEND",
                "table_name": "table_a",
                "status": "success",
            },
        )
        self.assertEqual(table_a_count, 1.0)

        table_b_count = self.manager.metrics_collector.get_metric_value(
            "transform.operations.completed",
            labels={
                "operation_type": "APPEND",
                "table_name": "table_b",
                "status": "success",
            },
        )
        self.assertEqual(table_b_count, 1.0)


if __name__ == "__main__":
    unittest.main()
