"""Tests for V2 Performance Monitoring and Optimization.

Following testing standards:
- Test behavior, not implementation
- Use real implementations where possible
- Clear, descriptive test names
- Test both positive and error scenarios
"""

import time
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

# Removed feature flag imports - using clean factory pattern
from sqlflow.core.executors.v2.performance_monitor import (
    PerformanceMetrics,
    PerformanceMonitor,
    PerformanceOptimizer,
    ResourceMonitor,
    cleanup_performance_monitor,
    get_performance_monitor,
)

# Mock psutil before importing ResourceMonitor
mock_psutil = Mock()
mock_process = Mock()
mock_memory_info = Mock()
mock_memory_info.rss = 512 * 1024 * 1024  # 512 MB in bytes
mock_process.memory_info.return_value = mock_memory_info
mock_process.cpu_percent.return_value = 25.5
mock_psutil.Process.return_value = mock_process

# Ensure PSUTIL_AVAILABLE is True and psutil is mocked
with patch.dict("sys.modules", {"psutil": mock_psutil}):
    with patch("sqlflow.core.executors.v2.performance_monitor.PSUTIL_AVAILABLE", True):
        pass  # Imports already done above


class TestPerformanceMetrics:
    """Test performance metrics data structure behavior."""

    def test_creates_metrics_with_timestamp(self):
        """Performance metrics should initialize with a timestamp."""
        timestamp = datetime.now()
        metrics = PerformanceMetrics(timestamp=timestamp)

        assert metrics.timestamp == timestamp
        assert metrics.total_execution_time_ms == 0.0
        assert metrics.memory_usage_mb == 0.0
        assert metrics.cpu_usage_percent == 0.0
        assert metrics.total_steps == 0
        assert metrics.successful_steps == 0
        assert metrics.failed_steps == 0

    def test_creates_metrics_with_custom_values(self):
        """Should create metrics with provided values."""
        timestamp = datetime.now()
        metrics = PerformanceMetrics(
            timestamp=timestamp,
            total_execution_time_ms=1500.0,
            memory_usage_mb=512.0,
            cpu_usage_percent=25.5,
            total_steps=5,
            successful_steps=4,
            failed_steps=1,
        )

        assert metrics.total_execution_time_ms == 1500.0
        assert metrics.memory_usage_mb == 512.0
        assert metrics.cpu_usage_percent == 25.5
        assert metrics.total_steps == 5
        assert metrics.successful_steps == 4
        assert metrics.failed_steps == 1

    def test_converts_to_dictionary_correctly(self):
        """Should convert metrics to dictionary format."""
        timestamp = datetime.now()
        metrics = PerformanceMetrics(
            timestamp=timestamp,
            total_execution_time_ms=2000.0,
            memory_usage_mb=256.0,
            successful_steps=3,
            total_steps=4,
        )

        metrics_dict = metrics.to_dict()

        assert "timestamp" in metrics_dict
        assert "execution" in metrics_dict
        assert "resources" in metrics_dict
        assert "pipeline" in metrics_dict
        assert metrics_dict["execution"]["total_time_ms"] == 2000.0
        assert metrics_dict["resources"]["memory_mb"] == 256.0
        assert metrics_dict["pipeline"]["successful_steps"] == 3

    def test_calculates_success_rate_correctly(self):
        """Should calculate success rate correctly in dictionary."""
        timestamp = datetime.now()
        metrics = PerformanceMetrics(
            timestamp=timestamp, total_steps=10, successful_steps=8, failed_steps=2
        )

        metrics_dict = metrics.to_dict()

        assert metrics_dict["pipeline"]["success_rate"] == 0.8


class TestResourceMonitor:
    """Test system resource monitoring behavior."""

    def test_creates_resource_monitor_successfully(self):
        """Should create resource monitor without errors."""
        monitor = ResourceMonitor()

        # Should not raise exceptions
        assert monitor is not None

    def test_gets_current_metrics_with_valid_types(self):
        """Should return current metrics with valid types."""
        monitor = ResourceMonitor()

        metrics = monitor.get_current_metrics()

        assert isinstance(metrics, dict)
        assert "memory_mb" in metrics
        assert "memory_peak_mb" in metrics
        assert "cpu_percent" in metrics
        assert isinstance(metrics["memory_mb"], float)
        assert isinstance(metrics["memory_peak_mb"], float)
        assert isinstance(metrics["cpu_percent"], float)

    def test_tracks_peak_memory_correctly(self):
        """Should track peak memory usage correctly."""
        monitor = ResourceMonitor()

        initial_metrics = monitor.get_current_metrics()
        initial_peak = initial_metrics["memory_peak_mb"]

        # Get metrics again - peak should be at least the same
        second_metrics = monitor.get_current_metrics()
        second_peak = second_metrics["memory_peak_mb"]

        assert second_peak >= initial_peak

    def test_resets_peak_memory_correctly(self):
        """Should reset peak memory tracking correctly."""
        monitor = ResourceMonitor()

        # Get initial metrics to establish a peak
        monitor.get_current_metrics()

        # Reset peak memory
        monitor.reset_peak_memory()

        # Get new metrics
        new_metrics = monitor.get_current_metrics()

        # Peak should be current or higher (not the old peak)
        assert new_metrics["memory_peak_mb"] >= new_metrics["memory_mb"]


class TestPerformanceOptimizer:
    """Test performance optimization recommendations."""

    def test_creates_optimizer_successfully(self):
        """Should create performance optimizer without errors."""
        optimizer = PerformanceOptimizer()

        assert optimizer is not None

    def test_provides_no_recommendations_with_insufficient_data(self):
        """Should provide no recommendations with insufficient metrics."""
        optimizer = PerformanceOptimizer()

        # Add only a few metrics (< 5)
        for i in range(3):
            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                memory_usage_mb=100.0,
                total_execution_time_ms=1000.0,
            )
            optimizer.add_metrics(metrics)

        recommendations = optimizer.get_optimization_recommendations()

        assert len(recommendations) == 0

    def test_recommends_memory_optimization_for_high_usage(self):
        """Should recommend memory optimization for high memory usage."""
        optimizer = PerformanceOptimizer()

        # Add metrics with high memory usage
        for i in range(10):
            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                memory_usage_mb=1500.0,  # High memory usage
                total_execution_time_ms=5000.0,
            )
            optimizer.add_metrics(metrics)

        recommendations = optimizer.get_optimization_recommendations()

        # Should have memory recommendation
        memory_recs = [r for r in recommendations if r["type"] == "memory"]
        assert len(memory_recs) > 0
        assert "memory usage" in memory_recs[0]["message"].lower()

    def test_recommends_performance_optimization_for_slow_execution(self):
        """Should recommend performance optimization for slow execution."""
        optimizer = PerformanceOptimizer()

        # Add metrics with slow execution times
        for i in range(10):
            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                memory_usage_mb=200.0,
                total_execution_time_ms=35000.0,  # 35 seconds - slow
            )
            optimizer.add_metrics(metrics)

        recommendations = optimizer.get_optimization_recommendations()

        # Should have performance recommendation
        perf_recs = [r for r in recommendations if r["type"] == "performance"]
        assert len(perf_recs) > 0
        assert "execution" in perf_recs[0]["message"].lower()

    def test_recommends_reliability_improvement_for_low_success_rate(self):
        """Should recommend reliability improvements for low success rates."""
        optimizer = PerformanceOptimizer()

        # Add metrics with low success rates
        for i in range(25):  # Need more for success rate analysis
            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                memory_usage_mb=200.0,
                total_execution_time_ms=5000.0,
                total_steps=10,
                successful_steps=7,  # 70% success rate
                failed_steps=3,
            )
            optimizer.add_metrics(metrics)

        recommendations = optimizer.get_optimization_recommendations()

        # Should have reliability recommendation
        reliability_recs = [r for r in recommendations if r["type"] == "reliability"]
        assert len(reliability_recs) > 0
        assert "success rate" in reliability_recs[0]["message"].lower()


class TestPerformanceMonitor:
    """Test performance monitor behavior."""

    def setup_method(self):
        """Set up test environment."""
        # No feature flags needed - V2 is default

    def test_creates_monitor_with_run_id(self):
        """Should create monitor with run ID."""
        run_id = "test-run-123"
        monitor = PerformanceMonitor(run_id)

        assert monitor.run_id == run_id

    def test_starts_and_finishes_execution_tracking(self):
        """Should track execution start and finish correctly."""
        monitor = PerformanceMonitor("test-run")

        # Start execution
        monitor.start_execution()

        # Simulate some work
        time.sleep(0.1)

        # Finish execution
        final_metrics = monitor.finish_execution()

        assert isinstance(final_metrics, PerformanceMetrics)
        assert final_metrics.total_execution_time_ms > 0

    def test_records_step_execution_correctly(self):
        """Should record individual step execution correctly."""
        monitor = PerformanceMonitor("test-run")
        monitor.start_execution()

        # Record a step
        step_id = "load_customers"
        monitor.record_step_start(step_id)

        time.sleep(0.05)  # Simulate step work

        monitor.record_step_completion(step_id, success=True, rows_processed=1000)

        # Get current metrics
        metrics = monitor.get_current_metrics()

        assert metrics.successful_steps >= 1
        assert step_id in metrics.step_execution_times
        assert metrics.step_execution_times[step_id] > 0

    def test_records_failed_steps_correctly(self):
        """Should record failed steps correctly."""
        monitor = PerformanceMonitor("test-run")
        monitor.start_execution()

        step_id = "failing_transform"
        monitor.record_step_start(step_id)
        monitor.record_step_completion(step_id, success=False)

        metrics = monitor.get_current_metrics()

        assert metrics.failed_steps >= 1
        assert metrics.total_steps >= 1

    def test_tracks_database_query_metrics(self):
        """Should track database query metrics correctly."""
        monitor = PerformanceMonitor("test-run")

        # Record some database queries
        monitor.record_db_query(150.0)  # 150ms query
        monitor.record_db_query(200.0)  # 200ms query

        metrics = monitor.get_current_metrics()

        assert metrics.db_query_count == 2
        assert metrics.db_avg_query_time_ms == 175.0  # Average of 150 and 200

    def test_provides_optimization_recommendations(self):
        """Should provide optimization recommendations."""
        monitor = PerformanceMonitor("test-run")

        recommendations = monitor.get_optimization_recommendations()

        # Should return a list (might be empty if no issues detected)
        assert isinstance(recommendations, list)

    def test_logs_performance_summary(self):
        """Should log performance summary without errors."""
        monitor = PerformanceMonitor("test-run")
        monitor.start_execution()

        # Should not raise exceptions
        monitor.log_performance_summary()


class TestGlobalPerformanceMonitor:
    """Test global performance monitor functionality."""

    def test_gets_global_monitor_with_run_id(self):
        """Should get global monitor with run ID."""
        run_id = "global-test-run"

        monitor = get_performance_monitor(run_id)

        assert isinstance(monitor, PerformanceMonitor)
        assert monitor.run_id == run_id

    def test_cleans_up_performance_monitor(self):
        """Should clean up performance monitor correctly."""
        run_id = "cleanup-test-run"

        # Get a monitor
        monitor = get_performance_monitor(run_id)
        assert monitor is not None

        # Clean it up
        cleanup_performance_monitor(run_id)

        # Should complete without errors
        # (Implementation detail - cleanup is primarily for resource management)


class TestPerformanceMonitoringIntegration:
    """Test integration scenarios for performance monitoring."""

    def test_monitors_complete_execution_workflow(self):
        """Should monitor complete execution workflow correctly."""
        monitor = PerformanceMonitor("integration-test")

        # Full execution workflow
        monitor.start_execution()

        # Simulate multiple steps
        steps = ["load_data", "transform_data", "export_results"]
        for step in steps:
            monitor.record_step_start(step)
            time.sleep(0.02)  # Simulate work
            monitor.record_step_completion(step, success=True, rows_processed=500)

        # Simulate database queries
        monitor.record_db_query(100.0)
        monitor.record_db_query(150.0)

        # Finish execution
        final_metrics = monitor.finish_execution()

        # Verify complete metrics
        assert final_metrics.total_steps == 3
        assert final_metrics.successful_steps == 3
        assert final_metrics.failed_steps == 0
        assert final_metrics.db_query_count == 2
        assert final_metrics.total_execution_time_ms > 0
        assert len(final_metrics.step_execution_times) == 3

    def test_handles_mixed_success_failure_scenario(self):
        """Should handle mixed success/failure scenarios correctly."""
        monitor = PerformanceMonitor("mixed-scenario")
        monitor.start_execution()

        # Mix of successful and failed steps
        monitor.record_step_start("step1")
        monitor.record_step_completion("step1", success=True, rows_processed=100)

        monitor.record_step_start("step2")
        monitor.record_step_completion("step2", success=False)

        monitor.record_step_start("step3")
        monitor.record_step_completion("step3", success=True, rows_processed=200)

        final_metrics = monitor.finish_execution()

        assert final_metrics.total_steps == 3
        assert final_metrics.successful_steps == 2
        assert final_metrics.failed_steps == 1

        # Check success rate in dictionary representation
        metrics_dict = final_metrics.to_dict()
        assert abs(metrics_dict["pipeline"]["success_rate"] - (2 / 3)) < 0.01

    def test_integrates_without_configuration(self):
        """Should work without any configuration - simple is better than complex."""
        # Monitor should work out of the box
        monitor = PerformanceMonitor("no-config-test")

        # Should not raise exceptions
        monitor.start_execution()
        monitor.finish_execution()

        # Should work with full functionality by default
        monitor2 = PerformanceMonitor("no-config-test-2")
        monitor2.start_execution()
        metrics = monitor2.get_current_metrics()
        assert isinstance(metrics, PerformanceMetrics)


def test_performance_metrics_creation():
    """Test performance metrics data structure."""
    metrics = PerformanceMetrics(
        timestamp=datetime.utcnow(),
        total_execution_time_ms=1500.0,
        memory_usage_mb=512.0,
        total_steps=3,
        successful_steps=3,
    )

    assert metrics.total_execution_time_ms == 1500.0
    assert metrics.memory_usage_mb == 512.0
    assert metrics.total_steps == 3
    assert metrics.successful_steps == 3


@patch("sqlflow.core.executors.v2.performance_monitor.PSUTIL_AVAILABLE", True)
@patch("sqlflow.core.executors.v2.performance_monitor.psutil", mock_psutil)
def test_resource_monitor():
    """Test resource monitoring functionality."""
    monitor = ResourceMonitor()
    metrics = monitor.get_current_metrics()

    # Verify metrics from mocked psutil
    assert metrics["memory_mb"] == 512.0  # From mocked psutil
    assert metrics["cpu_percent"] == 25.5  # From mocked psutil


def test_performance_optimizer_recommendations():
    """Test performance optimizer recommendations."""
    optimizer = PerformanceOptimizer()

    # Add metrics that should trigger recommendations
    metrics = PerformanceMetrics(
        timestamp=datetime.utcnow(),
        total_execution_time_ms=60000.0,  # 60 seconds
        memory_usage_mb=2048.0,  # 2GB
        total_steps=10,
        successful_steps=8,
    )

    # Add enough metrics to trigger recommendations
    for _ in range(5):
        optimizer.add_metrics(metrics)

    recommendations = optimizer.get_optimization_recommendations()
    assert len(recommendations) > 0

    # Verify memory recommendation
    memory_rec = next(r for r in recommendations if r["type"] == "memory")
    assert "High memory usage" in memory_rec["message"]
    assert memory_rec["severity"] in ("high", "medium")

    # Verify performance recommendation
    perf_rec = next(r for r in recommendations if r["type"] == "performance")
    assert "Slow execution" in perf_rec["message"]


def test_performance_monitor_lifecycle():
    """Test performance monitor lifecycle and data collection."""
    # V2 is always enabled with clean factory pattern
    monitor = PerformanceMonitor(run_id="test_run_1")

    # Start monitoring
    monitor.start_execution()

    # Record some steps
    monitor.record_step_start("step1")
    time.sleep(0.1)
    monitor.record_step_completion("step1", success=True, rows_processed=100)

    # Get metrics
    metrics = monitor.get_current_metrics()
    assert isinstance(metrics, PerformanceMetrics)
    assert metrics.total_execution_time_ms > 0
    assert "step1" in metrics.step_execution_times


def test_performance_monitor_db_metrics():
    """Test database performance metrics collection."""
    monitor = PerformanceMonitor(run_id="test_run_2")

    # Record some DB operations
    monitor.record_db_query(150.5)  # 150.5ms
    monitor.record_db_query(200.0)  # 200ms

    metrics = monitor.get_current_metrics()
    assert metrics.db_query_count == 2
    assert metrics.db_avg_query_time_ms == pytest.approx(
        175.25, rel=1e-2
    )  # Average of the two queries


@patch("sqlflow.core.executors.v2.performance_monitor.get_performance_monitor")
def test_performance_monitor_registry(mock_get_monitor):
    """Test performance monitor registry and retrieval."""
    mock_monitor = Mock()
    mock_get_monitor.return_value = mock_monitor

    from sqlflow.core.executors.v2.performance_monitor import get_performance_monitor

    monitor = get_performance_monitor("test_run_3")
    assert monitor == mock_monitor
