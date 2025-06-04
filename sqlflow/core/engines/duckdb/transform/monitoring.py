"""
Real-time monitoring and metrics collection for SQLFlow transform operations.

This module provides comprehensive monitoring capabilities including:
- Real-time performance metrics collection
- System resource monitoring
- Data quality metrics tracking
- Alerting and threshold management
- Operational dashboards and reporting
- Integration with external monitoring systems
"""

import json
import os
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class MetricType(Enum):
    """Types of metrics that can be collected."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertSeverity(Enum):
    """Alert severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class MetricPoint:
    """Individual metric data point."""

    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    unit: Optional[str] = None


@dataclass
class Alert:
    """Alert triggered by monitoring conditions."""

    name: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    metric_name: str
    current_value: float
    threshold_value: float
    labels: Dict[str, str] = field(default_factory=dict)
    resolved: bool = False
    resolution_timestamp: Optional[datetime] = None


@dataclass
class ThresholdRule:
    """Threshold rule for triggering alerts."""

    metric_name: str
    threshold_value: float
    operator: str  # "gt", "lt", "gte", "lte", "eq", "ne"
    severity: AlertSeverity
    message_template: str
    cooldown_seconds: int = 300  # 5 minutes default
    labels_filter: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Thread-safe metrics collection with real-time capabilities."""

    def __init__(self, retention_hours: int = 24, max_points_per_metric: int = 10000):
        """Initialize metrics collector.

        Args:
            retention_hours: How long to retain metric points
            max_points_per_metric: Maximum points to store per metric
        """
        self.retention_hours = retention_hours
        self.max_points_per_metric = max_points_per_metric
        self.metrics: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_points_per_metric)
        )
        self.metric_metadata: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()

        # Performance optimization: pre-compute retention cutoff
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5 minutes

        logger.info(f"MetricsCollector initialized with {retention_hours}h retention")

    def record_metric(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        labels: Optional[Dict[str, str]] = None,
        unit: Optional[str] = None,
    ) -> None:
        """Record a metric point.

        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            labels: Optional labels for metric
            unit: Optional unit for metric
        """
        with self.lock:
            point = MetricPoint(
                name=name,
                value=value,
                metric_type=metric_type,
                timestamp=datetime.now(),
                labels=labels or {},
                unit=unit,
            )

            self.metrics[name].append(point)

            # Update metadata
            self.metric_metadata[name] = {
                "type": metric_type.value,
                "unit": unit,
                "last_updated": point.timestamp,
                "sample_labels": list(point.labels.keys()),
            }

            # Periodic cleanup
            if time.time() - self._last_cleanup > self._cleanup_interval:
                self._cleanup_old_metrics()

    def get_metric_value(
        self, name: str, labels: Optional[Dict[str, str]] = None
    ) -> Optional[float]:
        """Get the latest value for a metric.

        Args:
            name: Metric name
            labels: Optional labels to filter by

        Returns:
            Latest metric value or None if not found
        """
        with self.lock:
            if name not in self.metrics:
                return None

            points = self.metrics[name]
            if not points:
                return None

            # Filter by labels if provided
            if labels:
                for point in reversed(points):
                    if all(point.labels.get(k) == v for k, v in labels.items()):
                        return point.value
                return None

            return points[-1].value

    def get_metric_history(
        self,
        name: str,
        since: Optional[datetime] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> List[MetricPoint]:
        """Get metric history.

        Args:
            name: Metric name
            since: Optional start time for history
            labels: Optional labels to filter by

        Returns:
            List of metric points
        """
        with self.lock:
            if name not in self.metrics:
                return []

            points = list(self.metrics[name])

            # Filter by time
            if since:
                points = [p for p in points if p.timestamp >= since]

            # Filter by labels
            if labels:
                points = [
                    p
                    for p in points
                    if all(p.labels.get(k) == v for k, v in labels.items())
                ]

            return points

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics.

        Returns:
            Dictionary with metrics summary
        """
        with self.lock:
            summary = {
                "total_metrics": len(self.metrics),
                "total_points": sum(len(points) for points in self.metrics.values()),
                "retention_hours": self.retention_hours,
                "metrics": {},
            }

            for name, points in self.metrics.items():
                if points:
                    values = [p.value for p in points]
                    summary["metrics"][name] = {
                        "count": len(points),
                        "latest_value": values[-1],
                        "min_value": min(values),
                        "max_value": max(values),
                        "avg_value": sum(values) / len(values),
                        "last_updated": points[-1].timestamp.isoformat(),
                        **self.metric_metadata.get(name, {}),
                    }

            return summary

    def _cleanup_old_metrics(self) -> None:
        """Clean up old metric points."""
        cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)
        removed_count = 0

        for name, points in self.metrics.items():
            original_len = len(points)
            # Remove old points from left side of deque
            while points and points[0].timestamp < cutoff_time:
                points.popleft()
                removed_count += 1

        self._last_cleanup = time.time()

        if removed_count > 0:
            logger.debug(f"Cleaned up {removed_count} old metric points")


class AlertManager:
    """Manage alerting based on metric thresholds."""

    def __init__(self, metrics_collector: MetricsCollector):
        """Initialize alert manager.

        Args:
            metrics_collector: Metrics collector to monitor
        """
        self.metrics_collector = metrics_collector
        self.threshold_rules: List[ThresholdRule] = []
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.alert_callbacks: List[Callable[[Alert], None]] = []
        self.lock = threading.RLock()

        # Cooldown tracking
        self.alert_cooldowns: Dict[str, datetime] = {}

        logger.info("AlertManager initialized")

    def add_threshold_rule(self, rule: ThresholdRule) -> None:
        """Add a threshold rule.

        Args:
            rule: Threshold rule to add
        """
        with self.lock:
            self.threshold_rules.append(rule)
            logger.info(
                f"Added threshold rule for {rule.metric_name}: {rule.operator} {rule.threshold_value}"
            )

    def add_alert_callback(self, callback: Callable[[Alert], None]) -> None:
        """Add callback for alert notifications.

        Args:
            callback: Function to call when alert is triggered
        """
        self.alert_callbacks.append(callback)

    def check_thresholds(self) -> List[Alert]:
        """Check all threshold rules and trigger alerts if needed.

        Returns:
            List of new alerts triggered
        """
        new_alerts = []

        with self.lock:
            for rule in self.threshold_rules:
                alert = self._check_single_threshold(rule)
                if alert:
                    new_alerts.append(alert)

        return new_alerts

    def _check_single_threshold(self, rule: ThresholdRule) -> Optional[Alert]:
        """Check a single threshold rule.

        Args:
            rule: Threshold rule to check

        Returns:
            Alert if threshold is breached, None otherwise
        """
        # Get current metric value
        current_value = self.metrics_collector.get_metric_value(
            rule.metric_name, rule.labels_filter
        )

        if current_value is None:
            return None

        # Check if threshold is breached
        threshold_breached = self._evaluate_threshold(
            current_value, rule.threshold_value, rule.operator
        )

        if not threshold_breached:
            # Check if we need to resolve an existing alert
            alert_key = self._get_alert_key(rule)
            if alert_key in self.active_alerts:
                self._resolve_alert(alert_key)
            return None

        # Check cooldown
        alert_key = self._get_alert_key(rule)
        if alert_key in self.alert_cooldowns:
            cooldown_end = self.alert_cooldowns[alert_key] + timedelta(
                seconds=rule.cooldown_seconds
            )
            if datetime.now() < cooldown_end:
                return None

        # Create alert
        alert = Alert(
            name=f"{rule.metric_name}_threshold",
            message=rule.message_template.format(
                metric_name=rule.metric_name,
                current_value=current_value,
                threshold_value=rule.threshold_value,
            ),
            severity=rule.severity,
            timestamp=datetime.now(),
            metric_name=rule.metric_name,
            current_value=current_value,
            threshold_value=rule.threshold_value,
            labels=rule.labels_filter,
        )

        # Store alert
        self.active_alerts[alert_key] = alert
        self.alert_history.append(alert)
        self.alert_cooldowns[alert_key] = datetime.now()

        # Notify callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

        logger.warning(f"Alert triggered: {alert.message}")
        return alert

    def _evaluate_threshold(
        self, current: float, threshold: float, operator: str
    ) -> bool:
        """Evaluate threshold condition.

        Args:
            current: Current value
            threshold: Threshold value
            operator: Comparison operator

        Returns:
            True if threshold is breached
        """
        operators = {
            "gt": lambda c, t: c > t,
            "lt": lambda c, t: c < t,
            "gte": lambda c, t: c >= t,
            "lte": lambda c, t: c <= t,
            "eq": lambda c, t: abs(c - t) < 1e-9,
            "ne": lambda c, t: abs(c - t) >= 1e-9,
        }

        return operators.get(operator, lambda c, t: False)(current, threshold)

    def _get_alert_key(self, rule: ThresholdRule) -> str:
        """Get unique key for alert.

        Args:
            rule: Threshold rule

        Returns:
            Unique alert key
        """
        labels_str = "_".join(f"{k}={v}" for k, v in sorted(rule.labels_filter.items()))
        return f"{rule.metric_name}_{rule.operator}_{rule.threshold_value}_{labels_str}"

    def _resolve_alert(self, alert_key: str) -> None:
        """Resolve an active alert.

        Args:
            alert_key: Alert key to resolve
        """
        if alert_key in self.active_alerts:
            alert = self.active_alerts[alert_key]
            alert.resolved = True
            alert.resolution_timestamp = datetime.now()
            del self.active_alerts[alert_key]
            logger.info(f"Resolved alert: {alert.name}")

    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts.

        Returns:
            List of active alerts
        """
        with self.lock:
            return list(self.active_alerts.values())

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary.

        Returns:
            Dictionary with alert summary
        """
        with self.lock:
            return {
                "active_alerts": len(self.active_alerts),
                "total_rules": len(self.threshold_rules),
                "alert_history_count": len(self.alert_history),
                "alerts_by_severity": {
                    severity.value: len(
                        [
                            a
                            for a in self.active_alerts.values()
                            if a.severity == severity
                        ]
                    )
                    for severity in AlertSeverity
                },
            }


class RealTimeMonitor:
    """Real-time monitoring system for transform operations."""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        alert_manager: AlertManager,
        monitoring_interval: float = 10.0,
    ):
        """Initialize real-time monitor.

        Args:
            metrics_collector: Metrics collector
            alert_manager: Alert manager
            monitoring_interval: Monitoring interval in seconds
        """
        self.metrics_collector = metrics_collector
        self.alert_manager = alert_manager
        self.monitoring_interval = monitoring_interval
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.system_metrics_enabled = True

        # Built-in threshold rules for common scenarios
        self._setup_default_thresholds()

        logger.info(f"RealTimeMonitor initialized with {monitoring_interval}s interval")

    def start(self) -> None:
        """Start real-time monitoring."""
        if self.running:
            logger.warning("Monitor is already running")
            return

        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop, daemon=True
        )
        self.monitor_thread.start()
        logger.info("Real-time monitoring started")

    def stop(self) -> None:
        """Stop real-time monitoring."""
        if not self.running:
            return

        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        logger.info("Real-time monitoring stopped")

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.running:
            try:
                # Collect system metrics
                if self.system_metrics_enabled:
                    self._collect_system_metrics()

                # Check alert thresholds
                self.alert_manager.check_thresholds()

                # Sleep until next monitoring cycle
                time.sleep(self.monitoring_interval)

            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                time.sleep(self.monitoring_interval)

    def _collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        try:
            import psutil

            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=None)
            self.metrics_collector.record_metric(
                "system.cpu.usage_percent",
                cpu_percent,
                MetricType.GAUGE,
                unit="percent",
            )

            # Memory metrics
            memory = psutil.virtual_memory()
            self.metrics_collector.record_metric(
                "system.memory.usage_percent",
                memory.percent,
                MetricType.GAUGE,
                unit="percent",
            )
            self.metrics_collector.record_metric(
                "system.memory.available_gb",
                memory.available / (1024**3),
                MetricType.GAUGE,
                unit="GB",
            )

            # Disk metrics
            disk = psutil.disk_usage("/")
            self.metrics_collector.record_metric(
                "system.disk.usage_percent",
                (disk.used / disk.total) * 100,
                MetricType.GAUGE,
                unit="percent",
            )

        except ImportError:
            # psutil not available, skip system metrics
            if self.system_metrics_enabled:
                logger.warning("psutil not available, disabling system metrics")
                self.system_metrics_enabled = False
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")

    def _setup_default_thresholds(self) -> None:
        """Setup default threshold rules."""
        # High CPU usage
        self.alert_manager.add_threshold_rule(
            ThresholdRule(
                metric_name="system.cpu.usage_percent",
                threshold_value=80.0,
                operator="gt",
                severity=AlertSeverity.MEDIUM,
                message_template="High CPU usage: {current_value:.1f}% (threshold: {threshold_value}%)",
                cooldown_seconds=300,
            )
        )

        # High memory usage
        self.alert_manager.add_threshold_rule(
            ThresholdRule(
                metric_name="system.memory.usage_percent",
                threshold_value=85.0,
                operator="gt",
                severity=AlertSeverity.HIGH,
                message_template="High memory usage: {current_value:.1f}% (threshold: {threshold_value}%)",
                cooldown_seconds=300,
            )
        )

        # Low available memory
        self.alert_manager.add_threshold_rule(
            ThresholdRule(
                metric_name="system.memory.available_gb",
                threshold_value=1.0,
                operator="lt",
                severity=AlertSeverity.CRITICAL,
                message_template="Low available memory: {current_value:.1f}GB (threshold: {threshold_value}GB)",
                cooldown_seconds=180,
            )
        )


class TransformOperationMonitor:
    """Monitor transform operations with detailed metrics."""

    def __init__(self, metrics_collector: MetricsCollector):
        """Initialize transform operation monitor.

        Args:
            metrics_collector: Metrics collector
        """
        self.metrics_collector = metrics_collector
        self.active_operations: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()

        logger.info("TransformOperationMonitor initialized")

    @contextmanager
    def monitor_operation(
        self, operation_type: str, table_name: str, estimated_rows: int = 0, **metadata
    ):
        """Monitor a transform operation.

        Args:
            operation_type: Type of operation (INCREMENTAL, APPEND, etc.)
            table_name: Table being operated on
            estimated_rows: Estimated number of rows
            **metadata: Additional operation metadata
        """
        operation_id = f"{operation_type}_{table_name}_{int(time.time())}"
        start_time = time.time()

        # Record operation start
        operation_info = {
            "operation_type": operation_type,
            "table_name": table_name,
            "estimated_rows": estimated_rows,
            "start_time": start_time,
            "metadata": metadata,
        }

        with self.lock:
            self.active_operations[operation_id] = operation_info

        self.metrics_collector.record_metric(
            "transform.operations.started",
            1,
            MetricType.COUNTER,
            labels={"operation_type": operation_type, "table_name": table_name},
        )

        try:
            yield operation_id

            # Record successful completion
            execution_time = time.time() - start_time

            self.metrics_collector.record_metric(
                "transform.operations.completed",
                1,
                MetricType.COUNTER,
                labels={
                    "operation_type": operation_type,
                    "table_name": table_name,
                    "status": "success",
                },
            )

            self.metrics_collector.record_metric(
                "transform.operations.execution_time",
                execution_time,
                MetricType.TIMER,
                labels={"operation_type": operation_type, "table_name": table_name},
                unit="seconds",
            )

            if estimated_rows > 0:
                throughput = (
                    estimated_rows / execution_time if execution_time > 0 else 0
                )
                self.metrics_collector.record_metric(
                    "transform.operations.throughput",
                    throughput,
                    MetricType.GAUGE,
                    labels={"operation_type": operation_type, "table_name": table_name},
                    unit="rows_per_second",
                )

            logger.info(f"Operation {operation_id} completed in {execution_time:.3f}s")

        except Exception as e:
            # Record failed completion
            execution_time = time.time() - start_time

            self.metrics_collector.record_metric(
                "transform.operations.completed",
                1,
                MetricType.COUNTER,
                labels={
                    "operation_type": operation_type,
                    "table_name": table_name,
                    "status": "error",
                },
            )

            self.metrics_collector.record_metric(
                "transform.operations.errors",
                1,
                MetricType.COUNTER,
                labels={
                    "operation_type": operation_type,
                    "table_name": table_name,
                    "error_type": type(e).__name__,
                },
            )

            logger.error(
                f"Operation {operation_id} failed after {execution_time:.3f}s: {e}"
            )
            raise

        finally:
            # Clean up active operation
            with self.lock:
                self.active_operations.pop(operation_id, None)

    @contextmanager
    def monitor_operation_with_result(
        self, operation_type: str, table_name: str, estimated_rows: int = 0, **metadata
    ):
        """Monitor a transform operation and check result for success/error status.

        Args:
            operation_type: Type of operation (INCREMENTAL, APPEND, etc.)
            table_name: Table being operated on
            estimated_rows: Estimated number of rows
            **metadata: Additional operation metadata
        """
        operation_id = f"{operation_type}_{table_name}_{int(time.time())}"
        start_time = time.time()

        # Record operation start
        operation_info = {
            "operation_type": operation_type,
            "table_name": table_name,
            "estimated_rows": estimated_rows,
            "start_time": start_time,
            "metadata": metadata,
        }

        with self.lock:
            self.active_operations[operation_id] = operation_info

        self.metrics_collector.record_metric(
            "transform.operations.started",
            1,
            MetricType.COUNTER,
            labels={"operation_type": operation_type, "table_name": table_name},
        )

        result = None
        exception_occurred = False

        try:
            # Yield a function that the caller can use to set the result
            def set_result(operation_result):
                nonlocal result
                result = operation_result

            yield set_result

        except Exception as e:
            exception_occurred = True
            # Record failed completion due to exception
            execution_time = time.time() - start_time

            self.metrics_collector.record_metric(
                "transform.operations.completed",
                1,
                MetricType.COUNTER,
                labels={
                    "operation_type": operation_type,
                    "table_name": table_name,
                    "status": "error",
                },
            )

            self.metrics_collector.record_metric(
                "transform.operations.errors",
                1,
                MetricType.COUNTER,
                labels={
                    "operation_type": operation_type,
                    "table_name": table_name,
                    "error_type": type(e).__name__,
                },
            )

            logger.error(
                f"Operation {operation_id} failed after {execution_time:.3f}s: {e}"
            )
            raise

        finally:
            # Clean up active operation
            with self.lock:
                self.active_operations.pop(operation_id, None)

            # Record completion metrics if no exception occurred
            if not exception_occurred:
                execution_time = time.time() - start_time

                # Determine success/error based on result
                success = True
                if result is not None and hasattr(result, "success"):
                    success = result.success

                status = "success" if success else "error"

                self.metrics_collector.record_metric(
                    "transform.operations.completed",
                    1,
                    MetricType.COUNTER,
                    labels={
                        "operation_type": operation_type,
                        "table_name": table_name,
                        "status": status,
                    },
                )

                self.metrics_collector.record_metric(
                    "transform.operations.execution_time",
                    execution_time,
                    MetricType.TIMER,
                    labels={"operation_type": operation_type, "table_name": table_name},
                    unit="seconds",
                )

                if estimated_rows > 0:
                    throughput = (
                        estimated_rows / execution_time if execution_time > 0 else 0
                    )
                    self.metrics_collector.record_metric(
                        "transform.operations.throughput",
                        throughput,
                        MetricType.GAUGE,
                        labels={
                            "operation_type": operation_type,
                            "table_name": table_name,
                        },
                        unit="rows_per_second",
                    )

                if success:
                    logger.info(
                        f"Operation {operation_id} completed successfully in {execution_time:.3f}s"
                    )
                else:
                    logger.warning(
                        f"Operation {operation_id} completed with errors in {execution_time:.3f}s"
                    )

    def get_active_operations(self) -> Dict[str, Dict[str, Any]]:
        """Get currently active operations.

        Returns:
            Dictionary of active operations
        """
        with self.lock:
            return dict(self.active_operations)

    def get_operation_metrics(self) -> Dict[str, Any]:
        """Get operation metrics summary.

        Returns:
            Dictionary with operation metrics
        """
        return {
            "active_operations": len(self.active_operations),
            "operation_types": list(
                set(op["operation_type"] for op in self.active_operations.values())
            ),
            "tables_being_processed": list(
                set(op["table_name"] for op in self.active_operations.values())
            ),
        }


class MonitoringManager:
    """Central monitoring management system."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize monitoring manager.

        Args:
            config: Optional configuration dictionary
        """
        config = config or {}

        # Initialize components
        self.metrics_collector = MetricsCollector(
            retention_hours=config.get("retention_hours", 24),
            max_points_per_metric=config.get("max_points_per_metric", 10000),
        )

        self.alert_manager = AlertManager(self.metrics_collector)

        self.real_time_monitor = RealTimeMonitor(
            self.metrics_collector,
            self.alert_manager,
            config.get("monitoring_interval", 10.0),
        )

        self.operation_monitor = TransformOperationMonitor(self.metrics_collector)

        # Configuration
        self.auto_start_monitoring = config.get("auto_start_monitoring", True)
        self.export_enabled = config.get("export_enabled", False)
        self.export_interval = config.get("export_interval", 60)  # seconds
        self.export_path = config.get("export_path", "/tmp/sqlflow_metrics")

        # Start monitoring if configured
        if self.auto_start_monitoring:
            self.start_monitoring()

        logger.info("MonitoringManager initialized")

    def start_monitoring(self) -> None:
        """Start all monitoring components."""
        self.real_time_monitor.start()

        if self.export_enabled:
            self._start_metric_export()

        logger.info("Monitoring started")

    def stop_monitoring(self) -> None:
        """Stop all monitoring components."""
        self.real_time_monitor.stop()
        logger.info("Monitoring stopped")

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive dashboard data.

        Returns:
            Dictionary with all monitoring data
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "metrics_summary": self.metrics_collector.get_metrics_summary(),
            "alert_summary": self.alert_manager.get_alert_summary(),
            "active_alerts": [
                {
                    "name": alert.name,
                    "message": alert.message,
                    "severity": alert.severity.value,
                    "timestamp": alert.timestamp.isoformat(),
                    "metric_name": alert.metric_name,
                    "current_value": alert.current_value,
                    "threshold_value": alert.threshold_value,
                }
                for alert in self.alert_manager.get_active_alerts()
            ],
            "operation_metrics": self.operation_monitor.get_operation_metrics(),
            "active_operations": self.operation_monitor.get_active_operations(),
        }

    def export_metrics(self, file_path: Optional[str] = None) -> str:
        """Export metrics to file.

        Args:
            file_path: Optional file path, defaults to configured path

        Returns:
            Path to exported file
        """
        if file_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"{self.export_path}/metrics_{timestamp}.json"

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Export data
        dashboard_data = self.get_dashboard_data()

        with open(file_path, "w") as f:
            json.dump(dashboard_data, f, indent=2, default=str)

        logger.info(f"Metrics exported to {file_path}")
        return file_path

    def _start_metric_export(self) -> None:
        """Start automatic metric export."""

        def export_loop():
            while self.real_time_monitor.running:
                try:
                    self.export_metrics()
                    time.sleep(self.export_interval)
                except Exception as e:
                    logger.error(f"Metric export failed: {e}")
                    time.sleep(self.export_interval)

        export_thread = threading.Thread(target=export_loop, daemon=True)
        export_thread.start()
        logger.info(f"Started automatic metric export every {self.export_interval}s")
