"""Observability Manager for V2 Executor.

This module provides comprehensive observability capabilities inspired by the
resilience module's performance monitoring. It offers automatic insights,
alerts, and performance analysis for pipeline execution.
"""

import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, ContextManager, Dict, Generator, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PerformanceAlert:
    """Represents a performance alert with actionable insights."""

    component: str
    alert_type: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    suggested_actions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary for serialization."""
        return {
            "component": self.component,
            "alert_type": self.alert_type,
            "message": self.message,
            "severity": self.severity.value,
            "timestamp": self.timestamp.isoformat(),
            "suggested_actions": self.suggested_actions,
            "metadata": self.metadata,
        }


@dataclass
class StepMetrics:
    """Metrics for a specific step type."""

    calls: int = 0
    total_time_ms: float = 0.0
    failures: int = 0
    total_rows: int = 0
    resource_usage: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    recent_durations: deque[float] = field(default_factory=lambda: deque(maxlen=100))

    def add_execution(
        self,
        duration_ms: float,
        rows: int = 0,
        success: bool = True,
        resource_usage: Optional[Dict[str, float]] = None,
    ):
        """Add execution data to metrics."""
        self.calls += 1

        # Handle invalid duration_ms gracefully
        if duration_ms is not None and isinstance(duration_ms, (int, float)):
            self.total_time_ms += duration_ms
            self.recent_durations.append(duration_ms)
        else:
            logger.warning(f"Invalid duration_ms: {duration_ms}, using 0.0")
            self.total_time_ms += 0.0
            self.recent_durations.append(0.0)

        # Handle invalid rows gracefully - only count for successful executions
        if (
            success
            and rows is not None
            and isinstance(rows, (int, float))
            and rows >= 0
        ):
            self.total_rows += int(rows)
        elif not success:
            # Failed executions don't contribute to row count
            pass
        else:
            logger.warning(f"Invalid rows value: {rows}, using 0")

        if not success:
            self.failures += 1

        if resource_usage and isinstance(resource_usage, dict):
            for resource, value in resource_usage.items():
                if isinstance(value, (int, float)):
                    self.resource_usage[resource] += value

    @property
    def avg_duration_ms(self) -> float:
        """Average execution duration."""
        return self.total_time_ms / max(self.calls, 1)

    @property
    def average_duration_ms(self) -> float:
        """Average execution duration (alias for compatibility)."""
        return self.avg_duration_ms

    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        if self.calls == 0:
            return 0.0
        return ((self.calls - self.failures) / self.calls) * 100

    @property
    def failure_rate(self) -> float:
        """Failure rate as a percentage."""
        return (self.failures / max(self.calls, 1)) * 100

    @property
    def throughput_rows_per_second(self) -> float:
        """Throughput in rows per second."""
        if self.total_time_ms <= 0 or self.total_rows <= 0:
            return 0.0
        return self.total_rows / (self.total_time_ms / 1000)


class ObservabilityManager:
    """
    Manages observability for the V2 Executor.

    Inspired by the resilience module's performance monitoring, this provides
    automatic insights, alerts, and performance analysis with minimal overhead.
    """

    def __init__(self, run_id: str):
        """Initialize the ObservabilityManager."""
        self.run_id = run_id
        self._lock = threading.RLock()
        self._step_metrics: Dict[str, StepMetrics] = defaultdict(StepMetrics)
        self._alerts: List[PerformanceAlert] = []
        self._run_start_time = datetime.utcnow()

        # Performance thresholds
        self.slow_step_threshold_ms = 30000  # 30 seconds
        self.critical_failure_rate = 25.0  # 25%

        logger.info(f"ObservabilityManager initialized for run {run_id}")

    def record_step_start(self, step_id: str, step_type: str) -> None:
        """Record the start of a step execution."""
        logger.debug(f"Step started: {step_id} ({step_type})")

    def record_step_success(self, step_id: str, event_data: Dict[str, Any]) -> None:
        """Record successful step completion."""
        duration_ms = event_data.get("duration_ms", 0.0)
        rows_affected = event_data.get("rows_affected", 0)
        resource_usage = event_data.get("resource_usage", {})
        step_type = event_data.get("step_type", "unknown")

        with self._lock:
            metrics = self._step_metrics[step_type]
            metrics.add_execution(
                duration_ms=duration_ms,
                rows=rows_affected,
                success=True,
                resource_usage=resource_usage,
            )

        # Check for performance alerts - only if duration is valid
        if duration_ms is not None and isinstance(duration_ms, (int, float)):
            self._check_performance_alerts(step_id, step_type, duration_ms, event_data)

        # Handle logging with safe duration formatting
        safe_duration = duration_ms if isinstance(duration_ms, (int, float)) else 0.0
        logger.debug(f"Step completed successfully: {step_id} ({safe_duration:.1f}ms)")

    def record_step_failure(
        self, step_id: str, step_type: str, error_message: str, duration_ms: float
    ) -> None:
        """Record step failure."""
        with self._lock:
            metrics = self._step_metrics[step_type]
            metrics.add_execution(duration_ms=duration_ms, success=False)

        # Generate failure alert
        self._generate_failure_alert(step_id, step_type, error_message)

        logger.error(f"Step failed: {step_id} ({step_type}): {error_message}")

    def _check_performance_alerts(
        self,
        step_id: str,
        step_type: str,
        duration_ms: float,
        event_data: Dict[str, Any],
    ) -> None:
        """Check for performance issues and generate alerts."""

        # Handle invalid duration_ms gracefully
        if duration_ms is None or not isinstance(duration_ms, (int, float)):
            logger.warning(
                f"Cannot check performance alerts for invalid duration: {duration_ms}"
            )
            return

        # Slow execution alerts
        if duration_ms > self.slow_step_threshold_ms:
            alert = PerformanceAlert(
                component=step_id,
                alert_type="slow_execution",
                message=f"Step {step_id} ({step_type}) took {duration_ms/1000:.1f}s",
                severity=AlertSeverity.WARNING,
                timestamp=datetime.utcnow(),
                suggested_actions=[
                    "Consider optimizing the step logic",
                    "Check for resource constraints",
                    "Review data volume and complexity",
                ],
                metadata={"duration_ms": duration_ms, "step_type": step_type},
            )

            with self._lock:
                self._alerts.append(alert)

            logger.warning(f"PERFORMANCE ALERT: {alert.message}")

    def _generate_failure_alert(
        self, step_id: str, step_type: str, error_message: str
    ) -> None:
        """Generate alert for step failure."""
        alert = PerformanceAlert(
            component=step_id,
            alert_type="step_failure",
            message=f"Step {step_id} ({step_type}) failed: {error_message}",
            severity=AlertSeverity.ERROR,
            timestamp=datetime.utcnow(),
            suggested_actions=[
                "Check step configuration and parameters",
                "Verify data source connectivity",
                "Review error logs for details",
            ],
            metadata={"error_message": error_message},
        )

        with self._lock:
            self._alerts.append(alert)

        logger.error(f"PERFORMANCE ALERT: {alert.message}")

    def record_recovery_attempt(
        self, component: str, recovery_strategy: str, success: bool
    ) -> None:
        """Record error recovery attempts as required by Week 7-8.

        Args:
            component: Component that attempted recovery
            recovery_strategy: Strategy used for recovery
            success: Whether recovery was successful
        """
        alert_type = "recovery_success" if success else "recovery_failure"
        severity = AlertSeverity.INFO if success else AlertSeverity.WARNING

        message = f"Recovery {('succeeded' if success else 'failed')} for {component} using {recovery_strategy}"

        alert = PerformanceAlert(
            component=component,
            alert_type=alert_type,
            message=message,
            severity=severity,
            timestamp=datetime.utcnow(),
            suggested_actions=(
                [
                    "Monitor component for stability",
                    "Review recovery strategy effectiveness",
                    "Consider improving error prevention",
                ]
                if success
                else [
                    "Investigate root cause of repeated failures",
                    "Consider alternative recovery strategies",
                    "Check system resources and connectivity",
                ]
            ),
            metadata={"recovery_strategy": recovery_strategy, "success": success},
        )

        with self._lock:
            self._alerts.append(alert)

        logger.info(f"RECOVERY ALERT: {message}")

    def check_system_health(self) -> Dict[str, Any]:
        """Perform comprehensive system health check as per Week 7-8 requirements.

        Returns:
            Health status with actionable insights
        """
        with self._lock:
            total_steps = sum(metrics.calls for metrics in self._step_metrics.values())
            total_failures = sum(
                metrics.failures for metrics in self._step_metrics.values()
            )

            health_status = {
                "overall_health": "healthy",
                "total_steps": total_steps,
                "total_failures": total_failures,
                "failure_rate": (total_failures / max(total_steps, 1)) * 100,
                "alerts_count": len(self._alerts),
                "critical_alerts": len(
                    [a for a in self._alerts if a.severity == AlertSeverity.CRITICAL]
                ),
                "recommendations": [],
            }

            # Determine overall health
            failure_rate = health_status["failure_rate"]
            if failure_rate > 50:
                health_status["overall_health"] = "critical"
                health_status["recommendations"].append(
                    "Immediate attention required - high failure rate"
                )
            elif failure_rate > 25:
                health_status["overall_health"] = "degraded"
                health_status["recommendations"].append(
                    "Review failing components and error patterns"
                )
            elif failure_rate > 10:
                health_status["overall_health"] = "warning"
                health_status["recommendations"].append(
                    "Monitor system for potential issues"
                )

            # Check for critical alerts
            if health_status["critical_alerts"] > 0:
                health_status["overall_health"] = "critical"
                health_status["recommendations"].append(
                    "Address critical alerts immediately"
                )

            # Performance recommendations
            slow_steps = []
            for step_type, metrics in self._step_metrics.items():
                if metrics.avg_duration_ms > self.slow_step_threshold_ms:
                    slow_steps.append(step_type)

            if slow_steps:
                health_status["recommendations"].append(
                    f"Optimize slow step types: {', '.join(slow_steps)}"
                )

            return health_status

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        with self._lock:
            total_steps = sum(metrics.calls for metrics in self._step_metrics.values())
            total_failures = sum(
                metrics.failures for metrics in self._step_metrics.values()
            )
            total_execution_time_ms = sum(
                metrics.total_time_ms for metrics in self._step_metrics.values()
            )
            total_rows_processed = sum(
                metrics.total_rows for metrics in self._step_metrics.values()
            )

            run_duration_ms = (
                datetime.utcnow() - self._run_start_time
            ).total_seconds() * 1000

            summary = {
                "run_id": self.run_id,
                "run_duration_ms": run_duration_ms,
                "total_steps": total_steps,
                "total_failures": total_failures,
                "failure_rate": (total_failures / max(total_steps, 1)) * 100,
                "total_execution_time_ms": total_execution_time_ms,
                "total_rows_processed": total_rows_processed,
                "throughput_rows_per_second": (
                    total_rows_processed / (total_execution_time_ms / 1000)
                    if total_execution_time_ms > 0
                    else 0
                ),
                "alerts": [alert.to_dict() for alert in self._alerts],
                "step_details": {},
            }

            # Add per-step-type details
            step_details: Dict[str, Dict[str, Any]] = {}
            for step_type, metrics in self._step_metrics.items():
                step_details[step_type] = {
                    "calls": metrics.calls,
                    "failures": metrics.failures,
                    "failure_rate": metrics.failure_rate,
                    "avg_duration_ms": metrics.avg_duration_ms,
                    "total_rows": metrics.total_rows,
                    "throughput_rows_per_second": metrics.throughput_rows_per_second,
                }

            summary["step_details"] = step_details
            return summary

    def get_alerts(
        self, severity: Optional[AlertSeverity] = None
    ) -> List[PerformanceAlert]:
        """Get alerts, optionally filtered by severity."""
        with self._lock:
            alerts = self._alerts.copy()

        if severity:
            alerts = [alert for alert in alerts if alert.severity == severity]

        return alerts

    @contextmanager
    def measure_execution(self, step_id: str, step_type: str) -> ContextManager[None]:
        """Context manager to measure execution time of a step."""
        start_time = time.perf_counter()
        logger.info(f"Starting step {step_id} ({step_type})")

        try:
            yield
        except Exception as e:
            logger.error(f"Error in step {step_id} ({step_type}): {e}")
            raise
        finally:
            end_time = time.perf_counter()
            duration_ms = (end_time - start_time) * 1000
            logger.info(
                f"Step {step_id} ({step_type}) completed in {duration_ms:.1f}ms"
            )

    @contextmanager
    def measure_scope(
        self, scope_name: str, metadata: Optional[Dict[str, Any]] = None
    ) -> Generator[None, None, None]:
        """Context manager for measuring execution scope.

        This provides the Week 7-8 requirement for comprehensive observability
        with automatic timing and error tracking.

        Args:
            scope_name: Name of the scope being measured
            metadata: Optional metadata to attach to measurements

        Example:
            with observability.measure_scope("data_processing", {"table": "customers"}):
                # Code to measure
                process_data()
        """
        start_time = time.perf_counter()
        scope_metadata = metadata or {}

        logger.debug(f"Starting scope measurement: {scope_name}")

        try:
            yield
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.debug(f"Completed scope: {scope_name} ({duration_ms:.1f}ms)")

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(f"Failed scope: {scope_name} ({duration_ms:.1f}ms) - {str(e)}")

            # Generate alert for scope failure
            alert = PerformanceAlert(
                component=scope_name,
                alert_type="scope_failure",
                message=f"Scope {scope_name} failed: {str(e)}",
                severity=AlertSeverity.ERROR,
                timestamp=datetime.utcnow(),
                suggested_actions=[
                    "Check error logs for details",
                    "Verify input data and configuration",
                    "Review scope implementation for errors",
                ],
                metadata={
                    **scope_metadata,
                    "duration_ms": duration_ms,
                    "error": str(e),
                },
            )

            with self._lock:
                self._alerts.append(alert)

            raise  # Re-raise the exception
