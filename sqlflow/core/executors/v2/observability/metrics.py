"""Simple observability manager for V2 executor.

Following Raymond Hettinger's philosophy:
- "Simple is better than complex"
- "Readability counts"
- "If the implementation is hard to explain, it's a bad idea"

This observability manager provides essential metrics collection
without the complexity of enterprise monitoring solutions.
It's designed to be easy to understand, test, and extend.
"""

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class PerformanceAlert:
    """Immutable performance alert following Zen of Python."""

    component: str
    alert_type: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    suggested_actions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepMetrics:
    """Metrics for a single step execution with aggregate capabilities."""

    # Individual execution tracking
    step_id: str = ""
    start_time: float = 0.0
    end_time: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    rows_affected: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Aggregate metrics for multiple executions
    calls: int = 0
    failures: int = 0
    total_time_ms: float = 0.0
    total_rows: int = 0

    def add_execution(
        self,
        duration_ms: float,
        rows: int = 0,
        success: bool = True,
        resource_usage: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add execution data to aggregate metrics."""
        self.calls += 1
        self.total_time_ms += duration_ms
        if success:
            self.total_rows += rows
        else:
            self.failures += 1

        if resource_usage:
            self.metadata.update(resource_usage)

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if self.calls == 0:
            return 0.0
        return ((self.calls - self.failures) / self.calls) * 100.0

    @property
    def average_duration_ms(self) -> float:
        """Average duration per call."""
        if self.calls == 0:
            return 0.0
        return self.total_time_ms / self.calls

    @property
    def throughput_rows_per_second(self) -> float:
        """Throughput in rows per second."""
        if self.total_time_ms == 0:
            return 0.0
        return self.total_rows / (self.total_time_ms / 1000.0)

    @property
    def duration_ms(self) -> float:
        """Duration in milliseconds for single execution."""
        if self.end_time is None:
            return (time.time() - self.start_time) * 1000
        return (self.end_time - self.start_time) * 1000

    @property
    def is_complete(self) -> bool:
        """Whether the step has completed."""
        return self.end_time is not None


class SimpleObservabilityManager:
    """Simple, Pythonic observability manager.

    Focuses on the essential metrics needed for pipeline monitoring:
    - Step execution times
    - Success/failure tracking
    - Row counts
    - Error collection

    Designed to be lightweight and easy to understand.
    """

    def __init__(
        self,
        execution_id: Optional[str] = None,
        duration_threshold_ms: int = 5000,
    ):
        self.execution_id = execution_id or str(time.time())
        self._step_metrics: Dict[str, StepMetrics] = {}
        self._alerts: List[PerformanceAlert] = []
        self._duration_threshold_ms = duration_threshold_ms
        self._start_time = time.time()
        self._current_step: Optional[str] = None

    def start_step(self, step_id: str) -> None:
        """Start monitoring a step."""
        if step_id in self._step_metrics:
            # Step already exists, which might indicate a retry
            existing = self._step_metrics[step_id]
            if not existing.is_complete:
                # Previous execution didn't complete, mark it as failed
                existing.end_time = time.time()
                existing.success = False
                existing.error_message = "Step interrupted by retry"

        self._step_metrics[step_id] = StepMetrics(
            step_id=step_id, start_time=time.time()
        )
        self._current_step = step_id

    def end_step(
        self, step_id: str, success: bool, error: Optional[str] = None
    ) -> None:
        """End monitoring a step."""
        if step_id not in self._step_metrics:
            # Step wasn't started properly, create a minimal record
            self._step_metrics[step_id] = StepMetrics(
                step_id=step_id,
                start_time=time.time(),
                end_time=time.time(),
                success=success,
                error_message=error or "Step not properly started",
            )
            return

        metrics = self._step_metrics[step_id]
        metrics.end_time = time.time()
        metrics.success = success
        if error:
            metrics.error_message = error

        self.check_for_performance_alerts(metrics)

        if step_id == self._current_step:
            self._current_step = None

    def check_for_performance_alerts(self, metrics: StepMetrics):
        """Check for performance issues and generate alerts."""
        if metrics.duration_ms > self._duration_threshold_ms:
            alert = PerformanceAlert(
                component="StepExecutor",
                alert_type="SlowStepWarning",
                message=f"Step '{metrics.step_id}' exceeded duration threshold: {metrics.duration_ms:.0f}ms > {self._duration_threshold_ms}ms",
                severity=AlertSeverity.WARNING,
                timestamp=datetime.now(),
                suggested_actions=["Optimize step logic", "Increase timeout"],
                metadata={
                    "step_id": metrics.step_id,
                    "duration_ms": metrics.duration_ms,
                },
            )
            self._alerts.append(alert)

    def record_rows_affected(self, step_id: str, rows: int) -> None:
        """Record number of rows affected by a step."""
        if step_id in self._step_metrics:
            self._step_metrics[step_id].rows_affected = rows

    def add_step_metadata(self, step_id: str, metadata: Dict[str, Any]) -> None:
        """Add metadata to a step."""
        if step_id in self._step_metrics:
            self._step_metrics[step_id].metadata.update(metadata)

    @contextmanager
    def measure_step(self, step_id: str):
        """Context manager for measuring step execution.

        Usage:
            with observability.measure_step("my_step"):
                # do work
                pass
        """
        self.start_step(step_id)
        try:
            yield
            self.end_step(step_id, success=True)
        except Exception as e:
            self.end_step(step_id, success=False, error=str(e))
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics in a simple dictionary format."""
        total_duration = (time.time() - self._start_time) * 1000
        completed_steps = [m for m in self._step_metrics.values() if m.is_complete]

        return {
            "execution_id": self.execution_id,
            "total_duration_ms": total_duration,
            "total_steps": len(self._step_metrics),
            "completed_steps": len(completed_steps),
            "successful_steps": len([m for m in completed_steps if m.success]),
            "failed_steps": len([m for m in completed_steps if not m.success]),
            "total_rows_affected": sum(m.rows_affected for m in completed_steps),
            "alerts_generated": len(self._alerts),
            "step_details": {
                step_id: {
                    "duration_ms": metrics.duration_ms,
                    "success": metrics.success,
                    "rows_affected": metrics.rows_affected,
                    "error_message": metrics.error_message,
                    "metadata": metrics.metadata,
                }
                for step_id, metrics in self._step_metrics.items()
            },
        }

    def get_failed_steps(self) -> List[StepMetrics]:
        """Get list of failed steps."""
        return [
            m for m in self._step_metrics.values() if m.is_complete and not m.success
        ]

    def get_alerts(self) -> List[PerformanceAlert]:
        """Get list of generated performance alerts."""
        return self._alerts.copy()

    def get_successful_steps(self) -> List[StepMetrics]:
        """Get list of successful steps."""
        return [m for m in self._step_metrics.values() if m.is_complete and m.success]

    @property
    def total_duration_ms(self) -> float:
        """Total execution duration in milliseconds."""
        return (time.time() - self._start_time) * 1000

    @property
    def has_failures(self) -> bool:
        """Whether any steps have failed."""
        return any(not m.success for m in self._step_metrics.values() if m.is_complete)

    def __str__(self) -> str:
        """Human-readable summary."""
        completed = len([m for m in self._step_metrics.values() if m.is_complete])
        successful = len(self.get_successful_steps())
        failed = len(self.get_failed_steps())

        return (
            f"Execution {self.execution_id}: "
            f"{completed} steps completed "
            f"({successful} successful, {failed} failed) "
            f"in {self.total_duration_ms:.1f}ms"
        )


# Factory function for creating observability managers
def create_observability_manager(
    execution_id: Optional[str] = None,
) -> SimpleObservabilityManager:
    """Factory function for creating observability managers.

    Provides a simple way to create observability managers with
    sensible defaults.
    """
    return SimpleObservabilityManager(execution_id)


# Aliases for backward compatibility and test expectations
ObservabilityManager = SimpleObservabilityManager
