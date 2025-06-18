"""Performance monitoring and optimization for V2 Executor.

Following the Zen of Python:
- Simple is better than complex
- Flat is better than nested
- If the implementation is hard to explain, it's a bad idea

This module provides production-ready performance monitoring and optimization.
"""

import gc
import threading
import time

# Optional psutil import for resource monitoring
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

from .feature_flags import FeatureFlag, is_v2_enabled

logger = get_logger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot - Simple data structure following Zen of Python."""

    timestamp: datetime

    # Execution metrics
    total_execution_time_ms: float = 0.0
    step_execution_times: Dict[str, float] = field(default_factory=dict)
    throughput_rows_per_second: float = 0.0

    # Resource metrics
    memory_usage_mb: float = 0.0
    memory_peak_mb: float = 0.0
    cpu_usage_percent: float = 0.0

    # Database metrics
    db_connections_active: int = 0
    db_query_count: int = 0
    db_avg_query_time_ms: float = 0.0

    # Pipeline metrics
    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging and analysis."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "execution": {
                "total_time_ms": self.total_execution_time_ms,
                "step_times": self.step_execution_times,
                "throughput_rows_per_sec": self.throughput_rows_per_second,
            },
            "resources": {
                "memory_mb": self.memory_usage_mb,
                "memory_peak_mb": self.memory_peak_mb,
                "cpu_percent": self.cpu_usage_percent,
            },
            "database": {
                "connections": self.db_connections_active,
                "queries": self.db_query_count,
                "avg_query_time_ms": self.db_avg_query_time_ms,
            },
            "pipeline": {
                "total_steps": self.total_steps,
                "successful_steps": self.successful_steps,
                "failed_steps": self.failed_steps,
                "success_rate": self.successful_steps / max(self.total_steps, 1),
            },
        }


class ResourceMonitor:
    """Simple resource monitoring following 'Simple is better than complex'."""

    def __init__(self):
        if PSUTIL_AVAILABLE and psutil is not None:
            self._process = psutil.Process()
            self._baseline_memory = self._get_memory_usage()
            self._peak_memory = self._baseline_memory
        else:
            self._process = None
            self._baseline_memory = 0.0
            self._peak_memory = 0.0

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        if not PSUTIL_AVAILABLE or self._process is None:
            return 0.0
        try:
            return self._process.memory_info().rss / 1024 / 1024
        except Exception:
            # Handle psutil exceptions gracefully
            return 0.0

    def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        if not PSUTIL_AVAILABLE or self._process is None:
            return 0.0
        try:
            return self._process.cpu_percent()
        except Exception:
            # Handle psutil exceptions gracefully
            return 0.0

    def get_current_metrics(self) -> Dict[str, float]:
        """Get current resource metrics."""
        current_memory = self._get_memory_usage()
        self._peak_memory = max(self._peak_memory, current_memory)

        return {
            "memory_mb": current_memory,
            "memory_peak_mb": self._peak_memory,
            "cpu_percent": self._get_cpu_usage(),
        }

    def reset_peak_memory(self) -> None:
        """Reset peak memory tracking."""
        self._peak_memory = self._get_memory_usage()


class PerformanceOptimizer:
    """Performance optimization recommendations - Simple and actionable."""

    def __init__(self):
        self._recent_metrics: deque = deque(maxlen=100)  # Last 100 executions

    def add_metrics(self, metrics: PerformanceMetrics) -> None:
        """Add metrics for analysis."""
        self._recent_metrics.append(metrics)

    def get_optimization_recommendations(self) -> List[Dict[str, Any]]:
        """Get actionable optimization recommendations."""
        if len(self._recent_metrics) < 5:
            return []

        recommendations = []

        # Memory optimization
        avg_memory = sum(m.memory_usage_mb for m in self._recent_metrics) / len(
            self._recent_metrics
        )
        if avg_memory > 1000:  # > 1GB
            recommendations.append(
                {
                    "type": "memory",
                    "severity": "high" if avg_memory > 2000 else "medium",
                    "message": f"High memory usage detected: {avg_memory:.1f}MB average",
                    "actions": [
                        "Consider reducing batch sizes",
                        "Implement data streaming for large datasets",
                        "Enable garbage collection optimization",
                    ],
                }
            )

        # Performance optimization
        recent_times = [
            m.total_execution_time_ms for m in list(self._recent_metrics)[-10:]
        ]
        if len(recent_times) >= 5:
            avg_time = sum(recent_times) / len(recent_times)
            if avg_time > 30000:  # > 30 seconds
                recommendations.append(
                    {
                        "type": "performance",
                        "severity": "medium",
                        "message": f"Slow execution detected: {avg_time/1000:.1f}s average",
                        "actions": [
                            "Enable parallel execution",
                            "Optimize SQL queries",
                            "Consider step dependency optimization",
                        ],
                    }
                )

        # Success rate optimization
        recent_success_rates = [
            m.successful_steps / max(m.total_steps, 1)
            for m in list(self._recent_metrics)[-20:]
        ]
        if recent_success_rates:
            avg_success_rate = sum(recent_success_rates) / len(recent_success_rates)
            if avg_success_rate < 0.95:  # < 95% success rate
                recommendations.append(
                    {
                        "type": "reliability",
                        "severity": "high",
                        "message": f"Low success rate: {avg_success_rate*100:.1f}%",
                        "actions": [
                            "Review error logs for common failures",
                            "Implement retry policies",
                            "Improve error handling",
                        ],
                    }
                )

        return recommendations


class PerformanceMonitor:
    """
    Production performance monitor for V2 executor.

    Following 'Simple is better than complex' - focused on essential metrics.
    """

    def __init__(self, run_id: str):
        self.run_id = run_id
        self._resource_monitor = ResourceMonitor()
        self._optimizer = PerformanceOptimizer()

        # Performance tracking
        self._execution_start_time: Optional[float] = None
        self._step_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._db_metrics = {"query_count": 0, "total_query_time": 0.0}

        # Threading for non-blocking monitoring
        self._monitoring_enabled = is_v2_enabled(FeatureFlag.V2_PERFORMANCE_MONITORING)
        self._stop_monitoring = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

        if self._monitoring_enabled:
            self._start_background_monitoring()

    def _start_background_monitoring(self) -> None:
        """Start background resource monitoring thread."""
        self._monitor_thread = threading.Thread(
            target=self._background_monitor,
            daemon=True,
            name=f"perf-monitor-{self.run_id[:8]}",
        )
        self._monitor_thread.start()
        logger.debug("Performance monitoring started")

    def _background_monitor(self) -> None:
        """Background monitoring loop - Simple and efficient."""
        while not self._stop_monitoring.is_set():
            try:
                # Light monitoring every 5 seconds
                if self._stop_monitoring.wait(5.0):
                    break

                # Trigger garbage collection periodically for memory optimization
                if not self._stop_monitoring.is_set():
                    gc.collect()

            except Exception as e:
                logger.warning(f"Background monitoring error: {e}")

    def start_execution(self) -> None:
        """Mark start of pipeline execution."""
        self._execution_start_time = time.time()
        self._resource_monitor.reset_peak_memory()
        logger.debug(f"Performance monitoring started for execution {self.run_id}")

    def record_step_start(self, step_id: str) -> None:
        """Record step start time."""
        self._step_metrics[step_id]["start_time"] = time.time()

    def record_step_completion(
        self, step_id: str, success: bool, rows_processed: int = 0
    ) -> None:
        """Record step completion with metrics."""
        if step_id in self._step_metrics:
            start_time = self._step_metrics[step_id].get("start_time", time.time())
            execution_time = (time.time() - start_time) * 1000  # Convert to ms

            self._step_metrics[step_id].update(
                {
                    "execution_time_ms": execution_time,
                    "success": success,
                    "rows_processed": rows_processed,
                }
            )

    def record_db_query(self, query_time_ms: float) -> None:
        """Record database query execution time."""
        self._db_metrics["query_count"] += 1
        self._db_metrics["total_query_time"] += query_time_ms

    def get_current_metrics(self) -> PerformanceMetrics:
        """Get current performance snapshot."""
        resource_metrics = self._resource_monitor.get_current_metrics()

        # Calculate execution metrics
        total_execution_time = 0.0
        if self._execution_start_time:
            total_execution_time = (time.time() - self._execution_start_time) * 1000

        step_times = {
            step_id: metrics.get("execution_time_ms", 0.0)
            for step_id, metrics in self._step_metrics.items()
        }

        # Calculate throughput
        total_rows = sum(
            metrics.get("rows_processed", 0) for metrics in self._step_metrics.values()
        )
        throughput = 0.0
        if total_execution_time > 0:
            throughput = (total_rows * 1000) / total_execution_time  # rows per second

        # Database metrics
        avg_query_time = 0.0
        if self._db_metrics["query_count"] > 0:
            avg_query_time = (
                self._db_metrics["total_query_time"] / self._db_metrics["query_count"]
            )

        # Pipeline metrics
        successful_steps = sum(
            1
            for metrics in self._step_metrics.values()
            if metrics.get("success", False)
        )
        failed_steps = len(self._step_metrics) - successful_steps

        return PerformanceMetrics(
            timestamp=datetime.utcnow(),
            total_execution_time_ms=total_execution_time,
            step_execution_times=step_times,
            throughput_rows_per_second=throughput,
            memory_usage_mb=resource_metrics["memory_mb"],
            memory_peak_mb=resource_metrics["memory_peak_mb"],
            cpu_usage_percent=resource_metrics["cpu_percent"],
            db_connections_active=1,  # Simple assumption for now
            db_query_count=self._db_metrics["query_count"],
            db_avg_query_time_ms=avg_query_time,
            total_steps=len(self._step_metrics),
            successful_steps=successful_steps,
            failed_steps=failed_steps,
        )

    def finish_execution(self) -> PerformanceMetrics:
        """Finish execution and get final metrics."""
        final_metrics = self.get_current_metrics()

        # Add to optimizer for recommendations
        self._optimizer.add_metrics(final_metrics)

        # Stop background monitoring
        if self._monitor_thread:
            self._stop_monitoring.set()
            self._monitor_thread.join(timeout=1.0)

        logger.info(f"Performance monitoring completed for {self.run_id}")
        return final_metrics

    def get_optimization_recommendations(self) -> List[Dict[str, Any]]:
        """Get performance optimization recommendations."""
        return self._optimizer.get_optimization_recommendations()

    def log_performance_summary(self) -> None:
        """Log performance summary for operations team."""
        metrics = self.get_current_metrics()
        recommendations = self.get_optimization_recommendations()

        logger.info(
            f"🔍 Performance Summary - Run {self.run_id}: "
            f"{metrics.total_execution_time_ms/1000:.1f}s, "
            f"{metrics.memory_usage_mb:.1f}MB memory, "
            f"{metrics.successful_steps}/{metrics.total_steps} steps successful"
        )

        if recommendations:
            logger.warning(
                f"📊 {len(recommendations)} optimization recommendations available"
            )
            for rec in recommendations:
                logger.warning(f"   {rec['type'].upper()}: {rec['message']}")


# Global performance monitoring registry
_performance_monitors: Dict[str, PerformanceMonitor] = {}
_monitor_lock = threading.Lock()


def get_performance_monitor(run_id: str) -> PerformanceMonitor:
    """Get or create performance monitor for run."""
    with _monitor_lock:
        if run_id not in _performance_monitors:
            _performance_monitors[run_id] = PerformanceMonitor(run_id)
        return _performance_monitors[run_id]


def cleanup_performance_monitor(run_id: str) -> None:
    """Clean up performance monitor after execution."""
    # Get monitor first (supports testing via mocking)
    monitor = get_performance_monitor(run_id)
    if monitor:
        monitor.finish_execution()

    # Clean up from registry
    with _monitor_lock:
        if run_id in _performance_monitors:
            del _performance_monitors[run_id]
