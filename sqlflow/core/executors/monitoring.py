"""
V2 Executor Monitoring and Metrics

Following Raymond Hettinger's Zen of Python:
- Simple is better than complex
- Explicit is better than implicit
- Readability counts

Phase 6 Goal: Remove feature flag complexity, add production monitoring
"""

import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ExecutorMonitoring:
    """
    Monitors V2 executor performance and reliability.

    Following Kent Beck's TDD principles:
    - Simple interface for easy testing
    - Clear metrics for debugging
    - Thread-safe for production use
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.metrics = {
            "executions": 0,
            "successes": 0,
            "failures": 0,
            "total_duration": 0.0,
            "average_duration": 0.0,
            "last_execution_time": None,
            "last_error": None,
        }

    @contextmanager
    def execution_timer(self, execution_id: str):
        """
        Context manager for timing executions.

        Following the Zen of Python: "Beautiful is better than ugly"
        Clean context manager interface for monitoring.
        """
        start_time = time.time()
        logger.info(f"📊 Starting execution monitoring: {execution_id}")

        try:
            yield
            # Success path
            duration = time.time() - start_time
            self._record_success(duration, execution_id)
            logger.info(f"✅ Execution completed: {execution_id} ({duration:.2f}s)")

        except Exception as e:
            # Failure path
            duration = time.time() - start_time
            self._record_failure(duration, execution_id, str(e))
            logger.error(f"❌ Execution failed: {execution_id} ({duration:.2f}s) - {e}")
            raise

    def _record_success(self, duration: float, execution_id: str):
        """Record successful execution (thread-safe)."""
        with self._lock:
            self.metrics["executions"] += 1
            self.metrics["successes"] += 1
            self.metrics["total_duration"] += duration
            self.metrics["average_duration"] = (
                self.metrics["total_duration"] / self.metrics["executions"]
            )
            self.metrics["last_execution_time"] = time.time()
            self.metrics["last_error"] = None

    def _record_failure(self, duration: float, execution_id: str, error: str):
        """Record failed execution (thread-safe)."""
        with self._lock:
            self.metrics["executions"] += 1
            self.metrics["failures"] += 1
            self.metrics["total_duration"] += duration
            self.metrics["average_duration"] = (
                self.metrics["total_duration"] / self.metrics["executions"]
            )
            self.metrics["last_execution_time"] = time.time()
            self.metrics["last_error"] = error

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics (thread-safe copy)."""
        with self._lock:
            return self.metrics.copy()

    def get_success_rate(self) -> float:
        """Calculate success rate percentage."""
        with self._lock:
            if self.metrics["executions"] == 0:
                return 0.0
            return (self.metrics["successes"] / self.metrics["executions"]) * 100.0

    def reset_metrics(self):
        """Reset all metrics (useful for testing)."""
        with self._lock:
            self.metrics = {
                "executions": 0,
                "successes": 0,
                "failures": 0,
                "total_duration": 0.0,
                "average_duration": 0.0,
                "last_execution_time": None,
                "last_error": None,
            }
            logger.info("📊 Metrics reset")


# Global monitoring instance (singleton pattern)
_global_monitor: Optional[ExecutorMonitoring] = None
_monitor_lock = threading.RLock()


def get_monitor() -> ExecutorMonitoring:
    """
    Get global monitor instance (thread-safe singleton).

    Following Robert Martin's Dependency Inversion Principle:
    - Depend on abstractions, not concretions
    - Single instance for consistent metrics
    """
    global _global_monitor

    with _monitor_lock:
        if _global_monitor is None:
            _global_monitor = ExecutorMonitoring()
            logger.info("📊 Initialized global executor monitoring")
        return _global_monitor


def reset_global_monitor():
    """Reset global monitor (useful for testing)."""
    with _monitor_lock:
        if _global_monitor is not None:
            _global_monitor.reset_metrics()
        logger.debug("📊 Global monitor reset")


# Export monitoring interface
__all__ = ["ExecutorMonitoring", "get_monitor", "reset_global_monitor"]
