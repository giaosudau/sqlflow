"""Enhanced logging infrastructure for SQLFlow debugging.

This module provides structured logging with performance metrics,
operation context, and debug mode support.
"""

import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class DebugLogger:
    """Enhanced logger for debugging SQLFlow operations."""

    def __init__(self, name: str, debug_mode: bool = False):
        """Initialize debug logger.

        Args:
            name: Logger name
            debug_mode: Whether to enable detailed debug logging
        """
        self.name = name
        self.debug_mode = debug_mode
        self.logger = get_logger(f"debug.{name}")
        self.operation_stack = []
        self.metrics = {}

    def set_debug_mode(self, enabled: bool) -> None:
        """Enable or disable debug mode.

        Args:
            enabled: Whether to enable debug mode
        """
        self.debug_mode = enabled
        self.logger.info(
            f"Debug mode {'enabled' if enabled else 'disabled'} for {self.name}"
        )

    @contextmanager
    def operation(self, operation: str, **context):
        """Context manager for tracking operations.

        Args:
            operation: Operation name
            **context: Additional context data
        """
        operation_id = f"{operation}_{int(time.time() * 1000)}"
        start_time = time.time()

        # Add to operation stack
        self.operation_stack.append(
            {
                "id": operation_id,
                "operation": operation,
                "start_time": start_time,
                "context": context,
            }
        )

        if self.debug_mode:
            self.logger.debug(
                f"Starting operation: {operation} (ID: {operation_id})",
                extra={
                    "operation": operation,
                    "operation_id": operation_id,
                    "context": context,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

        try:
            yield operation_id
        except Exception as e:
            self.log_error(
                f"Operation {operation} failed", exception=e, operation_id=operation_id
            )
            raise
        finally:
            # Remove from stack and calculate duration
            if self.operation_stack and self.operation_stack[-1]["id"] == operation_id:
                operation_info = self.operation_stack.pop()
                duration = time.time() - operation_info["start_time"]

                # Record metrics
                if operation not in self.metrics:
                    self.metrics[operation] = {
                        "count": 0,
                        "total_duration": 0,
                        "avg_duration": 0,
                        "min_duration": float("inf"),
                        "max_duration": 0,
                    }

                metrics = self.metrics[operation]
                metrics["count"] += 1
                metrics["total_duration"] += duration
                metrics["avg_duration"] = metrics["total_duration"] / metrics["count"]
                metrics["min_duration"] = min(metrics["min_duration"], duration)
                metrics["max_duration"] = max(metrics["max_duration"], duration)

                if self.debug_mode:
                    self.logger.debug(
                        f"Completed operation: {operation} (ID: {operation_id}) in {duration:.3f}s",
                        extra={
                            "operation": operation,
                            "operation_id": operation_id,
                            "duration": duration,
                            "context": operation_info["context"],
                            "timestamp": datetime.utcnow().isoformat(),
                        },
                    )

    def log_structured(self, level: str, message: str, **data) -> None:
        """Log structured data.

        Args:
            level: Log level (debug, info, warning, error)
            message: Log message
            **data: Structured data to include
        """
        extra_data = {
            "structured_data": data,
            "timestamp": datetime.utcnow().isoformat(),
            "logger_name": self.name,
        }

        # Add current operation context if available
        if self.operation_stack:
            current_operation = self.operation_stack[-1]
            extra_data["current_operation"] = {
                "id": current_operation["id"],
                "operation": current_operation["operation"],
                "duration": time.time() - current_operation["start_time"],
            }

        if level == "debug" and self.debug_mode:
            self.logger.debug(message, extra=extra_data)
        elif level == "info":
            self.logger.info(message, extra=extra_data)
        elif level == "warning":
            self.logger.warning(message, extra=extra_data)
        elif level == "error":
            self.logger.error(message, extra=extra_data)

    def log_performance(self, operation: str, duration: float, **metrics) -> None:
        """Log performance metrics.

        Args:
            operation: Operation name
            duration: Operation duration in seconds
            **metrics: Additional performance metrics
        """
        self.log_structured(
            "info",
            f"Performance: {operation}",
            operation=operation,
            duration=duration,
            metrics=metrics,
        )

    def log_error(
        self, message: str, exception: Optional[Exception] = None, **context
    ) -> None:
        """Log error with context.

        Args:
            message: Error message
            exception: Optional exception object
            **context: Additional error context
        """
        error_data = context.copy()
        if exception:
            error_data["exception_type"] = type(exception).__name__
            error_data["exception_message"] = str(exception)

        self.log_structured("error", message, **error_data)

    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics.

        Returns:
            Dictionary of operation metrics
        """
        return self.metrics.copy()

    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.metrics.clear()
        if self.debug_mode:
            self.logger.debug("Performance metrics reset")

    def log_watermark_operation(
        self,
        operation: str,
        pipeline: str,
        source: str,
        target: str,
        column: str,
        value: Any = None,
    ) -> None:
        """Log watermark operations for debugging.

        Args:
            operation: Type of watermark operation
            pipeline: Pipeline name
            source: Source name
            target: Target table name
            column: Cursor column name
            value: Watermark value (for updates)
        """
        self.log_structured(
            "info",
            f"Watermark {operation}",
            operation=operation,
            pipeline=pipeline,
            source=source,
            target=target,
            column=column,
            value=value,
        )

    def log_connector_operation(
        self, operation: str, connector_name: str, connector_type: str, **details
    ) -> None:
        """Log connector operations for debugging.

        Args:
            operation: Type of connector operation
            connector_name: Name of the connector
            connector_type: Type of the connector
            **details: Additional operation details
        """
        self.log_structured(
            "info",
            f"Connector {operation}",
            operation=operation,
            connector_name=connector_name,
            connector_type=connector_type,
            **details,
        )
