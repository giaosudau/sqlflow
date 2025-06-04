"""
Structured logging and distributed tracing for SQLFlow transform operations.

This module provides enhanced logging capabilities including:
- Structured logging with correlation IDs
- Distributed tracing for complex transform pipelines
- Performance tracing with detailed operation breakdowns
- Automatic PII detection and redaction
- Integration with observability platforms
- Log aggregation and searchability support
"""

import json
import re
import threading
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

# Get the base logger for structured logging
base_logger = get_logger(__name__)


class LogLevel(Enum):
    """Log levels for structured logging."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class SpanStatus(Enum):
    """Status values for trace spans."""

    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class LogEntry:
    """Structured log entry with correlation tracking."""

    timestamp: datetime
    level: LogLevel
    message: str
    correlation_id: str
    operation_type: str
    operation_id: Optional[str] = None
    span_id: Optional[str] = None
    trace_id: Optional[str] = None
    structured_data: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    sanitized: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "correlation_id": self.correlation_id,
            "operation_type": self.operation_type,
            "operation_id": self.operation_id,
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "structured_data": self.structured_data,
            "tags": self.tags,
            "sanitized": self.sanitized,
        }


@dataclass
class TraceSpan:
    """Distributed trace span for operation tracking."""

    span_id: str
    span_name: str
    trace_id: str
    parent_span_id: Optional[str]
    start_time: datetime
    end_time: Optional[datetime] = None
    status: SpanStatus = SpanStatus.OK
    operation_type: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)

    @property
    def duration_ms(self) -> Optional[int]:
        """Get span duration in milliseconds."""
        if self.end_time:
            return int((self.end_time - self.start_time).total_seconds() * 1000)
        return None

    @property
    def is_finished(self) -> bool:
        """Check if span is finished."""
        return self.end_time is not None

    def add_event(
        self, event_name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add event to the span."""
        event = {
            "timestamp": datetime.now().isoformat(),
            "name": event_name,
            "attributes": attributes or {},
        }
        self.events.append(event)

    def set_attribute(self, key: str, value: Any) -> None:
        """Set span attribute."""
        self.attributes[key] = value

    def set_tag(self, key: str, value: str) -> None:
        """Set span tag."""
        self.tags[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """Convert span to dictionary for serialization."""
        return {
            "span_id": self.span_id,
            "span_name": self.span_name,
            "trace_id": self.trace_id,
            "parent_span_id": self.parent_span_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "status": self.status.value,
            "operation_type": self.operation_type,
            "attributes": self.attributes,
            "events": self.events,
            "tags": self.tags,
        }


class PIIDetector:
    """Automatic PII detection and redaction for log sanitization."""

    def __init__(self):
        """Initialize PII detector with common patterns."""
        # Common PII patterns
        self.patterns = {
            "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
            "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
            "phone": re.compile(r"\b\d{3}-\d{3}-\d{4}\b|\(\d{3}\)\s?\d{3}-\d{4}\b"),
            "credit_card": re.compile(r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b"),
            "ip_address": re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
            "password": re.compile(
                r'password["\']?\s*[:=]\s*["\']?([^"\'\s]+)', re.IGNORECASE
            ),
            "api_key": re.compile(
                r'(?:api[_-]?key|token)["\']?\s*[:=]\s*["\']?([a-zA-Z0-9_-]{20,})',
                re.IGNORECASE,
            ),
        }

        # Sensitive field names
        self.sensitive_fields = {
            "password",
            "passwd",
            "secret",
            "token",
            "key",
            "api_key",
            "private_key",
            "access_token",
            "refresh_token",
            "auth_token",
            "session_id",
            "cookie",
            "authorization",
            "credentials",
        }

    def detect_pii(self, text: str) -> List[str]:
        """Detect PII types in text.

        Args:
            text: Text to scan for PII

        Returns:
            List of detected PII types
        """
        detected = []

        for pii_type, pattern in self.patterns.items():
            if pattern.search(text):
                detected.append(pii_type)

        return detected

    def sanitize_text(self, text: str) -> str:
        """Sanitize text by redacting PII.

        Args:
            text: Text to sanitize

        Returns:
            Sanitized text with PII redacted
        """
        sanitized = text

        for pii_type, pattern in self.patterns.items():
            if pii_type == "password" or pii_type == "api_key":
                # Special handling for key-value patterns
                sanitized = pattern.sub(
                    lambda m: m.group(0).replace(m.group(1), "***REDACTED***"),
                    sanitized,
                )
            else:
                # General redaction
                sanitized = pattern.sub(f"***{pii_type.upper()}_REDACTED***", sanitized)

        return sanitized

    def sanitize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize dictionary by redacting PII.

        Args:
            data: Dictionary to sanitize

        Returns:
            Sanitized dictionary
        """
        sanitized = {}

        for key, value in data.items():
            # Check if field name is sensitive
            if any(sensitive in key.lower() for sensitive in self.sensitive_fields):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, str):
                sanitized[key] = self.sanitize_text(value)
            elif isinstance(value, dict):
                sanitized[key] = self.sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    (
                        self.sanitize_text(item)
                        if isinstance(item, str)
                        else (
                            self.sanitize_dict(item) if isinstance(item, dict) else item
                        )
                    )
                    for item in value
                ]
            else:
                sanitized[key] = value

        return sanitized


class CorrelationContext:
    """Thread-local correlation context for tracking operations."""

    def __init__(self):
        """Initialize correlation context."""
        self.local = threading.local()

    def set_correlation_id(self, correlation_id: str) -> None:
        """Set correlation ID for current thread."""
        self.local.correlation_id = correlation_id

    def get_correlation_id(self) -> Optional[str]:
        """Get correlation ID for current thread."""
        return getattr(self.local, "correlation_id", None)

    def set_trace_context(self, trace_id: str, span_id: Optional[str] = None) -> None:
        """Set trace context for current thread."""
        self.local.trace_id = trace_id
        self.local.span_id = span_id

    def get_trace_context(self) -> tuple[Optional[str], Optional[str]]:
        """Get trace context for current thread."""
        trace_id = getattr(self.local, "trace_id", None)
        span_id = getattr(self.local, "span_id", None)
        return trace_id, span_id

    def clear(self) -> None:
        """Clear all context for current thread."""
        for attr in ["correlation_id", "trace_id", "span_id"]:
            if hasattr(self.local, attr):
                delattr(self.local, attr)


class StructuredLogger:
    """Enhanced logging with correlation IDs and structured data."""

    def __init__(self, name: str, enable_pii_detection: bool = True):
        """Initialize structured logger.

        Args:
            name: Logger name
            enable_pii_detection: Whether to enable PII detection and redaction
        """
        self.name = name
        self.base_logger = get_logger(name)
        self.enable_pii_detection = enable_pii_detection
        self.pii_detector = PIIDetector() if enable_pii_detection else None
        self.correlation_context = CorrelationContext()
        self.log_entries: deque[LogEntry] = deque(maxlen=10000)  # Keep recent entries
        self.lock = threading.RLock()

        base_logger.info(
            f"StructuredLogger initialized for {name} with PII detection: {enable_pii_detection}"
        )

    def _generate_correlation_id(self) -> str:
        """Generate a new correlation ID."""
        return str(uuid.uuid4())

    def _sanitize_if_needed(
        self, message: str, structured_data: Dict[str, Any]
    ) -> tuple[str, Dict[str, Any], bool]:
        """Sanitize message and data if PII detection is enabled.

        Returns:
            Tuple of (sanitized_message, sanitized_data, was_sanitized)
        """
        if not self.enable_pii_detection or not self.pii_detector:
            return message, structured_data, False

        # Check for PII
        message_pii = self.pii_detector.detect_pii(message)
        data_str = json.dumps(structured_data, default=str)
        data_pii = self.pii_detector.detect_pii(data_str)

        has_pii = bool(message_pii or data_pii)

        if has_pii:
            sanitized_message = self.pii_detector.sanitize_text(message)
            sanitized_data = self.pii_detector.sanitize_dict(structured_data)
            return sanitized_message, sanitized_data, True

        return message, structured_data, False

    def _log_structured(
        self,
        level: LogLevel,
        message: str,
        operation_type: str,
        operation_id: Optional[str] = None,
        **structured_data,
    ) -> str:
        """Internal method for structured logging.

        Returns:
            The correlation ID used for this log entry
        """
        correlation_id = self.correlation_context.get_correlation_id()
        if not correlation_id:
            correlation_id = self._generate_correlation_id()
            self.correlation_context.set_correlation_id(correlation_id)

        trace_id, span_id = self.correlation_context.get_trace_context()

        # Sanitize if needed
        sanitized_message, sanitized_data, was_sanitized = self._sanitize_if_needed(
            message, structured_data
        )

        # Create log entry
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=sanitized_message,
            correlation_id=correlation_id,
            operation_type=operation_type,
            operation_id=operation_id,
            span_id=span_id,
            trace_id=trace_id,
            structured_data=sanitized_data,
            sanitized=was_sanitized,
        )

        # Store entry
        with self.lock:
            self.log_entries.append(log_entry)

        # Log to base logger with structured format
        log_dict = log_entry.to_dict()
        structured_msg = f"{message} | {json.dumps(log_dict, default=str)}"

        # Map to standard logging levels
        if level == LogLevel.DEBUG:
            self.base_logger.debug(structured_msg)
        elif level == LogLevel.INFO:
            self.base_logger.info(structured_msg)
        elif level == LogLevel.WARNING:
            self.base_logger.warning(structured_msg)
        elif level == LogLevel.ERROR:
            self.base_logger.error(structured_msg)
        elif level == LogLevel.CRITICAL:
            self.base_logger.critical(structured_msg)

        return correlation_id

    def log_operation_start(
        self, operation_id: str, operation_type: str, context: Dict[str, Any]
    ) -> str:
        """Log operation start with correlation ID.

        Args:
            operation_id: Unique operation identifier
            operation_type: Type of operation (APPEND, MERGE, etc.)
            context: Additional context data

        Returns:
            Correlation ID for tracking
        """
        return self._log_structured(
            LogLevel.INFO,
            f"Operation {operation_type} started",
            operation_type,
            operation_id,
            **context,
        )

    def log_operation_progress(
        self,
        operation_id: str,
        operation_type: str,
        progress: float,
        details: Dict[str, Any],
    ) -> str:
        """Log operation progress with structured data.

        Args:
            operation_id: Operation identifier
            operation_type: Type of operation
            progress: Progress as float between 0.0 and 1.0
            details: Progress details

        Returns:
            Correlation ID for tracking
        """
        return self._log_structured(
            LogLevel.INFO,
            f"Operation {operation_type} progress: {progress:.1%}",
            operation_type,
            operation_id,
            progress=progress,
            **details,
        )

    def log_operation_complete(
        self, operation_id: str, operation_type: str, result: Dict[str, Any]
    ) -> str:
        """Log operation completion with results.

        Args:
            operation_id: Operation identifier
            operation_type: Type of operation
            result: Operation results

        Returns:
            Correlation ID for tracking
        """
        return self._log_structured(
            LogLevel.INFO,
            f"Operation {operation_type} completed",
            operation_type,
            operation_id,
            **result,
        )

    def log_operation_error(
        self,
        operation_id: str,
        operation_type: str,
        error: Exception,
        context: Dict[str, Any],
    ) -> str:
        """Log operation error with context.

        Args:
            operation_id: Operation identifier
            operation_type: Type of operation
            error: Exception that occurred
            context: Error context

        Returns:
            Correlation ID for tracking
        """
        return self._log_structured(
            LogLevel.ERROR,
            f"Operation {operation_type} failed: {str(error)}",
            operation_type,
            operation_id,
            error_type=type(error).__name__,
            error_message=str(error),
            **context,
        )

    def debug(self, message: str, operation_type: str = "DEBUG", **kwargs) -> str:
        """Log debug message."""
        return self._log_structured(LogLevel.DEBUG, message, operation_type, **kwargs)

    def info(self, message: str, operation_type: str = "INFO", **kwargs) -> str:
        """Log info message."""
        return self._log_structured(LogLevel.INFO, message, operation_type, **kwargs)

    def warning(self, message: str, operation_type: str = "WARNING", **kwargs) -> str:
        """Log warning message."""
        return self._log_structured(LogLevel.WARNING, message, operation_type, **kwargs)

    def error(self, message: str, operation_type: str = "ERROR", **kwargs) -> str:
        """Log error message."""
        return self._log_structured(LogLevel.ERROR, message, operation_type, **kwargs)

    def critical(self, message: str, operation_type: str = "CRITICAL", **kwargs) -> str:
        """Log critical message."""
        return self._log_structured(
            LogLevel.CRITICAL, message, operation_type, **kwargs
        )

    def get_log_entries(
        self,
        correlation_id: Optional[str] = None,
        operation_type: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> List[LogEntry]:
        """Get log entries with optional filtering.

        Args:
            correlation_id: Filter by correlation ID
            operation_type: Filter by operation type
            since: Filter by timestamp

        Returns:
            List of matching log entries
        """
        with self.lock:
            entries = list(self.log_entries)

        if correlation_id:
            entries = [e for e in entries if e.correlation_id == correlation_id]

        if operation_type:
            entries = [e for e in entries if e.operation_type == operation_type]

        if since:
            entries = [e for e in entries if e.timestamp >= since]

        return entries

    @contextmanager
    def operation_context(self, operation_id: str, operation_type: str, **context):
        """Context manager for operation logging.

        Args:
            operation_id: Operation identifier
            operation_type: Type of operation
            **context: Additional context
        """
        correlation_id = self.log_operation_start(operation_id, operation_type, context)

        try:
            yield correlation_id
            self.log_operation_complete(
                operation_id, operation_type, {"status": "success"}
            )
        except Exception as e:
            self.log_operation_error(operation_id, operation_type, e, context)
            raise


class DistributedTracer:
    """Distributed tracing for complex transform pipelines."""

    def __init__(self, service_name: str = "sqlflow-transform"):
        """Initialize distributed tracer.

        Args:
            service_name: Name of the service for tracing
        """
        self.service_name = service_name
        self.active_spans: Dict[str, TraceSpan] = {}
        self.completed_spans: deque[TraceSpan] = deque(maxlen=10000)
        self.correlation_context = CorrelationContext()
        self.lock = threading.RLock()

        base_logger.info(f"DistributedTracer initialized for service: {service_name}")

    def _generate_span_id(self) -> str:
        """Generate a new span ID."""
        return str(uuid.uuid4())[:16]

    def _generate_trace_id(self) -> str:
        """Generate a new trace ID."""
        return str(uuid.uuid4())[:32]

    def start_span(
        self,
        span_name: str,
        operation_type: str = "",
        parent_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Start a new trace span.

        Args:
            span_name: Name of the span
            operation_type: Type of operation being traced
            parent_context: Optional parent span context

        Returns:
            Span ID for the new span
        """
        span_id = self._generate_span_id()

        # Determine trace ID and parent
        if parent_context and "trace_id" in parent_context:
            trace_id = parent_context["trace_id"]
            parent_span_id = parent_context.get("span_id")
        else:
            # Check current context
            current_trace_id, current_span_id = (
                self.correlation_context.get_trace_context()
            )
            if current_trace_id:
                trace_id = current_trace_id
                parent_span_id = current_span_id
            else:
                # Start new trace
                trace_id = self._generate_trace_id()
                parent_span_id = None

        # Create span
        span = TraceSpan(
            span_id=span_id,
            span_name=span_name,
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            start_time=datetime.now(),
            operation_type=operation_type,
        )

        # Store span
        with self.lock:
            self.active_spans[span_id] = span

        # Set as current span in context
        self.correlation_context.set_trace_context(trace_id, span_id)

        base_logger.debug(
            f"Started span: {span_name} (ID: {span_id}, Trace: {trace_id})"
        )
        return span_id

    def add_span_event(
        self, span_id: str, event_name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add event to trace span.

        Args:
            span_id: Span identifier
            event_name: Name of the event
            attributes: Optional event attributes
        """
        with self.lock:
            if span_id in self.active_spans:
                self.active_spans[span_id].add_event(event_name, attributes)
                base_logger.debug(f"Added event '{event_name}' to span {span_id}")

    def set_span_attribute(self, span_id: str, key: str, value: Any) -> None:
        """Set span attribute.

        Args:
            span_id: Span identifier
            key: Attribute key
            value: Attribute value
        """
        with self.lock:
            if span_id in self.active_spans:
                self.active_spans[span_id].set_attribute(key, value)

    def set_span_tag(self, span_id: str, key: str, value: str) -> None:
        """Set span tag.

        Args:
            span_id: Span identifier
            key: Tag key
            value: Tag value
        """
        with self.lock:
            if span_id in self.active_spans:
                self.active_spans[span_id].set_tag(key, value)

    def finish_span(
        self,
        span_id: str,
        status: SpanStatus = SpanStatus.OK,
        result: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Finish trace span with status.

        Args:
            span_id: Span identifier
            status: Span completion status
            result: Optional result data
        """
        with self.lock:
            if span_id in self.active_spans:
                span = self.active_spans.pop(span_id)
                span.end_time = datetime.now()
                span.status = status

                if result:
                    span.attributes.update(result)

                self.completed_spans.append(span)

                # Clear from context if this is current span
                current_trace_id, current_span_id = (
                    self.correlation_context.get_trace_context()
                )
                if current_span_id == span_id:
                    # Set parent as current, or clear if no parent
                    if span.parent_span_id and span.parent_span_id in self.active_spans:
                        self.correlation_context.set_trace_context(
                            span.trace_id, span.parent_span_id
                        )
                    else:
                        self.correlation_context.clear()

                base_logger.debug(
                    f"Finished span: {span.span_name} (Duration: {span.duration_ms}ms, Status: {status.value})"
                )

    def get_active_spans(self) -> List[TraceSpan]:
        """Get currently active spans."""
        with self.lock:
            return list(self.active_spans.values())

    def get_trace_spans(self, trace_id: str) -> List[TraceSpan]:
        """Get all spans for a trace.

        Args:
            trace_id: Trace identifier

        Returns:
            List of spans in the trace
        """
        with self.lock:
            # Check both active and completed spans
            trace_spans = []

            for span in self.active_spans.values():
                if span.trace_id == trace_id:
                    trace_spans.append(span)

            for span in self.completed_spans:
                if span.trace_id == trace_id:
                    trace_spans.append(span)

            # Sort by start time
            trace_spans.sort(key=lambda s: s.start_time)
            return trace_spans

    @contextmanager
    def trace_operation(
        self, operation_name: str, operation_type: str = "", **attributes
    ):
        """Context manager for tracing operations.

        Args:
            operation_name: Name of the operation
            operation_type: Type of operation
            **attributes: Additional attributes
        """
        span_id = self.start_span(operation_name, operation_type)

        # Set initial attributes
        for key, value in attributes.items():
            self.set_span_attribute(span_id, key, value)

        try:
            yield span_id
            self.finish_span(span_id, SpanStatus.OK)
        except Exception as e:
            self.set_span_attribute(span_id, "error", str(e))
            self.set_span_attribute(span_id, "error_type", type(e).__name__)
            self.finish_span(span_id, SpanStatus.ERROR)
            raise

    def export_trace(self, trace_id: str) -> Dict[str, Any]:
        """Export trace data for external systems.

        Args:
            trace_id: Trace identifier

        Returns:
            Trace data in exportable format
        """
        spans = self.get_trace_spans(trace_id)

        return {
            "trace_id": trace_id,
            "service_name": self.service_name,
            "span_count": len(spans),
            "start_time": spans[0].start_time.isoformat() if spans else None,
            "end_time": (
                spans[-1].end_time.isoformat() if spans and spans[-1].end_time else None
            ),
            "total_duration_ms": sum(s.duration_ms or 0 for s in spans),
            "spans": [span.to_dict() for span in spans],
        }


class ObservabilityManager:
    """Central manager for logging and tracing integration."""

    def __init__(
        self, service_name: str = "sqlflow-transform", enable_pii_detection: bool = True
    ):
        """Initialize observability manager.

        Args:
            service_name: Name of the service
            enable_pii_detection: Whether to enable PII detection
        """
        self.service_name = service_name
        self.enable_pii_detection = enable_pii_detection

        # Initialize components
        self.structured_logger = StructuredLogger(
            f"{service_name}.main", enable_pii_detection
        )
        self.tracer = DistributedTracer(service_name)

        # Sync correlation contexts
        self.structured_logger.correlation_context = self.tracer.correlation_context

        base_logger.info(f"ObservabilityManager initialized for {service_name}")

    def get_logger(self, name: str) -> StructuredLogger:
        """Get a structured logger for a specific component.

        Args:
            name: Logger name

        Returns:
            Structured logger instance
        """
        logger = StructuredLogger(
            f"{self.service_name}.{name}", self.enable_pii_detection
        )
        logger.correlation_context = self.tracer.correlation_context
        return logger

    @contextmanager
    def operation_context(self, operation_name: str, operation_type: str, **context):
        """Combined logging and tracing context for operations.

        Args:
            operation_name: Name of the operation
            operation_type: Type of operation
            **context: Additional context
        """
        operation_id = str(uuid.uuid4())

        # Start tracing
        with self.tracer.trace_operation(
            operation_name, operation_type, **context
        ) as span_id:
            # Start logging with correlation
            with self.structured_logger.operation_context(
                operation_id, operation_type, span_id=span_id, **context
            ) as correlation_id:
                yield {
                    "operation_id": operation_id,
                    "correlation_id": correlation_id,
                    "span_id": span_id,
                    "trace_id": self.tracer.correlation_context.get_trace_context()[0],
                }

    def export_observability_data(
        self,
        trace_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Export combined logging and tracing data.

        Args:
            trace_id: Optional trace ID filter
            correlation_id: Optional correlation ID filter
            since: Optional time filter

        Returns:
            Combined observability data
        """
        data = {
            "service_name": self.service_name,
            "export_timestamp": datetime.now().isoformat(),
            "logs": [],
            "traces": [],
        }

        # Export logs
        log_entries = self.structured_logger.get_log_entries(
            correlation_id=correlation_id, since=since
        )
        data["logs"] = [entry.to_dict() for entry in log_entries]

        # Export traces
        if trace_id:
            trace_data = self.tracer.export_trace(trace_id)
            data["traces"] = [trace_data]
        else:
            # Export recent traces (simplified)
            recent_traces = set()
            for span in list(self.tracer.completed_spans):
                if since is None or span.start_time >= since:
                    recent_traces.add(span.trace_id)

            for tid in recent_traces:
                trace_data = self.tracer.export_trace(tid)
                data["traces"].append(trace_data)

        return data
