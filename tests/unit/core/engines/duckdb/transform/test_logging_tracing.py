"""
Unit tests for the structured logging and distributed tracing framework.

Tests the logging and tracing components including StructuredLogger,
DistributedTracer, PIIDetector, and ObservabilityManager.
"""

import unittest

from sqlflow.core.engines.duckdb.transform.logging_tracing import (
    DistributedTracer,
    LogLevel,
    PIIDetector,
    StructuredLogger,
)


class TestPIIDetector(unittest.TestCase):
    """Test PII detection and redaction functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.detector = PIIDetector()

    def test_detect_email(self):
        """Test email detection."""
        text = "Contact user@example.com for support"
        detected = self.detector.detect_pii(text)
        self.assertIn("email", detected)

    def test_sanitize_text(self):
        """Test text sanitization."""
        text = "Email user@test.com and call 555-123-4567"
        sanitized = self.detector.sanitize_text(text)

        self.assertNotIn("user@test.com", sanitized)
        self.assertNotIn("555-123-4567", sanitized)
        self.assertIn("***EMAIL_REDACTED***", sanitized)
        self.assertIn("***PHONE_REDACTED***", sanitized)


class TestStructuredLogger(unittest.TestCase):
    """Test structured logging functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = StructuredLogger("test.logger", enable_pii_detection=False)

    def test_basic_logging(self):
        """Test basic structured logging."""
        correlation_id = self.logger.info(
            "Test message", "TEST_OP", test_data="value", number=42
        )

        # Verify correlation ID was returned
        self.assertIsNotNone(correlation_id)

        # Verify log entry was stored
        entries = self.logger.get_log_entries(correlation_id=correlation_id)
        self.assertEqual(len(entries), 1)

        entry = entries[0]
        self.assertEqual(entry.message, "Test message")
        self.assertEqual(entry.level, LogLevel.INFO)


class TestDistributedTracer(unittest.TestCase):
    """Test distributed tracing functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.tracer = DistributedTracer("test-service")

    def test_span_creation(self):
        """Test basic span creation and management."""
        span_id = self.tracer.start_span("test-operation", "TEST_OP")

        # Verify span was created
        self.assertIsNotNone(span_id)
        active_spans = self.tracer.get_active_spans()
        self.assertEqual(len(active_spans), 1)


if __name__ == "__main__":
    unittest.main()
