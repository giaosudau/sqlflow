"""Tests for the V2 Observability PerformanceAlerts.

Following the TDD principle from our testing standards.
"""

import time

from sqlflow.core.executors.v2.observability.metrics import (
    AlertSeverity,
    SimpleObservabilityManager,
)


class TestPerformanceAlerts:
    """Test the PerformanceAlert generation."""

    def test_alert_generated_for_slow_step(self):
        """Test a PerformanceAlert is generated for a step exceeding the duration threshold."""
        manager = SimpleObservabilityManager(duration_threshold_ms=50)

        manager.start_step("slow_step")
        time.sleep(0.1)  # Simulate work
        manager.end_step("slow_step", success=True)

        alerts = manager.get_alerts()
        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.component == "StepExecutor"
        assert alert.alert_type == "SlowStepWarning"
        assert "exceeded duration threshold: 1" in alert.message  # Check for ~100ms
        assert alert.severity == AlertSeverity.WARNING
        assert alert.metadata["step_id"] == "slow_step"

    def test_no_alert_for_fast_step(self):
        """Test that no alert is generated for a fast step."""
        manager = SimpleObservabilityManager(duration_threshold_ms=5000)

        manager.start_step("fast_step")
        manager.end_step("fast_step", success=True)

        alerts = manager.get_alerts()
        assert len(alerts) == 0

    def test_get_alerts_returns_copy(self):
        """Test that get_alerts returns a copy, not the original list."""
        manager = SimpleObservabilityManager(duration_threshold_ms=10)
        manager.start_step("another_slow_step")
        time.sleep(0.02)
        manager.end_step("another_slow_step", success=True)

        alerts1 = manager.get_alerts()
        assert len(alerts1) == 1

        # Modify the returned list
        alerts1.clear()

        # The original list should be unaffected
        alerts2 = manager.get_alerts()
        assert len(alerts2) == 1

    def test_alerts_generated_in_metrics_report(self):
        """Test that the main metrics report includes the alert count."""
        manager = SimpleObservabilityManager(duration_threshold_ms=50)

        # A fast step (below threshold)
        manager.start_step("step1")
        manager.end_step("step1", success=True)

        # A slow step (above threshold)
        manager.start_step("step2")
        time.sleep(0.06)
        manager.end_step("step2", success=True)

        metrics = manager.get_metrics()
        assert metrics["alerts_generated"] == 1
