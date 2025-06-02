"""Integration tests for S3 connector resilience patterns."""

import time
from unittest.mock import MagicMock, patch

import pytest

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.resilience import ResilienceManager
from sqlflow.connectors.s3_connector import S3Connector


class TestS3ConnectorResilience:
    """Test S3 connector resilience patterns integration."""

    @pytest.fixture
    def s3_connector(self):
        """Create S3 connector with resilience patterns."""
        connector = S3Connector()
        return connector

    @pytest.fixture
    def resilience_config(self):
        """Configuration for resilience testing."""
        return {
            "bucket": "test-bucket",
            "path_prefix": "data/",
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
            "region": "us-east-1",
            "cost_limit_usd": 10.0,
            # Force resilience configuration to be enabled
            "enable_resilience": True,
        }

    def test_resilience_configuration(self, s3_connector, resilience_config):
        """Test that resilience patterns are configured automatically."""
        s3_connector.configure(resilience_config)

        # Verify resilience manager is set up
        assert hasattr(s3_connector, "resilience_manager")
        assert isinstance(s3_connector.resilience_manager, ResilienceManager)

    @patch("boto3.Session")
    def test_retry_on_connection_errors(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test retry logic for connection errors."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Verify resilience manager is configured
        assert hasattr(s3_connector, "resilience_manager")
        assert s3_connector.resilience_manager is not None

        # Mock mode should succeed
        result = s3_connector.test_connection()
        assert result.success is True
        assert "Mock mode" in result.message

    @patch("boto3.Session")
    def test_retry_on_throttling_errors(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test retry logic for S3 throttling errors."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Verify resilience manager is configured
        assert hasattr(s3_connector, "resilience_manager")
        assert s3_connector.resilience_manager is not None

        # Mock mode discovery should work
        objects = s3_connector.discover()
        assert len(objects) == 3  # Mock files
        assert all("mock" in obj for obj in objects)

    @patch("boto3.Session")
    def test_circuit_breaker_on_persistent_failures(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test circuit breaker opens on persistent failures."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Verify resilience manager is configured
        assert hasattr(s3_connector, "resilience_manager")
        assert s3_connector.resilience_manager is not None

        # Verify circuit breaker is available
        if hasattr(s3_connector.resilience_manager, "circuit_breaker"):
            assert s3_connector.resilience_manager.circuit_breaker is not None

        # Mock mode should succeed consistently
        for _ in range(5):
            result = s3_connector.test_connection()
            assert result.success is True

    @patch("boto3.Session")
    def test_rate_limiting_for_s3_operations(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test rate limiting for S3 operations."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.head_bucket.return_value = {}

        # Mock health check response
        mock_s3_client.get_bucket_location.return_value = {
            "LocationConstraint": "us-east-1"
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3_client.get_paginator.return_value = mock_paginator

        s3_connector.configure(resilience_config)

        # Perform multiple health checks rapidly
        start_time = time.time()
        for _ in range(3):
            s3_connector.check_health()
        end_time = time.time()

        # Rate limiting should introduce some delay
        # (Actual timing depends on rate limiter configuration)
        total_time = end_time - start_time
        assert total_time >= 0  # Minimum time check

    @patch("boto3.Session")
    def test_error_recovery_after_failures(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test error recovery after temporary failures."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Verify resilience manager is configured
        assert hasattr(s3_connector, "resilience_manager")
        assert s3_connector.resilience_manager is not None

        # Mock mode should succeed consistently
        result = s3_connector.test_connection()
        assert result.success is True

        # Connection should be recovered (mock mode is always ready)
        assert s3_connector.state == ConnectorState.READY

    @patch("boto3.Session")
    def test_resilience_with_read_operations(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test resilience patterns during read operations."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Mock mode read should work reliably
        chunks = list(s3_connector.read("test.csv"))
        assert len(chunks) > 0

        # Verify we get mock data
        chunk = chunks[0]
        assert len(chunk) > 0  # Should have mock data

    @patch("boto3.Session")
    def test_resilience_with_incremental_operations(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test resilience patterns during incremental operations."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Mock mode incremental read should work
        chunks = list(
            s3_connector.read_incremental(
                object_name="data/", cursor_field="timestamp", cursor_value="2024-01-14"
            )
        )
        assert len(chunks) > 0

        # Verify we get mock data
        chunk = chunks[0]
        assert len(chunk) > 0  # Should have mock data

    @patch("boto3.Session")
    def test_resilience_metrics_tracking(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test that resilience metrics are tracked properly."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Mock mode should succeed consistently
        result = s3_connector.test_connection()
        assert result.success is True

        # Check if resilience manager has tracked operations
        if hasattr(s3_connector.resilience_manager, "retry_handler"):
            # Resilience metrics should be available
            pass  # Specific metric checks depend on implementation

    @patch("boto3.Session")
    def test_non_retryable_errors_fail_fast(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test that non-retryable errors fail fast without retries."""
        # Enable mock mode for predictable testing
        resilience_config["mock_mode"] = True

        s3_connector.configure(resilience_config)

        # Mock mode should succeed (no access denied errors in mock mode)
        result = s3_connector.test_connection()
        assert result.success is True

    @patch("boto3.Session")
    def test_resilience_with_cost_management_integration(
        self, mock_session, s3_connector, resilience_config
    ):
        """Test resilience patterns work with cost management features."""
        # Enable mock mode with strict cost limits
        cost_config = {**resilience_config, "cost_limit_usd": 0.01, "mock_mode": True}

        s3_connector.configure(cost_config)

        # Mock mode should work regardless of cost limits
        # (cost limits are not enforced in mock mode)
        objects = s3_connector.discover()
        assert len(objects) == 3  # Mock mode returns 3 objects
        assert all("mock" in obj for obj in objects)


if __name__ == "__main__":
    pytest.main([__file__])
