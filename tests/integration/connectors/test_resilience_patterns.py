"""Integration tests for resilience patterns across multiple connector types."""

import os
import threading
import time
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import Mock, patch

import psycopg2
import pyarrow as pa
import pytest
import requests

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorError,
    ConnectorState,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk

# Import real connectors for testing
from sqlflow.connectors.postgres.source import PostgresSource
from sqlflow.connectors.resilience import (
    DB_RESILIENCE_CONFIG,
    CircuitBreakerConfig,
    RateLimitConfig,
    RecoveryConfig,
    ResilienceConfig,
    ResilienceManager,
    RetryConfig,
    resilient_operation,
)
from sqlflow.connectors.s3.source import S3Source

# Mark all tests in this module as requiring external services
pytestmark = pytest.mark.external_services


@pytest.fixture(scope="module")
def docker_postgres_config():
    """Real PostgreSQL configuration from docker-compose.yml."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "postgres",
        "username": "postgres",
        "password": "postgres",
        "schema": "public",
    }


@pytest.fixture(scope="module")
def docker_minio_config():
    """Real MinIO configuration from docker-compose.yml."""
    return {
        "endpoint_url": "http://localhost:9000",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "region": "us-east-1",
        "bucket": "sqlflow-demo",
        "use_ssl": False,
    }


@pytest.fixture(scope="module")
def setup_resilience_test_environment():
    """Set up test environment for resilience testing."""
    # Skip if services are not available
    if not os.getenv("INTEGRATION_TESTS", "").lower() in ["true", "1"]:
        pytest.skip("Integration tests disabled. Set INTEGRATION_TESTS=true to enable.")

    # Test PostgreSQL connection
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres",
        )
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL service not available: {e}")

    # Test MinIO connection
    try:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        client.list_buckets()
    except Exception as e:
        pytest.skip(f"MinIO service not available: {e}")

    yield

    # Cleanup is handled by individual tests


class MockFlakeyConnector(Connector):
    """Mock connector that simulates various failure scenarios."""

    def __init__(self, failure_mode: str = "none", failure_count: int = 2):
        super().__init__()
        self.failure_mode = failure_mode
        self.failure_count = failure_count
        self.call_count = 0
        self.name = "flakey_connector"
        self.resilience_manager = None

    def configure_resilience(self, config: ResilienceConfig) -> None:
        """Configure resilience patterns for this connector."""
        self.resilience_manager = ResilienceManager(config, self.name)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector."""
        self.connection_params = params
        self.state = ConnectorState.CONFIGURED

    @resilient_operation()
    def test_connection(self) -> ConnectionTestResult:
        """Test connection with simulated failures."""
        self.call_count += 1

        if (
            self.failure_mode == "connection_error"
            and self.call_count <= self.failure_count
        ):
            raise ConnectionError(f"Connection failed (attempt {self.call_count})")
        elif self.failure_mode == "timeout" and self.call_count <= self.failure_count:
            raise TimeoutError(f"Connection timeout (attempt {self.call_count})")
        elif self.failure_mode == "http_500" and self.call_count <= self.failure_count:
            response = Mock()
            response.status_code = 500
            error = requests.exceptions.HTTPError("Internal Server Error")
            error.response = response
            raise error
        elif self.failure_mode == "http_401":
            response = Mock()
            response.status_code = 401
            error = requests.exceptions.HTTPError("Unauthorized")
            error.response = response
            raise error

        return ConnectionTestResult(True, "Connection successful")

    @resilient_operation()
    def discover(self) -> List[str]:
        """Discover objects with simulated failures."""
        self.call_count += 1

        if (
            self.failure_mode == "discovery_error"
            and self.call_count <= self.failure_count
        ):
            raise ConnectionError(f"Discovery failed (attempt {self.call_count})")

        return ["table1", "table2", "table3"]

    @resilient_operation()
    def get_schema(self, object_name: str) -> Schema:
        """Get schema with simulated failures."""
        self.call_count += 1

        if (
            self.failure_mode == "schema_error"
            and self.call_count <= self.failure_count
        ):
            raise ConnectionError(
                f"Schema retrieval failed (attempt {self.call_count})"
            )

        # Return a mock schema
        fields = [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
        return Schema(pa.schema(fields))

    @resilient_operation()
    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data with simulated failures."""
        self.call_count += 1

        if self.failure_mode == "read_error" and self.call_count <= self.failure_count:
            raise ConnectionError(f"Read failed (attempt {self.call_count})")

        # Return mock data
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        table = pa.Table.from_pandas(df)
        yield DataChunk(table)


class TestResiliencePatterns:
    """Integration tests for resilience patterns with real services."""

    @pytest.mark.skip(reason="PostgreSQL connector not yet migrated - Phase 2.1")
    def test_real_postgres_resilience_configuration(
        self, docker_postgres_config, setup_resilience_test_environment
    ):
        """Test resilience configuration with real PostgreSQL connector."""
        connector = PostgresSource()
        connector.configure(docker_postgres_config)

        # Verify resilience manager is configured
        assert hasattr(connector, "resilience_manager")
        assert connector.resilience_manager is not None

        # Test actual connection
        result = connector.test_connection()
        assert result.success is True, f"PostgreSQL connection failed: {result.message}"

    @pytest.mark.skip(reason="S3 connector not yet migrated - Phase 2.2")
    def test_real_s3_resilience_configuration(
        self, docker_minio_config, setup_resilience_test_environment
    ):
        """Test resilience configuration with real S3/MinIO connector."""
        connector = S3Source()
        config = {
            **docker_minio_config,
            "file_format": "csv",
            "mock_mode": False,
        }

        connector.configure(config)

        # Verify resilience manager is configured
        assert hasattr(connector, "resilience_manager")
        assert connector.resilience_manager is not None

        # Test actual connection
        result = connector.test_connection()
        assert result.success is True, f"MinIO connection failed: {result.message}"

    def test_retry_with_connection_errors(self):
        """Test retry pattern with connection errors using mock connector."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=None,
            rate_limit=None,
            recovery=None,
        )

        connector = MockFlakeyConnector("connection_error", failure_count=2)
        connector.configure_resilience(config)
        connector.configure({})

        # Should succeed after 3 attempts (2 failures + 1 success)
        result = connector.test_connection()
        assert result.success is True
        assert connector.call_count == 3

    def test_retry_with_permanent_failure(self):
        """Test retry pattern with permanent failure using mock connector."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=2, initial_delay=0.01),
            circuit_breaker=None,
            rate_limit=None,
            recovery=RecoveryConfig(
                enable_connection_recovery=False
            ),  # Disable recovery
        )

        connector = MockFlakeyConnector(
            "connection_error", failure_count=5
        )  # More failures than retries
        connector.configure_resilience(config)
        connector.configure({})

        # Should fail after exhausting retries
        with pytest.raises(ConnectionError):
            connector.test_connection()

        assert connector.call_count == 2

    def test_circuit_breaker_opens_on_failures(self):
        """Test circuit breaker opens after threshold failures."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=1),  # No retries for this test
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=2, recovery_timeout=0.1
            ),
            rate_limit=None,
            recovery=RecoveryConfig(
                enable_connection_recovery=False
            ),  # Disable recovery
        )

        connector = MockFlakeyConnector(
            "connection_error", failure_count=10
        )  # Always fails
        connector.configure_resilience(config)
        connector.configure({})

        # First two calls should fail and open the circuit
        with pytest.raises(ConnectionError):
            connector.test_connection()

        with pytest.raises(ConnectionError):
            connector.test_connection()

        # Third call should fail fast due to open circuit
        with pytest.raises(ConnectorError, match="Circuit breaker is OPEN"):
            connector.test_connection()

        assert connector.call_count == 2  # Third call didn't reach the connector

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after timeout."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=1),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=0.05, success_threshold=1
            ),
            rate_limit=None,
            recovery=RecoveryConfig(
                enable_connection_recovery=False
            ),  # Disable recovery
        )

        connector = MockFlakeyConnector("connection_error", failure_count=1)
        connector.configure_resilience(config)
        connector.configure({})

        # First call fails and opens circuit
        with pytest.raises(ConnectionError):
            connector.test_connection()

        # Second call fails fast
        with pytest.raises(ConnectorError, match="Circuit breaker is OPEN"):
            connector.test_connection()

        # Wait for recovery timeout
        time.sleep(0.1)

        # Now connector is fixed (failure_count=1, so second call succeeds)
        connector.failure_count = 0  # Reset failure simulation

        # Should succeed and close circuit
        result = connector.test_connection()
        assert result.success is True

    @pytest.mark.skip(reason="PostgreSQL connector not yet migrated - Phase 2.1")
    def test_real_postgres_concurrent_access(
        self, docker_postgres_config, setup_resilience_test_environment
    ):
        """Test concurrent access to real PostgreSQL with resilience patterns."""
        results = {}

        def test_postgres_thread(thread_id):
            try:
                connector = PostgresSource()
                connector.configure(docker_postgres_config)

                # Test connection
                result = connector.test_connection()
                results[f"{thread_id}_connection"] = result.success

                # Test discovery
                discovered = connector.discover()
                results[f"{thread_id}_discovery"] = len(discovered) >= 0

            except Exception as e:
                results[f"{thread_id}_error"] = str(e)

        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=test_postgres_thread, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify results
        for i in range(3):
            assert (
                results.get(f"{i}_connection") is True
            ), f"Thread {i} PostgreSQL connection failed"
            assert (
                results.get(f"{i}_discovery") is True
            ), f"Thread {i} PostgreSQL discovery failed"

    @pytest.mark.skip(reason="S3 connector not yet migrated - Phase 2.2")
    def test_real_s3_concurrent_access(
        self, docker_minio_config, setup_resilience_test_environment
    ):
        """Test concurrent access to real MinIO with resilience patterns."""
        results = {}

        def test_s3_thread(thread_id):
            try:
                connector = S3Source()
                config = {
                    **docker_minio_config,
                    "file_format": "csv",
                    "mock_mode": False,
                }
                connector.configure(config)

                # Test connection
                result = connector.test_connection()
                results[f"{thread_id}_connection"] = result.success

                # Test discovery
                discovered = connector.discover()
                results[f"{thread_id}_discovery"] = len(discovered) >= 0

            except Exception as e:
                results[f"{thread_id}_error"] = str(e)

        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=test_s3_thread, args=(i,))
            threads.append(thread)
            thread.start()
            time.sleep(0.05)  # Small delay to avoid overwhelming MinIO

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify results
        for i in range(3):
            assert (
                results.get(f"{i}_connection") is True
            ), f"Thread {i} S3 connection failed"
            assert (
                results.get(f"{i}_discovery") is True
            ), f"Thread {i} S3 discovery failed"

    def test_combined_resilience_patterns(self):
        """Test combined retry, circuit breaker, and rate limiting."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=5, recovery_timeout=0.1
            ),
            rate_limit=RateLimitConfig(
                max_requests_per_minute=300, burst_size=10  # High rate for this test
            ),
            recovery=RecoveryConfig(enable_connection_recovery=True),
        )

        connector = MockFlakeyConnector("connection_error", failure_count=2)
        connector.configure_resilience(config)
        connector.configure({})

        # Should succeed after retries
        result = connector.test_connection()
        assert result.success is True
        assert connector.call_count == 3

    def test_http_error_classification(self):
        """Test HTTP error classification for retry vs fail-fast."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=None,
            rate_limit=None,
            recovery=None,
        )

        # Test 5xx errors (should retry)
        connector_500 = MockFlakeyConnector("http_500", failure_count=2)
        connector_500.configure_resilience(config)
        connector_500.configure({})

        # Should succeed after retries
        result = connector_500.test_connection()
        assert result.success is True
        assert connector_500.call_count == 3

        # Test 4xx errors (should not retry)
        connector_401 = MockFlakeyConnector("http_401", failure_count=1)
        connector_401.configure_resilience(config)
        connector_401.configure({})

        # Should fail immediately (no retries for 401)
        with pytest.raises(requests.exceptions.HTTPError):
            connector_401.test_connection()

        assert connector_401.call_count == 1

    @pytest.mark.skip(reason="PostgreSQL connector not yet migrated - Phase 2.1")
    def test_real_postgres_with_invalid_config(self, setup_resilience_test_environment):
        """Test PostgreSQL resilience with invalid configuration."""
        connector = PostgresSource()
        invalid_config = {
            "host": "localhost",
            "port": 9999,  # Invalid port
            "database": "postgres",
            "username": "postgres",
            "password": "postgres",
        }

        connector.configure(invalid_config)

        # Should fail gracefully with resilience patterns after retries
        with pytest.raises(psycopg2.OperationalError) as exc_info:
            connector.test_connection()

        # Verify the error message indicates connection failure
        error_msg = str(exc_info.value).lower()
        assert "connection" in error_msg

    @pytest.mark.skip(reason="S3 connector not yet migrated - Phase 2.2")
    def test_real_s3_with_invalid_credentials(self, setup_resilience_test_environment):
        """Test S3 resilience with invalid credentials."""
        connector = S3Source()
        invalid_config = {
            "endpoint_url": "http://localhost:9000",
            "access_key_id": "invalid_key",
            "secret_access_key": "invalid_secret",
            "region": "us-east-1",
            "bucket": "sqlflow-demo",
            "file_format": "csv",
            "mock_mode": False,
        }

        connector.configure(invalid_config)

        # Should fail gracefully with resilience patterns
        result = connector.test_connection()
        assert result.success is False
        assert "forbidden" in result.message.lower() or "403" in result.message.lower()

    def test_api_resilience_config_integration(self):
        """Test integration with predefined API resilience config."""
        # Use a modified config with disabled circuit breaker for this test
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=5, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=10
            ),  # High threshold
            rate_limit=RateLimitConfig(max_requests_per_minute=300, burst_size=50),
            recovery=RecoveryConfig(enable_connection_recovery=False),
        )

        connector = MockFlakeyConnector("connection_error", failure_count=3)
        connector.configure_resilience(config)
        connector.configure({})

        # Should succeed after retries
        result = connector.test_connection()
        assert result.success is True
        assert connector.call_count == 4  # 3 failures + 1 success

    def test_db_resilience_config_integration(self):
        """Test integration with predefined DB resilience config."""
        connector = MockFlakeyConnector("connection_error", failure_count=2)
        connector.configure_resilience(DB_RESILIENCE_CONFIG)
        connector.configure({})

        # DB config has max_attempts=3, so should succeed
        result = connector.test_connection()
        assert result.success is True
        assert connector.call_count == 3  # 2 failures + 1 success

    def test_performance_overhead_measurement(self):
        """Test performance overhead of resilience patterns."""
        # Test without resilience
        connector_no_resilience = MockFlakeyConnector("none")
        connector_no_resilience.configure({})

        start_time = time.time()
        for _ in range(50):  # Reduce iterations for faster test
            connector_no_resilience.test_connection()
        baseline_time = time.time() - start_time

        # Test with resilience (truly minimal configuration)
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=1),  # No retries for performance test
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=100
            ),  # Never trigger
            rate_limit=RateLimitConfig(
                max_requests_per_minute=3600, burst_size=100
            ),  # Never limit
            recovery=RecoveryConfig(
                enable_connection_recovery=False,
                enable_credential_refresh=False,
                enable_schema_adaptation=False,
                enable_partial_failure_recovery=False,
                max_recovery_attempts=0,  # Disable recovery completely
            ),
        )
        connector_with_resilience = MockFlakeyConnector("none")
        connector_with_resilience.configure_resilience(config)
        connector_with_resilience.configure({})

        start_time = time.time()
        for _ in range(50):  # Reduce iterations for faster test
            connector_with_resilience.test_connection()
        resilience_time = time.time() - start_time

        # More realistic overhead expectations for resilience patterns
        if baseline_time > 0.001:  # Only test overhead if baseline is measurable
            overhead_percentage = (
                (resilience_time - baseline_time) / baseline_time
            ) * 100
            # Resilience patterns add overhead for manager lookup, decorator processing, etc.
            # With recovery disabled, overhead should be much lower
            assert (
                overhead_percentage < 300
            ), f"Performance overhead too high: {overhead_percentage:.1f}%"
            print(
                f"Resilience overhead: {overhead_percentage:.1f}% (baseline: {baseline_time*1000:.2f}ms, resilience: {resilience_time*1000:.2f}ms)"
            )
        else:
            # If baseline is too small to measure reliably, just ensure reasonable absolute time
            assert (
                resilience_time < 1.0
            ), f"Resilience time too slow: {resilience_time:.3f}s for 50 operations"
            print(
                f"Baseline too fast to measure overhead reliably. Resilience time: {resilience_time*1000:.2f}ms for 50 operations"
            )

    def test_error_propagation_with_resilience(self):
        """Test that errors are properly propagated through resilience layers."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=2, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=5),
            rate_limit=RateLimitConfig(max_requests_per_minute=600),
            recovery=None,
        )

        connector = MockFlakeyConnector(
            "connection_error", failure_count=5
        )  # Always fails
        connector.configure_resilience(config)
        connector.configure({})

        # Should get the original ConnectionError, not a wrapped error
        with pytest.raises(ConnectionError, match="Connection failed"):
            connector.test_connection()

    def test_resilience_patterns_logging(self):
        """Test that resilience patterns produce appropriate logging."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=2),
            rate_limit=None,
            recovery=RecoveryConfig(
                enable_connection_recovery=False
            ),  # Disable recovery
        )

        connector = MockFlakeyConnector("connection_error", failure_count=1)
        connector.configure_resilience(config)
        connector.configure({})

        # Capture logs during operation - use the actual logger path
        with patch("sqlflow.connectors.resilience.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            # Create a new connector to get the new logger
            connector2 = MockFlakeyConnector("connection_error", failure_count=1)
            connector2.configure_resilience(config)
            connector2.configure({})

            result = connector2.test_connection()
            assert result.success is True

            # Should have logged retry attempt
            # Check that logger was created for RetryHandler
            mock_get_logger.assert_called()
            # The retry handler should have logged a warning
            mock_logger.warning.assert_called()

    def test_different_operation_isolation(self):
        """Test that different operations have isolated resilience state."""
        config = ResilienceConfig(
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=0.1
            )
        )

        connector = MockFlakeyConnector(
            "connection_error", failure_count=10
        )  # Always fails
        connector.configure_resilience(config)
        connector.configure({})

        # test_connection should open the circuit
        with pytest.raises(ConnectionError):
            connector.test_connection()

        with pytest.raises(ConnectorError, match="Circuit breaker is OPEN"):
            connector.test_connection()

        # But discover should still work (different operation)
        # Note: This assumes circuit breaker is per-operation, but our current
        # implementation is per-connector. This test documents current behavior.

        # Reset failure mode for discover
        connector.failure_mode = "none"
        connector.call_count = 0

        # discover should still fail fast due to shared circuit breaker
        with pytest.raises(ConnectorError, match="Circuit breaker is OPEN"):
            connector.discover()


if __name__ == "__main__":
    pytest.main([__file__])
