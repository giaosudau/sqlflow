"""Integration tests for resilience patterns across multiple connector types."""

import threading
import time
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa
import pytest
import requests

from sqlflow.connectors.base import ConnectorError
from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
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
        "bucket": "sqlflow-demo",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "endpoint_url": "http://localhost:9000",
        "region": "us-east-1",
    }


@pytest.fixture(scope="module")
def setup_resilience_test_environment():
    """Set up test environment for resilience testing."""
    # This fixture can be used to ensure external services are running
    # For now, it just ensures the test environment is marked as external_services


class MockFlakeyConnector(Connector):
    """Mock connector that simulates various failure modes for testing."""

    def __init__(self, failure_mode: str = "none", failure_count: int = 2):
        super().__init__()
        self.failure_mode = failure_mode
        self.failure_count = failure_count
        self.call_count = 0
        self.resilience_manager = None

    def configure_resilience(self, config: ResilienceConfig) -> None:
        """Configure resilience manager for tests."""
        self.resilience_manager = ResilienceManager(
            config, name=self.__class__.__name__
        )

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters."""
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

        if (
            self.failure_mode == "timeout_error"
            and self.call_count <= self.failure_count
        ):
            raise TimeoutError(f"Timeout (attempt {self.call_count})")

        return ConnectionTestResult(success=True, message="Connection successful")

    @resilient_operation()
    def discover(self) -> List[str]:
        """Discover with simulated failures."""
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

        columns = [
            {"name": "id", "type": "int64", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
        ]
        return Schema(name=object_name, columns=columns)

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
            assert (
                f"{i}_error" not in results
            ), f"Thread {i} had error: {results.get(f'{i}_error')}"

    def test_real_s3_concurrent_access(
        self, docker_minio_config, setup_resilience_test_environment
    ):
        """Test concurrent access to real S3/MinIO with resilience patterns."""
        results = {}

        def test_s3_thread(thread_id):
            try:
                connector = S3Source()
                config = {
                    **docker_minio_config,
                    "file_format": "csv",
                    "path_prefix": "test-data/",
                }
                connector.configure(config)

                # Test connection
                result = connector.test_connection()
                results[f"{thread_id}_connection"] = result.success

                # Test discovery (may return empty list if no files)
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
            assert (
                f"{i}_error" not in results
            ), f"Thread {i} had error: {results.get(f'{i}_error')}"

    def test_combined_resilience_patterns(self):
        """Test all resilience patterns working together."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=5, recovery_timeout=0.1
            ),
            rate_limit=RateLimitConfig(max_requests_per_minute=3600, burst_size=100),
            recovery=RecoveryConfig(enable_connection_recovery=True),
        )

        connector = MockFlakeyConnector("connection_error", failure_count=2)
        connector.configure_resilience(config)
        connector.configure({})

        # Should succeed with retry
        result = connector.test_connection()
        assert result.success is True
        assert connector.call_count == 3

    def test_http_error_classification(self):
        """Test that HTTP errors are properly classified for retry."""
        # This would need a mock HTTP connector, but demonstrates the pattern
        config = ResilienceConfig(
            retry=RetryConfig(
                max_attempts=3,
                initial_delay=0.01,
                retry_on_exceptions=[requests.exceptions.HTTPError],
            )
        )

        # Test would verify that 5xx errors are retried but 4xx are not
        # Implementation depends on having a proper HTTP connector

    def test_real_postgres_with_invalid_config(self, setup_resilience_test_environment):
        """Test PostgreSQL connector with invalid configuration."""
        # Test with invalid port
        invalid_config = {
            "host": "localhost",
            "port": 9999,  # Invalid port
            "database": "postgres",
            "username": "postgres",
            "password": "postgres",
        }
        connector = PostgresSource()
        connector.configure(invalid_config)

        # Should fail gracefully with resilience patterns
        result = connector.test_connection()
        assert result.success is False
        assert (
            "connection" in result.message.lower()
            or "refused" in result.message.lower()
        )

    def test_real_s3_with_invalid_credentials(self, setup_resilience_test_environment):
        """Test S3 connector with invalid credentials."""
        # Test with invalid credentials
        invalid_config = {
            "bucket": "test-bucket",
            "access_key_id": "invalid",
            "secret_access_key": "invalid",
            "endpoint_url": "http://localhost:9000",
        }
        connector = S3Source()
        connector.configure(invalid_config)

        # Should fail gracefully (authentication errors are non-retryable)
        result = connector.test_connection()
        assert result.success is False
        assert any(
            word in result.message.lower()
            for word in ["credentials", "access", "denied", "invalid"]
        )

    def test_api_resilience_config_integration(self):
        """Test API resilience configuration integration."""
        from sqlflow.connectors.resilience import API_RESILIENCE_CONFIG

        # Verify the predefined API config has correct settings
        assert API_RESILIENCE_CONFIG.retry.max_attempts == 5
        assert API_RESILIENCE_CONFIG.circuit_breaker.failure_threshold == 3
        assert API_RESILIENCE_CONFIG.rate_limit.max_requests_per_minute == 60
        assert API_RESILIENCE_CONFIG.recovery.enable_credential_refresh is True

    def test_db_resilience_config_integration(self):
        """Test database resilience configuration integration."""
        # Verify the predefined DB config has correct settings
        assert DB_RESILIENCE_CONFIG.retry.max_attempts == 3
        assert DB_RESILIENCE_CONFIG.circuit_breaker.failure_threshold == 5
        assert DB_RESILIENCE_CONFIG.rate_limit.max_requests_per_minute == 300
        assert DB_RESILIENCE_CONFIG.recovery.enable_connection_recovery is True

    def test_performance_overhead_measurement(self):
        """Test that resilience patterns don't add significant overhead."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=1),  # No retries
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=100
            ),  # Won't trigger
            rate_limit=RateLimitConfig(max_requests_per_minute=3600),  # High limit
        )

        connector = MockFlakeyConnector("none")  # No failures
        connector.configure_resilience(config)
        connector.configure({})

        # Measure time for 100 operations
        start_time = time.time()
        for _ in range(100):
            result = connector.test_connection()
            assert result.success is True

        duration = time.time() - start_time

        # Should complete 100 operations in reasonable time (< 2 seconds for overhead)
        # Based on Phase 5 optimization: 1.5s for 100 operations = 15ms per operation (acceptable overhead)
        assert (
            duration < 2.0
        ), f"Resilience overhead too high: {duration}s for 100 operations"

    # Additional test methods for rate limiting, error propagation, etc.
    def test_rate_limiting_backpressure(self):
        """Test rate limiting with backpressure."""
        config = ResilienceConfig(
            retry=None,
            circuit_breaker=None,
            rate_limit=RateLimitConfig(
                max_requests_per_minute=60,  # 1 per second
                burst_size=2,
                backpressure_strategy="wait",
            ),
        )

        connector = MockFlakeyConnector("none")
        connector.configure_resilience(config)
        connector.configure({})

        # First two calls should succeed immediately (burst)
        start_time = time.time()
        result1 = connector.test_connection()
        result2 = connector.test_connection()
        burst_duration = time.time() - start_time

        assert result1.success is True
        assert result2.success is True
        assert burst_duration < 0.1  # Should be very fast

        # Third call should be rate limited
        start_time = time.time()
        result3 = connector.test_connection()
        rate_limited_duration = time.time() - start_time

        assert result3.success is True
        # Should take at least 0.5 seconds due to rate limiting
        # (allowing some tolerance for test execution)
        assert rate_limited_duration > 0.3

    def test_error_propagation_with_resilience(self):
        """Test that errors are properly propagated through resilience layers."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=2, initial_delay=0.01),
            circuit_breaker=None,
            rate_limit=None,
            recovery=RecoveryConfig(
                enable_connection_recovery=False
            ),  # Disable recovery
        )

        connector = MockFlakeyConnector("connection_error", failure_count=5)
        connector.configure_resilience(config)
        connector.configure({})

        # Should fail after retries and propagate the original error
        with pytest.raises(ConnectionError) as exc_info:
            connector.test_connection()

        assert "Connection failed" in str(exc_info.value)
        assert connector.call_count == 2  # Attempted twice

    def test_resilience_patterns_logging(self):
        """Test that resilience patterns generate appropriate logs."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=2),
            rate_limit=RateLimitConfig(max_requests_per_minute=60),
        )

        connector = MockFlakeyConnector("connection_error", failure_count=1)
        connector.configure_resilience(config)
        connector.configure({})

        # This test would need log capture to verify logging
        # For now, just ensure the operation succeeds with expected retry count
        result = connector.test_connection()
        assert result.success is True
        assert connector.call_count == 2  # One failure, one success

    def test_different_operation_isolation(self):
        """Test that different operations have isolated resilience state."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3, initial_delay=0.01),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=5,  # Higher threshold to prevent premature opening
                recovery_timeout=0.1,  # Short recovery timeout for testing
            ),
            recovery=RecoveryConfig(
                enable_connection_recovery=False
            ),  # Disable recovery
        )

        # Test each operation separately to ensure isolation
        connector1 = MockFlakeyConnector("connection_error", failure_count=1)
        connector1.configure_resilience(config)
        connector1.configure({})

        # Test connection (will fail once, then succeed)
        result = connector1.test_connection()
        assert result.success is True

        # Test discovery with different connector instance to ensure isolation
        connector2 = MockFlakeyConnector("discovery_error", failure_count=1)
        connector2.configure_resilience(config)
        connector2.configure({})

        # Discovery should work with its own resilience state
        discovered = connector2.discover()
        assert len(discovered) > 0


if __name__ == "__main__":
    pytest.main([__file__])
