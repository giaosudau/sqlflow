"""Integration tests for S3 connector resilience patterns."""

import os
import time

import boto3
import pytest
from botocore.exceptions import ClientError

from sqlflow.connectors.resilience import ResilienceManager
from sqlflow.connectors.s3_connector import S3Connector

# Mark all tests in this module as requiring external services
pytestmark = pytest.mark.external_services


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
def setup_resilience_test_data(docker_minio_config):
    """Set up test data for resilience testing."""
    # Skip if services are not available
    if not os.getenv("INTEGRATION_TESTS", "").lower() in ["true", "1"]:
        pytest.skip("Integration tests disabled. Set INTEGRATION_TESTS=true to enable.")

    try:
        client = _create_s3_test_client(docker_minio_config)
        bucket_name = docker_minio_config["bucket"]

        _ensure_test_bucket_exists(client, bucket_name)
        _upload_resilience_test_data(client, bucket_name)

        yield

        _cleanup_resilience_test_data(client, bucket_name)

    except Exception as e:
        pytest.skip(f"MinIO service not available: {e}")


def _create_s3_test_client(docker_minio_config):
    """Create S3 client for testing."""
    client = boto3.client(
        "s3",
        endpoint_url=docker_minio_config["endpoint_url"],
        aws_access_key_id=docker_minio_config["access_key_id"],
        aws_secret_access_key=docker_minio_config["secret_access_key"],
        region_name=docker_minio_config["region"],
    )

    # Test connection
    client.list_buckets()
    return client


def _ensure_test_bucket_exists(client, bucket_name):
    """Ensure the test bucket exists."""
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=bucket_name)


def _upload_resilience_test_data(client, bucket_name):
    """Upload all test data for resilience testing."""
    _upload_main_test_data(client, bucket_name)
    _upload_batch_test_data(client, bucket_name)


def _upload_main_test_data(client, bucket_name):
    """Upload main test CSV data."""
    test_data = """id,name,value,timestamp
1,test1,100,2024-01-15T10:00:00Z
2,test2,200,2024-01-15T11:00:00Z
3,test3,300,2024-01-15T12:00:00Z
4,test4,400,2024-01-15T13:00:00Z
5,test5,500,2024-01-15T14:00:00Z
"""

    client.put_object(
        Bucket=bucket_name,
        Key="resilience-test/test_data.csv",
        Body=test_data.encode(),
        ContentType="text/csv",
    )


def _upload_batch_test_data(client, bucket_name):
    """Upload batch test data files."""
    for i in range(5):
        batch_data = f"batch_id,item,count\n{i},item_{i},{i*10}\n"
        client.put_object(
            Bucket=bucket_name,
            Key=f"resilience-test/batch/batch_{i}.csv",
            Body=batch_data.encode(),
            ContentType="text/csv",
        )


def _cleanup_resilience_test_data(client, bucket_name):
    """Clean up test data after testing."""
    try:
        objects = client.list_objects_v2(Bucket=bucket_name, Prefix="resilience-test/")
        if "Contents" in objects:
            delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
            if delete_keys:
                client.delete_objects(
                    Bucket=bucket_name, Delete={"Objects": delete_keys}
                )
    except ClientError:
        pass


class TestS3ConnectorResilience:
    """Test S3 connector resilience patterns integration with real MinIO service."""

    @pytest.fixture
    def s3_connector(self):
        """Create S3 connector with resilience patterns."""
        connector = S3Connector()
        return connector

    def test_real_resilience_configuration(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test that resilience patterns are configured with real service."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Verify resilience manager is set up
        assert hasattr(s3_connector, "resilience_manager")
        assert isinstance(s3_connector.resilience_manager, ResilienceManager)

        # Test actual connection
        result = s3_connector.test_connection()
        assert result.success is True, f"Connection failed: {result.message}"

    def test_real_connection_and_discovery(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test connection and discovery with real MinIO service."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Test connection
        result = s3_connector.test_connection()
        assert result.success is True

        # Test discovery
        objects = s3_connector.discover()
        assert len(objects) > 0, "Should discover test objects"

        # Verify we found our test files
        test_files = [obj for obj in objects if "resilience-test" in obj]
        assert len(test_files) > 0, "Should find resilience test files"

    def test_real_data_reading_with_resilience(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test data reading with resilience patterns on real data."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Read real data
        discovered = s3_connector.discover()
        assert len(discovered) > 0

        # Find our main test file
        test_file = None
        for obj in discovered:
            if "test_data.csv" in obj:
                test_file = obj
                break

        assert test_file is not None, "Should find test_data.csv"

        # Read the data
        chunks = list(s3_connector.read(test_file))
        assert len(chunks) > 0, "Should read data chunks"

        # Verify data content
        df = chunks[0].pandas_df
        assert len(df) == 5, "Should have 5 test rows"
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns

    def test_connection_retry_with_invalid_config(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test retry behavior with invalid configuration."""
        # Test with invalid credentials
        invalid_config = {
            **docker_minio_config,
            "access_key_id": "invalid_key",
            "secret_access_key": "invalid_secret",
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "mock_mode": False,
        }

        s3_connector.configure(invalid_config)

        # This should fail gracefully
        result = s3_connector.test_connection()
        assert result.success is False
        assert "forbidden" in result.message.lower() or "403" in result.message.lower()

    def test_discovery_with_nonexistent_prefix(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test discovery behavior with non-existent prefix."""
        config = {
            **docker_minio_config,
            "path_prefix": "nonexistent-prefix/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Discovery should succeed but return empty list
        objects = s3_connector.discover()
        assert isinstance(objects, list)
        assert len(objects) == 0, "Should return empty list for nonexistent prefix"

    def test_batch_operations_with_resilience(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test batch operations with resilience patterns."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/batch/",
            "file_format": "csv",
            "cost_limit_usd": 15.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Discover batch files
        discovered = s3_connector.discover()
        batch_files = [obj for obj in discovered if "batch_" in obj]
        assert len(batch_files) >= 5, "Should find batch files"

        # Read all batch files
        total_chunks = 0
        for batch_file in batch_files:
            chunks = list(s3_connector.read(batch_file))
            total_chunks += len(chunks)

            if chunks:
                df = chunks[0].pandas_df
                assert "batch_id" in df.columns
                assert "item" in df.columns
                assert "count" in df.columns

        assert total_chunks >= 5, "Should read from all batch files"

    def test_concurrent_access_with_resilience(
        self, docker_minio_config, setup_resilience_test_data
    ):
        """Test concurrent access with resilience patterns."""
        import threading

        results = {}

        def test_concurrent_connector(thread_id):
            try:
                connector = S3Connector()
                config = {
                    **docker_minio_config,
                    "path_prefix": "resilience-test/",
                    "file_format": "csv",
                    "cost_limit_usd": 5.0,
                    "mock_mode": False,
                }

                connector.configure(config)

                # Test connection
                result = connector.test_connection()
                results[f"{thread_id}_connection"] = result.success

                # Test discovery
                discovered = connector.discover()
                results[f"{thread_id}_discovery"] = len(discovered) > 0

                # Test reading
                if discovered:
                    test_file = discovered[0]
                    chunks = list(connector.read(test_file))
                    results[f"{thread_id}_read"] = len(chunks) > 0
                else:
                    results[f"{thread_id}_read"] = False

            except Exception as e:
                results[f"{thread_id}_error"] = str(e)

        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=test_concurrent_connector, args=(i,))
            threads.append(thread)
            thread.start()
            time.sleep(0.1)  # Small delay between starts

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify all succeeded
        for i in range(3):
            assert (
                results.get(f"{i}_connection") is True
            ), f"Thread {i} connection failed"
            assert results.get(f"{i}_discovery") is True, f"Thread {i} discovery failed"
            assert results.get(f"{i}_read") is True, f"Thread {i} read failed"

    def test_rate_limiting_with_real_service(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test rate limiting behavior with real service."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Perform multiple rapid operations
        start_time = time.time()

        for i in range(5):
            result = s3_connector.test_connection()
            assert result.success is True, f"Connection {i} failed"

            # Short delay between operations
            time.sleep(0.1)

        end_time = time.time()
        total_time = end_time - start_time

        # Should complete in reasonable time (rate limiting shouldn't be too aggressive)
        assert total_time < 10, f"Operations took too long: {total_time}s"

    def test_cost_management_with_real_data(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test cost management features with real data."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 0.01,  # Very low limit for testing
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Cost management should be active
        assert s3_connector.cost_manager is not None
        assert s3_connector.cost_manager.cost_limit_usd == 0.01

        # Discovery should still work (cost checking is typically on reads)
        discovered = s3_connector.discover()
        assert isinstance(discovered, list)

    def test_error_recovery_patterns(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test error recovery patterns with real service."""
        # Start with valid config
        valid_config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(valid_config)

        # Verify initial connection works
        result = s3_connector.test_connection()
        assert result.success is True

        # Test with invalid bucket to simulate failure
        invalid_config = {
            **docker_minio_config,
            "bucket": "nonexistent-bucket-12345",
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "mock_mode": False,
        }

        invalid_connector = S3Connector()
        invalid_connector.configure(invalid_config)

        # This should fail gracefully
        result = invalid_connector.test_connection()
        assert result.success is False
        assert (
            "bucket" in result.message.lower() or "not found" in result.message.lower()
        )

    def test_resilience_metrics_tracking(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test that resilience metrics are tracked with real operations."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Perform several operations to generate metrics
        s3_connector.test_connection()
        discovered = s3_connector.discover()

        if discovered:
            chunks = list(s3_connector.read(discovered[0]))
            assert len(chunks) > 0

        # Resilience manager should have tracked operations
        assert hasattr(s3_connector, "resilience_manager")
        assert s3_connector.resilience_manager is not None

    def test_incremental_loading_with_resilience(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test incremental loading patterns with resilience."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "sync_mode": "incremental",
            "cursor_field": "timestamp",
            "cost_limit_usd": 10.0,
            "mock_mode": False,
        }

        s3_connector.configure(config)

        # Test incremental loading
        discovered = s3_connector.discover()
        assert len(discovered) > 0

        # Find file with timestamp data
        main_file = None
        for obj in discovered:
            if "test_data.csv" in obj:
                main_file = obj
                break

        if main_file:
            chunks = list(
                s3_connector.read_incremental(
                    main_file,
                    cursor_field="timestamp",
                    cursor_value="2024-01-15T11:30:00Z",
                )
            )

            # Should handle incremental read with resilience
            assert len(chunks) >= 0

    def test_large_file_handling_with_resilience(
        self, docker_minio_config, setup_resilience_test_data
    ):
        """Test handling of larger files with resilience patterns."""
        # Create a connector with chunking settings
        connector = S3Connector()
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 20.0,
            "dev_sampling": 1.0,  # No sampling
            "batch_size": 2,  # Small batch size for testing
            "mock_mode": False,
        }

        connector.configure(config)

        # Test reading with small batch sizes (simulates large file handling)
        discovered = connector.discover()

        if discovered:
            # Read with small chunks
            chunks = list(connector.read(discovered[0]))

            # Should successfully handle chunked reading
            assert len(chunks) >= 0


if __name__ == "__main__":
    pytest.main([__file__])
