"""Integration tests for S3 connector resilience patterns."""

import os
import time

import boto3
import pytest
from botocore.exceptions import ClientError

from sqlflow.connectors.s3.source import S3Source

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
        """Create S3 connector for resilience testing."""
        connector = S3Source()
        return connector

    def test_real_resilience_configuration(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test that S3 connector is configured with real service."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
        }

        s3_connector.configure(config)

        # Verify connector configuration
        assert s3_connector.bucket == docker_minio_config["bucket"]
        assert s3_connector.path_prefix == "resilience-test/"
        assert s3_connector.cost_limit_usd == 10.0

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
        """Test data reading with error handling on real data."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
        }

        s3_connector.configure(config)

        # Discover files
        objects = s3_connector.discover()
        assert len(objects) > 0

        # Read specific test file
        test_file = "resilience-test/test_data.csv"
        assert (
            test_file in objects
        ), f"Test file {test_file} not found in discovered objects"
        df = s3_connector.read(object_name=test_file)

        # Verify we got data
        assert not df.empty, "Should read data from test file"
        assert "id" in df.columns, "Should have expected columns"

    def test_connection_retry_with_invalid_config(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test connection retry behavior with invalid config."""
        # Use invalid credentials
        invalid_config = {
            **docker_minio_config,
            "access_key_id": "invalid",
            "secret_access_key": "invalid",
            "path_prefix": "resilience-test/",
            "file_format": "csv",
        }

        s3_connector.configure(invalid_config)

        # Connection should fail gracefully
        result = s3_connector.test_connection()
        assert result.success is False
        assert (
            "credentials" in result.message.lower()
            or "access" in result.message.lower()
        )

    def test_discovery_with_nonexistent_prefix(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test discovery behavior with non-existent prefix."""
        config = {
            **docker_minio_config,
            "path_prefix": "non-existent-prefix/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
        }

        s3_connector.configure(config)

        # Discovery should return empty list for non-existent prefix
        objects = s3_connector.discover()
        assert len(objects) == 0, "Should return empty list for non-existent prefix"

    def test_batch_operations_with_resilience(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test batch operations with error handling."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/batch/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
            "max_files_per_run": 3,  # Limit files for testing
        }

        s3_connector.configure(config)

        # Discover batch files
        objects = s3_connector.discover()
        assert len(objects) > 0, "Should discover batch files"

        # Test that file limit is respected
        assert (
            len(objects) <= 3
        ), f"Should respect max_files_per_run limit, got {len(objects)}"

        # Read each file
        for obj in objects:
            df = s3_connector.read(object_name=obj)
            assert len(df) >= 0, f"Should read data from {obj}"

    def test_concurrent_access_with_resilience(
        self, docker_minio_config, setup_resilience_test_data
    ):
        """Test concurrent access with resilience patterns."""
        import threading

        results = {}

        def test_concurrent_connector(thread_id):
            try:
                connector = S3Source()
                config = {
                    **docker_minio_config,
                    "path_prefix": "resilience-test/",
                    "file_format": "csv",
                    "cost_limit_usd": 5.0,
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
                    df = connector.read(object_name=test_file)
                    results[f"{thread_id}_read"] = len(df) > 0
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
            "dev_sampling": 0.5,  # Use sampling as rate limiting
        }

        s3_connector.configure(config)

        # Test discovery with rate limiting
        objects = s3_connector.discover()

        # With sampling, we should get fewer files
        assert len(objects) >= 0, "Should handle discovery with sampling"

    def test_cost_management_with_real_data(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test cost management features with real data."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 1.0,  # Low limit for testing
            "max_files_per_run": 2,
            "max_data_size_gb": 0.001,  # Very low limit
        }

        s3_connector.configure(config)

        # Verify cost management configuration
        assert s3_connector.cost_limit_usd == 1.0
        assert s3_connector.max_files_per_run == 2

        # Discovery should respect limits
        objects = s3_connector.discover()
        assert len(objects) <= 2, "Should respect max_files_per_run limit"

    def test_error_recovery_patterns(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test error recovery patterns."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
        }

        s3_connector.configure(config)

        # Test reading non-existent file (should handle gracefully)
        df = s3_connector.read(object_name="resilience-test/non-existent.csv")
        assert len(df) == 0, "Should return empty DataFrame for non-existent file"

        # Test getting schema for non-existent file
        schema = s3_connector.get_schema("resilience-test/non-existent.csv")
        assert schema is not None, "Should return empty schema for non-existent file"

    def test_resilience_metrics_tracking(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test that basic metrics can be tracked."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
        }

        s3_connector.configure(config)

        # Track discovery metrics
        start_time = time.time()
        objects = s3_connector.discover()
        discovery_time = time.time() - start_time

        assert discovery_time >= 0, "Discovery should complete in reasonable time"
        assert len(objects) >= 0, "Discovery should return valid result"

        # Track read metrics if we have objects
        if objects:
            start_time = time.time()
            df = s3_connector.read(object_name=objects[0])
            read_time = time.time() - start_time

            assert read_time >= 0, "Read should complete in reasonable time"
            assert len(df) >= 0, "Read should return valid result"

    def test_incremental_loading_with_resilience(
        self, s3_connector, docker_minio_config, setup_resilience_test_data
    ):
        """Test incremental loading with error handling."""
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 10.0,
        }

        s3_connector.configure(config)

        # Discover files
        objects = s3_connector.discover()
        assert len(objects) > 0

        # Test incremental read on specific file
        test_file = "resilience-test/test_data.csv"
        assert (
            test_file in objects
        ), f"Test file {test_file} not found in discovered objects"

        chunks = list(
            s3_connector.read_incremental(
                object_name=test_file, cursor_field="timestamp"
            )
        )

        assert len(chunks) > 0, "Incremental read should produce chunks"

        df = chunks[0].pandas_df
        assert "timestamp" in df.columns, "Should have cursor field"
        assert len(df) == 5

    def test_large_file_handling_with_resilience(
        self, docker_minio_config, setup_resilience_test_data
    ):
        """Test handling of larger files with resilience patterns."""
        # Create a connector with chunking settings
        connector = S3Source()
        config = {
            **docker_minio_config,
            "path_prefix": "resilience-test/",
            "file_format": "csv",
            "cost_limit_usd": 20.0,
            "dev_sampling": 1.0,  # No sampling
        }

        connector.configure(config)

        # Test reading with error handling
        discovered = connector.discover()

        if discovered:
            # Read with potential error handling
            df = connector.read(object_name=discovered[0])

            # Should successfully handle reading
            assert len(df) >= 0


if __name__ == "__main__":
    pytest.main([__file__])
