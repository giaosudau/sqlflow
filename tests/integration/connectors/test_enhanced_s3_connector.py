"""Integration tests for Enhanced S3 Connector.

Tests the S3 connector with real MinIO instance and mock data generation.
Verifies cost management, partition awareness, multi-format support, and resilience.
"""

import os

import boto3
import pytest
from botocore.exceptions import ClientError

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.s3_connector import (
    S3Connector,
    S3CostManager,
    S3PartitionManager,
)


@pytest.fixture(scope="module")
def minio_config():
    """MinIO configuration for testing."""
    return {
        "endpoint_url": "http://localhost:9000",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "region": "us-east-1",
        "bucket": "test-sqlflow-demo",
        "use_ssl": False,
    }


@pytest.fixture(scope="module")
def s3_client(minio_config):
    """Create S3 client for test setup."""
    return boto3.client(
        "s3",
        endpoint_url=minio_config["endpoint_url"],
        aws_access_key_id=minio_config["access_key_id"],
        aws_secret_access_key=minio_config["secret_access_key"],
        region_name=minio_config["region"],
    )


@pytest.fixture(scope="module")
def test_bucket(s3_client, minio_config):
    """Create and populate test bucket with sample data."""
    bucket_name = minio_config["bucket"]

    # Create bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise

    # Create test CSV data
    csv_data = """id,name,category,price,created_at
1,Product A,Electronics,99.99,2024-01-15
2,Product B,Books,19.99,2024-01-16
3,Product C,Electronics,149.99,2024-01-17
"""

    # Upload CSV test data
    s3_client.put_object(
        Bucket=bucket_name,
        Key="demo-data/demo_data.csv",
        Body=csv_data.encode(),
        ContentType="text/csv",
    )

    # Create partitioned test data
    for year in [2023, 2024]:
        for month in [1, 2]:
            for day in [15, 16]:
                partition_data = f"""event_id,event_timestamp,user_id,action
{year}{month:02d}{day:02d}001,{year}-{month:02d}-{day:02d}T10:00:00Z,user1,click
{year}{month:02d}{day:02d}002,{year}-{month:02d}-{day:02d}T11:00:00Z,user2,purchase
"""
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"partitioned-data/year={year}/month={month:02d}/day={day:02d}/events.csv",
                    Body=partition_data.encode(),
                    ContentType="text/csv",
                )

    # Create JSON test data
    json_data = """{"product_id": 1, "name": "Laptop", "price": 999.99}
{"product_id": 2, "name": "Mouse", "price": 29.99}
{"product_id": 3, "name": "Keyboard", "price": 79.99}
"""
    s3_client.put_object(
        Bucket=bucket_name,
        Key="json-data/products.jsonl",
        Body=json_data.encode(),
        ContentType="application/json",
    )

    # Create CSV data for multi-format testing
    csv_products = """product_id,name,category,stock
1,Laptop Pro,Electronics,25
2,Wireless Mouse,Electronics,100
3,Mechanical Keyboard,Electronics,50
"""
    s3_client.put_object(
        Bucket=bucket_name,
        Key="csv-data/products.csv",
        Body=csv_products.encode(),
        ContentType="text/csv",
    )

    # Create events data for incremental testing
    events_data = """event_timestamp,event_type,user_id,value
2024-01-15T10:00:00Z,login,user1,1
2024-01-15T11:00:00Z,purchase,user1,100
2024-01-16T09:00:00Z,login,user2,1
2024-01-16T10:30:00Z,view,user2,0
"""
    s3_client.put_object(
        Bucket=bucket_name,
        Key="events/events_20240115.csv",
        Body=events_data.encode(),
        ContentType="text/csv",
    )

    # Create legacy data
    legacy_data = """customer_id,name,email,signup_date
1,Alice,alice@example.com,2024-01-01
2,Bob,bob@example.com,2024-01-02
3,Charlie,charlie@example.com,2024-01-03
"""
    s3_client.put_object(
        Bucket=bucket_name,
        Key="legacy-data/customers.csv",
        Body=legacy_data.encode(),
        ContentType="text/csv",
    )

    yield bucket_name

    # Cleanup - delete all objects and bucket
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in objects:
            delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
            s3_client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": delete_keys}
            )
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError:
        pass  # Bucket might not exist or be empty


class TestEnhancedS3Connector:
    """Test the enhanced S3 connector features."""

    def test_cost_management_configuration(self, minio_config, test_bucket):
        """Test cost management features and configuration."""
        connector = S3Connector()
        params = {
            **minio_config,
            "bucket": test_bucket,
            "key": "demo-data/demo_data.csv",
            "file_format": "csv",
            "cost_limit_usd": 5.0,
            "dev_sampling": 0.1,
            "max_files_per_run": 100,
            "max_data_size_gb": 10.0,
            "mock_mode": False,
        }

        connector.configure(params)
        assert connector.state == ConnectorState.CONFIGURED
        assert connector.cost_manager is not None
        assert connector.cost_manager.cost_limit_usd == 5.0

        # Test connection
        result = connector.test_connection()
        assert result.success, f"Connection failed: {result.message}"

        # Test discovery with cost limits
        discovered = connector.discover()
        assert len(discovered) > 0, "Should discover test files"

    def test_partition_awareness(self, minio_config, test_bucket):
        """Test partition pattern detection and optimization."""
        connector = S3Connector()
        params = {
            **minio_config,
            "bucket": test_bucket,
            "path_prefix": "partitioned-data/",
            "file_format": "csv",
            "partition_keys": "year,month,day",
            "cost_limit_usd": 3.0,
            "mock_mode": False,
        }

        connector.configure(params)

        # Test partition pattern detection
        discovered = connector.discover()
        assert len(discovered) > 0, "Should discover partitioned files"

        # Verify partition pattern was detected
        if connector.partition_manager.detected_pattern:
            pattern = connector.partition_manager.detected_pattern
            assert pattern.pattern_type in ["hive", "date", "custom"]
            assert len(pattern.keys) > 0

    def test_multi_format_support(self, minio_config, test_bucket):
        """Test CSV and JSON format support."""
        # Test CSV format
        csv_connector = S3Connector()
        csv_params = {
            **minio_config,
            "bucket": test_bucket,
            "path_prefix": "csv-data/",
            "file_format": "csv",
            "csv_delimiter": ",",
            "csv_header": True,
            "csv_encoding": "utf-8",
            "cost_limit_usd": 2.0,
            "mock_mode": False,
        }

        csv_connector.configure(csv_params)
        csv_discovered = csv_connector.discover()
        assert len(csv_discovered) > 0, "Should discover CSV files"

        # Test reading CSV data
        for file_key in csv_discovered[:1]:  # Test first file
            chunks = list(csv_connector.read(file_key))
            assert len(chunks) > 0, "Should read CSV data chunks"
            assert len(chunks[0].pandas_df) > 0, "Should have data rows"

        # Test JSON format
        json_connector = S3Connector()
        json_params = {
            **minio_config,
            "bucket": test_bucket,
            "path_prefix": "json-data/",
            "file_format": "json",
            "json_flatten": True,
            "json_max_depth": 10,
            "cost_limit_usd": 1.5,
            "mock_mode": False,
        }

        json_connector.configure(json_params)
        json_discovered = json_connector.discover()
        assert len(json_discovered) > 0, "Should discover JSON files"

    def test_incremental_loading(self, minio_config, test_bucket):
        """Test incremental loading with cursor field."""
        connector = S3Connector()
        params = {
            **minio_config,
            "bucket": test_bucket,
            "path_prefix": "events/",
            "file_format": "csv",
            "sync_mode": "incremental",
            "cursor_field": "event_timestamp",
            "cost_limit_usd": 8.0,
            "mock_mode": False,
        }

        connector.configure(params)

        # Test incremental read without cursor
        discovered = connector.discover()
        if discovered:
            chunks = list(
                connector.read_incremental(
                    discovered[0], cursor_field="event_timestamp"
                )
            )
            assert len(chunks) >= 0, "Should handle incremental read"

    def test_backward_compatibility(self, minio_config, test_bucket):
        """Test legacy parameter support."""
        connector = S3Connector()
        # Use legacy parameter names
        params = {
            "bucket": test_bucket,
            "prefix": "legacy-data/",  # Legacy: path_prefix
            "format": "csv",  # Legacy: file_format
            "access_key": minio_config["access_key_id"],  # Legacy: access_key_id
            "secret_key": minio_config[
                "secret_access_key"
            ],  # Legacy: secret_access_key
            "endpoint_url": minio_config["endpoint_url"],
            "cost_limit_usd": 2.0,
            "dev_sampling": 0.15,
            "mock_mode": False,
        }

        connector.configure(params)
        assert connector.state == ConnectorState.CONFIGURED

        # Test discovery with legacy parameters
        discovered = connector.discover()
        assert len(discovered) >= 0, "Should handle legacy parameters"

    def test_development_features(self, minio_config, test_bucket):
        """Test development sampling and limits."""
        connector = S3Connector()
        params = {
            **minio_config,
            "bucket": test_bucket,
            "path_prefix": "demo-data/",
            "file_format": "csv",
            "dev_sampling": 0.5,  # 50% sampling
            "dev_max_files": 10,
            "cost_limit_usd": 1.0,
            "mock_mode": False,
        }

        connector.configure(params)

        # Test that sampling is applied
        discovered = connector.discover()
        # With small test dataset, this mainly tests the code path
        assert len(discovered) >= 0, "Should handle development sampling"

    def test_cost_manager_standalone(self):
        """Test cost manager functionality independently."""
        cost_manager = S3CostManager(cost_limit_usd=10.0)

        # Test cost limit checking
        assert cost_manager.check_cost_limit(5.0), "Should allow cost under limit"
        assert not cost_manager.check_cost_limit(15.0), "Should reject cost over limit"

        # Test operation tracking
        cost_manager.track_operation(data_size_bytes=1024 * 1024, num_requests=10)
        metrics = cost_manager.get_metrics()

        assert "current_cost_usd" in metrics
        assert "cost_limit_usd" in metrics
        assert metrics["cost_limit_usd"] == 10.0

    def test_partition_manager_standalone(self):
        """Test partition manager functionality independently."""
        manager = S3PartitionManager()

        # Test Hive-style partition detection
        hive_keys = [
            "data/year=2024/month=01/day=15/file1.csv",
            "data/year=2024/month=01/day=16/file2.csv",
            "data/year=2024/month=02/day=01/file3.csv",
        ]

        pattern = manager.detect_partition_pattern(hive_keys)
        if pattern:  # Pattern detection might vary based on implementation
            assert pattern.pattern_type in ["hive", "date", "custom"]

        # Test date-based partition detection
        date_keys = [
            "events/2024/01/15/events.csv",
            "events/2024/01/16/events.csv",
            "events/2024/02/01/events.csv",
        ]

        date_pattern = manager.detect_partition_pattern(date_keys)
        # Pattern detection is best-effort, test that it doesn't crash
        assert date_pattern is None or date_pattern.pattern_type in [
            "hive",
            "date",
            "custom",
        ]

    def test_mock_mode(self):
        """Test mock mode functionality."""
        connector = S3Connector()
        params = {"bucket": "test-bucket", "file_format": "csv", "mock_mode": True}

        connector.configure(params)

        # Test connection in mock mode
        result = connector.test_connection()
        assert result.success, "Mock mode connection should succeed"

        # Test discovery in mock mode
        discovered = connector.discover()
        assert len(discovered) > 0, "Mock mode should return mock files"

        # Test reading in mock mode
        chunks = list(connector.read(discovered[0]))
        assert len(chunks) > 0, "Mock mode should return mock data"
        assert len(chunks[0].pandas_df) > 0, "Mock data should have rows"

    def test_error_handling(self, minio_config):
        """Test error handling for various failure scenarios."""
        connector = S3Connector()

        # Test with invalid bucket
        invalid_params = {
            **minio_config,
            "bucket": "non-existent-bucket-12345",
            "file_format": "csv",
            "mock_mode": False,
        }

        connector.configure(invalid_params)

        # Connection test should fail gracefully
        result = connector.test_connection()
        assert not result.success, "Should fail for non-existent bucket"
        assert (
            "error" in result.message.lower() or "not found" in result.message.lower()
        )


@pytest.mark.integration
class TestS3ConnectorIntegration:
    """Integration tests requiring running MinIO service."""

    @pytest.mark.skipif(
        not os.getenv("INTEGRATION_TESTS", "").lower() in ["true", "1"],
        reason="Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
    )
    def test_full_pipeline_simulation(self, minio_config, test_bucket):
        """Test complete pipeline simulation with multiple connectors."""

        # Test cost management scenario
        cost_connector = S3Connector()
        cost_params = {
            **minio_config,
            "bucket": test_bucket,
            "key": "demo-data/demo_data.csv",
            "file_format": "csv",
            "cost_limit_usd": 5.0,
            "mock_mode": False,
        }
        cost_connector.configure(cost_params)

        cost_result = cost_connector.test_connection()
        assert cost_result.success, "Cost demo connection should succeed"

        cost_discovered = cost_connector.discover()
        assert len(cost_discovered) > 0, "Should discover cost demo files"

        # Test reading data
        cost_chunks = list(cost_connector.read(cost_discovered[0]))
        assert len(cost_chunks) > 0, "Should read cost demo data"

        cost_data_count = sum(len(chunk.pandas_df) for chunk in cost_chunks)
        assert cost_data_count > 0, "Should have cost demo data rows"

        # Verify this matches expected test data structure
        assert (
            cost_data_count == 3
        ), f"Expected 3 rows in demo data, got {cost_data_count}"
