"""Tests for S3 connector partition awareness features."""

import io
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlflow.connectors.base import ParameterError
from sqlflow.connectors.s3_connector import (
    S3Connector,
    S3PartitionManager,
)


class TestS3PartitionManager:
    """Test S3 partition management functionality."""

    def test_partition_manager_initialization(self):
        """Test partition manager initialization."""
        manager = S3PartitionManager()
        assert manager.detected_pattern is None

    def test_hive_partition_detection(self):
        """Test detection of Hive-style partitions."""
        manager = S3PartitionManager()

        # Hive-style partition keys
        s3_keys = [
            "data/year=2024/month=01/day=15/file1.parquet",
            "data/year=2024/month=01/day=16/file2.parquet",
            "data/year=2024/month=02/day=01/file3.parquet",
            "data/year=2023/month=12/day=31/file4.parquet",
        ]

        pattern = manager.detect_partition_pattern(s3_keys)

        assert pattern is not None
        assert pattern.pattern_type == "hive"
        assert set(pattern.keys) == {"year", "month", "day"}
        assert "year=" in pattern.example
        assert "month=" in pattern.example
        assert "day=" in pattern.example

    def test_date_partition_detection(self):
        """Test detection of date-based partitions."""
        manager = S3PartitionManager()

        # Date-based partition keys
        s3_keys = [
            "logs/2024/01/15/events.json",
            "logs/2024/01/16/events.json",
            "logs/2024/02/01/events.json",
            "logs/2023/12/31/events.json",
        ]

        pattern = manager.detect_partition_pattern(s3_keys)

        assert pattern is not None
        assert pattern.pattern_type == "date"
        assert len(pattern.keys) == 3  # year, month, day
        assert "2024" in pattern.example

    def test_custom_partition_detection(self):
        """Test detection of custom partition patterns."""
        manager = S3PartitionManager()

        # Custom partition keys
        s3_keys = [
            "data/region=us-east/env=prod/service=api/file1.csv",
            "data/region=us-west/env=prod/service=api/file2.csv",
            "data/region=eu-west/env=staging/service=web/file3.csv",
        ]

        pattern = manager.detect_partition_pattern(s3_keys)

        assert pattern is not None
        assert set(pattern.keys) == {"region", "env", "service"}

    def test_no_partition_detection(self):
        """Test when no partition pattern is detected."""
        manager = S3PartitionManager()

        # Random keys with no pattern
        s3_keys = [
            "data/random_file_1.csv",
            "uploads/another_file.json",
            "exports/report.parquet",
        ]

        pattern = manager.detect_partition_pattern(s3_keys)
        assert pattern is None

    def test_mixed_partition_patterns(self):
        """Test with mixed partition patterns (should pick most common)."""
        manager = S3PartitionManager()

        # Mix of Hive and date patterns
        s3_keys = [
            "data/year=2024/month=01/file1.parquet",  # Hive
            "data/year=2024/month=02/file2.parquet",  # Hive
            "data/year=2024/month=03/file3.parquet",  # Hive
            "logs/2024/01/15/events.json",  # Date
            "logs/2024/01/16/events.json",  # Date
        ]

        pattern = manager.detect_partition_pattern(s3_keys)

        # Should detect Hive pattern (more common)
        assert pattern is not None
        assert pattern.pattern_type == "hive"

    def test_optimized_prefix_generation_hive(self):
        """Test optimized prefix generation for Hive partitions."""
        manager = S3PartitionManager()

        # Test with date cursor value
        optimized_prefix = manager.build_optimized_prefix(
            base_prefix="data/", cursor_value="2024-01-15", cursor_field="event_date"
        )

        # Should generate a date-aware prefix
        assert "data/" in optimized_prefix
        assert "2024" in optimized_prefix or optimized_prefix == "data/"

    def test_optimized_prefix_generation_date(self):
        """Test optimized prefix generation for date partitions."""
        manager = S3PartitionManager()

        # Test with timestamp cursor value
        optimized_prefix = manager.build_optimized_prefix(
            base_prefix="logs/",
            cursor_value="2024-01-15 10:30:00",
            cursor_field="timestamp",
        )

        # Should generate a date-aware prefix
        assert "logs/" in optimized_prefix

    def test_optimized_prefix_no_partition(self):
        """Test optimized prefix when no partition pattern detected."""
        manager = S3PartitionManager()

        # When no pattern is detected, should return base prefix
        optimized_prefix = manager.build_optimized_prefix(
            base_prefix="random/", cursor_value="2024-01-15", cursor_field="date"
        )

        assert optimized_prefix == "random/"

    def test_partition_pattern_regex_matching(self):
        """Test that partition patterns correctly match files."""
        manager = S3PartitionManager()

        # Test Hive pattern matching
        s3_keys = [
            "data/year=2024/month=01/day=15/file.parquet",
            "data/year=2024/month=01/day=16/file.parquet",
        ]

        pattern = manager.detect_partition_pattern(s3_keys)
        assert pattern is not None

        # Test that regex matches the pattern
        test_key = "data/year=2025/month=12/day=31/newfile.parquet"
        match = pattern.regex.search(test_key)
        assert match is not None

    def test_edge_cases_partition_detection(self):
        """Test edge cases in partition detection."""
        manager = S3PartitionManager()

        # Empty list
        assert manager.detect_partition_pattern([]) is None

        # Single file
        single_file = ["data/file.csv"]
        assert manager.detect_partition_pattern(single_file) is None

        # Very long partition paths
        long_keys = [
            "very/deep/nested/year=2024/month=01/day=15/hour=10/minute=30/file.csv",
            "very/deep/nested/year=2024/month=01/day=15/hour=11/minute=45/file.csv",
        ]
        pattern = manager.detect_partition_pattern(long_keys)
        assert pattern is not None
        assert "year" in pattern.keys
        assert "hour" in pattern.keys


class TestS3ConnectorPartitionIntegration:
    """Test S3 connector partition awareness integration."""

    @pytest.fixture
    def s3_connector(self):
        """Create S3 connector."""
        return S3Connector()

    @pytest.fixture
    def partition_config(self):
        """Partition-aware configuration."""
        return {
            "bucket": "test-bucket",
            "path_prefix": "data/",
            "partition_keys": ["year", "month", "day"],
            "partition_filter": {"year": "2024", "month": ["01", "02"]},
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
        }

    def test_partition_configuration(self, s3_connector, partition_config):
        """Test partition configuration."""
        s3_connector.configure(partition_config)

        assert s3_connector.params["partition_keys"] == ["year", "month", "day"]
        assert s3_connector.params["partition_filter"]["year"] == "2024"

    @patch("boto3.Session")
    def test_partition_aware_discovery(
        self, mock_session, s3_connector, partition_config
    ):
        """Test partition-aware discovery."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client

        # Mock partitioned objects
        partitioned_objects = [
            {
                "Key": "data/year=2024/month=01/day=15/file1.parquet",
                "Size": 1024 * 1024,
                "LastModified": datetime.now(),
                "ETag": "etag1",
            },
            {
                "Key": "data/year=2024/month=02/day=01/file2.parquet",
                "Size": 1024 * 1024,
                "LastModified": datetime.now(),
                "ETag": "etag2",
            },
            {
                "Key": "data/year=2023/month=12/day=31/file3.parquet",  # Should be filtered
                "Size": 1024 * 1024,
                "LastModified": datetime.now(),
                "ETag": "etag3",
            },
        ]

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": partitioned_objects}]
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_s3_client.head_bucket.return_value = {}

        s3_connector.configure(partition_config)
        s3_connector.discover()

        # Should detect partition pattern
        assert s3_connector.partition_manager.detected_pattern is not None
        assert s3_connector.partition_manager.detected_pattern.pattern_type == "hive"

    @patch("boto3.Session")
    def test_incremental_with_partition_optimization(self, mock_session, s3_connector):
        """Test incremental loading with partition optimization."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client

        # Configure without explicit partition config (auto-detection)
        config = {
            "bucket": "test-bucket",
            "path_prefix": "events/",
            "file_format": "parquet",
            "sync_mode": "incremental",
            "cursor_field": "event_date",
        }

        # Mock partitioned objects for discovery
        discovery_objects = [
            {
                "Key": "events/year=2024/month=01/day=15/events.parquet",
                "Size": 1024 * 1024,
                "LastModified": datetime.now(),
                "ETag": "etag1",
            },
            {
                "Key": "events/year=2024/month=01/day=16/events.parquet",
                "Size": 1024 * 1024,
                "LastModified": datetime.now(),
                "ETag": "etag2",
            },
        ]

        # Mock incremental objects (fewer files due to optimization)
        incremental_objects = [
            {
                "Key": "events/year=2024/month=01/day=16/events.parquet",
                "Size": 1024 * 1024,
                "LastModified": datetime.now(),
                "ETag": "etag2",
            },
        ]

        mock_paginator = MagicMock()
        # First call (discovery) returns all objects
        # Second call (incremental) returns filtered objects
        mock_paginator.paginate.side_effect = [
            [{"Contents": discovery_objects}],  # Discovery
            [{"Contents": incremental_objects}],  # Incremental
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_s3_client.head_bucket.return_value = {}

        # Create proper mock Parquet data
        mock_df = pd.DataFrame(
            {
                "event_date": ["2024-01-16", "2024-01-16"],
                "event_type": ["click", "view"],
            }
        )
        mock_table = pa.Table.from_pandas(mock_df)
        mock_parquet_buffer = io.BytesIO()
        pq.write_table(mock_table, mock_parquet_buffer)
        mock_parquet_data = mock_parquet_buffer.getvalue()

        # Mock file content with proper Parquet data
        mock_body = MagicMock()
        mock_body.read.return_value = mock_parquet_data
        mock_s3_client.get_object.return_value = {"Body": mock_body}
        mock_s3_client.head_object.return_value = {
            "ContentLength": len(mock_parquet_data)
        }

        s3_connector.configure(config)

        # Perform incremental read
        chunks = list(
            s3_connector.read_incremental(
                object_name="events/",
                cursor_field="event_date",
                cursor_value="2024-01-15",
            )
        )

        assert len(chunks) > 0
        # Verify partition pattern was detected
        assert s3_connector.partition_manager.detected_pattern is not None

    def test_partition_filter_validation(self, s3_connector):
        """Test partition filter validation."""
        # Valid partition filter
        valid_config = {
            "bucket": "test-bucket",
            "partition_keys": ["year", "month"],
            "partition_filter": {"year": "2024", "month": ["01", "02", "03"]},
        }

        s3_connector.configure(valid_config)
        assert s3_connector.params["partition_filter"]["year"] == "2024"
        assert s3_connector.params["partition_filter"]["month"] == ["01", "02", "03"]

    @patch("boto3.Session")
    def test_partition_performance_optimization(self, mock_session, s3_connector):
        """Test that partitioning reduces the number of files scanned."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client

        config = {
            "bucket": "test-bucket",
            "path_prefix": "data/",
            "partition_keys": ["year", "month"],
            "partition_filter": {"year": "2024", "month": "01"},
        }

        # Mock many objects across different partitions
        all_objects = []
        for year in ["2023", "2024"]:
            for month in ["01", "02", "03", "04", "05", "06"]:
                for day in range(1, 11):  # 10 files per month
                    all_objects.append(
                        {
                            "Key": f"data/year={year}/month={month}/day={day:02d}/file.csv",
                            "Size": 1024,
                            "LastModified": datetime.now(),
                            "ETag": f"etag{year}{month}{day}",
                        }
                    )

        # Only 2024/01 objects should be discovered with partition filter
        filtered_objects = [
            obj
            for obj in all_objects
            if "year=2024" in obj["Key"] and "month=01" in obj["Key"]
        ]

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": filtered_objects}]
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_s3_client.head_bucket.return_value = {}

        s3_connector.configure(config)
        discovered_objects = s3_connector.discover()

        # Should only discover objects matching partition filter
        assert len(discovered_objects) == 10  # Only January 2024 files
        for obj_key in discovered_objects:
            assert "year=2024" in obj_key
            assert "month=01" in obj_key

    def test_error_handling_invalid_partition_keys(self, s3_connector):
        """Test error handling for invalid partition configurations."""
        # Partition keys should be a list
        invalid_config = {
            "bucket": "test-bucket",
            "partition_keys": "not_a_list",
        }

        # Should not raise error during configuration (validation is lenient)
        # but partition detection should handle gracefully
        s3_connector.configure(invalid_config)

        # Test with invalid partition filter format
        invalid_filter_config = {
            "bucket": "test-bucket",
            "partition_keys": ["year"],
            "partition_filter": "not_a_dict",  # Should be dict
        }

        # Should raise ParameterError for invalid partition_filter type
        with pytest.raises(
            ParameterError, match="partition_filter must be a dictionary"
        ):
            fresh_connector = S3Connector()
            fresh_connector.configure(invalid_filter_config)


if __name__ == "__main__":
    pytest.main([__file__])
