"""Test S3 connector URI parsing and write functionality.

This module tests the S3 connector's ability to parse S3 URIs correctly
and write files to the intended destinations, covering the fixes for
proper destination handling in export operations.
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.s3_connector import S3Connector
from sqlflow.core.errors import ConnectorError


class TestS3ConnectorUriParsing:
    """Test class for S3 connector URI parsing and write functionality."""

    def test_parse_s3_uri_with_bucket_and_key(self):
        """Test parsing a complete S3 URI with both bucket and key.

        This ensures that URIs like 's3://bucket-name/path/to/file.csv'
        are correctly split into bucket and key components.
        """
        connector = S3Connector()

        uri = "s3://test-bucket/exports/data.csv"
        bucket, key = connector._parse_s3_uri(uri)

        assert bucket == "test-bucket"
        assert key == "exports/data.csv"

    def test_parse_s3_uri_with_nested_path(self):
        """Test parsing S3 URI with nested directory structure.

        This ensures that complex paths with multiple directory levels
        are correctly handled.
        """
        connector = S3Connector()

        uri = "s3://analytics-bucket/reports/2024/january/sales_summary.parquet"
        bucket, key = connector._parse_s3_uri(uri)

        assert bucket == "analytics-bucket"
        assert key == "reports/2024/january/sales_summary.parquet"

    def test_parse_s3_uri_bucket_only(self):
        """Test parsing S3 URI with only bucket name.

        This handles cases where the URI specifies only the bucket
        without a specific key.
        """
        connector = S3Connector()

        uri = "s3://test-bucket"
        bucket, key = connector._parse_s3_uri(uri)

        assert bucket == "test-bucket"
        assert key == ""

    def test_parse_s3_uri_bucket_with_trailing_slash(self):
        """Test parsing S3 URI with bucket and trailing slash.

        This ensures that URIs with trailing slashes are handled correctly.
        """
        connector = S3Connector()

        uri = "s3://test-bucket/"
        bucket, key = connector._parse_s3_uri(uri)

        assert bucket == "test-bucket"
        assert key == ""

    def test_parse_non_s3_uri_with_configured_bucket(self):
        """Test parsing non-S3 URI when connector has a configured bucket.

        This tests the fallback behavior when a simple key is provided
        instead of a full S3 URI.
        """
        connector = S3Connector()
        connector.bucket = "default-bucket"

        key_only = "exports/report.csv"
        bucket, key = connector._parse_s3_uri(key_only)

        assert bucket == "default-bucket"
        assert key == "exports/report.csv"

    def test_parse_non_s3_uri_without_configured_bucket(self):
        """Test parsing non-S3 URI when no bucket is configured.

        This should raise an error since we need either a full S3 URI
        or a configured default bucket.
        """
        connector = S3Connector()
        connector.bucket = None  # No default bucket

        key_only = "exports/report.csv"

        with pytest.raises(ConnectorError) as exc_info:
            connector._parse_s3_uri(key_only)

        assert "No bucket configured" in str(exc_info.value)

    def test_write_with_s3_uri_uses_parsed_bucket_and_key(self):
        """Test that write method correctly uses parsed bucket and key from S3 URI.

        This verifies the fix where the destination URI is properly parsed
        and used instead of generating random keys.
        """
        connector = S3Connector()
        connector.mock_mode = False

        # Mock the S3 client and export method
        with patch.object(connector, "s3_client") as mock_s3_client:
            with patch.object(connector, "_export_data") as mock_export_data:
                # Create test data
                test_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
                data_chunk = DataChunk(test_df)

                # Test S3 URI
                s3_uri = "s3://test-bucket/exports/test_file.csv"

                # Configure connector with different default bucket
                connector.bucket = "different-bucket"

                # Call write method
                connector.write(s3_uri, data_chunk)

                # Verify that _export_data was called with the correct key
                mock_export_data.assert_called_once_with(
                    data_chunk, "exports/test_file.csv"
                )

                # Verify that the bucket was temporarily switched
                # (The method should have restored the original bucket after export)
                assert connector.bucket == "different-bucket"

    def test_write_with_s3_uri_preserves_original_bucket(self):
        """Test that write method preserves the original configured bucket.

        This ensures that temporary bucket switching for URI-based exports
        doesn't permanently change the connector configuration.
        """
        connector = S3Connector()
        connector.mock_mode = False
        original_bucket = "original-bucket"
        connector.bucket = original_bucket

        with patch.object(connector, "s3_client"):
            with patch.object(connector, "_export_data") as mock_export_data:
                test_df = pd.DataFrame({"id": [1], "value": ["test"]})
                data_chunk = DataChunk(test_df)

                s3_uri = "s3://temporary-bucket/test.csv"

                connector.write(s3_uri, data_chunk)

                # Verify original bucket is preserved
                assert connector.bucket == original_bucket

    def test_write_with_mock_mode_logs_correctly(self):
        """Test that mock mode logging includes the correct destination information.

        This ensures that when in mock mode, the logging provides useful
        information about what would have been written where.
        """
        connector = S3Connector()
        connector.mock_mode = True

        test_df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        data_chunk = DataChunk(test_df)

        s3_uri = "s3://mock-bucket/mock-file.csv"

        with patch("sqlflow.connectors.s3_connector.logger") as mock_logger:
            connector.write(s3_uri, data_chunk)

            # Verify that debug logging was called with correct information
            mock_logger.debug.assert_called_with(
                "[MOCK] Would write 3 rows to s3://mock-bucket/mock-file.csv"
            )

    def test_write_handles_export_data_errors_gracefully(self):
        """Test that write method handles _export_data errors properly.

        This ensures that errors in the actual export process are wrapped
        and reported with useful context about the destination.
        """
        connector = S3Connector()
        connector.mock_mode = False
        connector.bucket = "test-bucket"

        test_df = pd.DataFrame({"id": [1], "value": ["test"]})
        data_chunk = DataChunk(test_df)

        with patch.object(connector, "s3_client"):
            with patch.object(
                connector, "_export_data", side_effect=Exception("Export failed")
            ):
                s3_uri = "s3://test-bucket/failing-export.csv"

                with pytest.raises(ConnectorError) as exc_info:
                    connector.write(s3_uri, data_chunk)

                assert "Write failed" in str(exc_info.value)
                assert "Export failed" in str(exc_info.value)

    def test_write_with_legacy_non_uri_behavior(self):
        """Test that write method maintains backward compatibility for non-URI destinations.

        This ensures that existing code using simple filenames still works
        with the UUID-based key generation.
        """
        connector = S3Connector()
        connector.mock_mode = False
        connector.bucket = "default-bucket"
        connector.prefix = "exports"
        connector.format = "csv"
        connector.filename_template = "{prefix}/{uuid}.{format}"

        test_df = pd.DataFrame({"id": [1], "value": ["test"]})
        data_chunk = DataChunk(test_df)

        with patch.object(connector, "s3_client"):
            with patch.object(connector, "_export_data") as mock_export_data:
                with patch(
                    "uuid.uuid4", return_value=Mock(spec=["__str__"])
                ) as mock_uuid:
                    mock_uuid.return_value.__str__ = Mock(return_value="test-uuid-123")

                    # Non-S3 URI destination
                    simple_filename = "simple_export.csv"

                    connector.write(simple_filename, data_chunk)

                    # Should have generated a UUID-based key
                    expected_key = "exports/test-uuid-123.csv"
                    mock_export_data.assert_called_once_with(data_chunk, expected_key)
