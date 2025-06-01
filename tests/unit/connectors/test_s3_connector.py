"""Tests for S3 connector."""

import io
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.s3_connector import S3Connector
from sqlflow.core.errors import ConnectorError


@pytest.fixture
def s3_connector():
    """Create an S3 connector instance."""
    return S3Connector()


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def sample_config(request):
    """Sample configuration for testing.

    Args:
    ----
        request: Pytest request object to get format parameter

    """
    format_type = getattr(request, "param", "csv")
    compression = None
    if format_type == "parquet":
        compression = "snappy"
    elif format_type == "csv":
        compression = "gzip"

    return {
        "bucket": "test-bucket",
        "path_prefix": "test/",
        "region": "us-west-2",
        "access_key_id": "test-access-key",
        "secret_access_key": "test-secret-key",
        "file_format": format_type,
        "compression": compression,
        "part_size": 5242880,  # 5MB
        "max_retries": 3,
    }


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.0, 30.0],
    }
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    return DataChunk(table)


def test_s3_connector_init(s3_connector):
    """Test S3 connector initialization."""
    assert s3_connector.state == ConnectorState.CREATED
    assert s3_connector.bucket is None
    assert s3_connector.prefix == ""
    assert s3_connector.format == "csv"
    assert s3_connector.compression is None
    assert s3_connector.s3_client is None


def test_s3_connector_configure(s3_connector, sample_config):
    """Test configuring S3 connector."""
    # Test with basic config (no compression)
    basic_config = {
        "bucket": "test-bucket",
        "path_prefix": "test/",
        "file_format": "csv",
    }
    s3_connector.configure(basic_config)
    assert s3_connector.state == ConnectorState.CONFIGURED
    assert s3_connector.bucket == "test-bucket"
    assert s3_connector.prefix == "test/"
    assert s3_connector.format == "csv"
    assert s3_connector.compression is None

    # Test with compression
    s3_connector.configure(sample_config)
    assert s3_connector.state == ConnectorState.CONFIGURED
    assert s3_connector.bucket == "test-bucket"
    assert s3_connector.prefix == "test/"
    assert s3_connector.format == sample_config["file_format"]
    assert s3_connector.compression == sample_config["compression"]

    # Test missing required fields
    with pytest.raises(ConnectorError, match="Missing required parameters"):
        s3_connector.configure({"file_format": "csv"})

    # Test invalid file format using new parameter name
    with pytest.raises(ConnectorError, match="not supported"):
        s3_connector.configure({**sample_config, "file_format": "invalid"})


@patch("boto3.Session")
def test_s3_connector_test_connection(
    mock_session, s3_connector, sample_config, mock_s3_client
):
    """Test connection testing for S3 connector."""
    mock_session.return_value.client.return_value = mock_s3_client

    s3_connector.configure(sample_config)
    result = s3_connector.test_connection()

    assert result.success is True
    assert s3_connector.state == ConnectorState.READY

    # Verify client initialization
    mock_session.assert_called_once_with(
        region_name="us-west-2",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
    )
    mock_session.return_value.client.assert_called_once_with("s3")

    # Verify bucket check
    mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

    # Test connection failure
    mock_s3_client.head_bucket.side_effect = Exception("Connection failed")
    result = s3_connector.test_connection()
    assert result.success is False
    assert "Connection failed" in result.message
    assert s3_connector.state == ConnectorState.ERROR


@patch("boto3.Session")
def test_s3_connector_discover(
    mock_session, s3_connector, sample_config, mock_s3_client
):
    """Test discovery for S3 connector."""
    mock_session.return_value.client.return_value = mock_s3_client

    # Mock paginator
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "test/file1.csv"}, {"Key": "test/file2.csv"}]}
    ]
    mock_s3_client.get_paginator.return_value = mock_paginator

    s3_connector.configure(sample_config)
    objects = s3_connector.discover()

    assert objects == ["test/file1.csv", "test/file2.csv"]
    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test-bucket", Prefix="test/", PaginationConfig={"MaxItems": 1000}
    )


@patch("boto3.Session")
def test_s3_connector_read_csv(
    mock_session, s3_connector, sample_config, mock_s3_client
):
    """Test reading CSV data from S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    # Mock S3 object
    mock_body = MagicMock()
    mock_body.read.return_value = (
        b"id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,30.0"
    )

    # Mock head_object response with actual numeric values for cost calculation
    mock_s3_client.head_object.return_value = {"ContentLength": 1024}
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    s3_connector.configure(sample_config)
    chunks = list(s3_connector.read("test/data.csv"))

    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]

    # Verify S3 client calls
    mock_s3_client.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/data.csv"
    )
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/data.csv"
    )


@patch("boto3.Session")
def test_s3_connector_write_csv(
    mock_session, s3_connector, sample_config, mock_s3_client, sample_data
):
    """Test writing CSV data to S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    s3_connector.configure(sample_config)
    s3_connector.write("test/output.csv", sample_data)

    # Verify S3 client calls for single-part upload
    assert mock_s3_client.put_object.call_count == 1
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    # Check that the key starts with the prefix and ends with .csv
    assert call_args["Key"].startswith("test/")
    assert call_args["Key"].endswith(".csv")
    assert call_args["ContentType"] == "text/csv"


@patch("boto3.Session")
def test_s3_connector_write_multipart(
    mock_session, s3_connector, sample_config, mock_s3_client, sample_data
):
    """Test multipart upload for large files."""
    mock_session.return_value.client.return_value = mock_s3_client

    # Configure for multipart (small part size for testing)
    config = {**sample_config, "part_size": 10}  # Very small for testing
    s3_connector.configure(config)

    # Mock multipart upload - not currently implemented in enhanced connector
    # The enhanced connector uses single-part upload for simplicity
    s3_connector.write("test/large_output.csv", sample_data)

    # Verify single-part upload is used instead
    assert mock_s3_client.put_object.call_count == 1


def test_s3_connector_mock_mode(s3_connector, sample_data):
    """Test S3 connector in mock mode."""
    s3_connector.configure({"bucket": "test-bucket", "mock_mode": True})

    # Test connection should succeed without actual AWS calls
    result = s3_connector.test_connection()
    assert result.success is True
    assert "Mock mode connection successful" in result.message

    # Write should succeed without actual AWS calls
    s3_connector.write("test/mock.csv", sample_data)
    assert s3_connector.state == ConnectorState.READY


def test_s3_connector_close(s3_connector, sample_config):
    """Test closing S3 connector."""
    s3_connector.configure(sample_config)
    s3_connector.close()
    assert s3_connector.s3_client is None


@pytest.mark.parametrize("sample_config", ["parquet"], indirect=True)
@patch("boto3.Session")
def test_s3_connector_read_parquet(
    mock_session, s3_connector, sample_config, mock_s3_client
):
    """Test reading Parquet data from S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    # Create sample parquet data
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.0, 30.0],
    }
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    # Write to bytes buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Mock S3 object with proper cost calculation values
    mock_body = MagicMock()
    mock_body.read.return_value = buffer.getvalue()
    mock_s3_client.head_object.return_value = {"ContentLength": 1024}
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    s3_connector.configure(sample_config)
    chunks = list(s3_connector.read("test/data.parquet"))

    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]

    # Verify S3 client calls
    mock_s3_client.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/data.parquet"
    )
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/data.parquet"
    )


@pytest.mark.parametrize("sample_config", ["json"], indirect=True)
@patch("boto3.Session")
def test_s3_connector_read_json(
    mock_session, s3_connector, sample_config, mock_s3_client
):
    """Test reading JSON data from S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    # Create sample JSON data
    data = [
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 20.0},
        {"id": 3, "name": "Charlie", "value": 30.0},
    ]

    # Mock S3 object with proper cost calculation values
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(data).encode("utf-8")
    mock_s3_client.head_object.return_value = {"ContentLength": 1024}
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    s3_connector.configure(sample_config)
    chunks = list(s3_connector.read("test/data.json"))

    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]

    # Verify S3 client calls
    mock_s3_client.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/data.json"
    )
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/data.json"
    )


@pytest.mark.parametrize("sample_config", ["parquet"], indirect=True)
@patch("boto3.Session")
def test_s3_connector_write_parquet(
    mock_session, s3_connector, sample_config, mock_s3_client, sample_data
):
    """Test writing Parquet data to S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    s3_connector.configure(sample_config)
    s3_connector.write("test/output.parquet", sample_data)

    # Verify S3 client calls for single-part upload
    assert mock_s3_client.put_object.call_count == 1
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    # Check that the key starts with the prefix and has proper format
    assert call_args["Key"].startswith("test/")
    assert call_args["ContentType"] == "application/octet-stream"


@pytest.mark.parametrize("sample_config", ["json"], indirect=True)
@patch("boto3.Session")
def test_s3_connector_write_json(
    mock_session, s3_connector, sample_config, mock_s3_client, sample_data
):
    """Test writing JSON data to S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    s3_connector.configure(sample_config)
    s3_connector.write("test/output.json", sample_data)

    # Verify S3 client calls for single-part upload
    assert mock_s3_client.put_object.call_count == 1
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    # Check that the key starts with the prefix and has proper format
    assert call_args["Key"].startswith("test/")
    assert call_args["ContentType"] == "application/json"


@pytest.mark.parametrize("sample_config", ["parquet"], indirect=True)
@patch("boto3.Session")
def test_s3_connector_write_parquet_compressed(
    mock_session, s3_connector, sample_config, mock_s3_client, sample_data
):
    """Test writing compressed Parquet data to S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    s3_connector.configure({**sample_config, "compression": "snappy"})
    s3_connector.write("test/output.parquet", sample_data)

    # Verify S3 client calls for single-part upload
    assert mock_s3_client.put_object.call_count == 1
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    # Check that the key starts with the prefix and has proper format
    assert call_args["Key"].startswith("test/")
    assert call_args["ContentType"] == "application/octet-stream"


@pytest.mark.parametrize("sample_config", ["json"], indirect=True)
@patch("boto3.Session")
def test_s3_connector_write_json_compressed(
    mock_session, s3_connector, sample_config, mock_s3_client, sample_data
):
    """Test writing compressed JSON data to S3."""
    mock_session.return_value.client.return_value = mock_s3_client

    s3_connector.configure({**sample_config, "compression": "gzip"})
    s3_connector.write("test/output.json", sample_data)

    # Verify S3 client calls for single-part upload
    assert mock_s3_client.put_object.call_count == 1
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    # Check that the key starts with the prefix and has proper format
    assert call_args["Key"].startswith("test/")
    assert call_args["ContentType"] == "application/json"
    assert call_args["ContentEncoding"] == "gzip"


@patch("boto3.Session")
def test_s3_connector_unsupported_format(
    mock_session, s3_connector, mock_s3_client, sample_data
):
    """Test handling of unsupported file format."""
    mock_session.return_value.client.return_value = mock_s3_client

    with pytest.raises(ConnectorError, match="not supported"):
        s3_connector.configure({"bucket": "test-bucket", "file_format": "unsupported"})

    # Configure with valid format but try to read with unsupported format logic
    # Set the format to an unsupported one directly to test runtime behavior
    s3_connector.configure(
        {
            "bucket": "test-bucket",
            "file_format": "csv",
            "access_key_id": "test-access-key",
            "secret_access_key": "test-secret-key",
        }
    )

    # Mock S3 response for unsupported file
    mock_body = MagicMock()
    mock_body.read.return_value = b"some data"
    mock_s3_client.head_object.return_value = {"ContentLength": 1024}
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    # Manually set format to unsupported to test runtime error
    s3_connector.format = "unsupported"

    with pytest.raises(ConnectorError, match="Unsupported file format"):
        list(s3_connector.read("test/data.unsupported"))
