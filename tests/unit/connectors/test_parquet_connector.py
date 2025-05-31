"""Tests for Parquet connector."""

import os
import tempfile
import time
import uuid
from typing import Generator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.parquet_connector import ParquetConnector
from sqlflow.core.errors import ConnectorError


@pytest.fixture
def sample_parquet_file() -> Generator[str, None, None]:
    """Create a sample Parquet file for testing.

    Yields
    ------
        Path to the sample Parquet file

    """
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.3, 30.1],
    }
    table = pa.Table.from_pydict(data)

    # Create a unique filename to avoid conflicts in parallel tests
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(
        temp_dir, f"test_parquet_{unique_id}_{int(time.time())}.parquet"
    )

    try:
        pq.write_table(table, file_path)
        yield file_path
    finally:
        # Clean up with retry logic for parallel test scenarios
        if os.path.exists(file_path):
            try:
                os.unlink(file_path)
            except (OSError, PermissionError):
                # In parallel tests, sometimes file is still being accessed
                # Just log and continue - temp files will eventually be cleaned up
                pass


@pytest.mark.serial
def test_parquet_connector_configure(sample_parquet_file):
    """Test configuring Parquet connector."""
    connector = ParquetConnector()
    assert connector.state == ConnectorState.CREATED

    connector.configure({"path": sample_parquet_file})
    assert connector.state == ConnectorState.CONFIGURED
    assert connector.path == sample_parquet_file
    assert connector.use_memory_map is True  # Default

    connector = ParquetConnector()
    connector.configure(
        {
            "path": sample_parquet_file,
            "use_memory_map": False,
        }
    )
    assert connector.state == ConnectorState.CONFIGURED
    assert connector.path == sample_parquet_file
    assert connector.use_memory_map is False

    connector = ParquetConnector()
    with pytest.raises(ConnectorError):
        connector.configure({})
    assert connector.state == ConnectorState.ERROR


@pytest.mark.serial
def test_parquet_connector_test_connection(sample_parquet_file):
    """Test connection testing for Parquet connector."""
    connector = ParquetConnector()
    connector.configure({"path": sample_parquet_file})

    result = connector.test_connection()
    assert result.success is True
    assert connector.state == ConnectorState.READY

    connector = ParquetConnector()
    connector.configure({"path": "/nonexistent/file.parquet"})
    result = connector.test_connection()
    assert result.success is False
    assert result.message is not None and len(result.message) > 0
    assert connector.state == ConnectorState.ERROR


@pytest.mark.serial
def test_parquet_connector_discover(sample_parquet_file):
    """Test discovery for Parquet connector."""
    connector = ParquetConnector()
    connector.configure({"path": sample_parquet_file})

    objects = connector.discover()
    assert len(objects) == 1

    expected_name = os.path.splitext(os.path.basename(sample_parquet_file))[0]
    assert objects[0] == expected_name

    connector = ParquetConnector()
    connector.configure({"path": "/nonexistent/file.parquet"})
    with pytest.raises(ConnectorError):
        connector.discover()


@pytest.mark.serial
def test_parquet_connector_get_schema(sample_parquet_file):
    """Test schema retrieval for Parquet connector."""
    connector = ParquetConnector()
    connector.configure({"path": sample_parquet_file})

    schema = connector.get_schema("any_name")  # Name is ignored for Parquet

    assert schema.arrow_schema.names == ["id", "name", "value"]

    connector = ParquetConnector()
    connector.configure({"path": "/nonexistent/file.parquet"})
    with pytest.raises(ConnectorError):
        connector.get_schema("any_name")


@pytest.mark.serial
def test_parquet_connector_read(sample_parquet_file):
    """Test reading data from Parquet connector."""
    connector = ParquetConnector()
    connector.configure({"path": sample_parquet_file})

    chunks = list(connector.read("any_name"))  # Name is ignored for Parquet
    assert len(chunks) == 1  # Small file, so only one chunk

    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]
    assert list(df["id"]) == [1, 2, 3]
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    chunks = list(connector.read("any_name", columns=["name"]))
    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 1)
    assert list(df.columns) == ["name"]
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    connector = ParquetConnector()
    connector.configure({"path": "/nonexistent/file.parquet"})
    with pytest.raises(ConnectorError):
        list(connector.read("any_name"))


@pytest.mark.serial
def test_parquet_connector_write():
    """Test writing data with Parquet connector."""
    connector = ParquetConnector()

    data = {
        "id": [10, 20, 30],
        "name": ["X", "Y", "Z"],
        "value": [1.1, 2.2, 3.3],
    }
    table = pa.Table.from_pydict(data)

    # Create unique filename for parallel test safety
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = tempfile.gettempdir()
    output_path = os.path.join(
        temp_dir, f"test_write_{unique_id}_{int(time.time())}.parquet"
    )

    try:
        connector.configure({"path": output_path})

        chunk = DataChunk(table)
        connector.write("any_name", chunk)

        written_table = pq.read_table(output_path)
        assert written_table.num_rows == 3
        assert written_table.column_names == ["id", "name", "value"]
    finally:
        if os.path.exists(output_path):
            os.remove(output_path)


@pytest.mark.serial
def test_parquet_connector_filters(sample_parquet_file):
    """Test filtering data with Parquet connector."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        "value": [10.5, 20.3, 30.1, 40.7, 50.9],
    }
    table = pa.Table.from_pydict(data)

    # Create unique filename for parallel test safety
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = tempfile.gettempdir()
    filter_test_file = os.path.join(
        temp_dir, f"test_filter_{unique_id}_{int(time.time())}.parquet"
    )

    try:
        pq.write_table(table, filter_test_file)

        connector = ParquetConnector()
        connector.configure({"path": filter_test_file})

        filters = {"id": 3}
        chunks = list(connector.read("test", filters=filters))
        assert len(chunks) == 1
        chunk = chunks[0]
        df = chunk.pandas_df
        assert len(df) == 1
        assert df["id"].iloc[0] == 3
    finally:
        if os.path.exists(filter_test_file):
            os.remove(filter_test_file)
