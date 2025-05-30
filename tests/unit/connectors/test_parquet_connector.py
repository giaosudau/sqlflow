"""Tests for Parquet connector."""

import os
import tempfile
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

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)

    yield f.name

    os.unlink(f.name)


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


def test_parquet_connector_write():
    """Test writing data with Parquet connector."""
    connector = ParquetConnector()

    data = {
        "id": [10, 20, 30],
        "name": ["X", "Y", "Z"],
        "value": [1.1, 2.2, 3.3],
    }
    table = pa.Table.from_pydict(data)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        output_path = f.name

    try:
        connector.configure({"path": output_path})

        chunk = DataChunk(table)
        connector.write("any_name", chunk)

        written_table = pq.read_table(output_path)
        assert written_table.num_rows == 3
        assert written_table.num_columns == 3
        assert written_table.column_names == ["id", "name", "value"]

        connector.write("any_name", chunk, mode="append")

        appended_table = pq.read_table(output_path)
        assert appended_table.num_rows == 6  # 3 original + 3 appended

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_parquet_connector_filters(sample_parquet_file):
    """Test filtering data with Parquet connector."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        "value": [10.5, 20.3, 30.1, 40.7, 50.9],
    }
    table = pa.Table.from_pydict(data)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        filter_test_file = f.name
        pq.write_table(table, filter_test_file)

    try:
        connector = ParquetConnector()
        connector.configure({"path": filter_test_file})

        filters = {"id": 3}
        chunks = list(connector.read("any_name", filters=filters))
        assert len(chunks) == 1
        chunk = chunks[0]
        df = chunk.pandas_df
        assert df.shape == (1, 3)
        assert list(df["id"]) == [3]
        assert list(df["name"]) == ["Charlie"]

        filters = {"value": {">": 30.0}}
        chunks = list(connector.read("any_name", filters=filters))
        assert len(chunks) == 1
        chunk = chunks[0]
        df = chunk.pandas_df
        assert df.shape == (
            3,
            3,
        )  # Matches rows 3, 4, and 5 due to floating point comparison
        assert set(df["id"]) == {3, 4, 5}
        assert set(df["name"]) == {"Charlie", "Dave", "Eve"}

    finally:
        if os.path.exists(filter_test_file):
            os.unlink(filter_test_file)
