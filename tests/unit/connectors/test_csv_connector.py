"""Tests for CSV connector."""

import os

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.csv_connector import CSVConnector
from sqlflow.core.errors import ConnectorError


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.csv"
    data = "id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,30.0"
    file_path.write_text(data)
    return str(file_path)


@pytest.fixture
def csv_connector():
    """Create a CSV connector instance."""
    return CSVConnector()


def test_csv_connector_init():
    """Test CSV connector initialization."""
    connector = CSVConnector()
    assert connector.state == ConnectorState.CREATED
    assert connector.path is None
    assert connector.delimiter == ","
    assert connector.has_header is True
    assert connector.quote_char == '"'
    assert connector.encoding == "utf-8"


def test_csv_connector_configure(csv_connector, sample_csv_file):
    """Test configuring CSV connector."""
    # Test default configuration
    csv_connector.configure({"path": sample_csv_file})
    assert csv_connector.state == ConnectorState.CONFIGURED
    assert csv_connector.path == sample_csv_file
    assert csv_connector.delimiter == ","
    assert csv_connector.has_header is True

    # Test custom configuration
    connector = CSVConnector()
    connector.configure(
        {
            "path": sample_csv_file,
            "delimiter": ";",
            "has_header": False,
            "quote_char": "'",
            "encoding": "latin-1",
        }
    )
    assert connector.state == ConnectorState.CONFIGURED
    assert connector.path == sample_csv_file
    assert connector.delimiter == ";"
    assert connector.has_header is False
    assert connector.quote_char == "'"
    assert connector.encoding == "latin-1"

    # Test missing path
    with pytest.raises(ConnectorError, match="Path is required"):
        csv_connector.configure({})


def test_csv_connector_test_connection(csv_connector, sample_csv_file):
    """Test connection testing for CSV connector."""
    # Test successful connection
    csv_connector.configure({"path": sample_csv_file})
    result = csv_connector.test_connection()
    assert result.success is True
    assert csv_connector.state == ConnectorState.READY

    # Test nonexistent file
    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    result = connector.test_connection()
    assert result.success is False
    assert "not found" in result.message
    assert connector.state == ConnectorState.ERROR

    # Test unconfigured connector
    connector = CSVConnector()
    with pytest.raises(ConnectorError, match="Invalid state"):
        connector.test_connection()


def test_csv_connector_discover(csv_connector, sample_csv_file):
    """Test discovery for CSV connector."""
    csv_connector.configure({"path": sample_csv_file})
    objects = csv_connector.discover()
    assert len(objects) == 1
    assert objects[0] == "test"  # Based on filename

    # Test nonexistent file
    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    with pytest.raises(ConnectorError, match="not found"):
        connector.discover()


def test_csv_connector_get_schema(csv_connector, sample_csv_file):
    """Test schema retrieval for CSV connector."""
    csv_connector.configure({"path": sample_csv_file})
    schema = csv_connector.get_schema("test")

    # Verify schema fields
    assert len(schema.arrow_schema) == 3
    fields = schema.arrow_schema.names
    assert fields == ["id", "name", "value"]

    # Test with no header
    connector = CSVConnector()
    connector.configure({"path": sample_csv_file, "has_header": False})
    schema = connector.get_schema("test")
    assert len(schema.arrow_schema) == 3


def test_csv_connector_read(csv_connector, sample_csv_file):
    """Test reading data from CSV connector."""
    csv_connector.configure({"path": sample_csv_file})
    chunks = list(csv_connector.read("test"))
    assert len(chunks) == 1  # Small file, single chunk

    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]
    assert list(df["id"]) == ["1", "2", "3"]
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    # Test reading specific columns
    chunks = list(csv_connector.read("test", columns=["name"]))
    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 1)
    assert list(df.columns) == ["name"]
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    # Test nonexistent file
    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    with pytest.raises(ConnectorError, match="not found"):
        list(connector.read("test"))


def test_csv_connector_write(csv_connector, tmp_path):
    """Test writing data to CSV."""
    output_file = str(tmp_path / "output.csv")
    csv_connector.configure({"path": output_file})

    # Create test data
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.0, 30.0],
    }
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    from sqlflow.connectors.data_chunk import DataChunk

    chunk = DataChunk(table)

    # Test write
    csv_connector.write("test", chunk)
    assert os.path.exists(output_file)

    # Verify written data
    read_df = pd.read_csv(output_file)
    assert read_df.shape == (3, 3)
    assert list(read_df.columns) == ["id", "name", "value"]
    assert list(read_df["name"]) == ["Alice", "Bob", "Charlie"]

    # Test append mode
    csv_connector.write("test", chunk, mode="append")
    read_df = pd.read_csv(output_file)
    assert read_df.shape == (6, 3)  # Original 3 rows + 3 new rows

    # Test write with custom delimiter
    output_file2 = str(tmp_path / "output2.csv")
    connector = CSVConnector()
    connector.configure(
        {
            "path": output_file2,
            "delimiter": ";",
            "quote_char": "'",
            "encoding": "latin-1",
        }
    )
    connector.write("test", chunk)

    # Verify custom format
    with open(output_file2, "r") as f:
        content = f.read()
        assert ";" in content  # Custom delimiter used
