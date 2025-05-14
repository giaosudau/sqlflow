"""Tests for CSV connector."""

import os
import tempfile
from typing import Generator

import pandas as pd
import pytest

from sqlflow.sqlflow.connectors.base import ConnectorState
from sqlflow.sqlflow.connectors.csv_connector import CSVConnector
from sqlflow.sqlflow.connectors.data_chunk import DataChunk
from sqlflow.sqlflow.core.errors import ConnectorError


@pytest.fixture
def sample_csv_file() -> Generator[str, None, None]:
    """Create a sample CSV file for testing.

    Yields:
        Path to the sample CSV file
    """
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        f.write(b"id,name,value\n")
        f.write(b"1,Alice,10.5\n")
        f.write(b"2,Bob,20.3\n")
        f.write(b"3,Charlie,30.1\n")

    yield f.name

    os.unlink(f.name)


def test_csv_connector_configure(sample_csv_file):
    """Test configuring CSV connector."""
    connector = CSVConnector()
    assert connector.state == ConnectorState.CREATED

    connector.configure({"path": sample_csv_file})
    assert connector.state == ConnectorState.CONFIGURED
    assert connector.path == sample_csv_file
    assert connector.delimiter == ","  # Default
    assert connector.has_header is True  # Default

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

    connector = CSVConnector()
    with pytest.raises(ConnectorError):
        connector.configure({})
    assert connector.state == ConnectorState.ERROR


def test_csv_connector_test_connection(sample_csv_file):
    """Test connection testing for CSV connector."""
    connector = CSVConnector()
    connector.configure({"path": sample_csv_file})

    result = connector.test_connection()
    assert result.success is True
    assert connector.state == ConnectorState.READY

    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    result = connector.test_connection()
    assert result.success is False
    assert "not found" in result.message
    assert connector.state == ConnectorState.ERROR


def test_csv_connector_discover(sample_csv_file):
    """Test discovery for CSV connector."""
    connector = CSVConnector()
    connector.configure({"path": sample_csv_file})

    objects = connector.discover()
    assert len(objects) == 1

    expected_name = os.path.splitext(os.path.basename(sample_csv_file))[0]
    assert objects[0] == expected_name

    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    with pytest.raises(ConnectorError):
        connector.discover()


def test_csv_connector_get_schema(sample_csv_file):
    """Test schema retrieval for CSV connector."""
    connector = CSVConnector()
    connector.configure({"path": sample_csv_file})

    schema = connector.get_schema("any_name")  # Name is ignored for CSV

    assert schema.arrow_schema.names == ["id", "name", "value"]

    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    with pytest.raises(ConnectorError):
        connector.get_schema("any_name")


def test_csv_connector_read(sample_csv_file):
    """Test reading data from CSV connector."""
    connector = CSVConnector()
    connector.configure({"path": sample_csv_file})

    chunks = list(connector.read("any_name"))  # Name is ignored for CSV
    assert len(chunks) == 1  # Small file, so only one chunk

    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]
    assert list(df["id"]) == ["1", "2", "3"]  # CSV reads as strings by default
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    chunks = list(connector.read("any_name", columns=["name"]))
    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 1)
    assert list(df.columns) == ["name"]
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    connector = CSVConnector()
    connector.configure({"path": "/nonexistent/file.csv"})
    with pytest.raises(ConnectorError):
        list(connector.read("any_name"))


def test_csv_connector_write():
    """Test writing data with CSV connector."""
    connector = CSVConnector()

    data = pd.DataFrame(
        {
            "id": [10, 20, 30],
            "name": ["X", "Y", "Z"],
            "value": [1.1, 2.2, 3.3],
        }
    )

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        output_path = f.name

    try:
        connector.configure({"path": output_path})

        chunk = DataChunk(data)
        connector.write("any_name", chunk)

        written_data = pd.read_csv(output_path)
        assert written_data.shape == (3, 3)
        assert list(written_data.columns) == ["id", "name", "value"]
        assert list(written_data["id"]) == [10, 20, 30]
        assert list(written_data["name"]) == ["X", "Y", "Z"]
        assert list(written_data["value"]) == [1.1, 2.2, 3.3]

        connector.write("any_name", chunk, mode="append")

        appended_data = pd.read_csv(output_path)
        assert appended_data.shape == (6, 3)  # 3 original + 3 appended

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)
