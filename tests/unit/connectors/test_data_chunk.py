"""Tests for DataChunk class."""

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.sqlflow.connectors.data_chunk import DataChunk


def test_data_chunk_from_arrow():
    """Test creating DataChunk from Arrow table."""
    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    table = pa.Table.from_pydict(data)

    chunk = DataChunk(table)

    assert chunk.arrow_table == table
    assert len(chunk) == 3
    assert chunk.schema == table.schema

    df = chunk.pandas_df
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 2)
    assert list(df.columns) == ["id", "name"]
    assert list(df["id"]) == [1, 2, 3]
    assert list(df["name"]) == ["a", "b", "c"]


def test_data_chunk_from_pandas():
    """Test creating DataChunk from pandas DataFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    df = pd.DataFrame(data)

    chunk = DataChunk(df)

    assert chunk.pandas_df is df
    assert len(chunk) == 3

    table = chunk.arrow_table
    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert table.num_columns == 2
    assert table.column_names == ["id", "name"]


def test_data_chunk_from_dict_list():
    """Test creating DataChunk from list of dictionaries."""
    data = [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]

    chunk = DataChunk(data)

    assert len(chunk) == 3

    df = chunk.pandas_df
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 2)
    assert list(df.columns) == ["id", "name"]
    assert list(df["id"]) == [1, 2, 3]
    assert list(df["name"]) == ["a", "b", "c"]


def test_data_chunk_with_schema():
    """Test creating DataChunk with explicit schema."""
    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    table = pa.Table.from_pydict(data)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("extra", pa.bool_()),  # Extra field not in data
        ]
    )

    chunk = DataChunk(table, schema=schema)

    assert chunk.schema == schema

    assert chunk.arrow_table == table


def test_data_chunk_invalid_type():
    """Test creating DataChunk with invalid data type."""
    with pytest.raises(TypeError):
        DataChunk("invalid")
