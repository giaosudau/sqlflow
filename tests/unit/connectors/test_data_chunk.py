"""Tests for DataChunk class."""

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.connectors.data_chunk import DataChunk


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


def test_data_chunk_vectorized_operations():
    """Test vectorized operations on DataChunk."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "value": [10, 20, 30, 40, 50],
    }
    chunk = DataChunk(pd.DataFrame(data))

    # Test vectorized operation
    def double_values(df):
        df["value"] = df["value"] * 2
        return df

    result = chunk.vectorized_operation(double_values)
    assert list(result.pandas_df["value"]) == [20, 40, 60, 80, 100]

    # Test filter operation
    def filter_condition(df):
        return df["value"] > 30

    filtered = chunk.filter(filter_condition)
    assert len(filtered) == 2
    assert list(filtered.pandas_df["id"]) == [4, 5]
    assert list(filtered.pandas_df["value"]) == [40, 50]


def test_data_chunk_select_columns():
    """Test column selection in DataChunk."""
    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "value": [10, 20, 30],
    }
    chunk = DataChunk(pd.DataFrame(data))

    # Select existing columns
    selected = chunk.select_columns(["id", "value"])
    assert list(selected.pandas_df.columns) == ["id", "value"]
    assert len(selected) == 3

    # Try to select non-existent column
    with pytest.raises(ValueError):
        chunk.select_columns(["nonexistent"])


def test_data_chunk_apply_transform():
    """Test applying transformations to DataChunk."""
    data = {
        "id": [1, 2, 3],
        "value": [10, 20, 30],
    }
    chunk = DataChunk(pd.DataFrame(data))

    def transform(df):
        df["value_squared"] = df["value"] ** 2
        return df

    result = chunk.apply_transform(transform)
    assert "value_squared" in result.pandas_df.columns
    assert list(result.pandas_df["value_squared"]) == [100, 400, 900]


def test_data_chunk_equality():
    """Test DataChunk equality comparison."""
    data1 = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    data2 = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    data3 = {
        "id": [1, 2, 4],
        "name": ["a", "b", "d"],
    }

    chunk1 = DataChunk(pd.DataFrame(data1))
    chunk2 = DataChunk(pd.DataFrame(data2))
    chunk3 = DataChunk(pd.DataFrame(data3))

    assert chunk1 == chunk2
    assert chunk1 != chunk3
    assert chunk1 != "not a DataChunk"


def test_data_chunk_schema_caching():
    """Test schema caching in DataChunk."""
    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    chunk = DataChunk(pd.DataFrame(data))

    # First access should compute schema
    schema1 = chunk.schema
    assert schema1 is not None

    # Second access should use cached schema
    schema2 = chunk.schema
    assert schema2 is schema1


def test_data_chunk_zero_copy_conversion():
    """Test zero-copy conversion between Arrow and pandas."""
    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    df = pd.DataFrame(data)
    chunk = DataChunk(df)

    # Convert to Arrow and back
    arrow_table = chunk.arrow_table
    chunk2 = DataChunk(arrow_table)
    df2 = chunk2.pandas_df

    # Verify data integrity
    pd.testing.assert_frame_equal(df, df2)
