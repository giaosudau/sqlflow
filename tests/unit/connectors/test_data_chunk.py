"""Tests for DataChunk class."""

from datetime import datetime

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.connectors.data_chunk import DataChunk


def test_data_chunk_from_arrow():
    """Test creating DataChunk from Arrow Table."""
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(table)
    assert isinstance(chunk.arrow_table, pa.Table)
    assert len(chunk) == 3
    assert chunk.schema == table.schema


def test_data_chunk_from_pandas():
    """Test creating DataChunk from pandas DataFrame."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)
    assert isinstance(chunk.pandas_df, pd.DataFrame)
    assert len(chunk) == 3
    assert list(chunk.schema.names) == ["a", "b"]


def test_data_chunk_from_dict_list():
    """Test creating DataChunk from list of dictionaries."""
    data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    chunk = DataChunk(data)
    assert isinstance(chunk.pandas_df, pd.DataFrame)
    assert len(chunk) == 2
    assert list(chunk.schema.names) == ["a", "b"]


def test_data_chunk_with_schema():
    """Test creating DataChunk with explicit schema."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df, schema=schema)
    assert chunk.schema == schema


def test_data_chunk_invalid_type():
    """Test creating DataChunk with invalid data type."""
    with pytest.raises(TypeError):
        DataChunk("invalid")


def test_data_chunk_vectorized_operations():
    """Test vectorized operations on DataChunk."""
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    chunk = DataChunk(df)

    # Test vectorized operation
    def add_columns(df):
        df["c"] = df["a"] + df["b"]
        return df

    result = chunk.vectorized_operation(add_columns)
    assert "c" in result.pandas_df.columns
    assert list(result.pandas_df["c"]) == [6, 8, 10, 12]

    # Test filter operation
    filtered = chunk.filter(lambda df: df["a"] > 2)
    assert len(filtered) == 2
    assert list(filtered.pandas_df["a"]) == [3, 4]


def test_data_chunk_select_columns():
    """Test column selection in DataChunk."""
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [3, 4]})
    chunk = DataChunk(df)

    # Test selecting existing columns
    selected = chunk.select_columns(["a", "c"])
    assert list(selected.pandas_df.columns) == ["a", "c"]

    # Test selecting non-existent columns
    with pytest.raises(ValueError):
        chunk.select_columns(["d", "e"])


def test_data_chunk_apply_transform():
    """Test applying transformations to DataChunk."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)

    def transform(df):
        df["a"] = df["a"] * 2
        return df

    result = chunk.apply_transform(transform)
    assert list(result.pandas_df["a"]) == [2, 4, 6]


def test_data_chunk_equality():
    """Test equality comparison between DataChunks."""
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df3 = pd.DataFrame({"a": [1, 3], "b": ["x", "y"]})

    chunk1 = DataChunk(df1)
    chunk2 = DataChunk(df2)
    chunk3 = DataChunk(df3)

    assert chunk1 == chunk2
    assert chunk1 != chunk3
    assert chunk1 != "not a DataChunk"


def test_data_chunk_schema_caching():
    """Test schema caching in DataChunk."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)

    # First access should compute schema
    schema1 = chunk.schema
    assert schema1 is not None

    # Second access should use cached schema
    schema2 = chunk.schema
    assert schema2 is schema1


def test_data_chunk_zero_copy_conversion():
    """Test zero-copy conversion between Arrow and pandas."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)

    # Test pandas to Arrow conversion
    arrow_table1 = chunk.arrow_table
    arrow_table2 = chunk.arrow_table
    assert arrow_table1 is arrow_table2  # Should use cached conversion

    # Test Arrow to pandas conversion
    chunk2 = DataChunk(arrow_table1)
    pandas_df1 = chunk2.pandas_df
    pandas_df2 = chunk2.pandas_df
    assert pandas_df1 is pandas_df2  # Should use cached conversion


def test_data_chunk_type_conversion_optimizations():
    """Test optimized type conversions in DataChunk."""
    # Test datetime handling
    df = pd.DataFrame(
        {
            "date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "int": [1, 2],
            "float": [1.5, 2.5],
            "str": ["a", "b"],
        }
    )
    chunk = DataChunk(df)

    # Convert to Arrow and back
    arrow_table = chunk.arrow_table
    result_df = DataChunk(arrow_table).pandas_df

    # Verify types are preserved
    assert result_df["date"].dtype == df["date"].dtype
    assert result_df["int"].dtype == df["int"].dtype
    assert result_df["float"].dtype == df["float"].dtype
    assert result_df["str"].dtype == df["str"].dtype

    # Test conversion caching
    arrow_table2 = chunk.arrow_table
    assert arrow_table2 is arrow_table  # Should use cached conversion

    # Test with different data types
    df2 = pd.DataFrame(
        {
            "bool": [True, False],
            "category": pd.Categorical(["a", "b"]),
            "null": [None, None],
        }
    )
    chunk2 = DataChunk(df2)

    # Convert to Arrow and back
    arrow_table3 = chunk2.arrow_table
    result_df2 = DataChunk(arrow_table3).pandas_df

    # Verify types are handled correctly
    assert result_df2["bool"].dtype == df2["bool"].dtype
    assert result_df2["category"].dtype == df2["category"].dtype
    assert result_df2["null"].isna().all()  # Null values preserved
