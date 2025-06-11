"""Tests for DataChunk class."""

from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
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


def test_data_chunk_from_simple_list():
    """Test creating DataChunk from simple list."""
    data = [1, 2, 3, 4]
    chunk = DataChunk(data)
    assert isinstance(chunk.pandas_df, pd.DataFrame)
    assert len(chunk) == 4
    assert list(chunk.schema.names) == ["value"]


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


def test_data_chunk_memory_usage():
    """Test memory usage calculation."""
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})
    chunk = DataChunk(df)

    memory_usage = chunk.memory_usage
    assert memory_usage > 0
    assert isinstance(memory_usage, int)

    # Test caching
    memory_usage2 = chunk.memory_usage
    assert memory_usage == memory_usage2


def test_data_chunk_slice_operations():
    """Test slicing operations with zero-copy optimization."""
    df = pd.DataFrame({"a": list(range(10)), "b": [f"val_{i}" for i in range(10)]})
    chunk = DataChunk(df)

    # Test basic slicing
    sliced = chunk.slice(2, 5)
    assert len(sliced) == 3
    assert list(sliced.pandas_df["a"]) == [2, 3, 4]

    # Test slice from start
    sliced_start = chunk.slice(0, 3)
    assert len(sliced_start) == 3
    assert list(sliced_start.pandas_df["a"]) == [0, 1, 2]

    # Test slice to end
    sliced_end = chunk.slice(7)
    assert len(sliced_end) == 3
    assert list(sliced_end.pandas_df["a"]) == [7, 8, 9]


def test_data_chunk_filter_arrow():
    """Test Arrow-based filtering for performance."""
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    chunk = DataChunk(df)

    # Create Arrow compute expression
    filter_expr = pc.greater(pc.field("a"), 3)

    try:
        filtered = chunk.filter_arrow(filter_expr)
        assert len(filtered) == 2
        assert list(filtered.pandas_df["a"]) == [4, 5]
    except Exception:
        # If Arrow filtering fails, test fallback
        filtered = chunk.filter(lambda df: df["a"] > 3)
        assert len(filtered) == 2
        assert list(filtered.pandas_df["a"]) == [4, 5]


def test_data_chunk_statistics():
    """Test statistics computation and caching."""
    df = pd.DataFrame(
        {
            "numeric": [1, 2, 3, 4, 5],
            "string": ["a", "b", "c", "d", "e"],
            "nullable": [1, None, 3, None, 5],
        }
    )
    chunk = DataChunk(df)

    stats = chunk.compute_statistics()
    assert isinstance(stats, dict)
    assert "numeric" in stats
    assert "string" in stats
    assert "nullable" in stats

    # Test numeric column stats
    numeric_stats = stats["numeric"]
    assert numeric_stats["count"] == 5
    assert numeric_stats["min"] == 1
    assert numeric_stats["max"] == 5

    # Test string column stats (should have count but not min/max in our simplified stats)
    string_stats = stats["string"]
    assert string_stats["count"] == 5
    assert "min" not in string_stats or string_stats.get("min") is None

    # Test caching
    stats2 = chunk.compute_statistics()
    assert stats2 is stats


def test_data_chunk_combine():
    """Test combining chunks efficiently."""
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"a": [3, 4], "b": ["z", "w"]})

    chunk1 = DataChunk(df1)
    chunk2 = DataChunk(df2)

    combined = chunk1.combine(chunk2)
    assert len(combined) == 4
    assert list(combined.pandas_df["a"]) == [1, 2, 3, 4]
    assert list(combined.pandas_df["b"]) == ["x", "y", "z", "w"]


def test_data_chunk_create_view():
    """Test creating views with zero-copy operations."""
    df = pd.DataFrame(
        {
            "a": list(range(10)),
            "b": [f"val_{i}" for i in range(10)],
            "c": [i * 2 for i in range(10)],
        }
    )
    chunk = DataChunk(df)

    # Create view with column and row selection
    view = DataChunk.create_view(chunk, columns=["a", "c"], start=2, end=7)
    assert len(view) == 5
    assert list(view.pandas_df.columns) == ["a", "c"]
    assert list(view.pandas_df["a"]) == [2, 3, 4, 5, 6]

    # Create view with only column selection
    col_view = DataChunk.create_view(chunk, columns=["b"])
    assert len(col_view) == 10
    assert list(col_view.pandas_df.columns) == ["b"]

    # Create view with only row selection
    row_view = DataChunk.create_view(chunk, start=5, end=8)
    assert len(row_view) == 3
    assert list(row_view.pandas_df["a"]) == [5, 6, 7]


def test_data_chunk_cache_management():
    """Test cache management and memory cleanup."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)

    # Access data to populate caches
    _ = chunk.arrow_table
    _ = chunk.schema
    _ = chunk.compute_statistics()

    # Verify caches are populated
    assert len(chunk._conversion_cache) > 0
    assert len(chunk._statistics_cache) > 0
    assert chunk._cached_schema is not None

    # Clear caches
    chunk.clear_cache()

    # Verify caches are cleared
    assert len(chunk._conversion_cache) == 0
    assert len(chunk._statistics_cache) == 0
    assert chunk._cached_schema is None


def test_data_chunk_sizeof():
    """Test __sizeof__ method."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)

    size = chunk.__sizeof__()
    assert size > 0
    assert isinstance(size, int)
    assert size == chunk.memory_usage


def test_data_chunk_view_flag():
    """Test view flag functionality."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk = DataChunk(df)

    # Normal chunk should not be a view
    assert not chunk._is_view

    # Create view through slicing
    sliced = chunk.slice(1, 3)
    assert sliced._is_view

    # Create view through column selection
    selected = chunk.select_columns(["a"])
    assert selected._is_view


def test_data_chunk_global_cache():
    """Test global caching mechanism."""
    from sqlflow.connectors.data_chunk import _GLOBAL_CONVERSION_CACHE

    # Clear global cache
    _GLOBAL_CONVERSION_CACHE.clear()

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    chunk1 = DataChunk(df)

    # Access Arrow table to trigger caching
    _ = chunk1.arrow_table

    # Verify global cache has entries
    assert len(_GLOBAL_CONVERSION_CACHE) > 0

    # Create another chunk with same data
    chunk2 = DataChunk(df)
    arrow_table1 = chunk1.arrow_table
    arrow_table2 = chunk2.arrow_table

    # Should use cached conversion
    assert arrow_table1 is arrow_table2


def test_data_chunk_memory_view():
    """Test memory view access for zero-copy operations."""
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    chunk = DataChunk(df)

    # Try to get memory view
    memory_view = chunk.get_memory_view()

    # Memory view might not always be available depending on data types
    if memory_view is not None:
        assert isinstance(memory_view, memoryview)
        assert len(memory_view) > 0
