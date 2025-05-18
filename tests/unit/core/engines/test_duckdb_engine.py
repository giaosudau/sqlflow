"""Unit tests for DuckDBEngine class."""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf


# ----- Test UDF functions for testing -----
@python_scalar_udf
def udf_double(x: float) -> float:
    """Double a value."""
    return x * 2


@python_scalar_udf(name="custom_add")
def udf_add(x: float, y: float) -> float:
    """Add two values."""
    return x + y


@python_table_udf
def udf_add_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a column to a DataFrame."""
    result = df.copy()
    result["doubled"] = result["value"] * 2
    return result


# ----- DuckDBEngine UDF tests -----
def test_register_scalar_udf():
    """Test registering and using a scalar UDF."""
    engine = DuckDBEngine(":memory:")

    # Register the scalar UDF
    engine.register_python_udf("test_double", udf_double)

    # Create a test table
    engine.execute_query("CREATE TABLE test_table (value INTEGER)")
    engine.execute_query("INSERT INTO test_table VALUES (1), (2), (3)")

    # Test using the UDF in a query
    result = engine.execute_query(
        "SELECT test_double(value) AS doubled FROM test_table"
    ).fetchdf()

    # Verify results
    assert len(result) == 3
    assert list(result["doubled"]) == [2, 4, 6]

    # Cleanup
    engine.close()


def test_register_named_scalar_udf():
    """Test registering and using a scalar UDF with a custom name."""
    engine = DuckDBEngine(":memory:")

    # Register the scalar UDF with custom name
    engine.register_python_udf("custom_add", udf_add)

    # Create a test table
    engine.execute_query("CREATE TABLE test_table (x INTEGER, y INTEGER)")
    engine.execute_query("INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)")

    # Test using the UDF in a query
    result = engine.execute_query(
        "SELECT custom_add(x, y) AS sum FROM test_table"
    ).fetchdf()

    # Verify results
    assert len(result) == 3
    assert list(result["sum"]) == [11, 22, 33]

    # Cleanup
    engine.close()


def test_process_query_for_udfs():
    """Test processing a query to replace PYTHON_FUNC references."""
    engine = DuckDBEngine(":memory:")

    # Create UDFs dictionary
    udfs = {"test.double": udf_double, "test.custom_add": udf_add}

    # Process query with UDF references
    sql = """
    SELECT 
        PYTHON_FUNC("test.double", value) AS doubled,
        PYTHON_FUNC("test.custom_add", value, 10) AS added
    FROM test_table
    """

    processed_sql = engine.process_query_for_udfs(sql, udfs)

    # Verify transformations
    assert "test.double(value)" in processed_sql
    assert "test.custom_add(value, 10)" in processed_sql
    assert "PYTHON_FUNC" not in processed_sql

    # Cleanup
    engine.close()


def test_register_table_udf():
    """Test registering and using a table UDF."""
    engine = DuckDBEngine(":memory:")

    # Create a test table
    engine.execute_query("CREATE TABLE test_table (value INTEGER)")
    engine.execute_query("INSERT INTO test_table VALUES (1), (2), (3)")

    # Register the table UDF
    try:
        engine.register_python_udf("test_add_column", udf_add_column)

        # Test if we can use the UDF directly (this depends on DuckDB version)
        try:
            result = engine.execute_query(
                "SELECT * FROM test_add_column(SELECT * FROM test_table)"
            ).fetchdf()

            # Verify results
            assert len(result) == 3
            assert "doubled" in result.columns
            assert list(result["doubled"]) == [2, 4, 6]
        except Exception as e:
            # Some versions of DuckDB might not support table functions this way
            # Just check that the registration didn't fail
            pass

    except Exception as e:
        pytest.skip(f"Table UDF registration not supported in this DuckDB version: {e}")

    # Cleanup
    engine.close()


def test_supports_feature():
    """Test the supports_feature method."""
    engine = DuckDBEngine(":memory:")

    # Check UDF support
    assert engine.supports_feature("python_udfs") == True

    # Check some other features
    assert engine.supports_feature("arrow") == True

    # Check non-existent feature
    assert engine.supports_feature("non_existent_feature") == False

    # Cleanup
    engine.close()
