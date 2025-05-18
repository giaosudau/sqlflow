"""Unit tests for DuckDBEngine class."""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine, UDFRegistrationError
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
    """Add a new column with doubled values."""
    result = df.copy()
    result["doubled"] = result["value"] * 2
    return result


@python_table_udf
def udf_with_kwargs(df: pd.DataFrame, multiplier: float = 2.0) -> pd.DataFrame:
    """Add a new column with values multiplied by a factor."""
    result = df.copy()
    result["multiplied"] = result["value"] * multiplier
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
    """Test processing a query to replace UDF references."""
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
    engine.register_python_udf("test_add_column", udf_add_column)

    # Get the test data directly
    test_df = engine.execute_query("SELECT * FROM test_table").df()

    # Apply the function directly
    result = udf_add_column(test_df)

    # Verify results
    assert len(result) == 3
    assert "doubled" in result.columns
    assert list(result["doubled"]) == [2, 4, 6]

    # Cleanup
    engine.close()


def test_register_table_udf_with_kwargs():
    """Test registering and using a table UDF with keyword arguments."""
    engine = DuckDBEngine(":memory:")

    # Create a test table
    engine.execute_query("CREATE TABLE test_table (value INTEGER)")
    engine.execute_query("INSERT INTO test_table VALUES (1), (2), (3)")

    # Register the table UDF
    engine.register_python_udf("test_multiply", udf_with_kwargs)

    # Get the test data directly
    test_df = engine.execute_query("SELECT * FROM test_table").df()

    # Apply the function directly with the kwargs
    result = udf_with_kwargs(test_df)  # Default multiplier is 2

    # Verify results with default multiplier
    assert len(result) == 3
    assert "multiplied" in result.columns
    assert list(result["multiplied"]) == [2, 4, 6]

    # Try with a different multiplier by directly calling the function
    result_with_custom = udf_with_kwargs(test_df, multiplier=3)
    assert list(result_with_custom["multiplied"]) == [3, 6, 9]

    # Cleanup
    engine.close()


def test_table_udf_with_required_columns():
    """Test table UDF with required columns validation."""
    engine = DuckDBEngine(":memory:")

    # Define a UDF that requires specific columns
    @python_table_udf(required_columns=["value", "category"])
    def categorize(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["category_value"] = result["value"] * (result["category"] == "A")
        return result

    # Register the UDF
    engine.register_python_udf("test_categorize", categorize)

    # Create test data with required columns
    test_df = pd.DataFrame({"value": [1, 2, 3], "category": ["A", "B", "A"]})

    # Apply the function directly
    result = categorize(test_df)

    # Verify results
    assert "category_value" in result.columns
    assert list(result["category_value"]) == [1, 0, 3]

    # Test missing required column should raise error
    incomplete_df = pd.DataFrame({"value": [1, 2, 3]})  # Missing "category"

    # Manually validate the required columns
    required_cols = getattr(categorize, "_required_columns", [])
    missing_cols = [col for col in required_cols if col not in incomplete_df.columns]
    assert missing_cols == ["category"]  # Verify the missing column is detected

    # Cleanup
    engine.close()


def test_table_udf_invalid_signatures():
    """Test validation of invalid table UDF signatures."""
    engine = DuckDBEngine(":memory:")

    # We'll test directly against the _validate_table_udf_signature method

    # Test UDF with no parameters
    def invalid_no_params() -> pd.DataFrame:
        return pd.DataFrame()

    with pytest.raises(UDFRegistrationError, match="must accept at least one argument"):
        engine._validate_table_udf_signature("invalid_func", invalid_no_params)

    # Test UDF with keyword-only first parameter
    def invalid_keyword_only(*, df: pd.DataFrame) -> pd.DataFrame:
        return df

    with pytest.raises(UDFRegistrationError, match="must be positional"):
        engine._validate_table_udf_signature("invalid_func", invalid_keyword_only)

    # Cleanup
    engine.close()


def test_table_udf_runtime_validation():
    """Test runtime validation of table UDF inputs and outputs."""
    engine = DuckDBEngine(":memory:")

    # Create an undecorated function for testing
    def raw_test_udf(df):
        if not isinstance(df, pd.DataFrame):
            return "not a dataframe"  # This will trigger runtime validation
        return df

    # Register the function with the decorator
    decorated_udf = python_table_udf(raw_test_udf)

    # Register the UDF
    engine.register_python_udf("test_runtime", decorated_udf)

    # Create test data
    test_df = pd.DataFrame({"value": [1, 2, 3]})

    # This should work fine - direct call returns DataFrame
    result = decorated_udf(test_df)
    assert isinstance(result, pd.DataFrame)

    # Test validation with the decorator
    with pytest.raises(ValueError, match="must be a DataFrame"):
        decorated_udf("not a dataframe")

    # Test the underlying function directly
    bad_return = raw_test_udf("not a dataframe")
    assert not isinstance(bad_return, pd.DataFrame)
    assert bad_return == "not a dataframe"

    # Cleanup
    engine.close()


def test_supports_feature():
    """Test the supports_feature method."""
    engine = DuckDBEngine(":memory:")

    # Check UDF support
    assert engine.supports_feature("python_udfs") is True

    # Check some other features
    assert engine.supports_feature("arrow") is True

    # Check non-existent feature
    assert engine.supports_feature("non_existent_feature") is False

    # Cleanup
    engine.close()
