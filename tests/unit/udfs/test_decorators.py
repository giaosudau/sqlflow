"""Unit tests for UDF decorators."""

import pandas as pd
import pytest

from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf


def test_python_scalar_udf_simple_decorator():
    """Test the python_scalar_udf decorator without parameters."""

    @python_scalar_udf
    def add_tax(price, tax_rate=0.1):
        return price * (1 + tax_rate)

    # Check that the function still works as expected
    assert round(add_tax(100), 2) == 110.0
    assert round(add_tax(100, 0.2), 2) == 120.0

    # Check that the function is marked as a UDF
    assert hasattr(add_tax, "_is_sqlflow_udf")
    assert getattr(add_tax, "_is_sqlflow_udf") is True

    # Check UDF type
    assert hasattr(add_tax, "_udf_type")
    assert getattr(add_tax, "_udf_type") == "scalar"

    # Check UDF name
    assert hasattr(add_tax, "_udf_name")
    assert getattr(add_tax, "_udf_name") == "add_tax"


def test_python_scalar_udf_with_name():
    """Test the python_scalar_udf decorator with a custom name."""

    @python_scalar_udf(name="calculate_tax")
    def add_tax(price, tax_rate=0.1):
        return price * (1 + tax_rate)

    # Check that the function still works as expected
    assert round(add_tax(100), 2) == 110.0

    # Check UDF name
    assert hasattr(add_tax, "_udf_name")
    assert getattr(add_tax, "_udf_name") == "calculate_tax"


def test_python_table_udf_simple_decorator():
    """Test the python_table_udf decorator without parameters."""

    @python_table_udf
    def add_totals(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["total"] = result["price"] * result["quantity"]
        return result

    # Create a test DataFrame
    test_df = pd.DataFrame({"price": [10, 20, 30], "quantity": [1, 2, 3]})

    # Check that the function still works as expected
    result_df = add_totals(test_df)
    assert "total" in result_df.columns
    assert result_df["total"].tolist() == [10, 40, 90]

    # Check that the function is marked as a UDF
    assert hasattr(add_totals, "_is_sqlflow_udf")
    assert getattr(add_totals, "_is_sqlflow_udf") is True

    # Check UDF type
    assert hasattr(add_totals, "_udf_type")
    assert getattr(add_totals, "_udf_type") == "table"

    # Check UDF name
    assert hasattr(add_totals, "_udf_name")
    assert getattr(add_totals, "_udf_name") == "add_totals"

    # Check signature metadata
    assert hasattr(add_totals, "_signature")
    assert "pandas.core.frame.DataFrame" in getattr(add_totals, "_signature")

    # Check parameter info
    assert hasattr(add_totals, "_param_info")
    param_info = getattr(add_totals, "_param_info")
    assert "df" in param_info
    assert "pandas.core.frame.DataFrame" in param_info["df"]["annotation"]


def test_python_table_udf_with_kwargs():
    """Test the python_table_udf decorator with keyword arguments."""

    @python_table_udf
    def add_tax(df: pd.DataFrame, tax_rate: float = 0.1) -> pd.DataFrame:
        result = df.copy()
        result["tax"] = result["price"] * tax_rate
        return result

    # Create a test DataFrame
    test_df = pd.DataFrame({"price": [100, 200, 300]})

    # Test with default tax_rate
    result_df = add_tax(test_df)
    assert result_df["tax"].tolist() == [10.0, 20.0, 30.0]

    # Test with custom tax_rate
    result_df = add_tax(test_df, tax_rate=0.2)
    assert result_df["tax"].tolist() == [20.0, 40.0, 60.0]

    # Check parameter info includes kwargs
    param_info = getattr(add_tax, "_param_info")
    assert "tax_rate" in param_info
    assert param_info["tax_rate"]["default"] == 0.1


def test_python_table_udf_with_required_columns():
    """Test the python_table_udf decorator with required columns."""

    @python_table_udf(required_columns=["price", "quantity"])
    def calculate_total(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["total"] = result["price"] * result["quantity"]
        return result

    # Test with valid DataFrame
    valid_df = pd.DataFrame({"price": [10, 20], "quantity": [2, 3], "other": [1, 2]})
    result = calculate_total(valid_df)
    assert result["total"].tolist() == [20, 60]

    # Test with missing columns
    invalid_df = pd.DataFrame({"price": [10, 20], "other": [1, 2]})
    with pytest.raises(ValueError) as excinfo:
        calculate_total(invalid_df)
    assert "missing from input DataFrame: ['quantity']" in str(excinfo.value)


def test_python_table_udf_invalid_signatures():
    """Test the python_table_udf decorator with invalid function signatures."""

    # Test with no parameters
    with pytest.raises(ValueError) as excinfo:

        @python_table_udf
        def invalid_no_params() -> pd.DataFrame:
            return pd.DataFrame()

    assert "must accept at least one argument" in str(excinfo.value)

    # Test with keyword-only first parameter
    with pytest.raises(ValueError) as excinfo:

        @python_table_udf
        def invalid_kwargs_first(*, df: pd.DataFrame) -> pd.DataFrame:
            return df

    assert "must be positional" in str(excinfo.value)

    # Test with positional-only additional parameters
    with pytest.raises(ValueError) as excinfo:

        @python_table_udf
        def invalid_params(df: pd.DataFrame, rate: float, /) -> pd.DataFrame:
            return df

    assert "must be keyword arguments" in str(excinfo.value)


def test_python_table_udf_runtime_validation():
    """Test runtime validation in the python_table_udf decorator."""

    @python_table_udf
    def valid_udf(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # Test with missing DataFrame argument
    with pytest.raises(ValueError) as excinfo:
        valid_udf()
    assert "requires a DataFrame argument" in str(excinfo.value)

    # Test with wrong type for DataFrame argument
    with pytest.raises(ValueError) as excinfo:
        valid_udf([1, 2, 3])
    assert "must be a DataFrame" in str(excinfo.value)

    # Test with invalid return type
    @python_table_udf
    def invalid_return(df: pd.DataFrame) -> pd.DataFrame:
        return "not a dataframe"

    with pytest.raises(ValueError) as excinfo:
        invalid_return(pd.DataFrame())
    assert "must return a pandas DataFrame" in str(excinfo.value)
