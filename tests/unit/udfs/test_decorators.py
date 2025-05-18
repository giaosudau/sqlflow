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
    def add_totals(df):
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


def test_python_table_udf_with_name():
    """Test the python_table_udf decorator with a custom name."""

    @python_table_udf(name="calculate_totals")
    def add_totals(df):
        result = df.copy()
        result["total"] = result["price"] * result["quantity"]
        return result

    # Check UDF name
    assert hasattr(add_totals, "_udf_name")
    assert getattr(add_totals, "_udf_name") == "calculate_totals"


def test_python_table_udf_validates_return_type():
    """Test that python_table_udf validates the return type is a DataFrame."""

    @python_table_udf
    def invalid_udf(df):
        return "not a dataframe"

    # Check that calling the function raises a ValueError
    with pytest.raises(ValueError) as excinfo:
        invalid_udf(pd.DataFrame())

    # Check error message
    assert "must return a pandas DataFrame" in str(excinfo.value)
