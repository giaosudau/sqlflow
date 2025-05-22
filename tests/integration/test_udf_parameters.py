"""Integration tests for Python UDFs with different parameter configurations."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


@pytest.fixture
def parameter_test_env() -> Dict[str, Any]:
    """Create a test environment with UDFs that have various parameter configurations."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up directories
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create UDF file with different parameter configurations
        udf_file = create_parameter_udf_file(udf_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "udf_file": udf_file,
        }


def create_parameter_udf_file(udf_dir: str) -> Path:
    """Create a UDF file with functions that have various parameter configurations."""
    udf_file = Path(udf_dir) / "parameter_handlers.py"
    with open(udf_file, "w") as f:
        f.write(
            """
import pandas as pd
from datetime import datetime
from sqlflow.udfs import python_scalar_udf, python_table_udf

# Single parameter UDF
@python_scalar_udf
def square(x: float) -> float:
    \"\"\"Square a number.\"\"\"
    if x is None:
        return None
    return x * x

# Multiple parameter UDF
@python_scalar_udf
def add_values(a: float, b: float) -> float:
    \"\"\"Add two values together.\"\"\"
    if a is None or b is None:
        return None
    return a + b

# Optional parameter UDF
@python_scalar_udf
def apply_discount(price: float, discount_rate: float = 0.1) -> float:
    \"\"\"Apply a discount to a price.\"\"\"
    if price is None:
        return None
    return price * (1 - discount_rate)

# Multiple optional parameters UDF
@python_scalar_udf
def format_price(
    price: float, 
    currency: str = "$", 
    decimals: int = 2, 
    show_currency: bool = True
) -> str:
    \"\"\"Format a price with currency and decimal places.\"\"\"
    if price is None:
        return None
    formatted = f"{price:.{decimals}f}"
    return f"{currency}{formatted}" if show_currency else formatted

# Table UDF with DataFrame only
@python_table_udf(output_schema={
    "id": "INTEGER",
    "amount": "DOUBLE",
    "has_discount": "BOOLEAN"
})
def flag_discounted_items(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Flag items that have a discount applied.\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["amount"] = df["amount"] if "amount" in df.columns else pd.Series(dtype="float64")
    
    # Check for discounted items (where discount column exists and is not null/zero)
    if "discount" in df.columns:
        result["has_discount"] = (df["discount"] > 0)
    else:
        result["has_discount"] = False
        
    return result

# Table UDF with DataFrame + parameters
@python_table_udf(output_schema={
    "id": "INTEGER",
    "amount": "DOUBLE",
    "discounted_amount": "DOUBLE"
})
def apply_discount_to_table(df: pd.DataFrame, discount_rate: float = 0.1) -> pd.DataFrame:
    \"\"\"Apply a discount rate to all amounts in a table.\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["amount"] = df["amount"] if "amount" in df.columns else pd.Series(dtype="float64")
    
    # Apply discount to amounts
    if "amount" in df.columns:
        result["discounted_amount"] = df["amount"] * (1 - discount_rate)
    else:
        result["discounted_amount"] = pd.Series(dtype="float64")
        
    return result

# Table UDF with multiple keyword arguments
@python_table_udf(output_schema={
    "id": "INTEGER", 
    "amount": "DOUBLE",
    "formatted_amount": "VARCHAR",
    "category": "VARCHAR"
})
def format_and_categorize(
    df: pd.DataFrame, 
    currency: str = "$", 
    low_threshold: float = 100, 
    high_threshold: float = 500
) -> pd.DataFrame:
    \"\"\"Format amounts and categorize based on thresholds.\"\"\"
    result = pd.DataFrame()
    
    # Copy required columns
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["amount"] = df["amount"] if "amount" in df.columns else pd.Series(dtype="float64")
    
    # Format amounts with currency
    if "amount" in df.columns:
        result["formatted_amount"] = df["amount"].apply(
            lambda x: f"{currency}{x:.2f}" if pd.notna(x) else None
        )
    else:
        result["formatted_amount"] = pd.Series(dtype="object")
    
    # Categorize based on thresholds
    if "amount" in df.columns:
        conditions = [
            df["amount"] < low_threshold,
            (df["amount"] >= low_threshold) & (df["amount"] < high_threshold),
            df["amount"] >= high_threshold
        ]
        choices = ["low", "medium", "high"]
        result["category"] = pd.Series(dtype="object")
        
        for condition, choice in zip(conditions, choices):
            result.loc[condition, "category"] = choice
    else:
        result["category"] = pd.Series(dtype="object")
        
    return result
"""
        )
    return udf_file


def test_scalar_udf_single_parameter(parameter_test_env: Dict[str, Any]) -> None:
    """Test scalar UDF with a single parameter."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(parameter_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.parameter_handlers.square" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data
    engine.execute_query(
        """
        CREATE TABLE numbers AS
        SELECT * FROM (
            VALUES
            (2),
            (5),
            (10),
            (NULL)
        ) AS t(value);
        """
    )

    # Apply the square UDF
    engine.execute_query(
        """
        CREATE TABLE squared_results AS
        SELECT
            value,
            square(value) AS squared
        FROM numbers;
        """
    )

    # Get results from the engine
    results = engine.execute_query("SELECT * FROM squared_results").fetchdf()

    # Verify results
    assert len(results) == 4

    # Filter for specific values and check the result
    value_2_rows = results[results["value"] == 2]
    assert len(value_2_rows) == 1
    assert value_2_rows["squared"].values[0] == 4

    value_5_rows = results[results["value"] == 5]
    assert len(value_5_rows) == 1
    assert value_5_rows["squared"].values[0] == 25

    value_10_rows = results[results["value"] == 10]
    assert len(value_10_rows) == 1
    assert value_10_rows["squared"].values[0] == 100

    # Check NULL handling
    null_rows = results[results["value"].isna()]
    assert len(null_rows) == 1
    assert pd.isna(null_rows["squared"].values[0])


def test_scalar_udf_multiple_parameters(parameter_test_env: Dict[str, Any]) -> None:
    """Test scalar UDF with multiple parameters."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(parameter_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.parameter_handlers.add_values" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data
    engine.execute_query(
        """
        CREATE TABLE value_pairs AS
        SELECT * FROM (
            VALUES
            (10, 20),
            (5.5, 4.5),
            (NULL, 10),
            (20, NULL)
        ) AS t(a, b);
        """
    )

    # Apply the add_values UDF
    engine.execute_query(
        """
        CREATE TABLE sum_results AS
        SELECT
            a, b,
            add_values(a, b) AS sum
        FROM value_pairs;
        """
    )

    # Get results from the engine
    results = engine.execute_query("SELECT * FROM sum_results").fetchdf()

    # Verify results
    assert len(results) == 4
    assert results.iloc[0]["sum"] == 30
    assert results.iloc[1]["sum"] == 10
    assert pd.isna(results.iloc[2]["sum"])
    assert pd.isna(results.iloc[3]["sum"])


def test_scalar_udf_optional_parameters(parameter_test_env: Dict[str, Any]) -> None:
    """Test scalar UDF with optional parameters."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(parameter_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.parameter_handlers.apply_discount" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data
    engine.execute_query(
        """
        CREATE TABLE prices AS
        SELECT * FROM (
            VALUES
            (100),
            (200),
            (NULL)
        ) AS t(price);
        """
    )

    # Apply the apply_discount UDF with default discount rate
    engine.execute_query(
        """
        CREATE TABLE default_discount_results AS
        SELECT
            price,
            apply_discount(price) AS discounted_price
        FROM prices;
        """
    )

    # Apply the apply_discount UDF with custom discount rate
    engine.execute_query(
        """
        CREATE TABLE custom_discount_results AS
        SELECT
            price,
            apply_discount(price, 0.2) AS discounted_price
        FROM prices;
        """
    )

    # Get results from the engine
    default_results = engine.execute_query(
        "SELECT * FROM default_discount_results"
    ).fetchdf()
    custom_results = engine.execute_query(
        "SELECT * FROM custom_discount_results"
    ).fetchdf()

    # Verify default discount results (10%)
    assert len(default_results) == 3

    price_100_default = default_results[default_results["price"] == 100]
    assert len(price_100_default) == 1
    assert price_100_default["discounted_price"].values[0] == 90

    price_200_default = default_results[default_results["price"] == 200]
    assert len(price_200_default) == 1
    assert price_200_default["discounted_price"].values[0] == 180

    null_price_default = default_results[default_results["price"].isna()]
    assert len(null_price_default) == 1
    assert pd.isna(null_price_default["discounted_price"].values[0])

    # Verify custom discount results (20%)
    assert len(custom_results) == 3

    price_100_custom = custom_results[custom_results["price"] == 100]
    assert len(price_100_custom) == 1
    assert price_100_custom["discounted_price"].values[0] == 80

    price_200_custom = custom_results[custom_results["price"] == 200]
    assert len(price_200_custom) == 1
    assert price_200_custom["discounted_price"].values[0] == 160

    null_price_custom = custom_results[custom_results["price"].isna()]
    assert len(null_price_custom) == 1
    assert pd.isna(null_price_custom["discounted_price"].values[0])


def test_table_udf_with_parameters(parameter_test_env: Dict[str, Any]) -> None:
    """Test table UDF with parameters."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(parameter_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.parameter_handlers.apply_discount_to_table" in udfs

    # Create test DataFrame directly
    test_df = pd.DataFrame({"id": [1, 2, 3, 4], "amount": [100.0, 200.0, 300.0, None]})

    # Get the table UDF function
    apply_discount_func = udfs["python_udfs.parameter_handlers.apply_discount_to_table"]

    # Execute table UDF with default discount rate (10%)
    default_results = apply_discount_func(test_df)

    # Execute table UDF with custom discount rate (20%)
    custom_results = apply_discount_func(test_df, discount_rate=0.2)

    # Verify default discount results (10%)
    assert len(default_results) == 4
    assert default_results.iloc[0]["discounted_amount"] == 90.0
    assert default_results.iloc[1]["discounted_amount"] == 180.0
    assert default_results.iloc[2]["discounted_amount"] == 270.0
    assert pd.isna(default_results.iloc[3]["discounted_amount"])

    # Verify custom discount results (20%)
    assert len(custom_results) == 4
    assert custom_results.iloc[0]["discounted_amount"] == 80.0
    assert custom_results.iloc[1]["discounted_amount"] == 160.0
    assert custom_results.iloc[2]["discounted_amount"] == 240.0
    assert pd.isna(custom_results.iloc[3]["discounted_amount"])


def test_table_udf_multiple_kwargs(parameter_test_env: Dict[str, Any]) -> None:
    """Test table UDF with multiple keyword arguments."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(parameter_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.parameter_handlers.format_and_categorize" in udfs

    # Create test DataFrame directly
    test_df = pd.DataFrame({"id": [1, 2, 3, 4], "amount": [50.0, 150.0, 600.0, None]})

    # Get the table UDF function
    format_func = udfs["python_udfs.parameter_handlers.format_and_categorize"]

    # Execute table UDF with default parameters
    default_results = format_func(test_df)

    # Execute table UDF with custom parameters
    custom_results = format_func(
        test_df, currency="€", low_threshold=200, high_threshold=600
    )

    # Verify default results
    assert len(default_results) == 4
    assert default_results.iloc[0]["formatted_amount"] == "$50.00"
    assert default_results.iloc[0]["category"] == "low"
    assert default_results.iloc[1]["formatted_amount"] == "$150.00"
    assert default_results.iloc[1]["category"] == "medium"
    assert default_results.iloc[2]["formatted_amount"] == "$600.00"
    assert default_results.iloc[2]["category"] == "high"
    assert pd.isna(default_results.iloc[3]["formatted_amount"])
    assert pd.isna(default_results.iloc[3]["category"])

    # Verify custom results
    assert len(custom_results) == 4
    assert custom_results.iloc[0]["formatted_amount"] == "€50.00"
    assert custom_results.iloc[0]["category"] == "low"
    assert custom_results.iloc[1]["formatted_amount"] == "€150.00"
    assert custom_results.iloc[1]["category"] == "low"  # Now low with custom threshold
    assert custom_results.iloc[2]["formatted_amount"] == "€600.00"
    assert custom_results.iloc[2]["category"] == "high"
    assert pd.isna(custom_results.iloc[3]["formatted_amount"])
    assert pd.isna(custom_results.iloc[3]["category"])
