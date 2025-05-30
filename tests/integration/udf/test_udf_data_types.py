"""Integration tests for Python UDFs with different data types."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


@pytest.fixture
def data_type_test_env() -> Dict[str, Any]:
    """Create a test environment with UDFs that handle various data types."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up directories
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create UDF file with different data type handlers
        udf_file = create_data_type_udf_file(udf_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "udf_file": udf_file,
        }


def create_data_type_udf_file(udf_dir: str) -> Path:
    """Create a UDF file with functions that handle various data types."""
    udf_file = Path(udf_dir) / "data_type_handlers.py"
    with open(udf_file, "w") as f:
        f.write(
            """
import pandas as pd
from datetime import datetime
from sqlflow.udfs import python_scalar_udf, python_table_udf

# Integer handling UDFs
@python_scalar_udf
def double_int(value: int) -> int:
    \"\"\"Double an integer value.\"\"\"
    if value is None:
        return None
    return value * 2

# Float handling UDFs
@python_scalar_udf
def round_float(value: float, decimals: int = 2) -> float:
    \"\"\"Round a float to the specified number of decimal places.\"\"\"
    if value is None:
        return None
    return round(value, decimals)

# String handling UDFs
@python_scalar_udf
def capitalize_text(text: str) -> str:
    \"\"\"Capitalize the first letter of text.\"\"\"
    if text is None:
        return None
    return text.capitalize() if text else ""

@python_scalar_udf
def handle_special_chars(text: str) -> str:
    \"\"\"Safely handle text with special characters.\"\"\"
    if text is None:
        return None
    # Replace common problematic characters
    return text.replace("'", "''").replace("\\\\", "\\\\\\\\")

# Date handling UDFs
@python_scalar_udf
def format_date(date_str: str, format_str: str = "%Y-%m-%d", 
                output_format: str = "%B %d, %Y") -> str:
    \"\"\"Format a date string to a different format.\"\"\"
    if date_str is None:
        return None
    try:
        dt = datetime.strptime(date_str, format_str)
        return dt.strftime(output_format)
    except ValueError:
        return f"Invalid date: {date_str}"

# Boolean handling UDFs
@python_scalar_udf
def bool_to_text(value: bool) -> str:
    \"\"\"Convert boolean to descriptive text.\"\"\"
    if value is None:
        return "Unknown"
    return "Yes" if value else "No"

# Table UDF for mixed data types
@python_table_udf(output_schema={
    "int_col": "INTEGER", 
    "float_col": "DOUBLE",
    "string_col": "VARCHAR",
    "bool_col": "BOOLEAN"
})
def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Convert columns to specific types.\"\"\"
    result = pd.DataFrame()
    
    # Handle integer column with potential nulls
    if "int_col" in df.columns:
        result["int_col"] = pd.to_numeric(df["int_col"], errors="coerce").astype("Int64")
    else:
        result["int_col"] = pd.Series(dtype="Int64")  # Nullable integer type
        
    # Handle float column
    if "float_col" in df.columns:
        result["float_col"] = pd.to_numeric(df["float_col"], errors="coerce")
    else:
        result["float_col"] = pd.Series(dtype="float64")
        
    # Handle string column
    if "string_col" in df.columns:
        result["string_col"] = df["string_col"].astype(str).replace("nan", None)
    else:
        result["string_col"] = pd.Series(dtype="object")
        
    # Handle boolean column
    if "bool_col" in df.columns:
        # Convert various values to boolean
        bool_map = {"true": True, "false": False, 
                    "yes": True, "no": False}
        result["bool_col"] = df["bool_col"].astype(str).str.lower().map(bool_map)
    else:
        result["bool_col"] = pd.Series(dtype="boolean")
        
    return result

# Table UDF for handling NULL values
@python_table_udf(output_schema={
    "col": "VARCHAR", 
    "has_null": "BOOLEAN",
    "null_replaced": "VARCHAR"
})
def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Handle NULL values in DataFrame.\"\"\"
    result = pd.DataFrame()
    
    # Copy original column
    result["col"] = df["col"] if "col" in df.columns else None
    
    # Check for NULL values
    result["has_null"] = df["col"].isna() if "col" in df.columns else None
    
    # Replace NULL values with default text
    result["null_replaced"] = df["col"].fillna("DEFAULT") if "col" in df.columns else "DEFAULT"
    
    return result
"""
        )
    return udf_file


def test_integer_handling(data_type_test_env: Dict[str, Any]) -> None:
    """Test UDFs that handle integer data types."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(data_type_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.data_type_handlers.double_int" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data
    engine.execute_query(
        """
        CREATE TABLE int_test AS
        SELECT * FROM (
            VALUES
            (1),
            (100),
            (NULL),
            (-5)
        ) AS t(value);
        """
    )

    # Apply the double_int UDF
    engine.execute_query(
        """
        CREATE TABLE int_results AS
        SELECT
            value,
            double_int(value) AS doubled
        FROM int_test;
        """
    )

    # Get results from the engine
    results = engine.execute_query("SELECT * FROM int_results").fetchdf()

    # Verify results
    assert len(results) == 4

    # Filter for specific values and check the result
    value_1_rows = results[results["value"] == 1]
    assert len(value_1_rows) == 1
    assert value_1_rows["doubled"].values[0] == 2

    value_100_rows = results[results["value"] == 100]
    assert len(value_100_rows) == 1
    assert value_100_rows["doubled"].values[0] == 200

    value_neg5_rows = results[results["value"] == -5]
    assert len(value_neg5_rows) == 1
    assert value_neg5_rows["doubled"].values[0] == -10

    # Check NULL handling
    null_rows = results[results["value"].isna()]
    assert len(null_rows) == 1
    assert pd.isna(null_rows["doubled"].values[0])


def test_string_handling(data_type_test_env: Dict[str, Any]) -> None:
    """Test UDFs that handle string data types."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(data_type_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.data_type_handlers.capitalize_text" in udfs
    assert "python_udfs.data_type_handlers.handle_special_chars" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data with various string types
    engine.execute_query(
        """
        CREATE TABLE string_test AS
        SELECT * FROM (
            VALUES
            ('hello world', 'text with ''quotes'''),
            ('ALL CAPS', 'text with \\backslash'),
            (NULL, 'normal text'),
            ('', NULL)
        ) AS t(text1, text2);
        """
    )

    # Apply the string UDFs
    engine.execute_query(
        """
        CREATE TABLE string_results AS
        SELECT
            text1,
            text2,
            capitalize_text(text1) AS capitalized,
            handle_special_chars(text2) AS escaped
        FROM string_test;
        """
    )

    # Get results from the engine
    results = engine.execute_query("SELECT * FROM string_results").fetchdf()

    # Verify results
    assert len(results) == 4
    assert results.iloc[0]["capitalized"] == "Hello world"
    assert results.iloc[1]["capitalized"] == "All caps"
    assert pd.isna(results.iloc[2]["capitalized"])
    assert results.iloc[3]["capitalized"] == ""

    # Check special character handling
    assert results.iloc[0]["escaped"] == "text with ''quotes''"
    assert results.iloc[1]["escaped"] == "text with \\\\backslash"


def test_null_handling_table_udf(data_type_test_env: Dict[str, Any]) -> None:
    """Test table UDF that handles NULL values."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(data_type_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.data_type_handlers.handle_nulls" in udfs

    # Create test DataFrame directly
    test_df = pd.DataFrame({"col": ["Value A", "Value B", None, ""]})

    # Get the table UDF function
    handle_nulls_func = udfs["python_udfs.data_type_handlers.handle_nulls"]

    # Execute table UDF directly
    results_df = handle_nulls_func(test_df)

    # Verify results
    assert len(results_df) == 4
    assert results_df.iloc[0]["col"] == "Value A"
    assert bool(results_df.iloc[0]["has_null"]) is False
    assert results_df.iloc[0]["null_replaced"] == "Value A"

    # Check NULL handling
    assert pd.isna(results_df.iloc[2]["col"])
    assert bool(results_df.iloc[2]["has_null"]) is True
    assert results_df.iloc[2]["null_replaced"] == "DEFAULT"

    # Check empty string (not NULL)
    assert results_df.iloc[3]["col"] == ""
    assert bool(results_df.iloc[3]["has_null"]) is False
    assert results_df.iloc[3]["null_replaced"] == ""
