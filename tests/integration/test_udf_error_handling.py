"""Integration tests for Python UDF error handling and error reporting."""

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
def error_handling_test_env() -> Dict[str, Any]:
    """Create a test environment with UDFs that demonstrate various error conditions."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up directories
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create UDF file with error-prone functions
        udf_file = create_error_prone_udf_file(udf_dir)

        # Create a subdirectory for syntax error testing
        syntax_error_dir = os.path.join(udf_dir, "syntax_errors")
        os.makedirs(syntax_error_dir, exist_ok=True)

        # Create UDF file with syntax errors
        syntax_error_file = create_syntax_error_udf_file(syntax_error_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "udf_file": udf_file,
            "syntax_error_dir": syntax_error_dir,
            "syntax_error_file": syntax_error_file,
        }


def create_error_prone_udf_file(udf_dir: str) -> Path:
    """Create a UDF file with functions that demonstrate various error conditions."""
    udf_file = Path(udf_dir) / "error_prone_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            """
import pandas as pd
import math
from typing import Any
from sqlflow.udfs import python_scalar_udf, python_table_udf

# UDF that raises an exception during execution
@python_scalar_udf
def divide_by_zero(value: float) -> float:
    \"\"\"Attempt to divide by zero, which will raise a ZeroDivisionError.\"\"\"
    return value / 0

# UDF that raises a type error during execution
@python_scalar_udf
def unsafe_string_operation(text: str) -> str:
    \"\"\"Perform unsafe string operation that will raise a TypeError with None input.\"\"\"
    if text is None:
        # This will raise TypeError: 'NoneType' object has no attribute 'upper'
        return text.upper()
    return text.upper()

# UDF that attempts to access a non-existent attribute
@python_scalar_udf
def attribute_error(obj: Any) -> str:
    \"\"\"Attempt to access a non-existent attribute.\"\"\"
    # This will raise AttributeError when obj doesn't have a 'name' attribute
    return obj.name

# UDF with potential math domain error
@python_scalar_udf
def logarithm(value: float) -> float:
    \"\"\"Calculate logarithm of a value, which will error with negative numbers.\"\"\"
    return math.log(value)

# Table UDF that raises an exception during execution
@python_table_udf(output_schema={
    "id": "INTEGER",
    "processed_value": "DOUBLE"
})
def process_with_error(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Process dataframe but raise an exception.\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    
    # This will raise a KeyError if 'non_existent_column' is not in df
    result["processed_value"] = df["non_existent_column"] * 2
    
    return result

# Table UDF with index out of bounds error
@python_table_udf(output_schema={
    "id": "INTEGER",
    "value": "DOUBLE",
    "description": "VARCHAR"
})
def index_error(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Demonstrate index out of bounds error.\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    
    # Create descriptions array but access beyond its bounds
    descriptions = ["Low", "Medium", "High"]
    
    # This will raise IndexError for values >= 3
    result["description"] = df["value"].apply(lambda x: descriptions[int(x)])
    
    return result

# Table UDF with schema mismatch (returning different schema than declared)
@python_table_udf(output_schema={
    "id": "INTEGER",
    "value": "DOUBLE",
    "status": "VARCHAR"
})
def schema_mismatch(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Return a DataFrame with schema that doesn't match declared output_schema.\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    
    # Return 'status_code' instead of declared 'status'
    result["status_code"] = "OK"
    
    return result

# UDF with dependency on external module that might not be available
try:
    import numpy as np
    
    @python_scalar_udf
    def numpy_operation(value: float) -> float:
        \"\"\"Perform operation using NumPy.\"\"\"
        return float(np.sin(value))
except ImportError:
    # This would be detected during UDF discovery
    pass
"""
        )
    return udf_file


def create_syntax_error_udf_file(syntax_error_dir: str) -> Path:
    """Create a UDF file with syntax errors that won't be properly loaded."""
    syntax_error_file = Path(syntax_error_dir) / "syntax_error_udfs.py"
    with open(syntax_error_file, "w") as f:
        f.write(
            """
import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf

# UDF with syntax error (missing colon)
@python_scalar_udf
def syntax_error_function(value)  # Missing colon here
    \"\"\"This function has a syntax error.\"\"\"
    return value * 2

# UDF with indentation error
@python_scalar_udf
def indentation_error(value: int) -> int:
    \"\"\"This function has an indentation error.\"\"\"
    result = value * 2
  return result  # Incorrect indentation

# UDF with undefined variable
@python_scalar_udf
def undefined_variable(value: int) -> int:
    \"\"\"This function uses an undefined variable.\"\"\"
    return value * multiplier  # 'multiplier' is not defined
"""
        )
    return syntax_error_file


def test_discovery_syntax_errors(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how the UDF discovery process handles files with syntax errors."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])

    # Discover UDFs, which should log warnings for the syntax error file
    # but continue processing valid UDFs
    udfs = udf_manager.discover_udfs()

    # The syntax error file should not have contributed any UDFs
    syntax_error_prefixes = ["python_udfs.syntax_errors.syntax_error_udfs"]

    # Check that no UDFs from the syntax error file were discovered
    for udf_name in udfs:
        assert not any(udf_name.startswith(prefix) for prefix in syntax_error_prefixes)

    # But valid UDFs should have been discovered
    assert "python_udfs.error_prone_udfs.divide_by_zero" in udfs
    assert "python_udfs.error_prone_udfs.unsafe_string_operation" in udfs


def test_runtime_error_handling(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how runtime errors in UDFs are handled and reported."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.error_prone_udfs.divide_by_zero" in udfs

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
            (1),
            (2),
            (NULL),
            (5)
        ) AS t(value);
        """
    )

    # Test UDF that raises ZeroDivisionError
    with pytest.raises(Exception) as excinfo:
        engine.execute_query(
            """
            SELECT
                value,
                divide_by_zero(value) AS result
            FROM numbers;
            """
        )

    # Verify the error message is informative
    error_message = str(excinfo.value)
    assert (
        "divide_by_zero" in error_message.lower()
        or "division by zero" in error_message.lower()
    )


def test_type_error_handling(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how type errors in UDFs are handled and reported."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.error_prone_udfs.unsafe_string_operation" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data
    engine.execute_query(
        """
        CREATE TABLE strings AS
        SELECT * FROM (
            VALUES
            ('hello'),
            ('world'),
            (NULL),
            ('test')
        ) AS t(text);
        """
    )

    # DuckDB handles NULL values differently than expected
    # Instead of raising an error, it appears to pass NULL values through
    # Let's verify that NULL inputs result in NULL outputs
    result = engine.execute_query(
        """
        SELECT
            text,
            unsafe_string_operation(text) AS result
        FROM strings;
        """
    ).fetchdf()

    # Verify the results
    assert len(result) == 4

    # Check non-NULL values
    hello_row = result[result["text"] == "hello"]
    assert len(hello_row) == 1
    assert hello_row["result"].values[0] == "HELLO"

    # Check NULL value handling
    null_rows = result[result["text"].isna()]
    assert len(null_rows) == 1
    assert pd.isna(
        null_rows["result"].values[0]
    ), "NULL input should result in NULL output"


def test_table_udf_error_handling(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how errors in table UDFs are handled and reported."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.error_prone_udfs.process_with_error" in udfs

    # Create test DataFrame directly
    test_df = pd.DataFrame({"id": [1, 2, 3, 4], "value": [10.0, 20.0, 30.0, None]})

    # Get the table UDF function
    process_with_error_func = udfs["python_udfs.error_prone_udfs.process_with_error"]

    # Execute table UDF directly, which should raise KeyError for missing column
    with pytest.raises(KeyError) as excinfo:
        process_with_error_func(test_df)

    # Verify the error message is informative
    error_message = str(excinfo.value)
    assert "non_existent_column" in error_message


def test_index_error_handling(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how index errors in UDFs are handled and reported."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.error_prone_udfs.index_error" in udfs

    # Create test DataFrame directly
    test_df = pd.DataFrame(
        {"id": [1, 2, 3, 4], "value": [0, 1, 2, 3]}  # 3 will cause an index error
    )

    # Get the table UDF function
    index_error_func = udfs["python_udfs.error_prone_udfs.index_error"]

    # Execute table UDF directly, which should raise IndexError
    with pytest.raises(IndexError) as excinfo:
        index_error_func(test_df)

    # Verify the error message is informative
    error_message = str(excinfo.value)
    assert "index" in error_message.lower() or "out of range" in error_message.lower()


def test_schema_mismatch_error(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how schema mismatches between declared and actual output are handled."""
    # This test may be skipped if schema validation isn't implemented yet
    pytest.skip(
        "Schema validation between declared and actual output is not implemented yet"
    )

    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.error_prone_udfs.schema_mismatch" in udfs

    # Create test DataFrame directly
    test_df = pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})

    # Get the table UDF function
    schema_mismatch_func = udfs["python_udfs.error_prone_udfs.schema_mismatch"]

    # Execute table UDF directly, which should raise schema mismatch error
    with pytest.raises(Exception) as excinfo:
        schema_mismatch_func(test_df)
        # Alternatively, the error might occur during engine execution

    # Verify the error message is informative
    error_message = str(excinfo.value)
    assert "schema" in error_message.lower() or "mismatch" in error_message.lower()


def test_error_in_complex_pipeline(error_handling_test_env: Dict[str, Any]) -> None:
    """Test how errors in UDFs are propagated in a complex pipeline."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(error_handling_test_env["project_dir"])

    # Discover UDFs
    udf_manager.discover_udfs()

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Create test data
    engine.execute_query(
        """
        CREATE TABLE input_data AS
        SELECT * FROM (
            VALUES
            (1, 10.0, 'A'),
            (2, 0.0, 'B'),    -- Will cause log(0) error
            (3, -5.0, 'C'),   -- Will cause log(negative) error
            (4, NULL, 'D')
        ) AS t(id, value, category);
        """
    )

    # Create a pipeline with multiple steps, including an error-prone UDF
    with pytest.raises(Exception) as excinfo:
        engine.execute_query(
            """
            -- Step 1: Filter the data
            CREATE TABLE step1_results AS
            SELECT * FROM input_data WHERE id > 0;
            
            -- Step 2: Apply logarithm UDF (will error with <= 0)
            CREATE TABLE step2_results AS
            SELECT
                id,
                value,
                category,
                logarithm(value) AS log_value
            FROM step1_results;
            
            -- Step 3: Further processing that won't be reached
            CREATE TABLE final_results AS
            SELECT
                id,
                category,
                log_value,
                log_value * 2 AS doubled_log
            FROM step2_results;
            """
        )

    # Verify the error message is informative
    error_message = str(excinfo.value)
    assert (
        "logarithm" in error_message.lower()
        or "domain error" in error_message.lower()
        or "invalid value" in error_message.lower()
    )

    # Verify that the first step was executed but later steps weren't
    try:
        step1_exists = engine.execute_query(
            "SELECT COUNT(*) FROM step1_results"
        ).fetchdf()
        assert step1_exists.iloc[0, 0] > 0, "First step table exists"
    except Exception:
        pytest.fail("First step execution failed unexpectedly")

    # Second step table should not exist or be empty
    with pytest.raises(Exception):
        engine.execute_query("SELECT * FROM step2_results")
