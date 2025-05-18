"""Integration tests for Python UDFs."""

import os
import tempfile
from pathlib import Path

import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.udfs.manager import PythonUDFManager


def create_test_udf_file(udf_dir):
    """Create a test UDF file with sample UDFs."""
    udf_file = Path(udf_dir) / "test_udf.py"
    with open(udf_file, "w") as f:
        f.write(
            """
from sqlflow.udfs import python_scalar_udf, python_table_udf
import pandas as pd

@python_scalar_udf
def add_numbers(a: int, b: int) -> int:
    \"\"\"Add two numbers together.\"\"\"
    return a + b

@python_scalar_udf(name="multiply")
def multiply_numbers(a: int, b: int) -> int:
    \"\"\"Multiply two numbers together.\"\"\"
    return a * b

@python_table_udf
def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Filter rows where value > 5.\"\"\"
    return df[df["value"] > 5]
"""
        )
    return udf_file


def create_test_pipeline_file(pipeline_dir, udf_dir):
    """Create a test pipeline file that uses Python UDFs."""
    pipeline_file = Path(pipeline_dir) / "test_udf_pipeline.sf"
    with open(pipeline_file, "w") as f:
        f.write(
            """
-- Generate some test data
CREATE TABLE source_data AS
SELECT * FROM (
    VALUES
    (1, 2, 3),
    (4, 5, 6),
    (7, 8, 9)
) AS t(id, value, extra);

-- Test scalar UDF
CREATE TABLE scalar_results AS
SELECT
    id,
    value,
    PYTHON_FUNC("test_udf.add_numbers", id, value) AS sum,
    PYTHON_FUNC("test_udf.multiply", id, value) AS product
FROM source_data;

-- Test table UDF with simplified syntax
CREATE TABLE table_results AS
SELECT * FROM PYTHON_FUNC("test_udf.filter_rows", source_data);
"""
        )
    return pipeline_file


@pytest.fixture
def test_env():
    """Create a test environment with UDFs and a pipeline."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up directories
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        pipeline_dir = os.path.join(tmp_dir, "pipelines")
        os.makedirs(udf_dir, exist_ok=True)
        os.makedirs(pipeline_dir, exist_ok=True)

        # Create test files
        udf_file = create_test_udf_file(udf_dir)
        pipeline_file = create_test_pipeline_file(pipeline_dir, udf_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "pipeline_dir": pipeline_dir,
            "udf_file": udf_file,
            "pipeline_file": pipeline_file,
        }


def test_udf_discovery_and_execution(test_env):
    """Test that UDFs can be discovered and executed in a pipeline."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered correctly
    assert "test_udf.add_numbers" in udfs
    assert "test_udf.multiply" in udfs
    assert "test_udf.filter_rows" in udfs

    # Set up engine and executor
    engine = DuckDBEngine()
    executor = LocalExecutor(engine=engine)

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Execute the pipeline
    result = executor.execute_file(test_env["pipeline_file"])

    # Verify execution was successful
    assert result.success

    # Get results from the engine
    scalar_results = engine.query("SELECT * FROM scalar_results")
    table_results = engine.query("SELECT * FROM table_results")

    # Verify scalar UDF results
    assert len(scalar_results) == 3
    for row in scalar_results:
        assert row["sum"] == row["id"] + row["value"]
        assert row["product"] == row["id"] * row["value"]

    # Verify table UDF results
    assert len(table_results) == 1  # Only one row should have value > 5
    assert table_results[0]["value"] > 5


def test_udf_error_handling(test_env):
    """Test error handling for UDFs that raise exceptions."""
    # Create a UDF that will raise an exception
    error_udf_file = Path(test_env["udf_dir"]) / "error_udf.py"
    with open(error_udf_file, "w") as f:
        f.write(
            """
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def division(a: int, b: int) -> float:
    \"\"\"Divide two numbers.\"\"\"
    return a / b
"""
        )

    # Create a pipeline that will cause a division by zero
    error_pipeline_file = Path(test_env["pipeline_dir"]) / "error_pipeline.sf"
    with open(error_pipeline_file, "w") as f:
        f.write(
            """
-- Generate test data with a zero value
CREATE TABLE division_data AS
SELECT * FROM (
    VALUES
    (10, 2),
    (10, 0)  -- Will cause division by zero
) AS t(a, b);

-- This will fail on the second row
CREATE TABLE division_results AS
SELECT
    a, b, 
    PYTHON_FUNC("error_udf.division", a, b) AS result
FROM division_data;
"""
        )

    # Set up UDF manager and discover UDFs
    udf_manager = PythonUDFManager(test_env["project_dir"])
    udfs = udf_manager.discover_udfs()
    assert "error_udf.division" in udfs

    # Set up engine and executor
    engine = DuckDBEngine()
    executor = LocalExecutor(engine=engine)
    udf_manager.register_udfs_with_engine(engine)

    # Execute the pipeline and expect it to fail
    result = executor.execute_file(error_pipeline_file)
    assert not result.success
    assert "division by zero" in str(result.error).lower()


def test_udf_parameter_validation():
    """Test parameter validation for UDFs."""
    # This test would validate that UDFs receive the correct parameter types
    # and that appropriate error messages are generated for type mismatches
    pass  # To be implemented


def test_udf_performance_with_large_dataset():
    """Test UDF performance with a larger dataset."""
    # This test would create a larger dataset and measure performance
    # of both scalar and table UDFs
    pass  # To be implemented


def test_udf_with_dependencies():
    """Test UDFs with external dependencies."""
    # This test would verify that UDFs can use external libraries
    pass  # To be implemented
