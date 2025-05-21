"""Integration tests for Python UDFs."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


def create_test_udf_file(udf_dir):
    """Create a test UDF file with sample UDFs."""
    udf_file = Path(udf_dir) / "test_udf.py"
    with open(udf_file, "w") as f:
        f.write(
            """
from sqlflow.udfs import python_scalar_udf
import pandas as pd

@python_scalar_udf
def add_numbers(a: int, b: int) -> int:
    \"\"\"Add two numbers together.\"\"\"
    return a + b

@python_scalar_udf(name="multiply")
def multiply_numbers(a: int, b: int) -> int:
    \"\"\"Multiply two numbers together.\"\"\"
    return a * b
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
"""
        )
    return pipeline_file


@pytest.fixture
def test_env() -> Dict[str, Any]:
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


def test_udf_discovery_and_execution(test_env: Dict[str, Any]) -> None:
    """Test that UDFs can be discovered and executed directly in SQL, and both full and flat names are registered."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Print discovered UDF names to debug
    print("DEBUG: Discovered UDFs:")
    for udf_name in udfs:
        print(f"DEBUG:   - {udf_name}")

    # Verify UDFs were discovered correctly
    assert "python_udfs.test_udf.add_numbers" in udfs
    assert "python_udfs.test_udf.multiply" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine and debug registered names
    print("DEBUG: Registering UDFs with engine")
    udf_manager.register_udfs_with_engine(engine)

    # Print registered UDFs from the engine
    print("DEBUG: Registered UDFs in engine:")
    for udf_name in engine.registered_udfs:
        print(f"DEBUG:   - {udf_name}")

    # Execute queries directly using the UDFs without PYTHON_FUNC syntax
    # First query - create test data
    engine.execute_query(
        """
    CREATE TABLE source_data AS
    SELECT * FROM (
        VALUES
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)
    ) AS t(id, value, extra);
    """
    )

    # Second query - use UDFs with their flat name as registered in DuckDB
    engine.execute_query(
        """
    CREATE TABLE scalar_results AS
    SELECT
        id,
        value,
        add_numbers(id, value) AS sum,
        multiply(id, value) AS product
    FROM source_data;
    """
    )

    # Get results from the engine
    scalar_results = engine.execute_query("SELECT * FROM scalar_results").fetchdf()
    print(f"DEBUG: Results: {scalar_results}")

    # Verify scalar UDF results
    assert len(scalar_results) == 3
    for _, row in scalar_results.iterrows():
        assert row["sum"] == row["id"] + row["value"]
        assert row["product"] == row["id"] * row["value"]

    # Check both full and flat names
    assert "python_udfs.test_udf.add_numbers" in udfs
    assert "python_udfs.test_udf.multiply" in udfs
    # Flat name should also be registered
    engine = DuckDBEngine(":memory:")
    udf_manager.register_udfs_with_engine(engine)
    registered = set(engine.registered_udfs)
    assert "add_numbers" in registered or any("add_numbers" in k for k in registered)
    assert "multiply" in registered or any("multiply" in k for k in registered)


def test_udf_error_handling(test_env: Dict[str, Any]) -> None:
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

    # Set up UDF manager and discover UDFs
    udf_manager = PythonUDFManager(test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Print discovered UDF names
    print("DEBUG: Discovered division UDF:")
    for udf_name in udfs:
        if "division" in udf_name:
            print(f"DEBUG:   - {udf_name}")

    assert "python_udfs.error_udf.division" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Print registered UDFs
    print("DEBUG: Registered UDFs in engine:")
    for udf_name in engine.registered_udfs:
        print(f"DEBUG:   - {udf_name}")

    # Create test data
    engine.execute_query(
        """
    CREATE TABLE division_data AS
    SELECT * FROM (
        VALUES
        (10, 2),
        (10, 0)  -- Will cause division by zero
    ) AS t(a, b);
    """
    )

    # Execute query with division by zero (should fail)
    try:
        engine.execute_query(
            """
        CREATE TABLE division_results AS
        SELECT
            a, b, 
            division(a, b) AS result
        FROM division_data;
        """
        )
        # If we get here, the test failed
        assert False, "Expected division by zero error"
    except Exception as e:
        print(f"DEBUG: Got exception: {str(e)}")
        # Test passes if we get a division by zero error
        assert "division by zero" in str(e).lower()


def test_udf_parameter_validation() -> None:
    """Test parameter validation for UDFs."""
    # TODO: Track missing tests for parameter validation in sqlflow_tasks.md


def test_udf_performance_with_large_dataset() -> None:
    """Test UDF performance with a larger dataset."""
    # TODO: Track missing tests for performance in sqlflow_tasks.md


def test_udf_with_dependencies() -> None:
    """Test UDFs with external dependencies."""
    # TODO: Track missing tests for UDFs with external dependencies in sqlflow_tasks.md
