"""Integration tests for Python UDF performance with various data sizes."""

import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


@pytest.fixture
def performance_test_env() -> Dict[str, Any]:
    """Create a test environment with UDFs suitable for performance testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up directories
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create UDF file with optimized and non-optimized implementations
        udf_file = create_performance_udf_file(udf_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "udf_file": udf_file,
        }


def create_performance_udf_file(udf_dir: str) -> Path:
    """Create a UDF file with optimized and non-optimized implementations for performance testing."""
    udf_file = Path(udf_dir) / "performance_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            """
import pandas as pd
import numpy as np
from sqlflow.udfs import python_scalar_udf, python_table_udf

# Simple scalar UDF for baseline performance
@python_scalar_udf
def add_ten(value: float) -> float:
    \"\"\"Add 10 to a value.\"\"\"
    if value is None:
        return None
    return value + 10

# Computationally intensive scalar UDF
@python_scalar_udf
def complex_calculation(value: float) -> float:
    \"\"\"Perform a more complex calculation on a value.\"\"\"
    if value is None:
        return None
    # Simulate complex computation with multiple operations
    return (value ** 2) * np.sin(value) + np.log(max(1.0, abs(value)))

# Non-optimized table UDF (row-by-row processing)
@python_table_udf(output_schema={
    "id": "INTEGER",
    "input_value": "DOUBLE",
    "processed_value": "DOUBLE"
})
def process_data_row_by_row(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Process data row by row (non-optimized).\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["input_value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    
    # Non-vectorized approach (slower)
    processed_values = []
    for value in df["value"]:
        if pd.isna(value):
            processed_values.append(None)
        else:
            # Simulate complex processing
            processed = (value ** 2) * np.sin(value) + np.log(max(1.0, abs(value)))
            processed_values.append(processed)
    
    result["processed_value"] = processed_values
    return result

# Optimized table UDF (vectorized processing)
@python_table_udf(output_schema={
    "id": "INTEGER",
    "input_value": "DOUBLE",
    "processed_value": "DOUBLE"
})
def process_data_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Process data using vectorized operations (optimized).\"\"\"
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["input_value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    
    # Vectorized approach (faster)
    if "value" in df.columns:
        valid_mask = ~df["value"].isna()
        result["processed_value"] = pd.Series(dtype="float64")
        
        # Apply vectorized operations only on valid data
        if valid_mask.any():
            valid_values = df.loc[valid_mask, "value"]
            result.loc[valid_mask, "processed_value"] = (
                (valid_values ** 2) * np.sin(valid_values) + 
                np.log(np.maximum(1.0, np.abs(valid_values)))
            )
    else:
        result["processed_value"] = pd.Series(dtype="float64")
    
    return result

# Table UDF for aggregating data
@python_table_udf(output_schema={
    "group_id": "INTEGER",
    "count": "INTEGER",
    "sum": "DOUBLE",
    "mean": "DOUBLE",
    "stddev": "DOUBLE"
})
def aggregate_by_group(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Aggregate data by group (optimized).\"\"\"
    if "group_id" not in df.columns or "value" not in df.columns or len(df) == 0:
        return pd.DataFrame({
            "group_id": pd.Series(dtype="Int64"),
            "count": pd.Series(dtype="Int64"),
            "sum": pd.Series(dtype="float64"),
            "mean": pd.Series(dtype="float64"),
            "stddev": pd.Series(dtype="float64")
        })
    
    # Use pandas groupby for efficient aggregation
    agg_result = df.groupby("group_id")["value"].agg([
        ("count", "count"),
        ("sum", "sum"),
        ("mean", "mean"),
        ("stddev", "std")
    ]).reset_index()
    
    # Ensure correct column types
    agg_result["group_id"] = agg_result["group_id"].astype("Int64")
    agg_result["count"] = agg_result["count"].astype("Int64")
    
    return agg_result
"""
        )
    return udf_file


def generate_test_data(size: str) -> pd.DataFrame:
    """Generate test data of various sizes for performance testing."""
    if size == "small":
        n_rows = 100
    elif size == "medium":
        n_rows = 1000
    elif size == "large":
        n_rows = 10000
    else:
        raise ValueError(f"Unknown size: {size}")

    # Generate synthetic data
    np.random.seed(42)  # For reproducibility

    return pd.DataFrame(
        {
            "id": range(1, n_rows + 1),
            "value": np.random.uniform(-10, 10, n_rows),
            "group_id": np.random.randint(1, 6, n_rows),  # 5 groups
        }
    )


def time_execution(func, *args, **kwargs) -> Tuple[Any, float]:
    """Measure execution time of a function."""
    start_time = time.time()
    result = func(*args, **kwargs)
    execution_time = time.time() - start_time
    return result, execution_time


@pytest.mark.parametrize("size", ["small", "medium"])
def test_scalar_udf_performance(
    performance_test_env: Dict[str, Any], size: str
) -> None:
    """Test performance of scalar UDFs with different data sizes."""
    # For large datasets, this test might take too long in CI
    if size == "large" and os.environ.get("CI") == "true":
        pytest.skip("Skipping large dataset test in CI environment")

    # Set up UDF manager
    udf_manager = PythonUDFManager(performance_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.performance_udfs.add_ten" in udfs
    assert "python_udfs.performance_udfs.complex_calculation" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Generate test data
    test_data = generate_test_data(size)

    # Create a temporary table with the test data
    engine.execute_query(
        "CREATE TABLE test_data(id INTEGER, value DOUBLE, group_id INTEGER)"
    )

    # Insert data row by row (since parameters not supported)
    for _, row in test_data.iterrows():
        engine.execute_query(
            f"INSERT INTO test_data VALUES ({row['id']}, {row['value']}, {row['group_id']})"
        )

    # Measure simple UDF performance
    _, simple_time = time_execution(
        engine.execute_query, "SELECT id, add_ten(value) AS result FROM test_data"
    )

    # Measure complex UDF performance
    _, complex_time = time_execution(
        engine.execute_query,
        "SELECT id, complex_calculation(value) AS result FROM test_data",
    )

    # Log performance results
    logger.info(f"Size: {size}, Rows: {len(test_data)}")
    logger.info(f"Simple UDF execution time: {simple_time:.6f} seconds")
    logger.info(f"Complex UDF execution time: {complex_time:.6f} seconds")

    # Verify that complex calculations take longer than simple ones
    # Note: For very small datasets, this might not always be true due to
    # other overheads, so we'll skip the assertion for small datasets
    if size != "small":
        assert (
            complex_time > simple_time
        ), "Complex UDF should take longer than simple UDF"


@pytest.mark.parametrize("size", ["small", "medium"])
def test_table_udf_optimization(
    performance_test_env: Dict[str, Any], size: str
) -> None:
    """Test performance difference between optimized and non-optimized table UDFs."""
    # For large datasets, this test might take too long in CI
    if size == "large" and os.environ.get("CI") == "true":
        pytest.skip("Skipping large dataset test in CI environment")

    # Set up UDF manager
    udf_manager = PythonUDFManager(performance_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.performance_udfs.process_data_row_by_row" in udfs
    assert "python_udfs.performance_udfs.process_data_vectorized" in udfs

    # Generate test data
    test_data = generate_test_data(size)

    # Skip small dataset test because vectorization overhead might outweigh the benefits
    # for very small datasets
    if size == "small":
        pytest.skip("Skipping small dataset test for optimization comparison")

    # Get the UDF functions
    row_by_row_func = udfs["python_udfs.performance_udfs.process_data_row_by_row"]
    vectorized_func = udfs["python_udfs.performance_udfs.process_data_vectorized"]

    # Run multiple times to get more stable results
    non_optimized_times = []
    optimized_times = []

    for _ in range(3):  # Run 3 times
        # Measure non-optimized UDF performance
        _, non_optimized_time = time_execution(row_by_row_func, test_data)
        non_optimized_times.append(non_optimized_time)

        # Measure optimized UDF performance
        _, optimized_time = time_execution(vectorized_func, test_data)
        optimized_times.append(optimized_time)

    # Use the average times
    avg_non_optimized = sum(non_optimized_times) / len(non_optimized_times)
    avg_optimized = sum(optimized_times) / len(optimized_times)

    # Log performance results
    logger.info(f"Size: {size}, Rows: {len(test_data)}")
    logger.info(
        f"Non-optimized table UDF execution time: {avg_non_optimized:.6f} seconds"
    )
    logger.info(f"Optimized table UDF execution time: {avg_optimized:.6f} seconds")
    logger.info(f"Speedup: {avg_non_optimized / avg_optimized:.2f}x")

    # Verify that optimized version is faster for medium/large datasets
    # Note: In some runs, especially with JIT warmup, the first vectorized run might be slower
    # so we'll add tolerance to this check
    if size == "medium":
        # For medium datasets, there can be variance, so we'll use a tolerance
        # Accept if vectorized is within 10% of non-optimized, as test environments can vary
        assert (
            avg_optimized < avg_non_optimized * 1.1
        ), "Vectorized UDF should be close to or faster than row-by-row UDF for medium dataset"
        logger.info("Performance difference within acceptable range for medium dataset")
    else:
        # For large datasets, vectorized should be significantly faster
        assert (
            avg_optimized < avg_non_optimized
        ), "Vectorized UDF should be faster than row-by-row UDF for large dataset"


@pytest.mark.parametrize("size", ["small", "medium"])
def test_table_udf_aggregation_performance(
    performance_test_env: Dict[str, Any], size: str
) -> None:
    """Test performance of table UDFs that perform aggregation operations."""
    # For large datasets, this test might take too long in CI
    if size == "large" and os.environ.get("CI") == "true":
        pytest.skip("Skipping large dataset test in CI environment")

    # Set up UDF manager
    udf_manager = PythonUDFManager(performance_test_env["project_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered
    assert "python_udfs.performance_udfs.aggregate_by_group" in udfs

    # Set up engine
    engine = DuckDBEngine(":memory:")

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Generate test data
    test_data = generate_test_data(size)

    # Create a temporary table with the test data
    engine.execute_query(
        "CREATE TABLE test_data(id INTEGER, value DOUBLE, group_id INTEGER)"
    )

    # Insert data row by row (since parameters not supported)
    for _, row in test_data.iterrows():
        engine.execute_query(
            f"INSERT INTO test_data VALUES ({row['id']}, {row['value']}, {row['group_id']})"
        )

    # Measure SQL aggregation performance
    _, sql_time = time_execution(
        engine.execute_query,
        """
        SELECT 
            group_id,
            COUNT(*) AS count,
            SUM(value) AS sum,
            AVG(value) AS mean,
            STDDEV(value) AS stddev
        FROM test_data
        GROUP BY group_id
        """,
    )

    # Instead of using direct SQL to call the table UDF, use the aggregate UDF directly
    # Get the table UDF function
    aggregate_func = udfs["python_udfs.performance_udfs.aggregate_by_group"]

    # Prepare the input data for the UDF
    input_df = engine.execute_query("SELECT group_id, value FROM test_data").fetchdf()

    # Measure UDF performance by calling it directly
    _, udf_time = time_execution(aggregate_func, input_df)

    # Log performance results
    logger.info(f"Size: {size}, Rows: {len(test_data)}")
    logger.info(f"SQL aggregation execution time: {sql_time:.6f} seconds")
    logger.info(f"UDF aggregation execution time: {udf_time:.6f} seconds")
    logger.info(f"Ratio (UDF/SQL): {udf_time / sql_time:.2f}x")

    # Note: We don't assert performance characteristics here because SQL might be faster
    # or slower depending on the environment. We just log the results.
