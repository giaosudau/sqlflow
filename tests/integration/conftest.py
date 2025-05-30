"""Shared fixtures for integration tests."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor


@pytest.fixture(scope="function")
def temp_db() -> Generator[str, None, None]:
    """Create a temporary database file for testing.

    Yields:
        Path to temporary database file
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    yield db_path

    # Clean up after test
    try:
        os.unlink(db_path)
    except (OSError, PermissionError):
        pass  # Handle case where file is locked or already removed


@pytest.fixture(scope="function")
def duckdb_engine() -> Generator[DuckDBEngine, None, None]:
    """Create a DuckDBEngine with an in-memory database.

    Yields:
        Configured DuckDBEngine instance
    """
    engine = DuckDBEngine(database_path=":memory:")
    yield engine
    engine.close()


@pytest.fixture(scope="function")
def local_executor(duckdb_engine: DuckDBEngine) -> LocalExecutor:
    """Create a LocalExecutor with a configured DuckDBEngine.

    Args:
        duckdb_engine: DuckDB engine fixture

    Returns:
        Configured LocalExecutor instance
    """
    executor = LocalExecutor()
    executor.duckdb_engine = duckdb_engine
    return executor


@pytest.fixture(scope="session")
def sample_users_data() -> pd.DataFrame:
    """Create sample user data for testing.

    Returns:
        DataFrame with sample user records
    """
    return pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "name": [
                "John Doe",
                "Jane Smith",
                "Bob Johnson",
                "Alice Brown",
                "Charlie Wilson",
            ],
            "email": [
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
                "alice@example.com",
                "charlie@example.com",
            ],
            "is_active": [True, True, False, True, True],
            "age": [25, 30, 35, 28, 42],
            "signup_date": [
                "2023-01-15",
                "2023-02-20",
                "2023-03-10",
                "2023-04-05",
                "2023-05-12",
            ],
        }
    )


@pytest.fixture(scope="session")
def sample_orders_data() -> pd.DataFrame:
    """Create sample order data for testing.

    Returns:
        DataFrame with sample order records
    """
    return pd.DataFrame(
        {
            "order_id": [101, 102, 103, 104, 105],
            "user_id": [1, 2, 1, 3, 4],
            "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Tablet"],
            "price": [999.99, 25.50, 75.00, 299.99, 449.99],
            "quantity": [1, 2, 1, 1, 1],
            "order_date": [
                "2023-06-01",
                "2023-06-02",
                "2023-06-03",
                "2023-06-04",
                "2023-06-05",
            ],
        }
    )


@pytest.fixture(scope="function")
def temp_udf_directory() -> Generator[Dict[str, Any], None, None]:
    """Create a temporary directory structure for UDF testing.

    Yields:
        Dictionary with project directory paths
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up directories
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create output directory
        output_dir = os.path.join(tmp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        # Create profiles directory
        profiles_dir = os.path.join(tmp_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "output_dir": output_dir,
            "profiles_dir": profiles_dir,
        }


@pytest.fixture(scope="function")
def basic_udf_file(temp_udf_directory: Dict[str, Any]) -> Path:
    """Create a basic UDF file for testing.

    Args:
        temp_udf_directory: Temporary directory fixture

    Returns:
        Path to created UDF file
    """
    udf_file = Path(temp_udf_directory["udf_dir"]) / "basic_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Basic UDFs for testing."""

import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def add_numbers(a: float, b: float) -> float:
    """Add two numbers together."""
    return a + b


@python_scalar_udf
def format_name(first_name: str, last_name: str) -> str:
    """Format first and last name."""
    return f"{first_name} {last_name}".strip()


@python_table_udf(output_schema={
    "user_id": "INTEGER",
    "total_orders": "INTEGER",
    "avg_order_value": "DOUBLE"
})
def calculate_user_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate user statistics from orders data."""
    return df.groupby("user_id").agg({
        "order_id": "count",
        "price": "mean"
    }).rename(columns={
        "order_id": "total_orders",
        "price": "avg_order_value"
    }).reset_index()
'''
        )
    return udf_file


@pytest.fixture(scope="session")
def large_dataset() -> pd.DataFrame:
    """Create a large dataset for performance testing.

    Returns:
        DataFrame with 10,000 records for performance tests
    """
    import numpy as np

    np.random.seed(42)  # For reproducible tests
    n_records = 10000

    return pd.DataFrame(
        {
            "id": range(1, n_records + 1),
            "value": np.random.uniform(0, 1000, n_records),
            "category": np.random.choice(["A", "B", "C", "D"], n_records),
            "text": [f"sample_text_{i}" for i in range(n_records)],
            "flag": np.random.choice([True, False], n_records),
        }
    )


@pytest.fixture(scope="function")
def clean_environment():
    """Ensure clean test environment by removing any global state."""
    # This fixture can be used to clean up any global state between tests
    yield
    # Add cleanup code here if needed
