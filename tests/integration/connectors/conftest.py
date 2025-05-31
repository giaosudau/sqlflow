"""Connector-specific fixtures for integration tests."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pyarrow as pa
import pyarrow.csv as csv_arrow
import pyarrow.parquet as pq
import pytest

from sqlflow.core.executors.local_executor import LocalExecutor


@pytest.fixture(scope="function")
def temp_connector_project() -> Generator[Dict[str, Any], None, None]:
    """Create a temporary project directory for connector testing.

    Yields
    ------
        Dictionary with project directory paths and structure
    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_connector_test_") as tmp_dir:
        # Create standard SQLFlow project structure
        project_paths = {
            "project_dir": tmp_dir,
            "data_dir": Path(tmp_dir) / "data",
            "output_dir": Path(tmp_dir) / "output",
            "pipelines_dir": Path(tmp_dir) / "pipelines",
            "profiles_dir": Path(tmp_dir) / "profiles",
        }

        # Create directories
        for path in project_paths.values():
            if isinstance(path, Path):
                path.mkdir(exist_ok=True)

        yield project_paths


@pytest.fixture(scope="function")
def connector_executor(temp_connector_project: Dict[str, Any]) -> LocalExecutor:
    """Create a LocalExecutor configured for connector testing.

    Args:
    ----
        temp_connector_project: Temporary project directory fixture

    Returns:
    -------
        Configured LocalExecutor with temporary project context
    """
    # Use project_dir parameter in constructor for UDF discovery
    # This avoids issues with os.getcwd() during parallel test execution
    executor = LocalExecutor(project_dir=temp_connector_project["project_dir"])
    return executor


@pytest.fixture(scope="function")
def sample_csv_data(temp_connector_project: Dict[str, Any]) -> Path:
    """Create a sample CSV file for testing.

    Args:
    ----
        temp_connector_project: Temporary project directory fixture

    Returns:
    -------
        Path to the created CSV file
    """
    csv_content = """customer_id,name,email,status
1,Alice Johnson,alice@example.com,active
2,Bob Smith,bob@example.com,inactive
3,Charlie Brown,charlie@example.com,active"""

    csv_file = temp_connector_project["data_dir"] / "sample_data.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def sample_data() -> pa.Table:
    """Create sample data for testing.

    Returns
    -------
        PyArrow Table with sample data
    """
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.5, 20.3, 30.1, 40.7, 50.9],
        "active": [True, False, True, True, False],
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def sample_csv_file(sample_data) -> Generator[str, None, None]:
    """Create a sample CSV file for testing.

    Yields
    ------
        Path to the sample CSV file
    """
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        csv_arrow.write_csv(sample_data, f.name)

    yield f.name

    os.unlink(f.name)


@pytest.fixture
def sample_parquet_file(sample_data) -> Generator[str, None, None]:
    """Create a sample Parquet file for testing.

    Yields
    ------
        Path to the sample Parquet file
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(sample_data, f.name)

    yield f.name

    os.unlink(f.name)


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for testing.

    Yields
    ------
        Path to the temporary directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir
