"""Connector-specific fixtures for integration tests."""

import tempfile
from pathlib import Path
from typing import Dict, Any, Generator

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
    executor = LocalExecutor(project_dir=temp_connector_project["project_dir"])
    return executor


@pytest.fixture(scope="function")
def sample_csv_data(temp_connector_project: Dict[str, Any]) -> Path:
    """Create sample CSV data for connector testing.

    Args:
    ----
        temp_connector_project: Temporary project directory fixture

    Returns:
    -------
        Path to created CSV file with sample data
    """
    csv_file = temp_connector_project["data_dir"] / "sample_data.csv"
    csv_content = """id,name,value,category
1,Product A,100.0,electronics
2,Product B,200.0,clothing
3,Product C,150.0,electronics
4,Product D,75.0,books
5,Product E,300.0,clothing"""

    csv_file.write_text(csv_content)
    return csv_file
