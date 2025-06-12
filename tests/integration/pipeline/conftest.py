"""Pipeline-specific fixtures for integration tests."""

import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner_main import OperationPlanner
from sqlflow.parser.ast import Pipeline


@pytest.fixture(scope="function")
def temp_pipeline_project() -> Generator[Dict[str, Any], None, None]:
    """Create a temporary project directory for pipeline testing.

    Yields
    ------
        Dictionary with project directory paths and structure
    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_pipeline_test_") as tmp_dir:
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
def pipeline_executor(temp_pipeline_project: Dict[str, Any]) -> LocalExecutor:
    """Create a LocalExecutor configured for pipeline testing.

    Args:
    ----
        temp_pipeline_project: Temporary project directory fixture

    Returns:
    -------
        Configured LocalExecutor with temporary project context
    """
    executor = LocalExecutor(project_dir=temp_pipeline_project["project_dir"])
    return executor


@pytest.fixture(scope="function")
def operation_planner() -> OperationPlanner:
    """Create an OperationPlanner for testing.

    Returns:
    -------
        Configured OperationPlanner instance
    """
    return OperationPlanner()


@pytest.fixture(scope="function")
def sample_pipeline() -> Pipeline:
    """Create a sample pipeline for testing.

    Returns:
    -------
        Sample Pipeline instance with basic structure
    """
    return Pipeline()
