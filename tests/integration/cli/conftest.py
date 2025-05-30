"""CLI-specific fixtures for integration tests."""

import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pytest
from typer.testing import CliRunner

from sqlflow.cli.main import app


@pytest.fixture(scope="function")
def temp_cli_project() -> Generator[Dict[str, Any], None, None]:
    """Create a temporary project directory for CLI testing.

    Yields
    ------
        Dictionary with project directory paths and structure
    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_cli_test_") as tmp_dir:
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
def cli_runner(temp_cli_project: Dict[str, Any]) -> CliRunner:
    """Create a CliRunner configured for CLI testing.

    Args:
    ----
        temp_cli_project: Temporary project directory fixture

    Returns:
    -------
        Configured CliRunner with temporary project context
    """
    return CliRunner()


@pytest.fixture(scope="function")
def cli_app():
    """Provide the CLI app for testing.

    Returns:
    -------
        The SQLFlow CLI app instance
    """
    return app
