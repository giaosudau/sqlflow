"""Tests for pipeline commands."""

import os
import tempfile
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from sqlflow.cli.main import app


@pytest.fixture
def runner():
    """Create a CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def sample_project():
    """Create a sample project for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)

        with open(os.path.join(pipelines_dir, "test.sf"), "w") as f:
            f.write(
                """
SOURCE sample TYPE CSV PARAMS {
    "path": "data/sample.csv",
    "has_header": true
};

LOAD raw_data FROM sample;
            """
            )

        with open(os.path.join(tmpdir, "sqlflow.yml"), "w") as f:
            f.write(
                """
name: test_project
version: 0.1.0
default_profile: default
paths:
    pipelines: pipelines
            """
            )

        yield tmpdir


def test_list_command(runner, sample_project):
    """Test the list command."""
    with patch("os.getcwd", return_value=sample_project):
        result = runner.invoke(app, ["pipeline", "list"])
        assert result.exit_code == 0
        assert "test" in result.stdout


def test_compile_command(runner, sample_project):
    """Test the compile command."""
    with patch("os.getcwd", return_value=sample_project):
        result = runner.invoke(app, ["pipeline", "compile", "test"])
        assert result.exit_code == 0
        assert "source_sample" in result.stdout
        assert "load_raw_data" in result.stdout


def test_run_command(runner, sample_project):
    """Test the run command."""
    with patch("os.getcwd", return_value=sample_project):
        result = runner.invoke(
            app, ["pipeline", "run", "test", "--vars", '{"date": "2023-10-25"}']
        )
        assert result.exit_code == 0
        assert "test" in result.stdout
        assert "2023-10-25" in result.stdout
