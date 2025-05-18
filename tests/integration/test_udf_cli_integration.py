"""Integration tests for UDF CLI commands."""

import os
import tempfile
from pathlib import Path

from typer.testing import CliRunner

from sqlflow.cli.main import app


def create_test_udf_file(udf_dir):
    """Create a test UDF file with sample UDFs."""
    os.makedirs(udf_dir, exist_ok=True)
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


def test_udf_list_command():
    """Test the 'sqlflow udf list' command with real UDFs."""
    runner = CliRunner()

    # Create a temporary project structure
    with tempfile.TemporaryDirectory() as tmp_dir:
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        create_test_udf_file(udf_dir)

        # Directly use sqlflow udf list command
        result = runner.invoke(
            app, ["udf", "list", "--plain", "--project-dir", tmp_dir]
        )

        # Verify command was successful
        assert result.exit_code == 0

        # Verify all UDFs are listed in the output
        assert "python_udfs.test_udf.add_numbers (scalar)" in result.stdout
        assert "python_udfs.test_udf.multiply (scalar)" in result.stdout
        assert "python_udfs.test_udf.filter_rows (table)" in result.stdout


def test_udf_info_command():
    """Test the 'sqlflow udf info' command with real UDFs."""
    runner = CliRunner()

    # Create a temporary project structure
    with tempfile.TemporaryDirectory() as tmp_dir:
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        create_test_udf_file(udf_dir)

        # Test info command for a specific UDF
        result = runner.invoke(
            app,
            [
                "udf",
                "info",
                "python_udfs.test_udf.add_numbers",
                "--plain",
                "--project-dir",
                tmp_dir,
            ],
        )

        # Verify command was successful
        assert result.exit_code == 0

        # Verify UDF information is displayed correctly
        assert "UDF: python_udfs.test_udf.add_numbers" in result.stdout
        assert "Type: scalar" in result.stdout
        assert "Add two numbers together" in result.stdout
        assert "PYTHON_FUNC" in result.stdout


def test_udf_info_command_not_found():
    """Test the 'sqlflow udf info' command with a non-existent UDF."""
    runner = CliRunner()

    # Create a temporary project structure
    with tempfile.TemporaryDirectory() as tmp_dir:
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        create_test_udf_file(udf_dir)

        # Test info command for a non-existent UDF
        result = runner.invoke(
            app,
            ["udf", "info", "non_existent_udf", "--plain", "--project-dir", tmp_dir],
        )

        # Verify command was successful (the command itself, not finding the UDF)
        assert result.exit_code == 0

        # Verify "not found" message is displayed
        assert "not found" in result.stdout
