"""Tests for UDF CLI commands."""

from unittest import mock

from typer.testing import CliRunner

from sqlflow.cli.commands.udf import app
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf


# Sample test UDFs
@python_scalar_udf
def sample_double(x: float) -> float:
    """Double a value."""
    return x * 2


@python_table_udf
def sample_add_column(df):
    """Add a column to a DataFrame."""
    result = df.copy()
    result["doubled"] = result["value"] * 2
    return result


runner = CliRunner()


def test_list_udfs_empty():
    """Test listing UDFs when none are found."""
    with mock.patch("sqlflow.udfs.manager.PythonUDFManager.discover_udfs") as mock_discover:
        mock_discover.return_value = {}
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "No Python UDFs found" in result.stdout


def test_list_udfs():
    """Test listing UDFs with mock data."""
    with mock.patch("sqlflow.udfs.manager.PythonUDFManager.discover_udfs") as mock_discover:
        # Mock UDFs dictionary
        mock_discover.return_value = {
            "test_module.sample_double": sample_double,
            "test_module.sample_add_column": sample_add_column,
        }
        
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        
        # Check that both UDFs are listed in the output
        assert "sample_double" in result.stdout
        assert "sample_add_column" in result.stdout
        assert "scalar" in result.stdout
        assert "table" in result.stdout


def test_udf_info_not_found():
    """Test getting info for a UDF that doesn't exist."""
    with mock.patch("sqlflow.udfs.manager.PythonUDFManager.discover_udfs") as mock_discover:
        mock_discover.return_value = {}
        result = runner.invoke(app, ["info", "non_existent_udf"])
        assert result.exit_code == 0
        assert "not found" in result.stdout


def test_udf_info():
    """Test getting detailed info for a specific UDF."""
    with mock.patch("sqlflow.udfs.manager.PythonUDFManager.discover_udfs") as mock_discover:
        # Mock UDFs dictionary
        mock_discover.return_value = {
            "test_module.sample_double": sample_double
        }
        
        result = runner.invoke(app, ["info", "test_module.sample_double"])
        assert result.exit_code == 0
        
        # Check output contains detailed information
        assert "sample_double" in result.stdout
        assert "scalar" in result.stdout
        assert "Double a value" in result.stdout
        assert "SELECT PYTHON_FUNC" in result.stdout 