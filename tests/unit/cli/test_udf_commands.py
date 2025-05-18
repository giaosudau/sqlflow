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
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        mock_discover.return_value = {}
        result = runner.invoke(app, ["list", "--plain"])
        assert result.exit_code == 0
        assert "No Python UDFs found" in result.stdout


def test_list_udfs():
    """Test listing UDFs with mock data."""
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        # Set up sample UDFs with proper attributes for the test
        double_func = sample_double
        double_func._udf_name = "sample_double"
        double_func._udf_type = "scalar"

        add_column_func = sample_add_column
        add_column_func._udf_name = "sample_add_column"
        add_column_func._udf_type = "table"

        # Mock UDFs dictionary
        mock_discover.return_value = {
            "test_module.sample_double": double_func,
            "test_module.sample_add_column": add_column_func,
        }

        # Use plain text output for easier testing
        result = runner.invoke(app, ["list", "--plain"])
        assert result.exit_code == 0

        # With plain output, we can check for exact strings
        assert "test_module.sample_double (scalar): Double a value" in result.stdout
        assert (
            "test_module.sample_add_column (table): Add a column to a DataFrame"
            in result.stdout
        )


def test_udf_info_not_found():
    """Test getting info for a UDF that doesn't exist."""
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        # Need to return non-empty dict so we get the "not found" message
        double_func = sample_double
        mock_discover.return_value = {"some_other_udf": double_func}

        result = runner.invoke(app, ["info", "non_existent_udf", "--plain"])
        assert result.exit_code == 0
        assert "not found" in result.stdout


def test_udf_info():
    """Test getting detailed info for a specific UDF."""
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        # Set up sample UDF with proper attributes
        double_func = sample_double
        double_func._udf_name = "sample_double"
        double_func._udf_type = "scalar"

        # Mock UDFs dictionary
        mock_discover.return_value = {"test_module.sample_double": double_func}

        # Use plain text output for easier testing
        result = runner.invoke(app, ["info", "test_module.sample_double", "--plain"])
        assert result.exit_code == 0

        # Check output contains detailed information
        assert "UDF: test_module.sample_double" in result.stdout
        assert "Type: scalar" in result.stdout
        assert "Double a value" in result.stdout
        assert "SELECT PYTHON_FUNC" in result.stdout
