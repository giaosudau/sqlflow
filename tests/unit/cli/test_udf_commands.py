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
        assert "No Python UDFs found in the project" in result.stdout


def test_list_udfs():
    """Test listing UDFs with mock data."""
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.list_udfs"
    ) as mock_list_udfs, mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        # Mock the list_udfs method to return a list with our test UDFs
        mock_list_udfs.return_value = [
            {
                "name": "sample_double",
                "full_name": "test_module.sample_double",
                "type": "scalar",
                "docstring_summary": "Double a value.",
                "module": "test_module",
            },
            {
                "name": "sample_add_column",
                "full_name": "test_module.sample_add_column",
                "type": "table",
                "docstring_summary": "Add a column to a DataFrame.",
                "module": "test_module",
            },
        ]

        # Need to mock discover_udfs to return non-empty result to avoid early return
        mock_discover.return_value = {"dummy": lambda: None}

        # Use plain text output for easier testing
        result = runner.invoke(app, ["list", "--plain"])
        assert result.exit_code == 0

        # Check for format in actual CLI output
        assert "test_module.sample_double (scalar): Double a value" in result.stdout
        assert (
            "test_module.sample_add_column (table): Add a column to a DataFrame"
            in result.stdout
        )


def test_udf_info_not_found():
    """Test getting info for a UDF that doesn't exist."""
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.get_udf_info"
    ) as mock_get_info, mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        # Mock the get_udf_info method to return None
        mock_get_info.return_value = None

        # Need to mock discover_udfs to return non-empty result to avoid early return
        mock_discover.return_value = {"dummy": lambda: None}

        result = runner.invoke(app, ["info", "non_existent_udf", "--plain"])
        assert result.exit_code == 0
        assert "UDF 'non_existent_udf' not found" in result.stdout


def test_udf_info():
    """Test getting detailed info for a specific UDF."""
    with mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.get_udf_info"
    ) as mock_get_info, mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.validate_udf_metadata"
    ) as mock_validate, mock.patch(
        "sqlflow.udfs.manager.PythonUDFManager.discover_udfs"
    ) as mock_discover:
        # Mock the get_udf_info method to return our test UDF info
        mock_get_info.return_value = {
            "name": "sample_double",
            "full_name": "test_module.sample_double",
            "type": "scalar",
            "docstring": "Double a value.",
            "docstring_summary": "Double a value.",
            "module": "test_module",
            "signature": "(x: float) -> float",
            "formatted_signature": "(x: float) -> float",
            "file_path": "/path/to/sample.py",
            "param_details": {
                "x": {
                    "type": "float",
                    "has_default": False,
                }
            },
        }

        # Mock validation to return no warnings
        mock_validate.return_value = []

        # Need to mock discover_udfs to return non-empty result to avoid early return
        mock_discover.return_value = {"dummy": lambda: None}

        # Use plain text output for easier testing
        result = runner.invoke(app, ["info", "test_module.sample_double", "--plain"])
        assert result.exit_code == 0

        # Check output for actual CLI format
        assert "UDF: test_module.sample_double" in result.stdout
        assert "Type: scalar" in result.stdout
        assert "Signature: (x: float) -> float" in result.stdout



