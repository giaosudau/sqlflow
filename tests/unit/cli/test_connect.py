import os
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from sqlflow.cli.main import app as main_app

runner = CliRunner()


def make_profile(profiles_dir: Path, name: str, type_: str, params: dict = None):
    params = params or {}
    params["type"] = type_
    profile = {name: params}

    os.makedirs(profiles_dir, exist_ok=True)
    with (profiles_dir / "dev.yml").open("w") as f:
        yaml.safe_dump(profile, f)
    return profiles_dir / "dev.yml"


@pytest.fixture
def mock_csv_profile(tmp_path):
    """Create a mock CSV profile for testing."""
    profiles_dir = tmp_path / "profiles"
    csv_file = tmp_path / "dummy.csv"
    csv_file.write_text("col1,col2\n1,2\n")
    make_profile(profiles_dir, "csv_test", "CSV", {"path": str(csv_file)})
    os.chdir(tmp_path)
    return profiles_dir


def test_connect_list_basic(mock_csv_profile):
    """Test basic connect list functionality."""
    result = runner.invoke(main_app, ["connect", "list", "--profile", "dev"])
    assert result.exit_code == 0
    assert "csv_test" in result.output
    assert "CSV" in result.output
    assert "✓ Ready" in result.output  # CSV file exists, should be ready


def test_connect_list_empty_profile(tmp_path):
    """Test connect list with empty profile."""
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "dev.yml").write_text("{}")
    os.chdir(tmp_path)

    result = runner.invoke(main_app, ["connect", "list", "--profile", "dev"])
    assert result.exit_code == 1
    assert "not found or empty" in result.output


def test_connect_list_invalid_profile(tmp_path):
    """Test connect list with invalid profile."""
    os.chdir(tmp_path)
    result = runner.invoke(main_app, ["connect", "list", "--profile", "nonexistent"])
    assert result.exit_code == 1
    assert "not found or empty" in result.output


def test_connect_test_success(mock_csv_profile):
    """Test successful connection test."""
    result = runner.invoke(
        main_app, ["connect", "test", "csv_test", "--profile", "dev"]
    )
    assert result.exit_code == 0
    assert "succeeded" in result.output
    assert "✓" in result.output


def test_connect_test_verbose(mock_csv_profile):
    """Test connection test with verbose output."""
    result = runner.invoke(
        main_app, ["connect", "test", "csv_test", "--profile", "dev", "--verbose"]
    )
    assert result.exit_code == 0
    assert "Testing connection 'csv_test'" in result.output
    assert "Type: CSV" in result.output
    assert "Parameters:" in result.output
    assert "path:" in result.output


def test_connect_test_nonexistent_profile(mock_csv_profile):
    """Test connection test with nonexistent profile."""
    result = runner.invoke(
        main_app, ["connect", "test", "nonexistent", "--profile", "dev"]
    )
    assert result.exit_code == 1
    assert "not found" in result.output


def test_connect_test_invalid_type(tmp_path):
    """Test connection test with invalid connector type."""
    profiles_dir = tmp_path / "profiles"
    make_profile(profiles_dir, "invalid_test", "INVALID")
    os.chdir(tmp_path)

    result = runner.invoke(
        main_app, ["connect", "test", "invalid_test", "--profile", "dev"]
    )
    assert result.exit_code == 2
    assert "Error" in result.output


def test_connect_test_missing_required_params(tmp_path):
    """Test connection test with missing required parameters."""
    profiles_dir = tmp_path / "profiles"
    make_profile(profiles_dir, "csv_test", "CSV")  # Missing path parameter
    os.chdir(tmp_path)

    result = runner.invoke(
        main_app, ["connect", "test", "csv_test", "--profile", "dev"]
    )
    assert result.exit_code == 2
    assert "Error" in result.output


def test_connect_test_bad_profile(mock_csv_profile):
    """Test connection test with malformed profile."""
    # Overwrite dev.yml with a bad profile
    bad_profile = {"bad_profile": {}}
    with (mock_csv_profile / "dev.yml").open("w") as f:
        yaml.safe_dump(bad_profile, f)

    result = runner.invoke(
        main_app, ["connect", "test", "bad_profile", "--profile", "dev"]
    )
    assert "missing 'type'" in result.output
    assert result.exit_code == 1
