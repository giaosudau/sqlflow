"""Tests for environment variable utilities."""

import os
import tempfile
from pathlib import Path

from sqlflow.utils.env import (
    find_project_root,
    get_env_var,
    list_env_vars,
    load_dotenv_file,
    setup_environment,
)


class TestEnvironmentUtils:
    """Test environment variable utilities."""

    def test_get_env_var_existing(self):
        """Test getting an existing environment variable."""
        # Set a test environment variable
        os.environ["TEST_VAR"] = "test_value"

        try:
            result = get_env_var("TEST_VAR")
            assert result == "test_value"
        finally:
            # Clean up
            del os.environ["TEST_VAR"]

    def test_get_env_var_missing_with_default(self):
        """Test getting a missing environment variable with default."""
        result = get_env_var("NONEXISTENT_VAR", "default_value")
        assert result == "default_value"

    def test_get_env_var_missing_without_default(self):
        """Test getting a missing environment variable without default."""
        result = get_env_var("NONEXISTENT_VAR")
        assert result is None

    def test_list_env_vars_no_prefix(self):
        """Test listing all environment variables."""
        # Set test variables
        os.environ["TEST_VAR1"] = "value1"
        os.environ["TEST_VAR2"] = "value2"

        try:
            result = list_env_vars()
            assert isinstance(result, dict)
            assert "TEST_VAR1" in result
            assert "TEST_VAR2" in result
            assert result["TEST_VAR1"] == "value1"
            assert result["TEST_VAR2"] == "value2"
        finally:
            # Clean up
            del os.environ["TEST_VAR1"]
            del os.environ["TEST_VAR2"]

    def test_list_env_vars_with_prefix(self):
        """Test listing environment variables with prefix filter."""
        # Set test variables
        os.environ["SQLFLOW_VAR1"] = "value1"
        os.environ["SQLFLOW_VAR2"] = "value2"
        os.environ["OTHER_VAR"] = "value3"

        try:
            result = list_env_vars("SQLFLOW_")
            assert isinstance(result, dict)
            assert "SQLFLOW_VAR1" in result
            assert "SQLFLOW_VAR2" in result
            assert "OTHER_VAR" not in result
            assert result["SQLFLOW_VAR1"] == "value1"
            assert result["SQLFLOW_VAR2"] == "value2"
        finally:
            # Clean up
            del os.environ["SQLFLOW_VAR1"]
            del os.environ["SQLFLOW_VAR2"]
            del os.environ["OTHER_VAR"]

    def test_find_project_root_found(self):
        """Test finding project root when profiles directory exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir).resolve()

            # Create a profiles directory
            profiles_dir = temp_path / "profiles"
            profiles_dir.mkdir()

            # Create a subdirectory to search from
            subdir = temp_path / "subdir"
            subdir.mkdir()

            result = find_project_root(str(subdir))
            assert result.resolve() == temp_path

    def test_find_project_root_not_found(self):
        """Test finding project root when no profiles directory exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = find_project_root(temp_dir)
            assert result is None

    def test_load_dotenv_file_exists(self):
        """Test loading .env file when it exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"

            # Create a .env file
            env_content = "TEST_ENV_VAR=test_value\nANOTHER_VAR=another_value\n"
            env_file.write_text(env_content)

            # Load the .env file
            result = load_dotenv_file(temp_path)

            # If python-dotenv is available, it should load successfully
            # If not available, it should return False gracefully
            try:
                pass
                # python-dotenv is available, should succeed
                assert result is True
                assert os.environ.get("TEST_ENV_VAR") == "test_value"
                assert os.environ.get("ANOTHER_VAR") == "another_value"
            except ImportError:
                # python-dotenv not available, should return False
                assert result is False

            # Clean up environment variables if they were set
            if "TEST_ENV_VAR" in os.environ:
                del os.environ["TEST_ENV_VAR"]
            if "ANOTHER_VAR" in os.environ:
                del os.environ["ANOTHER_VAR"]

    def test_load_dotenv_file_not_exists(self):
        """Test loading .env file when it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = load_dotenv_file(temp_path)
            assert result is False

    def test_setup_environment_with_project(self):
        """Test setting up environment when SQLFlow project exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir).resolve()

            # Create profiles directory (makes it a SQLFlow project)
            profiles_dir = temp_path / "profiles"
            profiles_dir.mkdir()

            # Create .env file
            env_file = temp_path / ".env"
            env_file.write_text("SETUP_TEST_VAR=setup_value\n")

            # Test setup from subdirectory
            subdir = temp_path / "subdir"
            subdir.mkdir()

            result = setup_environment(str(subdir))

            # If python-dotenv is available, it should load successfully
            # If not available, it should return False gracefully
            try:
                pass
                # python-dotenv is available, should succeed
                assert result is True
                assert os.environ.get("SETUP_TEST_VAR") == "setup_value"
            except ImportError:
                # python-dotenv not available, should return False
                assert result is False

            # Clean up if variable was set
            if "SETUP_TEST_VAR" in os.environ:
                del os.environ["SETUP_TEST_VAR"]

    def test_setup_environment_no_project(self):
        """Test setting up environment when no SQLFlow project exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = setup_environment(temp_dir)
            assert result is False
