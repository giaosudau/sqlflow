"""Tests for profile validation functionality."""

import os
import tempfile

import pytest
import yaml

from sqlflow.project import Project


class TestProfileValidation:
    """Test suite for profile validation."""

    def test_valid_profile_memory_mode(self):
        """Test that a valid memory mode profile passes validation."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create valid memory mode profile
            profile_content = {
                "engines": {"duckdb": {"mode": "memory", "memory_limit": "2GB"}},
                "variables": {"env": "test"},
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should load without error
            project = Project(tmp_dir, "test")
            assert project.profile["engines"]["duckdb"]["mode"] == "memory"

    def test_valid_profile_persistent_mode(self):
        """Test that a valid persistent mode profile passes validation."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create valid persistent mode profile
            profile_content = {
                "engines": {
                    "duckdb": {
                        "mode": "persistent",
                        "path": "target/test.duckdb",
                        "memory_limit": "4GB",
                    }
                }
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should load without error
            project = Project(tmp_dir, "test")
            assert project.profile["engines"]["duckdb"]["mode"] == "persistent"
            assert project.profile["engines"]["duckdb"]["path"] == "target/test.duckdb"

    def test_invalid_profile_missing_path_for_persistent(self):
        """Test that persistent mode without path raises validation error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create invalid persistent mode profile (missing path)
            profile_content = {
                "engines": {
                    "duckdb": {
                        "mode": "persistent"
                        # Missing path field!
                    }
                }
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should raise validation error
            with pytest.raises(ValueError) as exc_info:
                Project(tmp_dir, "test")

            error_msg = str(exc_info.value)
            assert "DuckDB persistent mode requires 'path' field" in error_msg
            assert (
                "Add 'path: \"path/to/database.duckdb\"' to engines.duckdb" in error_msg
            )

    def test_invalid_profile_empty_path_for_persistent(self):
        """Test that persistent mode with empty path raises validation error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create invalid persistent mode profile (empty path)
            profile_content = {
                "engines": {"duckdb": {"mode": "persistent", "path": ""}}  # Empty path
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should raise validation error
            with pytest.raises(ValueError) as exc_info:
                Project(tmp_dir, "test")

            error_msg = str(exc_info.value)
            assert "DuckDB persistent mode 'path' field cannot be empty" in error_msg

    def test_invalid_profile_missing_engines(self):
        """Test that profile without engines section raises validation error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create invalid profile (missing engines)
            profile_content = {
                "variables": {"env": "test"}
                # Missing engines section!
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should raise validation error
            with pytest.raises(ValueError) as exc_info:
                Project(tmp_dir, "test")

            error_msg = str(exc_info.value)
            assert "Missing required 'engines' section" in error_msg

    def test_invalid_duckdb_mode(self):
        """Test that invalid DuckDB mode raises validation error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create invalid profile (invalid mode)
            profile_content = {
                "engines": {"duckdb": {"mode": "invalid_mode"}}  # Invalid mode
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should raise validation error
            with pytest.raises(ValueError) as exc_info:
                Project(tmp_dir, "test")

            error_msg = str(exc_info.value)
            assert "Invalid DuckDB mode 'invalid_mode'" in error_msg
            assert "Must be 'memory' or 'persistent'" in error_msg

    def test_deprecated_nested_format_auto_fix(self):
        """Test that deprecated nested format is auto-fixed with warning."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create profile with deprecated nested format
            profile_content = {
                "test": {  # Profile name as wrapper (deprecated)
                    "engines": {"duckdb": {"mode": "memory"}},
                    "variables": {"env": "test"},
                }
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should load successfully (auto-fixed)
            project = Project(tmp_dir, "test")
            assert project.profile["engines"]["duckdb"]["mode"] == "memory"
            assert project.profile["variables"]["env"] == "test"

    def test_deprecated_database_path_auto_fix(self):
        """Test that deprecated database_path field is auto-fixed with warning."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create profile with deprecated database_path field
            profile_content = {
                "engines": {
                    "duckdb": {
                        "mode": "persistent",
                        "database_path": "target/test.duckdb",  # Deprecated field name
                    }
                }
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should load successfully (auto-fixed)
            project = Project(tmp_dir, "test")
            assert project.profile["engines"]["duckdb"]["mode"] == "persistent"
            # Should be auto-renamed to 'path'
            assert project.profile["engines"]["duckdb"]["path"] == "target/test.duckdb"
            # Old field should be removed
            assert "database_path" not in project.profile["engines"]["duckdb"]

    def test_invalid_variables_section(self):
        """Test that non-dict variables section raises validation error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create invalid profile (variables not a dict)
            profile_content = {
                "engines": {"duckdb": {"mode": "memory"}},
                "variables": "invalid_string",  # Should be a dict
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should raise validation error
            with pytest.raises(ValueError) as exc_info:
                Project(tmp_dir, "test")

            error_msg = str(exc_info.value)
            assert "'variables' section must be a dictionary" in error_msg

    def test_combined_deprecated_format_and_field(self):
        """Test that both deprecated format and field name are auto-fixed."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create profile with both deprecated features
            profile_content = {
                "test": {  # Deprecated wrapper
                    "engines": {
                        "duckdb": {
                            "mode": "persistent",
                            "database_path": "target/test.duckdb",  # Deprecated field
                        }
                    }
                }
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should load successfully (both issues auto-fixed)
            project = Project(tmp_dir, "test")
            assert project.profile["engines"]["duckdb"]["mode"] == "persistent"
            assert project.profile["engines"]["duckdb"]["path"] == "target/test.duckdb"
            assert "database_path" not in project.profile["engines"]["duckdb"]

    def test_empty_profile_file(self):
        """Test that empty profile file is handled gracefully."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create empty profile file
            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                f.write("")  # Empty file

            # Should load with default empty profile
            project = Project(tmp_dir, "test")
            assert project.profile == {}

    def test_validation_error_format(self):
        """Test that validation errors provide helpful format guidance."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create profiles directory
            profiles_dir = os.path.join(tmp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create invalid profile
            profile_content = {
                "engines": {
                    "duckdb": {
                        "mode": "persistent"
                        # Missing path
                    }
                },
                "variables": "invalid",  # Should be dict
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            # Should raise validation error with format guidance
            with pytest.raises(ValueError) as exc_info:
                Project(tmp_dir, "test")

            error_msg = str(exc_info.value)
            # Should include multiple errors
            assert "1." in error_msg and "2." in error_msg
            # Should include format guidance
            assert "Profile format should be:" in error_msg
            assert "engines:" in error_msg
            assert "duckdb:" in error_msg
            assert "mode: memory|persistent" in error_msg
            assert "See documentation for complete profile specification." in error_msg
