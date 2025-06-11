"""Tests for LocalExecutor profile integration functionality."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import yaml

from sqlflow.core.config_resolver import ConfigurationResolver
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.profiles import ProfileManager


class TestLocalExecutorProfileIntegration:
    """Test LocalExecutor profile integration functionality."""

    @pytest.fixture
    def temp_profile_dir(self):
        """Create temporary profile directory with test profiles."""
        with tempfile.TemporaryDirectory() as temp_dir:
            profiles_dir = os.path.join(temp_dir, "profiles")
            os.makedirs(profiles_dir)

            # Create test profile
            profile_content = {
                "version": "1.0",
                "variables": {"env": "test", "db_host": "localhost"},
                "connectors": {
                    "csv_default": {
                        "type": "csv",
                        "params": {
                            "has_header": True,
                            "delimiter": ",",
                            "encoding": "utf-8",
                        },
                    },
                    "postgres_test": {
                        "type": "postgres",
                        "params": {
                            "host": "${db_host}",
                            "port": 5432,
                            "database": "test_db",
                            "user": "test_user",
                        },
                    },
                },
                "engines": {"duckdb": {"mode": "memory", "path": ":memory:"}},
            }

            profile_path = os.path.join(profiles_dir, "test.yml")
            with open(profile_path, "w") as f:
                yaml.dump(profile_content, f)

            yield temp_dir

    @pytest.fixture
    def executor_with_profile(self, temp_profile_dir):
        """Create LocalExecutor with profile support."""
        from sqlflow.project import Project

        project = Project(temp_profile_dir, "test")
        executor = LocalExecutor(project=project, profile_name="test")
        return executor

    def test_executor_initialization_with_profile_manager(self, executor_with_profile):
        """Test LocalExecutor initializes with ProfileManager and ConfigurationResolver."""
        executor = executor_with_profile

        # Verify profile-related components are initialized
        assert hasattr(executor, "profile_manager")
        assert hasattr(executor, "config_resolver")
        assert isinstance(executor.profile_manager, ProfileManager)
        assert isinstance(executor.config_resolver, ConfigurationResolver)

    def test_profile_based_source_creation_csv(self, executor_with_profile):
        """Test creating profile-based CSV source."""
        step = {
            "id": "step_0",
            "type": "source",
            "name": "test_data",
            "is_from_profile": True,
            "profile_connector_name": "csv_default",
            "params": {"path": "./data/test.csv"},  # OPTIONS override
        }

        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "test_data"
        )

        assert result["status"] == "success"
        assert result["source_name"] == "test_data"
        assert result["connector_type"] == "csv"

        # Verify source definition is stored with resolved config
        source_def = executor_with_profile.source_definitions.get("test_data")
        assert source_def is not None
        assert source_def["connector_type"] == "csv"
        assert source_def["is_from_profile"] is True
        # Path from OPTIONS should override profile defaults
        assert source_def["params"]["path"] == "./data/test.csv"
        # Profile defaults should be preserved
        assert source_def["params"]["has_header"] is True
        assert source_def["params"]["delimiter"] == ","

    def test_profile_based_source_creation_postgres(self, executor_with_profile):
        """Test creating profile-based PostgreSQL source with variable substitution."""
        step = {
            "id": "step_0",
            "type": "source",
            "name": "users",
            "is_from_profile": True,
            "profile_connector_name": "postgres_test",
            "params": {"table": "users", "schema": "public"},
        }

        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "users"
        )

        assert result["status"] == "success"
        assert result["source_name"] == "users"
        assert result["connector_type"] == "postgres"

        # Verify variable substitution occurred
        source_def = executor_with_profile.source_definitions.get("users")
        assert source_def["params"]["host"] == "localhost"  # ${db_host} resolved
        assert source_def["params"]["table"] == "users"  # From OPTIONS
        assert source_def["params"]["port"] == 5432  # From profile

    def test_traditional_source_unchanged(self, executor_with_profile):
        """Test traditional source creation remains unchanged."""
        step = {
            "id": "step_0",
            "type": "source",
            "name": "test_csv",
            "connector_type": "csv",
            "params": {"path": "./data/test.csv", "has_header": True},
            "is_from_profile": False,
        }

        result = executor_with_profile._handle_traditional_source(
            step, "step_0", "test_csv"
        )

        assert result["status"] == "success"
        assert result["source_name"] == "test_csv"
        assert result["connector_type"] == "csv"

        # Verify traditional source is stored correctly
        source_def = executor_with_profile.source_definitions.get("test_csv")
        assert source_def["connector_type"] == "csv"
        assert source_def["is_from_profile"] is False
        assert source_def["params"]["path"] == "./data/test.csv"

    def test_profile_based_source_missing_profile(self, executor_with_profile):
        """Test error handling for missing profile connector."""
        step = {
            "id": "step_0",
            "type": "source",
            "name": "test_data",
            "is_from_profile": True,
            "profile_connector_name": "nonexistent_connector",
            "params": {},
        }

        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "test_data"
        )

        assert result["status"] == "error"
        assert result["source_name"] == "test_data"
        assert "nonexistent_connector" in result["error"]
        assert "not found" in result["error"]

    def test_configuration_priority_options_override_profile(
        self, executor_with_profile
    ):
        """Test that OPTIONS override profile PARAMS correctly."""
        step = {
            "id": "step_0",
            "type": "source",
            "name": "test_csv",
            "is_from_profile": True,
            "profile_connector_name": "csv_default",
            "params": {
                "path": "./custom/path.csv",
                "delimiter": "|",  # Override profile default
                "has_header": False,  # Override profile default
            },
        }

        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "test_csv"
        )

        assert result["status"] == "success"

        source_def = executor_with_profile.source_definitions.get("test_csv")
        # OPTIONS should override profile defaults
        assert source_def["params"]["delimiter"] == "|"
        assert source_def["params"]["has_header"] is False
        assert source_def["params"]["path"] == "./custom/path.csv"
        # Non-overridden profile defaults should remain
        assert source_def["params"]["encoding"] == "utf-8"

    def test_variable_substitution_in_profile_params(self, executor_with_profile):
        """Test variable substitution in profile parameters."""
        step = {
            "id": "step_0",
            "type": "source",
            "name": "pg_source",
            "is_from_profile": True,
            "profile_connector_name": "postgres_test",
            "params": {"table": "users"},
        }

        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "pg_source"
        )

        assert result["status"] == "success"

        source_def = executor_with_profile.source_definitions.get("pg_source")
        # Variable substitution should have occurred
        assert source_def["params"]["host"] == "localhost"  # ${db_host} resolved

    def test_performance_profile_resolution_under_10ms(self, executor_with_profile):
        """Test profile resolution performance is under 10ms."""
        import time

        step = {
            "id": "step_0",
            "type": "source",
            "name": "perf_test",
            "is_from_profile": True,
            "profile_connector_name": "csv_default",
            "params": {"path": "./test.csv"},
        }

        start_time = time.time()
        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "perf_test"
        )
        end_time = time.time()

        resolution_time_ms = (end_time - start_time) * 1000

        assert result["status"] == "success"
        assert resolution_time_ms < 10  # Less than 10ms requirement

    def test_profile_based_source_with_nested_config_merge(self, executor_with_profile):
        """Test deep merging of nested configuration objects."""
        # Create a test profile with nested config
        nested_profile_content = {
            "version": "1.0",
            "connectors": {
                "s3_default": {
                    "type": "s3",
                    "params": {
                        "connection": {
                            "aws_access_key_id": "default_key",
                            "aws_secret_access_key": "default_secret",
                            "region": "us-east-1",
                        },
                        "format": "parquet",
                    },
                }
            },
        }

        # Mock the profile to include nested config
        executor_with_profile.profile_manager.load_profile = MagicMock(
            return_value=nested_profile_content
        )

        step = {
            "id": "step_0",
            "type": "source",
            "name": "s3_data",
            "is_from_profile": True,
            "profile_connector_name": "s3_default",
            "params": {
                "connection": {"region": "us-west-2"},  # Override nested value
                "bucket": "my-bucket",  # Add new value
            },
        }

        result = executor_with_profile._handle_profile_based_source(
            step, "step_0", "s3_data"
        )

        assert result["status"] == "success"

        source_def = executor_with_profile.source_definitions.get("s3_data")
        # Nested merge should preserve non-overridden values
        assert source_def["params"]["connection"]["aws_access_key_id"] == "default_key"
        assert (
            source_def["params"]["connection"]["aws_secret_access_key"]
            == "default_secret"
        )
        # Override should work for nested values
        assert source_def["params"]["connection"]["region"] == "us-west-2"
        # New values should be added
        assert source_def["params"]["bucket"] == "my-bucket"
        # Profile defaults should be preserved
        assert source_def["params"]["format"] == "parquet"

    def test_source_execution_dispatching(self, executor_with_profile):
        """Test that _execute_source_definition correctly dispatches to profile vs traditional."""
        # Test profile-based dispatching
        profile_step = {
            "id": "step_0",
            "type": "source",
            "name": "profile_source",
            "is_from_profile": True,
            "profile_connector_name": "csv_default",
            "params": {"path": "./test.csv"},
        }

        with patch.object(
            executor_with_profile, "_handle_profile_based_source"
        ) as mock_profile:
            mock_profile.return_value = {"status": "success"}
            executor_with_profile._execute_source_definition(profile_step)
            mock_profile.assert_called_once()

        # Test traditional dispatching
        traditional_step = {
            "id": "step_1",
            "type": "source",
            "name": "traditional_source",
            "connector_type": "csv",
            "params": {"path": "./test.csv"},
            "is_from_profile": False,
        }

        with patch.object(
            executor_with_profile, "_handle_traditional_source"
        ) as mock_traditional:
            mock_traditional.return_value = {"status": "success"}
            executor_with_profile._execute_source_definition(traditional_step)
            mock_traditional.assert_called_once()
