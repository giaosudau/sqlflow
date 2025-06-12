"""Tests for variable substitution in profile configurations.

This module tests the ability to use environment variables and other variable sources
in profile YAML files, e.g., host: ${POSTGRES_HOST}.
"""

import os
import shutil
import tempfile
from unittest import mock

import pytest
import yaml

from sqlflow.core.profiles import ProfileManager


class TestProfileVariableSubstitution:
    """Test variable substitution within profile configurations."""

    @pytest.fixture
    def temp_profile_dir(self):
        """Create temporary directory for profile tests."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_environment_variable_substitution_in_connector_params(
        self, temp_profile_dir
    ):
        """Test that environment variables are substituted in connector params."""
        # Create profile with environment variable references
        profile_data = {
            "version": "1.0",
            "variables": {
                "db_name": "analytics",
                "csv_delimiter": ",",
            },
            "connectors": {
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "host": "${POSTGRES_HOST}",
                        "port": "${POSTGRES_PORT|5432}",  # With default
                        "database": "${db_name}",  # From profile variables
                        "user": "${POSTGRES_USER}",
                        "password": "${POSTGRES_PASSWORD}",
                        "schema": "public",
                    },
                },
                "csv_data": {
                    "type": "csv",
                    "params": {
                        "path": "${DATA_PATH}/customers.csv",
                        "delimiter": "${csv_delimiter}",  # From profile variables
                        "has_header": True,
                    },
                },
            },
        }

        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Set environment variables
        env_vars = {
            "POSTGRES_HOST": "db.example.com",
            "POSTGRES_USER": "sqlflow_user",
            "POSTGRES_PASSWORD": "secret123",
            "DATA_PATH": "/data/warehouse",
        }

        with mock.patch.dict(os.environ, env_vars, clear=False):
            # Create ProfileManager
            profile_manager = ProfileManager(temp_profile_dir, "test")

            # Get connector profile
            postgres_profile = profile_manager.get_connector_profile("postgres_main")

            # Variables should be substituted
            assert postgres_profile.params["host"] == "db.example.com"
            assert postgres_profile.params["port"] == "5432"  # Default value
            assert (
                postgres_profile.params["database"] == "analytics"
            )  # Profile variable
            assert postgres_profile.params["user"] == "sqlflow_user"
            assert postgres_profile.params["password"] == "secret123"
            assert (
                postgres_profile.params["schema"] == "public"
            )  # No substitution needed

            # Test CSV connector
            csv_profile = profile_manager.get_connector_profile("csv_data")
            assert csv_profile.params["path"] == "/data/warehouse/customers.csv"
            assert csv_profile.params["delimiter"] == ","  # Profile variable

    def test_variable_substitution_in_profile_variables_section(self, temp_profile_dir):
        """Test that variables section itself can contain environment variable references."""
        profile_data = {
            "version": "1.0",
            "variables": {
                "database_url": "${DB_HOST}:${DB_PORT|5432}/${DB_NAME}",
                "s3_bucket": "${S3_BUCKET}",
                "environment": "${ENVIRONMENT|development}",
            },
            "connectors": {
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "connection_string": "${database_url}",
                    },
                },
                "s3_data": {
                    "type": "s3",
                    "params": {
                        "bucket": "${s3_bucket}",
                        "region": "us-east-1",
                    },
                },
            },
        }

        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Set environment variables
        env_vars = {
            "DB_HOST": "localhost",
            "DB_NAME": "warehouse",
            "S3_BUCKET": "my-data-bucket",
        }

        with mock.patch.dict(os.environ, env_vars, clear=False):
            profile_manager = ProfileManager(temp_profile_dir, "test")

            # Get variables (should be substituted)
            variables = profile_manager.get_variables()
            assert variables["database_url"] == "localhost:5432/warehouse"
            assert variables["s3_bucket"] == "my-data-bucket"
            assert variables["environment"] == "development"  # Default value

            # Get connector profiles (should use substituted variables)
            postgres_profile = profile_manager.get_connector_profile("postgres_main")
            assert (
                postgres_profile.params["connection_string"]
                == "localhost:5432/warehouse"
            )

            s3_profile = profile_manager.get_connector_profile("s3_data")
            assert s3_profile.params["bucket"] == "my-data-bucket"

    def test_nested_variable_substitution(self, temp_profile_dir):
        """Test complex nested variable substitution scenarios."""
        profile_data = {
            "version": "1.0",
            "variables": {
                "base_path": "/data/${ENVIRONMENT}",
                "input_path": "${base_path}/input",
                "output_path": "${base_path}/output",
            },
            "connectors": {
                "input_csv": {
                    "type": "csv",
                    "params": {
                        "path": "${input_path}/customers.csv",
                    },
                },
                "output_csv": {
                    "type": "csv",
                    "params": {
                        "path": "${output_path}/processed.csv",
                    },
                },
            },
        }

        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        with mock.patch.dict(os.environ, {"ENVIRONMENT": "staging"}, clear=False):
            profile_manager = ProfileManager(temp_profile_dir, "test")

            # Variables should be resolved in proper order
            variables = profile_manager.get_variables()
            assert variables["base_path"] == "/data/staging"
            assert variables["input_path"] == "/data/staging/input"
            assert variables["output_path"] == "/data/staging/output"

            # Connector params should use fully resolved paths
            input_profile = profile_manager.get_connector_profile("input_csv")
            assert input_profile.params["path"] == "/data/staging/input/customers.csv"

            output_profile = profile_manager.get_connector_profile("output_csv")
            assert output_profile.params["path"] == "/data/staging/output/processed.csv"

    def test_missing_environment_variable_handling(self, temp_profile_dir):
        """Test handling of missing environment variables."""
        profile_data = {
            "version": "1.0",
            "connectors": {
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "host": "${MISSING_HOST}",  # No default
                        "port": "${MISSING_PORT|5432}",  # With default
                        "database": "test_db",
                    },
                },
            },
        }

        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        profile_manager = ProfileManager(temp_profile_dir, "test")

        # Get connector profile
        postgres_profile = profile_manager.get_connector_profile("postgres_main")

        # Missing variable without default should remain as placeholder
        assert postgres_profile.params["host"] == "${MISSING_HOST}"

        # Missing variable with default should use default
        assert postgres_profile.params["port"] == "5432"

        # Regular values should be unchanged
        assert postgres_profile.params["database"] == "test_db"

    def test_variable_substitution_performance(self, temp_profile_dir):
        """Test that variable substitution doesn't significantly impact performance."""
        import time

        # Create a profile with many variables and connectors
        profile_data = {
            "version": "1.0",
            "variables": {
                f"var_{i}": f"${{ENV_VAR_{i}|default_{i}}}" for i in range(50)
            },
            "connectors": {
                f"connector_{i}": {
                    "type": "csv",
                    "params": {
                        "path": "/data/${var_" + str(i) + "}/file_" + str(i) + ".csv",
                        "delimiter": ",",
                    },
                }
                for i in range(20)
            },
        }

        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        profile_manager = ProfileManager(temp_profile_dir, "test")

        # Measure loading time
        start_time = time.time()

        # Load variables
        variables = profile_manager.get_variables()

        # Load some connector profiles
        for i in range(5):
            profile_manager.get_connector_profile(f"connector_{i}")

        elapsed_time = (time.time() - start_time) * 1000  # Convert to ms

        # Should complete in reasonable time (< 100ms)
        assert (
            elapsed_time < 100
        ), f"Variable substitution took {elapsed_time:.2f}ms, should be < 100ms"

        # Verify some substitutions worked
        assert len(variables) == 50
        assert "default_0" in variables["var_0"]  # Default was used

    def test_integration_with_config_resolver(self, temp_profile_dir):
        """Test that ProfileManager works correctly with ConfigurationResolver."""
        from sqlflow.core.config_resolver import ConfigurationResolver

        profile_data = {
            "version": "1.0",
            "variables": {
                "db_host": "${POSTGRES_HOST|localhost}",
                "db_port": "${POSTGRES_PORT|5432}",
            },
            "connectors": {
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": "${db_port}",
                        "database": "analytics",
                    },
                },
            },
        }

        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Set environment variables
        env_vars = {"POSTGRES_HOST": "prod-db.example.com"}

        with mock.patch.dict(os.environ, env_vars, clear=False):
            profile_manager = ProfileManager(temp_profile_dir, "test")
            resolver = ConfigurationResolver(profile_manager)

            # Resolve configuration with runtime options
            resolved_config = resolver.resolve_config(
                profile_name="postgres_main",
                options={"sslmode": "require"},  # Runtime override
                variables={"extra_var": "extra_value"},  # Additional variables
            )

            # Should contain substituted values from profile
            assert resolved_config["host"] == "prod-db.example.com"
            assert resolved_config["port"] == "5432"  # Default used
            assert resolved_config["database"] == "analytics"

            # Should contain runtime options
            assert resolved_config["sslmode"] == "require"

            # Should have connector type
            assert resolved_config["type"] == "postgres"
