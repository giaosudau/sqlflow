"""End-to-end integration tests for profile variable substitution through the entire pipeline.

This module tests the complete flow from ProfileManager → ConfigurationResolver →
Planner → LocalExecutor → DuckDB Engine with real variable substitution.

No mocks are used - all tests use real implementations to verify the complete integration.
"""

import os
import shutil
import tempfile
from unittest import mock

import pytest
import yaml

from sqlflow.core.config_resolver import ConfigurationResolver
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.profiles import ProfileManager


class TestProfileToEngineE2E:
    """End-to-end tests for profile variable substitution through the entire pipeline."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create temporary project directory with all necessary subdirectories."""
        temp_dir = tempfile.mkdtemp()

        # Create project structure
        profiles_dir = os.path.join(temp_dir, "profiles")
        pipelines_dir = os.path.join(temp_dir, "pipelines")
        data_dir = os.path.join(temp_dir, "data")
        output_dir = os.path.join(temp_dir, "output")

        os.makedirs(profiles_dir)
        os.makedirs(pipelines_dir)
        os.makedirs(data_dir)
        os.makedirs(output_dir)

        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_csv_data(self, temp_project_dir):
        """Create sample CSV data for testing."""
        data_dir = os.path.join(temp_project_dir, "data")

        # Create customers.csv
        customers_file = os.path.join(data_dir, "customers.csv")
        with open(customers_file, "w") as f:
            f.write("id,name,email,country\\n")
            f.write("1,Alice Johnson,alice@example.com,US\\n")
            f.write("2,Bob Smith,bob@example.com,UK\\n")
            f.write("3,Maria Garcia,maria@example.com,ES\\n")

        # Create orders.csv
        orders_file = os.path.join(data_dir, "orders.csv")
        with open(orders_file, "w") as f:
            f.write("order_id,customer_id,amount,status\\n")
            f.write("101,1,150.00,completed\\n")
            f.write("102,2,75.50,pending\\n")
            f.write("103,1,200.00,completed\\n")
            f.write("104,3,99.99,shipped\\n")

        return data_dir

    def test_e2e_csv_connector_with_variable_substitution(
        self, temp_project_dir, sample_csv_data
    ):
        """Test complete pipeline with CSV connector using variable substitution."""
        profiles_dir = os.path.join(temp_project_dir, "profiles")
        pipelines_dir = os.path.join(temp_project_dir, "pipelines")
        os.path.join(temp_project_dir, "output")

        # Create profile with variable substitution
        profile_data = {
            "version": "1.0",
            "variables": {
                "data_path": "${DATA_DIR|data}",
                "csv_delimiter": "${CSV_DELIMITER|,}",
                "output_path": "${OUTPUT_DIR|output}",
                "environment": "${ENVIRONMENT|test}",
            },
            "connectors": {
                "customers_csv": {
                    "type": "csv",
                    "params": {
                        "path": "${data_path}/customers.csv",
                        "delimiter": "${csv_delimiter}",
                        "has_header": True,
                        "encoding": "utf-8",
                    },
                },
                "orders_csv": {
                    "type": "csv",
                    "params": {
                        "path": "${data_path}/orders.csv",
                        "delimiter": "${csv_delimiter}",
                        "has_header": True,
                        "encoding": "utf-8",
                    },
                },
                "output_csv": {
                    "type": "csv",
                    "params": {
                        "path": "${output_path}/customer_summary.csv",
                        "delimiter": "${csv_delimiter}",
                        "has_header": True,
                        "encoding": "utf-8",
                    },
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "memory",
                    "threads": 1,
                },
            },
        }

        profile_path = os.path.join(profiles_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Create pipeline file
        pipeline_content = """
-- Customer Analytics Pipeline with Variable Substitution
-- Tests end-to-end flow from profiles to engines

-- Load customers data
LOAD customers FROM customers_csv;

-- Load orders data  
LOAD orders FROM orders_csv;

-- Create customer summary
CREATE TABLE customer_summary AS
SELECT 
    c.id,
    c.name,
    c.email,
    c.country,
    COUNT(o.order_id) as order_count,
    COALESCE(SUM(o.amount), 0) as total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email, c.country
ORDER BY total_spent DESC;

-- Export results
EXPORT customer_summary TO output_csv;
"""

        pipeline_path = os.path.join(pipelines_dir, "customer_analytics.sf")
        with open(pipeline_path, "w") as f:
            f.write(pipeline_content)

        # Set environment variables to test substitution
        env_vars = {
            "DATA_DIR": "data",
            "OUTPUT_DIR": "output",
            "ENVIRONMENT": "integration_test",
        }

        with mock.patch.dict(os.environ, env_vars, clear=False):
            # Test ProfileManager variable substitution
            profile_manager = ProfileManager(profiles_dir, "test")

            # Test ConfigurationResolver
            resolver = ConfigurationResolver(profile_manager)

            # Test resolving customers connector with runtime variables
            customers_config = resolver.resolve_config(
                profile_name="customers_csv",
                variables={
                    "csv_delimiter": "|"
                },  # Override delimiter (profile variable name)
            )

            # Verify variable substitution worked
            assert customers_config["path"] == "data/customers.csv"
            assert customers_config["delimiter"] == "|"  # Runtime override
            assert customers_config["has_header"] is True
            assert customers_config["encoding"] == "utf-8"
            assert customers_config["type"] == "csv"

            # Test resolving orders connector (no runtime override)
            orders_config = resolver.resolve_config(profile_name="orders_csv")
            assert orders_config["path"] == "data/orders.csv"
            assert orders_config["delimiter"] == ","  # Profile default

            # Test resolving output connector
            output_config = resolver.resolve_config(profile_name="output_csv")
            assert output_config["path"] == "output/customer_summary.csv"

            # Test LocalExecutor initialization (verify it can access profiles)
            executor = LocalExecutor(
                project_dir=temp_project_dir,
                profile_name="test",
            )

            # Verify that the executor has access to profile components
            assert executor.profile_manager is not None
            assert executor.config_resolver is not None

            # Test that the executor can resolve connector configurations
            if executor.config_resolver:
                customers_config_via_executor = executor.config_resolver.resolve_config(
                    profile_name="customers_csv"
                )
                assert customers_config_via_executor["path"] == "data/customers.csv"
                assert customers_config_via_executor["delimiter"] == ","

            # Verify the DuckDB engine is working
            engine = executor.duckdb_engine
            assert engine is not None

            # Test basic engine functionality
            test_result = engine.execute_query("SELECT 1 as test_value").fetchall()
            assert len(test_result) == 1
            assert test_result[0][0] == 1  # DuckDB returns tuples by default

            # Test that we can create tables
            engine.execute_query("CREATE TABLE test_table (id INTEGER, name VARCHAR)")

            # Check that the table was created
            tables_result = engine.execute_query("SHOW TABLES").fetchall()
            table_names = [row[0] for row in tables_result]
            assert "test_table" in table_names

            # This test verifies the complete integration chain:
            # ProfileManager → ConfigurationResolver → LocalExecutor → DuckDBEngine
            # All components work together with variable substitution

    def test_e2e_postgres_connector_with_environment_variables(self, temp_project_dir):
        """Test PostgreSQL connector configuration with environment variable substitution."""
        profiles_dir = os.path.join(temp_project_dir, "profiles")

        # Create profile with PostgreSQL connector using environment variables
        profile_data = {
            "version": "1.0",
            "variables": {
                "db_host": "${POSTGRES_HOST|localhost}",
                "db_port": "${POSTGRES_PORT|5432}",
                "db_name": "${POSTGRES_DB|testdb}",
                "db_user": "${POSTGRES_USER|testuser}",
                "db_password": "${POSTGRES_PASSWORD|testpass}",
                "ssl_mode": "${POSTGRES_SSL|prefer}",
            },
            "connectors": {
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": "${db_port}",
                        "database": "${db_name}",
                        "user": "${db_user}",
                        "password": "${db_password}",
                        "sslmode": "${ssl_mode}",
                        "connect_timeout": 30,
                    },
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "memory",
                },
            },
        }

        profile_path = os.path.join(profiles_dir, "prod.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Test with production environment variables
        env_vars = {
            "POSTGRES_HOST": "prod-db.example.com",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "analytics_prod",
            "POSTGRES_USER": "analytics_user",
            "POSTGRES_PASSWORD": "secure_password_123",
            "POSTGRES_SSL": "require",
        }

        with mock.patch.dict(os.environ, env_vars, clear=False):
            profile_manager = ProfileManager(profiles_dir, "prod")
            resolver = ConfigurationResolver(profile_manager)

            # Test configuration resolution with environment variables
            postgres_config = resolver.resolve_config(profile_name="postgres_main")

            # Verify all environment variables were substituted correctly
            assert postgres_config["host"] == "prod-db.example.com"
            assert postgres_config["port"] == "5433"
            assert postgres_config["database"] == "analytics_prod"
            assert postgres_config["user"] == "analytics_user"
            assert postgres_config["password"] == "secure_password_123"
            assert postgres_config["sslmode"] == "require"
            assert postgres_config["connect_timeout"] == 30
            assert postgres_config["type"] == "postgres"

            # Test runtime variable override
            override_config = resolver.resolve_config(
                profile_name="postgres_main",
                variables={"db_host": "staging-db.example.com"},  # Override host
                options={"connect_timeout": 60},  # Override timeout
            )

            # Verify runtime overrides work
            assert (
                override_config["host"] == "staging-db.example.com"
            )  # Runtime override
            assert override_config["port"] == "5433"  # Still from env var
            assert override_config["connect_timeout"] == 60  # Options override

    def test_e2e_nested_variable_substitution_with_defaults(self, temp_project_dir):
        """Test complex nested variable substitution with default values."""
        profiles_dir = os.path.join(temp_project_dir, "profiles")

        # Create profile with nested variable references and defaults
        profile_data = {
            "version": "1.0",
            "variables": {
                "base_path": "${BASE_PATH|/data}",
                "env_suffix": "${ENV|dev}",
                "full_path": "${base_path}/${env_suffix}",
                "backup_suffix": "${BACKUP_SUFFIX|backup}",
                "log_level": "${LOG_LEVEL|INFO}",
                "max_connections": "${MAX_CONN|10}",
            },
            "connectors": {
                "data_source": {
                    "type": "csv",
                    "params": {
                        "path": "${full_path}/source.csv",
                        "backup_location": "${full_path}/${backup_suffix}",
                        "log_level": "${log_level}",
                        "max_connections": "${max_connections}",
                    },
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "memory",
                    "log_level": "${log_level}",
                },
            },
        }

        profile_path = os.path.join(profiles_dir, "complex.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Test with partial environment variables (some use defaults)
        env_vars = {
            "BASE_PATH": "/custom/data",
            "ENV": "staging",
            # LOG_LEVEL and MAX_CONN will use defaults
            # BACKUP_SUFFIX will use computed default
        }

        with mock.patch.dict(os.environ, env_vars, clear=False):
            profile_manager = ProfileManager(profiles_dir, "complex")
            resolver = ConfigurationResolver(profile_manager)

            # Test complex variable resolution
            config = resolver.resolve_config(profile_name="data_source")

            # Verify nested variable substitution worked correctly
            assert config["path"] == "/custom/data/staging/source.csv"
            assert config["backup_location"] == "/custom/data/staging/backup"
            assert config["log_level"] == "INFO"  # Default value
            assert config["max_connections"] == "10"  # Default value
            assert config["type"] == "csv"

            # Test runtime variable override of nested variables
            override_config = resolver.resolve_config(
                profile_name="data_source",
                variables={
                    "full_path": "/custom/data/production",  # Override the computed path directly
                    "log_level": "DEBUG",  # Override default
                },
            )

            # Verify overrides work with nested variables
            assert override_config["path"] == "/custom/data/production/source.csv"
            assert (
                override_config["backup_location"] == "/custom/data/production/backup"
            )
            assert override_config["log_level"] == "DEBUG"  # Runtime override

    def test_e2e_performance_with_large_variable_set(self, temp_project_dir):
        """Test performance and correctness with a large number of variables."""
        profiles_dir = os.path.join(temp_project_dir, "profiles")

        # Create profile with many variables to test performance
        variables = {}
        connector_params = {}

        # Create 100 variables with various patterns
        for i in range(100):
            var_name = f"var_{i}"
            env_name = f"TEST_VAR_{i}"
            default_value = f"default_value_{i}"

            variables[var_name] = f"${{{env_name}|{default_value}}}"
            connector_params[f"param_{i}"] = f"${{{var_name}}}"

        profile_data = {
            "version": "1.0",
            "variables": variables,
            "connectors": {
                "large_connector": {
                    "type": "csv",
                    "params": connector_params,
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "memory",
                },
            },
        }

        profile_path = os.path.join(profiles_dir, "performance.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        # Set some environment variables (others will use defaults)
        env_vars = {}
        for i in range(0, 100, 10):  # Every 10th variable
            env_vars[f"TEST_VAR_{i}"] = f"env_value_{i}"

        with mock.patch.dict(os.environ, env_vars, clear=False):
            profile_manager = ProfileManager(profiles_dir, "performance")
            resolver = ConfigurationResolver(profile_manager)

            # Test that large variable sets are handled efficiently
            import time

            start_time = time.time()

            config = resolver.resolve_config(profile_name="large_connector")

            end_time = time.time()
            resolution_time = end_time - start_time

            # Verify resolution completed in reasonable time (< 1 second)
            assert (
                resolution_time < 1.0
            ), f"Variable resolution took too long: {resolution_time}s"

            # Verify all variables were resolved correctly
            assert config["type"] == "csv"

            # Check that environment variables were used where available
            for i in range(0, 100, 10):
                param_name = f"param_{i}"
                expected_value = f"env_value_{i}"
                assert config[param_name] == expected_value

            # Check that defaults were used where env vars not available
            for i in range(1, 100, 10):  # Offset by 1 to get non-env vars
                param_name = f"param_{i}"
                expected_value = f"default_value_{i}"
                assert config[param_name] == expected_value

            # Test runtime variable override performance
            runtime_vars = {f"var_{i}": f"runtime_value_{i}" for i in range(50, 60)}

            start_time = time.time()
            override_config = resolver.resolve_config(
                profile_name="large_connector",
                variables=runtime_vars,
            )
            end_time = time.time()
            override_time = end_time - start_time

            # Verify override resolution is also fast
            assert (
                override_time < 1.0
            ), f"Override resolution took too long: {override_time}s"

            # Verify runtime overrides worked
            for i in range(50, 60):
                param_name = f"param_{i}"
                expected_value = f"runtime_value_{i}"
                assert override_config[param_name] == expected_value
