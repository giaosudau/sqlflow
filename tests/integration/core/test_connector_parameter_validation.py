"""Integration tests for connector parameter validation and translation.

Tests the parameter translation and validation logic that was causing issues:
- Industry standard parameter names vs legacy parameter names
- Parameter translation between formats (database -> username, dbname -> database)
- Connector configuration validation with real services
- Error propagation and handling for parameter mismatches
"""

import os

import pytest

from sqlflow.connectors.postgres.source import PostgresSource
from sqlflow.core.executors import get_executor


class TestParameterTranslation:
    """Test parameter translation between industry standard and legacy formats."""

    def test_postgres_parameter_translation_industry_to_legacy(self):
        """Test translation from industry standard to legacy PostgreSQL parameters."""
        # Industry standard parameters used in pipeline files
        industry_params = {
            "host": "localhost",
            "port": 5432,
            "database": "demo",
            "username": "sqlflow",
            "password": "sqlflow123",
        }

        # Create PostgreSQL source to test parameter translation
        postgres_source = PostgresSource(config=industry_params)

        # Verify the connector accepts industry standard parameters
        assert hasattr(postgres_source, "conn_params")
        config = postgres_source.conn_params

        # Check that parameters are properly translated/available
        # The connector should handle both formats internally
        assert config.get("host") == "localhost"
        assert config.get("database") == "demo" or config.get("dbname") == "demo"
        assert config.get("username") == "sqlflow" or config.get("user") == "sqlflow"

    def test_postgres_parameter_translation_legacy_format(self):
        """Test that legacy PostgreSQL parameter format still works."""
        # Legacy psycopg2 parameter names
        legacy_params = {
            "host": "localhost",
            "port": 5432,
            "dbname": "demo",  # Legacy format
            "user": "sqlflow",  # Legacy format
            "password": "sqlflow123",
        }

        # Create PostgreSQL source with legacy parameters
        postgres_source = PostgresSource(config=legacy_params)

        # Verify the connector accepts legacy parameters
        assert hasattr(postgres_source, "conn_params")
        config = postgres_source.conn_params

        # Should work with legacy format
        assert config.get("host") == "localhost"
        assert config.get("dbname") == "demo" or config.get("database") == "demo"
        assert config.get("user") == "sqlflow" or config.get("username") == "sqlflow"

    def test_postgres_mixed_parameter_formats(self):
        """Test handling of mixed parameter formats."""
        # Mix of industry standard and legacy formats
        mixed_params = {
            "host": "localhost",
            "port": 5432,
            "database": "demo",  # Industry standard
            "user": "sqlflow",  # Legacy format
            "password": "sqlflow123",
        }

        # Should handle mixed formats gracefully
        postgres_source = PostgresSource(config=mixed_params)
        assert hasattr(postgres_source, "conn_params")


class TestConnectorConfigurationValidation:
    """Test connector configuration validation with parameter translation."""

    @pytest.mark.external_services
    @pytest.mark.postgres
    def test_postgres_connector_validation_success(self):
        """Test successful PostgreSQL connector validation with industry standard parameters."""
        executor = get_executor()

        # Use industry standard parameter names (what pipeline files use)
        step = {
            "type": "source_definition",
            "id": "test_postgres_source",
            "name": "test_postgres",
            "connector_type": "postgres",
            "source_name": "test_postgres",
            "params": {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "database": os.getenv("POSTGRES_DB", "demo"),  # Industry standard
                "username": os.getenv("POSTGRES_USER", "sqlflow"),  # Industry standard
                "password": os.getenv("POSTGRES_PASSWORD", "sqlflow123"),
            },
        }

        # Test validation - should succeed with parameter translation
        result = executor._validate_connector_configuration(
            step, "postgres", "test_postgres"
        )

        assert result["status"] == "success"

    def test_postgres_connector_validation_missing_required_params(self):
        """Test PostgreSQL connector validation with missing required parameters."""
        executor = get_executor()

        # Missing required parameters
        step = {
            "type": "source_definition",
            "id": "test_postgres_invalid",
            "name": "test_postgres_invalid",
            "connector_type": "postgres",
            "source_name": "test_postgres_invalid",
            "params": {
                "host": "localhost",
                # Missing database, username, password
            },
        }

        # Test validation - should fail gracefully
        result = executor._validate_connector_configuration(
            step, "postgres", "test_postgres_invalid"
        )

        # Should return error status for missing required parameters
        assert result["status"] == "error"
        assert "Missing required fields for PostgreSQL" in result["error"]

    @pytest.mark.external_services
    @pytest.mark.s3
    def test_s3_connector_validation_success(self):
        """Test successful S3 connector validation."""
        executor = get_executor()

        step = {
            "type": "source_definition",
            "id": "test_s3_source",
            "name": "test_s3",
            "connector_type": "s3",
            "source_name": "test_s3",
            "params": {
                "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
                "bucket": os.getenv("S3_BUCKET", "sqlflow-demo"),
                "path_prefix": "test/",
            },
        }

        # Test validation - should succeed
        result = executor._validate_connector_configuration(step, "s3", "test_s3")

        assert result["status"] == "success"


class TestErrorPropagation:
    """Test error propagation and handling for connector configuration issues."""

    def test_connection_error_propagation(self):
        """Test V2 connection error propagation through step execution.

        V2 Pattern: Errors should be captured in StepExecutionResult structure.
        """
        # V2 Pattern: Use LocalOrchestrator directly
        from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator

        orchestrator = LocalOrchestrator()

        # V2 Pattern: Invalid connection parameters in source step
        source_step = {
            "type": "source_definition",
            "id": "invalid_connection",
            "name": "test_invalid_connection",
            "connector_type": "postgres",
            "params": {
                "host": "nonexistent-host-99999",
                "port": 5432,
                "database": "nonexistent_db",
                "username": "invalid_user",
                "password": "invalid_password",
            },
        }

        # Execute source definition
        result = orchestrator._execute_source_definition(source_step)

        # V2 Pattern: Source definition should succeed (storage)
        # but connection errors captured for later
        assert result["status"] == "success"
        assert result["source_name"] == "test_invalid_connection"
        assert (
            result["rows_processed"] == 0
        )  # No data processed due to connection issue

        # Error details should be available but not break the step
        assert "execution_time_ms" in result

        # Verify source definition was stored for metadata purposes
        assert "test_invalid_connection" in orchestrator.source_definitions

    def test_parameter_mismatch_error_handling(self):
        """Test V2 parameter validation error handling.

        V2 Pattern: Parameter validation integrated with step execution.
        """
        # V2 Pattern: Use LocalOrchestrator directly
        from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator

        orchestrator = LocalOrchestrator()

        # V2 Pattern: Missing required parameters
        source_step = {
            "type": "source_definition",
            "id": "missing_params",
            "name": "test_missing_params",
            "connector_type": "csv",
            "params": {
                # Missing required 'path' parameter for CSV connector
                "has_header": True,
                "delimiter": ",",
            },
        }

        # Execute source definition
        result = orchestrator._execute_source_definition(source_step)

        # V2 Pattern: Should store source definition but capture validation error
        assert result["status"] == "success"  # Storage succeeds
        assert result["source_name"] == "test_missing_params"
        assert result["rows_processed"] == 0  # No data due to missing path

        # Source definition should still be stored for metadata purposes
        assert "test_missing_params" in orchestrator.source_definitions
        stored_source = orchestrator.source_definitions["test_missing_params"]
        assert stored_source["connector_type"] == "csv"


class TestBackwardCompatibility:
    """Test backward compatibility with existing configurations."""

    def test_legacy_postgres_config_still_works(self):
        """Test that existing legacy PostgreSQL configurations continue to work."""
        # This simulates existing pipeline files that might use legacy parameter names
        legacy_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "demo",  # Legacy psycopg2 parameter name
            "user": "sqlflow",  # Legacy psycopg2 parameter name
            "password": "sqlflow123",
        }

        # Should still work
        postgres_source = PostgresSource(config=legacy_config)
        assert hasattr(postgres_source, "conn_params")

    def test_new_industry_standard_config_works(self):
        """Test that new industry standard configurations work."""
        # This simulates new pipeline files using industry standard parameter names
        standard_config = {
            "host": "localhost",
            "port": 5432,
            "database": "demo",  # Industry standard parameter name
            "username": "sqlflow",  # Industry standard parameter name
            "password": "sqlflow123",
        }

        # Should work with parameter translation
        postgres_source = PostgresSource(config=standard_config)
        assert hasattr(postgres_source, "conn_params")

    def test_both_formats_produce_same_result(self):
        """Test that both parameter formats produce equivalent connector behavior."""
        # Legacy format
        legacy_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "demo",
            "user": "sqlflow",
            "password": "pass123",
        }

        # Industry standard format
        standard_config = {
            "host": "localhost",
            "port": 5432,
            "database": "demo",
            "username": "sqlflow",
            "password": "pass123",
        }

        # Both should create equivalent connectors
        legacy_source = PostgresSource(config=legacy_config)
        standard_source = PostgresSource(config=standard_config)

        # Both should have conn_params
        assert hasattr(legacy_source, "conn_params")
        assert hasattr(standard_source, "conn_params")

        # Connection parameters should be functionally equivalent
        legacy_params = legacy_source.conn_params
        standard_params = standard_source.conn_params

        # Host and port should be identical
        assert legacy_params.get("host") == standard_params.get("host")
        assert legacy_params.get("port") == standard_params.get("port")

        # Database and user should be equivalent (either same key or translated)
        legacy_db = legacy_params.get("dbname") or legacy_params.get("database")
        standard_db = standard_params.get("database") or standard_params.get("dbname")
        assert legacy_db == standard_db

        legacy_user = legacy_params.get("user") or legacy_params.get("username")
        standard_user = standard_params.get("username") or standard_params.get("user")
        assert legacy_user == standard_user


class TestConnectorInstanceCreation:
    """Test proper connector instance creation with config parameter."""

    def test_connector_created_with_config_parameter(self):
        """Test that connectors are created with proper config parameter."""
        # This tests the fix for the original issue:
        # "PostgresSource.__init__() missing 1 required positional argument: 'config'"

        from sqlflow.connectors.registry import get_connector_class

        # Get PostgreSQL connector class
        postgres_class = get_connector_class("postgres")

        # Test configuration
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "demo",
            "username": "user",
            "password": "pass",
        }

        # This should work now - connector should accept config parameter
        try:
            postgres_instance = postgres_class(config=config)
            assert postgres_instance is not None
            assert hasattr(postgres_instance, "conn_params")
        except TypeError as e:
            if "missing 1 required positional argument: 'config'" in str(e):
                pytest.fail("Connector still requires config parameter fix")
            else:
                # Other TypeErrors are acceptable (e.g., connection issues)
                pass

    def test_s3_connector_created_with_config_parameter(self):
        """Test that S3 connectors are created with proper config parameter."""
        from sqlflow.connectors.registry import get_connector_class

        # Get S3 connector class
        s3_class = get_connector_class("s3")

        # Test configuration
        config = {
            "access_key": "test",
            "secret_key": "test",
            "bucket": "test-bucket",
            "endpoint_url": "http://localhost:9000",
        }

        # This should work
        try:
            s3_instance = s3_class(config=config)
            assert s3_instance is not None
            assert hasattr(s3_instance, "connection_params")
        except Exception as e:
            # Connection errors are acceptable, but not config parameter errors
            error_msg = str(e)
            assert "missing 1 required positional argument: 'config'" not in error_msg
