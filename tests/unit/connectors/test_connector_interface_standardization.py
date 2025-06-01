"""Tests for standardized connector interface compliance.

This module tests that all connectors implement the standardized interface
correctly, including parameter validation, health monitoring, and error handling.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from sqlflow.connectors.base import (
    ConnectorState,
    IncrementalError,
    ParameterError,
    ParameterValidator,
)
from sqlflow.connectors.csv_connector import CSVConnector
from sqlflow.connectors.data_chunk import DataChunk


class TestParameterValidationFramework:
    """Test the parameter validation framework."""

    def test_parameter_validator_initialization(self):
        """Test parameter validator initialization."""
        validator = ParameterValidator("TEST")
        assert validator.connector_type == "TEST"
        assert isinstance(validator.required_params, list)
        assert isinstance(validator.optional_params, dict)

    def test_parameter_validator_missing_required_params(self):
        """Test validation fails for missing required parameters."""
        validator = ParameterValidator("TEST")
        validator.required_params = ["host", "database"]

        params = {"host": "localhost"}  # Missing database

        with pytest.raises(ParameterError) as exc_info:
            validator.validate(params)

        assert "Missing required parameters" in str(exc_info.value)
        assert "database" in str(exc_info.value)

    def test_parameter_validator_type_conversion(self):
        """Test parameter type conversion."""
        validator = ParameterValidator("TEST")
        validator.required_params = []
        validator.optional_params = {"port": 5432, "timeout_seconds": 300}

        params = {"port": "5432", "timeout_seconds": "300"}  # String values

        result = validator.validate(params)

        assert result["port"] == 5432  # Converted to int
        assert result["timeout_seconds"] == 300  # Converted to int

    def test_parameter_validator_defaults(self):
        """Test that default values are applied."""
        validator = ParameterValidator("TEST")
        validator.required_params = []
        validator.optional_params = {"batch_size": 10000, "max_retries": 3}

        params = {}  # No parameters provided

        result = validator.validate(params)

        assert result["batch_size"] == 10000
        assert result["max_retries"] == 3


class TestCSVConnectorStandardization:
    """Test CSV connector compliance with standardized interface."""

    @pytest.fixture
    def temp_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value,updated_at\n")
            f.write("1,Alice,100,2024-01-01\n")
            f.write("2,Bob,200,2024-01-02\n")
            f.write("3,Charlie,300,2024-01-03\n")
            temp_path = f.name

        yield temp_path

        # Cleanup
        Path(temp_path).unlink(missing_ok=True)

    def test_csv_connector_parameter_validation(self, temp_csv_file):
        """Test CSV connector parameter validation."""
        connector = CSVConnector()

        # Test valid parameters
        valid_params = {
            "path": temp_csv_file,
            "delimiter": ",",
            "has_header": True,
            "encoding": "utf-8",
        }

        # Should not raise an exception
        connector.configure(valid_params)
        assert connector.state == ConnectorState.CONFIGURED

    def test_csv_connector_missing_required_params(self):
        """Test CSV connector fails with missing required parameters."""
        connector = CSVConnector()

        # Missing required 'path' parameter
        invalid_params = {"delimiter": ",", "has_header": True}

        with pytest.raises(ParameterError) as exc_info:
            connector.configure(invalid_params)

        assert "Missing required parameters" in str(exc_info.value)
        assert "path" in str(exc_info.value)

    def test_csv_connector_health_monitoring(self, temp_csv_file):
        """Test CSV connector health monitoring capabilities."""
        connector = CSVConnector()
        connector.configure({"path": temp_csv_file})

        # Test health check
        health_status = connector.check_health()

        assert isinstance(health_status, dict)
        assert "status" in health_status
        assert "connected" in health_status
        assert "response_time_ms" in health_status
        assert "last_check" in health_status
        assert "capabilities" in health_status

        # Should be healthy for valid file
        assert health_status["status"] == "healthy"
        assert health_status["connected"] is True
        assert health_status["capabilities"]["incremental"] is True

    def test_csv_connector_health_check_unhealthy(self):
        """Test CSV connector health check for non-existent file."""
        connector = CSVConnector()
        connector.configure({"path": "/nonexistent/file.csv"})

        health_status = connector.check_health()

        assert health_status["status"] == "unhealthy"
        assert health_status["connected"] is False
        assert "error" in health_status

    def test_csv_connector_performance_metrics(self, temp_csv_file):
        """Test CSV connector performance metrics."""
        connector = CSVConnector()
        connector.configure({"path": temp_csv_file})

        metrics = connector.get_performance_metrics()

        assert isinstance(metrics, dict)
        assert "connection_time_ms" in metrics
        assert "query_time_ms" in metrics
        assert "rows_per_second" in metrics
        assert "bytes_transferred" in metrics

    def test_csv_connector_incremental_validation(self, temp_csv_file):
        """Test CSV connector incremental parameter validation."""
        connector = CSVConnector()

        # Test incremental parameters
        incremental_params = {
            "path": temp_csv_file,
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
        }

        # Should not raise an exception
        connector.configure(incremental_params)
        assert connector.state == ConnectorState.CONFIGURED

    def test_csv_connector_incremental_missing_cursor_field(self, temp_csv_file):
        """Test CSV connector fails with incremental mode but missing cursor_field."""
        connector = CSVConnector()

        # Missing cursor_field for incremental mode
        invalid_params = {
            "path": temp_csv_file,
            "sync_mode": "incremental",
            # Missing cursor_field
        }

        with pytest.raises(IncrementalError) as exc_info:
            connector.configure(invalid_params)

        assert "cursor_field" in str(exc_info.value)

    def test_csv_connector_supports_incremental(self, temp_csv_file):
        """Test CSV connector incremental support detection."""
        connector = CSVConnector()
        connector.configure({"path": temp_csv_file})

        assert connector.supports_incremental() is True

    def test_csv_connector_incremental_reading(self, temp_csv_file):
        """Test CSV connector incremental reading functionality."""
        connector = CSVConnector()
        connector.configure({"path": temp_csv_file})

        # Test incremental reading with cursor value
        chunks = list(
            connector.read_incremental(
                object_name=temp_csv_file,
                cursor_field="updated_at",
                cursor_value="2024-01-02",
                batch_size=10,
            )
        )

        assert len(chunks) > 0
        # Should only get records after 2024-01-02
        for chunk in chunks:
            df = chunk.pandas_df
            if not df.empty and "updated_at" in df.columns:
                # All values should be > 2024-01-02
                assert all(df["updated_at"] > "2024-01-02")

    def test_csv_connector_cursor_value_extraction(self, temp_csv_file):
        """Test CSV connector cursor value extraction."""
        connector = CSVConnector()
        connector.configure({"path": temp_csv_file})

        # Create test data chunk
        test_data = pd.DataFrame(
            {"id": [1, 2, 3], "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"]}
        )
        chunk = DataChunk(test_data)

        cursor_value = connector.get_cursor_value(chunk, "updated_at")
        assert cursor_value == "2024-01-03"  # Maximum value

    def test_csv_connector_error_handling(self):
        """Test CSV connector standardized error handling."""
        connector = CSVConnector()

        # Test with invalid path type - use a type that can't be converted to string
        with pytest.raises(ParameterError):
            connector.configure(
                {"path": {"invalid": "dict"}}
            )  # Dict can't be converted to string

    def test_csv_connector_state_management(self, temp_csv_file):
        """Test CSV connector state management."""
        connector = CSVConnector()

        # Initial state
        assert connector.state == ConnectorState.CREATED

        # After configuration
        connector.configure({"path": temp_csv_file})
        assert connector.state == ConnectorState.CONFIGURED

        # After successful connection test
        result = connector.test_connection()
        assert result.success is True
        assert connector.state == ConnectorState.READY


class TestConnectorInterfaceCompliance:
    """Test that connectors comply with the standardized interface."""

    def test_connector_has_required_methods(self):
        """Test that connectors implement all required interface methods."""
        connector = CSVConnector()

        # Configuration methods
        assert hasattr(connector, "configure")
        assert hasattr(connector, "validate_params")
        assert hasattr(connector, "validate_incremental_params")

        # Connection methods
        assert hasattr(connector, "test_connection")
        assert hasattr(connector, "check_health")
        assert hasattr(connector, "get_performance_metrics")

        # Discovery methods
        assert hasattr(connector, "discover")
        assert hasattr(connector, "get_schema")

        # Reading methods
        assert hasattr(connector, "read")
        assert hasattr(connector, "read_incremental")
        assert hasattr(connector, "supports_incremental")
        assert hasattr(connector, "get_cursor_value")

    def test_connector_has_required_attributes(self):
        """Test that connectors have all required attributes."""
        connector = CSVConnector()

        assert hasattr(connector, "state")
        assert hasattr(connector, "name")
        assert hasattr(connector, "connector_type")
        assert hasattr(connector, "connection_params")
        assert hasattr(connector, "is_connected")
        assert hasattr(connector, "health_status")

    def test_connector_parameter_validator_integration(self):
        """Test that connectors integrate with parameter validation framework."""
        connector = CSVConnector()

        assert hasattr(connector, "_parameter_validator")
        assert connector._parameter_validator is not None
        assert isinstance(connector._parameter_validator, ParameterValidator)

    def test_connector_standardized_exceptions(self):
        """Test that connectors use standardized exception types."""
        connector = CSVConnector()

        # Test ParameterError
        with pytest.raises(ParameterError):
            connector.configure({})  # Missing required parameters

        # Test IncrementalError for unsupported incremental
        # First configure the connector properly
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,test\n")
            temp_path = f.name

        try:
            connector.configure({"path": temp_path})

            with patch.object(connector, "supports_incremental", return_value=False):
                with pytest.raises(IncrementalError):
                    list(connector.read_incremental("test", "field", "value"))
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestIndustryStandardParameters:
    """Test industry-standard parameter compatibility."""

    def test_airbyte_compatible_parameters(self, temp_csv_file):
        """Test Airbyte-compatible parameter format."""
        connector = CSVConnector()

        # Airbyte-style parameters
        airbyte_params = {
            "path": temp_csv_file,
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "primary_key": ["id"],
            "batch_size": 5000,
        }

        # Should work without issues
        connector.configure(airbyte_params)
        assert connector.state == ConnectorState.CONFIGURED

    def test_fivetran_compatible_parameters(self, temp_csv_file):
        """Test Fivetran-compatible parameter format."""
        connector = CSVConnector()

        # Fivetran-style parameters
        fivetran_params = {
            "path": temp_csv_file,
            "sync_mode": "full_refresh",
            "timeout_seconds": 600,
            "max_retries": 5,
        }

        # Should work without issues
        connector.configure(fivetran_params)
        assert connector.state == ConnectorState.CONFIGURED

    @pytest.fixture
    def temp_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value,updated_at\n")
            f.write("1,Alice,100,2024-01-01\n")
            f.write("2,Bob,200,2024-01-02\n")
            f.write("3,Charlie,300,2024-01-03\n")
            temp_path = f.name

        yield temp_path

        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
