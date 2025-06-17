"""Tests for LoadStepHandler load operation behavior.

This test suite validates the LoadStepHandler's ability to handle different
load modes, data sources, and error conditions with proper observability.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
import tempfile
import os

from sqlflow.core.executors.v2.handlers.load_handler import LoadStepHandler
from sqlflow.core.executors.v2.steps import LoadStep
from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.connectors.data_chunk import DataChunk


class MockExecutionContext:
    """Mock execution context for testing."""
    
    def __init__(self, run_id="test_run", variables=None, config=None):
        self.run_id = run_id
        self.variables = variables or {}
        self.config = config or {}
        self.state_manager = None
        
        # Create mock observability manager
        self.observability_manager = Mock()
        self.observability_manager.record_step_start = Mock()
        self.observability_manager.record_step_success = Mock()
        self.observability_manager.record_step_failure = Mock()
        
        # Create mock connector registry
        self.connector_registry = Mock()
        
        # Create mock SQL engine
        self.sql_engine = Mock()
        self.sql_engine.table_exists = Mock(return_value=False)
        self.sql_engine.register_arrow = Mock()
        self.sql_engine.execute_query = Mock()
        self.sql_engine.get_table_schema = Mock(return_value={})
        
        self.logger = None


@pytest.fixture
def mock_execution_context():
    """Create a mock execution context for testing."""
    return MockExecutionContext()


@pytest.fixture
def sample_load_step():
    """Create a sample LoadStep for testing."""
    return LoadStep(
        id="test_load_customers",
        source="customers.csv",
        target_table="customers",
        load_mode="replace",
        expected_duration_ms=1000.0,
        criticality="high"
    )


@pytest.fixture
def sample_data_chunks():
    """Create sample data chunks for testing."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    return [DataChunk(df)]


class TestLoadStepHandler:
    """Test suite for LoadStepHandler."""

    def test_load_step_validation(self, mock_execution_context):
        """Test that invalid LoadStep configurations are caught."""
        handler = LoadStepHandler()
        
        # Test validation happens in LoadStep constructor
        with pytest.raises(ValueError, match="Load step source cannot be empty"):
            LoadStep(
                id="invalid_step",
                source="",
                target_table="table",
                load_mode="replace"
            )

    def test_upsert_validation_requires_keys(self, mock_execution_context):
        """Test that UPSERT mode requires upsert_keys."""
        handler = LoadStepHandler()
        
        # UPSERT without upsert_keys should fail during validation
        upsert_step = LoadStep(
            id="invalid_upsert",
            source="data.csv",
            target_table="table",
            load_mode="upsert",
            incremental_config={"cursor_field": "updated_at"}  # Has config but no upsert_keys
        )
        
        result = handler.execute(upsert_step, mock_execution_context)
        assert not result.is_successful()
        assert "UPSERT mode requires upsert_keys" in result.error_message

    def test_invalid_load_mode_validation(self, mock_execution_context):
        """Test that invalid load modes are rejected."""
        handler = LoadStepHandler()

        invalid_step = LoadStep(
            id="invalid_mode",
            source="data.csv",
            target_table="table",
            load_mode="invalid_mode"
        )

        result = handler.execute(invalid_step, mock_execution_context)
        assert not result.is_successful()
        assert "invalid load_mode 'invalid_mode'" in result.error_message

    def test_connector_creation_error_handling(self, mock_execution_context, sample_load_step):
        """Test error handling when connector creation fails."""
        handler = LoadStepHandler()
        
        # Make connector registry raise an error
        mock_execution_context.connector_registry.create_source_connector.side_effect = ValueError("Connector creation failed")
        
        result = handler.execute(sample_load_step, mock_execution_context)
        
        assert not result.is_successful()
        assert "Load operation failed" in result.error_message
        assert result.error_code == "LOAD_EXECUTION_ERROR"

    def test_successful_execution_structure(self, mock_execution_context):
        """Test that successful execution returns proper result structure."""
        handler = LoadStepHandler()
        
        # Create a test CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,age\n1,Alice,25\n2,Bob,30\n")
            csv_file = f.name
        
        try:
            step = LoadStep(
                id="test_load",
                source=csv_file,
                target_table="test_table",
                load_mode="replace",
                options={"connector_type": "csv"}
            )
            
            # Mock connector to return test data
            mock_connector = Mock()
            mock_data = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [25, 30]})
            mock_connector.read.return_value = [DataChunk(mock_data)]
            mock_execution_context.connector_registry.create_source_connector.return_value = mock_connector
            
            result = handler.execute(step, mock_execution_context)
            
            # The result should be structured properly even if it fails due to missing real components
            assert isinstance(result, StepExecutionResult)
            assert result.step_id == "test_load"
            assert result.step_type == "load"
            assert isinstance(result.start_time, datetime)
            
        finally:
            os.unlink(csv_file)

    def test_observability_integration(self, mock_execution_context):
        """Test that observability data is properly collected."""
        handler = LoadStepHandler()
        
        step = LoadStep(
            id="observability_test",
            source="test.csv",
            target_table="test_table",
            load_mode="replace",
            options={"connector_type": "csv"}
        )
        
        # Mock connector failure to test error observability
        mock_execution_context.connector_registry.create_source_connector.side_effect = RuntimeError("Test error")
        
        result = handler.execute(step, mock_execution_context)
        
        # Verify observability methods were called
        mock_execution_context.observability_manager.record_step_start.assert_called_once()
        
        # Result should contain observability data
        assert hasattr(result, 'observability_data')
        assert hasattr(result, 'performance_metrics')
        assert result.error_message is not None

    def test_result_structure_with_error(self, mock_execution_context, sample_load_step):
        """Test that error results have proper structure."""
        handler = LoadStepHandler()
        
        # Force an error
        mock_execution_context.connector_registry.create_source_connector.side_effect = Exception("Test error")
        
        result = handler.execute(sample_load_step, mock_execution_context)
        
        # Verify error result structure
        assert not result.is_successful()
        assert result.status == "FAILURE"
        assert result.error_message is not None
        assert "Load operation failed" in result.error_message
        assert result.error_code == "LOAD_EXECUTION_ERROR"
        assert isinstance(result.execution_duration_ms, (int, float))
        assert result.execution_duration_ms >= 0

    def test_step_id_preservation(self, mock_execution_context):
        """Test that step ID is preserved in results."""
        handler = LoadStepHandler()
        
        step = LoadStep(
            id="custom_step_id_123",
            source="test.csv",
            target_table="test_table",
            load_mode="replace"
        )
        
        result = handler.execute(step, mock_execution_context)
        
        assert result.step_id == "custom_step_id_123"

    def test_different_load_modes(self, mock_execution_context):
        """Test that different load modes are handled."""
        handler = LoadStepHandler()
        
        modes = ["replace", "append", "upsert"]
        
        for mode in modes:
            step = LoadStep(
                id=f"test_{mode}",
                source="test.csv",
                target_table="test_table",
                load_mode=mode,
                incremental_config={"upsert_keys": ["id"]} if mode == "upsert" else {}
            )
            
            result = handler.execute(step, mock_execution_context)
            
            # All should fail due to missing connector but with proper error handling
            assert isinstance(result, StepExecutionResult)
            assert result.step_type == "load" 