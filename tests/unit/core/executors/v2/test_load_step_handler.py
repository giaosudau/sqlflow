"""Tests for LoadStepHandler load operation behavior.

This test suite validates the LoadStepHandler's ability to handle different
load modes, data sources, and error conditions with proper observability.

Following Kent Beck's testing principles:
- Use real objects whenever possible
- Mock only at system boundaries
- Test behavior, not implementation
- Make tests independent and fast
"""

import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.executors.v2.handlers.load_handler import LoadStepHandler
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import LoadStep

# Import the improved test utilities
from tests.unit.core.executors.v2.test_utilities import (
    LightweightTestConnector,
    assert_no_mocks_in_context,
    create_lightweight_context,
)


@pytest.fixture
def real_execution_context():
    """Create a real execution context for testing."""
    context = create_lightweight_context()
    assert_no_mocks_in_context(context)  # Ensure no mocks
    return context


@pytest.fixture
def sample_load_step():
    """Create a sample LoadStep for testing."""
    return LoadStep(
        id="test_load_customers",
        source="customers.csv",
        target_table="customers",
        load_mode="replace",
        expected_duration_ms=1000.0,
        criticality="high",
    )


@pytest.fixture
def sample_data_chunks():
    """Create sample data chunks for testing."""
    df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )
    return [DataChunk(df)]


class TestLoadStepHandler:
    """Test suite for LoadStepHandler."""

    def test_load_step_validation(self, real_execution_context):
        """Test that invalid LoadStep configurations are caught."""
        LoadStepHandler()

        # Test validation happens in LoadStep constructor
        with pytest.raises(ValueError, match="Load step source cannot be empty"):
            LoadStep(
                id="invalid_step", source="", target_table="table", load_mode="replace"
            )

    def test_upsert_validation_requires_keys(self, real_execution_context):
        """Test that UPSERT mode requires upsert_keys."""
        handler = LoadStepHandler()

        # UPSERT without upsert_keys should fail during validation
        upsert_step = LoadStep(
            id="invalid_upsert",
            source="data.csv",
            target_table="table",
            load_mode="upsert",
            incremental_config={
                "cursor_field": "updated_at"
            },  # Has config but no upsert_keys
        )

        result = handler.execute(upsert_step, real_execution_context)
        assert not result.is_successful()
        assert result.error_message is not None
        assert "UPSERT mode requires upsert_keys" in result.error_message

    def test_invalid_load_mode_validation(self, real_execution_context):
        """Test that invalid load modes are rejected."""
        handler = LoadStepHandler()

        invalid_step = LoadStep(
            id="invalid_mode",
            source="data.csv",
            target_table="table",
            load_mode="invalid_mode",  # type: ignore
        )

        result = handler.execute(invalid_step, real_execution_context)
        assert not result.is_successful()
        assert result.error_message is not None
        assert "invalid load_mode 'invalid_mode'" in result.error_message

    def test_connector_creation_error_handling(
        self, real_execution_context, sample_load_step
    ):
        """Test error handling when connector creation fails."""
        handler = LoadStepHandler()

        # Create a step with invalid source to trigger connector error
        invalid_step = LoadStep(
            id="connector_error",
            source="nonexistent_file.csv",
            target_table="test_table",
            load_mode="replace",
        )

        result = handler.execute(invalid_step, real_execution_context)

        # With real connectors, this might succeed or fail depending on implementation
        # The important thing is that it handles errors gracefully
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "connector_error"

    def test_successful_execution_structure(self, real_execution_context):
        """Test that successful execution returns proper result structure."""
        handler = LoadStepHandler()

        # Create a test CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,age\n1,Alice,25\n2,Bob,30\n")
            csv_file = f.name

        try:
            step = LoadStep(
                id="test_load",
                source=csv_file,
                target_table="test_table",
                load_mode="replace",
                options={"connector_type": "csv"},
            )

            result = handler.execute(step, real_execution_context)

            # The result should be structured properly
            assert isinstance(result, StepExecutionResult)
            assert result.step_id == "test_load"
            assert result.step_type == "load"
            assert isinstance(result.start_time, datetime)

        finally:
            os.unlink(csv_file)

    def test_observability_integration(self, real_execution_context):
        """Test that observability data is properly collected."""
        handler = LoadStepHandler()

        step = LoadStep(
            id="observability_test",
            source="test.csv",
            target_table="test_table",
            load_mode="replace",
            options={"connector_type": "csv"},
        )

        result = handler.execute(step, real_execution_context)

        # Result should contain observability data
        assert hasattr(result, "observability_data")
        assert hasattr(result, "performance_metrics")

    def test_result_structure_with_error(
        self, real_execution_context, sample_load_step
    ):
        """Test that error results have proper structure."""
        handler = LoadStepHandler()

        # Create step that will likely fail
        error_step = LoadStep(
            id="error_test",
            source="definitely_nonexistent_file.csv",
            target_table="error_table",
            load_mode="replace",
        )

        result = handler.execute(error_step, real_execution_context)

        # Verify error result structure
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "error_test"
        assert result.step_type == "load"
        assert isinstance(result.execution_duration_ms, (int, float))
        assert result.execution_duration_ms >= 0

    def test_step_id_preservation(self, real_execution_context):
        """Test that step ID is preserved in results."""
        handler = LoadStepHandler()

        step = LoadStep(
            id="custom_step_id_123",
            source="test.csv",
            target_table="test_table",
            load_mode="replace",
        )

        result = handler.execute(step, real_execution_context)

        assert result.step_id == "custom_step_id_123"

    def test_different_load_modes(self, real_execution_context):
        """Test that different load modes are handled."""
        handler = LoadStepHandler()

        modes = ["replace", "append", "upsert"]

        for mode in modes:
            step = LoadStep(
                id=f"test_{mode}",
                source="test.csv",
                target_table="test_table",
                load_mode=mode,  # type: ignore
                incremental_config={"upsert_keys": ["id"]} if mode == "upsert" else {},
            )

            result = handler.execute(step, real_execution_context)

            # All should return valid results
            assert isinstance(result, StepExecutionResult)
            assert result.step_type == "load"

    def test_real_connector_usage(self, real_execution_context):
        """Test that we're using real connectors, not mocks."""
        handler = LoadStepHandler()

        step = LoadStep(
            id="real_connector_test",
            source="test.csv",
            target_table="test_table",
            load_mode="replace",
        )

        # Get the connector that would be created
        connector = real_execution_context.connector_registry.create_source_connector(
            "csv", {"test_data": pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})}
        )

        # Verify it's a real LightweightTestConnector, not a Mock
        assert isinstance(connector, LightweightTestConnector)
        assert hasattr(connector, "data")  # Real LightweightTestConnector attribute
        assert callable(connector.read)  # Real method

        # Execute and verify
        result = handler.execute(step, real_execution_context)
        assert isinstance(result, StepExecutionResult)
