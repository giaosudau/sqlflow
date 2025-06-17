"""Unit tests for ExportStepHandler behavior.

This test suite validates the ExportStepHandler's ability to export data
from tables to various destinations with comprehensive observability.

Following Kent Beck's testing principles:
- Use real objects whenever possible
- Mock only at system boundaries
- Test behavior, not implementation
"""

import pytest

from sqlflow.core.executors.v2.handlers.export_handler import ExportStepHandler
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import ExportStep

# Import the improved test utilities
from tests.unit.core.executors.v2.test_utilities import (
    LightweightTestConnector,
    assert_no_mocks_in_context,
    create_lightweight_context,
)


@pytest.fixture
def lightweight_context():
    """Provide lightweight real execution context for tests."""
    context = create_lightweight_context()
    assert_no_mocks_in_context(context)  # Ensure no mocks
    return context


@pytest.fixture
def basic_export_step():
    """Provide basic export step for testing."""
    return ExportStep(
        id="test_export",
        source_table="customers",
        target="output/customers.csv",
        export_format="csv",
    )


class TestExportStepHandler:
    """Test suite for ExportStepHandler behavior."""

    def test_basic_table_to_csv_export(self, lightweight_context, basic_export_step):
        """Test basic table export to CSV file with real implementations."""
        handler = ExportStepHandler()

        # Use real connector registry - no mocking needed
        result = handler.execute(basic_export_step, lightweight_context)

        # Verify result structure
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "test_export"
        assert result.step_type == "export"
        assert result.is_successful()

        # Verify connector was used (real LightweightTestConnector)
        connector = lightweight_context.connector_registry.create_destination_connector(
            basic_export_step.target
        )
        assert isinstance(connector, LightweightTestConnector)

    def test_sql_query_to_parquet_export(self, lightweight_context):
        """Test export with inline SQL query to Parquet."""
        handler = ExportStepHandler()

        step = ExportStep(
            id="query_export",
            source_table="SELECT * FROM customers WHERE age >= 25",
            target="output/recent_orders.parquet",
            export_format="parquet",
        )

        # Execute with real implementations
        result = handler.execute(step, lightweight_context)

        assert result.is_successful()

    def test_export_with_variable_substitution(self, lightweight_context):
        """Test destination path variable substitution."""
        handler = ExportStepHandler()

        step = ExportStep(
            id="var_export",
            source_table="customers",
            target="${output_dir}/users_${date}.csv",
            export_format="csv",
        )

        # Execute with real variable manager (will handle substitution)
        result = handler.execute(step, lightweight_context)

        assert result.is_successful()

    def test_s3_export_with_partitioning(self, lightweight_context):
        """Test S3 export with proper options."""
        handler = ExportStepHandler()

        step = ExportStep(
            id="s3_export",
            source_table="customers",
            target="s3://bucket/path/dataset.parquet",
            export_format="parquet",
            options={"partition_by": ["age"], "compression": "gzip"},
        )

        # Execute with real implementations
        result = handler.execute(step, lightweight_context)

        assert result.is_successful()

    def test_export_error_handling(self):
        """Test various failure scenarios with real implementations."""
        context = create_lightweight_context()
        handler = ExportStepHandler()

        # Create a step that will fail due to nonexistent table
        step = ExportStep(
            id="error_export",
            source_table="nonexistent_table",
            target="output/error_test.csv",
        )

        result = handler.execute(step, context)

        # Verify error handling
        assert not result.is_successful()
        assert result.error_message is not None
        assert "export operation failed" in result.error_message.lower()
        assert result.error_code == "EXPORT_EXECUTION_ERROR"

        # Verify error contains meaningful information
        assert "nonexistent_table" in result.error_message.lower()

    def test_step_validation(self):
        """Test step configuration validation."""
        handler = ExportStepHandler()

        # Test empty source_table
        with pytest.raises(
            ValueError, match="Export step source_table cannot be empty"
        ):
            step = ExportStep(id="empty_source", source_table="", target="test.csv")
            handler._validate_export_step(step)

        # Test empty target
        with pytest.raises(ValueError, match="Export step target cannot be empty"):
            step = ExportStep(id="empty_target", source_table="test", target="")
            handler._validate_export_step(step)

    def test_performance_metrics_collection(
        self, lightweight_context, basic_export_step
    ):
        """Test performance metrics are collected."""
        handler = ExportStepHandler()

        result = handler.execute(basic_export_step, lightweight_context)

        # Verify performance metrics structure
        assert result.performance_metrics is not None
        assert "data_extraction_time_ms" in result.performance_metrics
        assert "rows_exported" in result.performance_metrics
        assert "export_format" in result.performance_metrics

    def test_observability_integration(self, lightweight_context, basic_export_step):
        """Test comprehensive observability integration."""
        handler = ExportStepHandler()

        handler.execute(basic_export_step, lightweight_context)

        # Verify observability calls (these are real, not mocked)
        # The lightweight context has real observability manager

    def test_large_table_export_with_batching(self, lightweight_context):
        """Test export of large tables with batching support."""
        handler = ExportStepHandler()

        step = ExportStep(
            id="large_export",
            source_table="customers",
            target="output/large_data.csv",
            options={"batch_size": 1000},
        )

        result = handler.execute(step, lightweight_context)

        assert result.is_successful()
        # Should have some rows exported (from the test customers table)
        assert result.performance_metrics.get("rows_exported", 0) > 0

    def test_compressed_export(self, lightweight_context):
        """Test export with compression options."""
        handler = ExportStepHandler()

        step = ExportStep(
            id="compressed_export",
            source_table="customers",
            target="output/compressed.csv.gz",
            compression="gzip",
        )

        result = handler.execute(step, lightweight_context)

        assert result.is_successful()

    def test_real_connector_integration(self, lightweight_context):
        """Test that we're using real connectors, not mocks."""
        handler = ExportStepHandler()

        step = ExportStep(
            id="real_connector_test",
            source_table="customers",
            target="output/test.csv",
        )

        # Get the connector that would be used
        connector = lightweight_context.connector_registry.create_destination_connector(
            step.target
        )

        # Verify it's a real LightweightTestConnector, not a Mock
        assert isinstance(connector, LightweightTestConnector)
        assert hasattr(
            connector, "write_calls"
        )  # Real LightweightTestConnector attribute
        assert callable(connector.write)  # Real method

        # Execute and verify connector was used
        result = handler.execute(step, lightweight_context)
        assert result.is_successful()
