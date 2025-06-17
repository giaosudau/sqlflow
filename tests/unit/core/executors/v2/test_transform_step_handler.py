"""Unit tests for TransformStepHandler behavior.

This test suite validates the TransformStepHandler's ability to execute SQL
transformations with UDF support, variable substitution, and comprehensive observability.
"""

import pytest

from sqlflow.core.executors.v2.handlers.transform_handler import TransformStepHandler
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import TransformStep


def _create_lightweight_context(variables=None):
    """Helper to create execution context with real implementations - no mocks!"""
    from sqlflow.connectors.registry.enhanced_registry import EnhancedConnectorRegistry
    from sqlflow.core.engines.duckdb.engine import DuckDBEngine
    from sqlflow.core.executors.v2.context import ExecutionContext
    from sqlflow.core.executors.v2.observability import ObservabilityManager
    from sqlflow.core.state.backends import DuckDBStateBackend
    from sqlflow.core.state.watermark_manager import WatermarkManager
    from sqlflow.core.variables.manager import VariableConfig, VariableManager

    # Use ALL real implementations - following the "reduce mocking" principle
    engine = DuckDBEngine(database_path=":memory:")

    # Create variable manager with optional variables
    if variables:
        variable_config = VariableConfig(cli_variables=variables)
        variable_manager = VariableManager(variable_config)
    else:
        variable_manager = VariableManager()

    observability = ObservabilityManager(run_id="test_run")

    # Real connector registry and watermark manager
    connector_registry = EnhancedConnectorRegistry()
    state_backend = DuckDBStateBackend()  # Uses in-memory DuckDB by default
    watermark_manager = WatermarkManager(state_backend)

    context = ExecutionContext(
        sql_engine=engine,
        variable_manager=variable_manager,
        observability_manager=observability,
        connector_registry=connector_registry,
        watermark_manager=watermark_manager,
        run_id="test_run",
    )

    # Create test table for consistent testing
    import pandas as pd

    test_data = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 22]}
    )
    engine.register_table("customers", test_data)

    return context


@pytest.fixture
def lightweight_context():
    """Provide execution context with lightweight real implementations."""
    return _create_lightweight_context()


@pytest.fixture
def mock_context():
    """Legacy fixture for backwards compatibility - now uses lightweight implementations."""
    return _create_lightweight_context()


@pytest.fixture
def basic_transform_step():
    """Provide basic transform step for testing."""
    return TransformStep(
        id="test_transform",
        sql="SELECT * FROM customers WHERE age > 25",
        target_table="adult_customers",
        operation_type="create_table",
    )


class TestTransformStepHandler:
    """Test suite for TransformStepHandler behavior."""

    def test_basic_sql_execution(self):
        """Test simple CREATE TABLE AS operations with real DuckDB."""
        # Use real context instead of mock
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        step = TransformStep(
            id="test_transform",
            sql="SELECT * FROM customers WHERE age > 25",
            target_table="adult_customers",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        # Verify result structure
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "test_transform"
        assert result.step_type == "transform"
        assert result.is_successful()

        # Verify table was actually created with correct data
        assert context.sql_engine.table_exists("adult_customers")

        # Verify correct number of rows (age > 25: Bob=30 only, Alice=25 doesn't qualify for >25)
        result_data = context.sql_engine.execute_query(
            "SELECT COUNT(*) as count FROM adult_customers"
        ).fetchone()
        assert result_data[0] == 1  # Only Bob (30) qualifies for age > 25

        # Verify observability was tracked
        assert result.performance_metrics is not None
        assert result.execution_duration_ms > 0

    def test_create_or_replace_mode(self):
        """Test CREATE OR REPLACE table operations."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        # First create the table, then test replacement
        step1 = TransformStep(
            id="initial_transform",
            sql="SELECT * FROM customers",
            target_table="customer_summary",
            operation_type="create_table",
        )

        step2 = TransformStep(
            id="replace_transform",
            sql="SELECT * FROM customers WHERE age > 30",
            target_table="customer_summary",
            operation_type="create_table",
            metadata={"is_replace": True},
        )

        # Execute initial creation
        result1 = handler.execute(step1, context)
        assert result1.is_successful()

        # Get initial count
        initial_count = context.sql_engine.execute_query(
            "SELECT COUNT(*) FROM customer_summary"
        ).fetchone()[0]

        # Execute replacement
        result2 = handler.execute(step2, context)
        assert result2.is_successful()

        # Verify table still exists and has different data
        assert context.sql_engine.table_exists("customer_summary")

        # Verify data was replaced (different count due to WHERE clause)
        final_count = context.sql_engine.execute_query(
            "SELECT COUNT(*) FROM customer_summary"
        ).fetchone()[0]

        # Should have different counts (filtered vs unfiltered)
        assert final_count != initial_count

    @pytest.mark.skip(
        reason="UDF integration requires UDF manager setup - covered in integration tests"
    )
    def test_sql_with_udf_integration(self):
        """Test SQL execution with Python UDF calls - skipped for unit test."""
        # This test is better suited for integration tests where UDF manager is properly configured

    def test_variable_substitution_in_sql(self):
        """Test ${variable} replacement with real variable manager."""

        # Create context with variables configured using our updated helper
        test_variables = {"source_table": "customers", "min_age": "25"}
        context = _create_lightweight_context(variables=test_variables)

        handler = TransformStepHandler()

        step = TransformStep(
            id="var_transform",
            sql="SELECT * FROM ${source_table} WHERE age >= ${min_age}",
            target_table="filtered_data",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded (variables were substituted correctly)
        assert result.is_successful()

        # Verify table was created
        assert context.sql_engine.table_exists("filtered_data")

        # Verify correct filtering (age >= 25: Alice=25, Bob=30)
        result_data = context.sql_engine.execute_query(
            "SELECT COUNT(*) as count FROM filtered_data"
        ).fetchone()
        assert result_data[0] == 2

    def test_error_propagation_from_sql_engine(self):
        """Test that SQL errors are properly caught and reported."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        # Create a step that will fail due to nonexistent table
        step = TransformStep(
            id="error_test",
            sql="SELECT * FROM nonexistent_table",
            target_table="error_result",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        # Verify error handling
        assert not result.is_successful()
        assert result.error_message is not None
        assert "transform operation failed" in result.error_message.lower()
        assert result.error_code == "TRANSFORM_EXECUTION_ERROR"

        # Verify the error contains meaningful information
        assert "nonexistent_table" in result.error_message.lower()

    def test_step_validation(self):
        """Test step configuration validation."""
        handler = TransformStepHandler()

        # Test empty SQL
        with pytest.raises(ValueError, match="Transform step SQL cannot be empty"):
            step = TransformStep(id="empty_sql", sql="", target_table="test")
            handler._validate_transform_step(step)

        # Test operation requiring target_table without it
        with pytest.raises(ValueError, match="requires target_table"):
            step = TransformStep(
                id="no_target",
                sql="SELECT 1",
                operation_type="create_table",
                target_table=None,
            )
            handler._validate_transform_step(step)

    def test_schema_collection(self):
        """Test schema information collection for observability."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        step = TransformStep(
            id="schema_test",
            sql="SELECT id, name, age FROM customers",
            target_table="schema_test_table",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded
        assert result.is_successful()

        # Verify schema was collected and is meaningful
        assert result.output_schema is not None
        assert isinstance(result.output_schema, dict)

        # Verify table was created and we can query its schema
        assert context.sql_engine.table_exists("schema_test_table")
        actual_schema = context.sql_engine.get_table_schema("schema_test_table")
        assert actual_schema is not None

    def test_performance_metrics_collection(self):
        """Test performance metrics are collected."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        step = TransformStep(
            id="metrics_test",
            sql="SELECT * FROM customers",
            target_table="metrics_test_table",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded
        assert result.is_successful()

        # Verify performance metrics structure
        assert result.performance_metrics is not None
        assert "sql_execution_time_ms" in result.performance_metrics
        assert "sql_query" in result.performance_metrics

    def test_observability_integration(self):
        """Test comprehensive observability integration."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        step = TransformStep(
            id="observability_test",
            sql="SELECT * FROM customers",
            target_table="observability_test_table",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded
        assert result.is_successful()

        # Verify observability data was collected
        assert result.observability_data is not None
        assert result.execution_duration_ms > 0

        # Verify performance metrics are part of observability
        assert result.performance_metrics is not None

    def test_select_operation_type(self):
        """Test direct SELECT query execution."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        step = TransformStep(
            id="select_transform",
            sql="SELECT COUNT(*) as customer_count FROM customers",
            operation_type="select",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded
        assert result.is_successful()

        # For SELECT operations, verify we get meaningful result data
        assert result.performance_metrics is not None
        assert "sql_query" in result.performance_metrics

    def test_insert_operation_type(self):
        """Test INSERT INTO operations."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        # First create a target table
        context.sql_engine.execute_query(
            "CREATE TABLE customers_backup AS SELECT * FROM customers WHERE 1=0"  # Empty copy
        )

        step = TransformStep(
            id="insert_transform",
            sql="SELECT * FROM customers WHERE age > 25",
            target_table="customers_backup",
            operation_type="insert",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded
        assert result.is_successful()

        # Verify data was inserted
        count_result = context.sql_engine.execute_query(
            "SELECT COUNT(*) FROM customers_backup"
        ).fetchone()
        assert count_result[0] > 0

    def test_view_materialization(self):
        """Test CREATE VIEW operations."""
        context = _create_lightweight_context()
        handler = TransformStepHandler()

        step = TransformStep(
            id="view_transform",
            sql="SELECT * FROM customers WHERE age > 25",
            target_table="active_customers_view",
            operation_type="create_table",
            materialization="view",
        )

        result = handler.execute(step, context)

        # Verify operation succeeded
        assert result.is_successful()

        # Verify view was created (views appear as tables in DuckDB)
        assert context.sql_engine.table_exists("active_customers_view")

        # Verify we can query the view
        view_result = context.sql_engine.execute_query(
            "SELECT COUNT(*) FROM active_customers_view"
        ).fetchone()
        assert view_result[0] > 0
