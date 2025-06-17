"""Integration tests for TransformStepHandler with real SQL engine.

This test suite validates TransformStepHandler behavior with actual DuckDB engine,
real UDF integration, and complete variable substitution.
"""

from unittest.mock import Mock

import pandas as pd
import pytest

from sqlflow.connectors.registry.enhanced_registry import EnhancedConnectorRegistry
from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.handlers.transform_handler import TransformStepHandler
from sqlflow.core.executors.v2.observability import ObservabilityManager
from sqlflow.core.executors.v2.steps import TransformStep
from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.core.variables.manager import VariableManager


@pytest.fixture
def real_execution_context():
    """Create real execution context with DuckDB engine."""
    engine = DuckDBEngine(database_path=":memory:")
    observability = ObservabilityManager(run_id="integration_test")
    variables = VariableManager()
    connector_registry = Mock(spec=EnhancedConnectorRegistry)
    watermark_manager = Mock(spec=WatermarkManager)

    context = ExecutionContext(
        sql_engine=engine,
        connector_registry=connector_registry,
        variable_manager=variables,
        watermark_manager=watermark_manager,
        observability_manager=observability,
        run_id="integration_test",
    )

    # Set up test data
    test_data = pd.DataFrame(
        {
            "customer_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 22, 35, 28],
            "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
        }
    )

    engine.register_table("customers", test_data)

    return context


class TestTransformStepHandlerIntegration:
    """Integration test suite for TransformStepHandler."""

    def test_real_sql_execution(self, real_execution_context):
        """Test actual SQL execution with real DuckDB engine."""
        handler = TransformStepHandler()

        step = TransformStep(
            id="real_transform",
            sql="SELECT customer_id, name, age FROM customers WHERE age >= 25",
            target_table="adult_customers",
            operation_type="create_table",
        )

        result = handler.execute(step, real_execution_context)

        assert result.is_successful()

        # Verify table was created
        assert real_execution_context.sql_engine.table_exists("adult_customers")

        # Verify data integrity - should have 4 customers age >= 25
        adult_data = real_execution_context.sql_engine.execute_query(
            "SELECT COUNT(*) as count FROM adult_customers"
        ).fetchone()
        assert adult_data[0] == 4

    def test_variable_substitution_integration(self):
        """Test actual variable substitution."""
        # Create execution context with pre-configured variables
        engine = DuckDBEngine(database_path=":memory:")
        observability = ObservabilityManager(run_id="integration_test")

        # Create VariableManager with pre-configured variables
        from sqlflow.core.variables.manager import VariableConfig

        config = VariableConfig(cli_variables={"min_age": "25", "target_city": "NYC"})
        variables = VariableManager(config)

        connector_registry = Mock(spec=EnhancedConnectorRegistry)
        watermark_manager = Mock(spec=WatermarkManager)

        context = ExecutionContext(
            sql_engine=engine,
            connector_registry=connector_registry,
            variable_manager=variables,
            watermark_manager=watermark_manager,
            observability_manager=observability,
            run_id="integration_test",
        )

        # Set up test data
        test_data = pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 22, 35, 28],
                "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
            }
        )
        engine.register_table("customers", test_data)

        handler = TransformStepHandler()

        step = TransformStep(
            id="var_test",
            sql="SELECT * FROM customers WHERE age >= ${min_age} AND city = '${target_city}'",
            target_table="filtered_customers",
            operation_type="create_table",
        )

        result = handler.execute(step, context)

        assert result.is_successful()

        # Verify substitution worked
        filtered_data = context.sql_engine.execute_query(
            "SELECT name FROM filtered_customers"
        ).fetchone()
        assert filtered_data[0] == "Alice"

    def test_error_handling_with_real_engine(self, real_execution_context):
        """Test error handling with real SQL errors."""
        handler = TransformStepHandler()

        step = TransformStep(
            id="error_test",
            sql="SELECT * FROM nonexistent_table",
            target_table="error_result",
            operation_type="create_table",
        )

        result = handler.execute(step, real_execution_context)

        assert not result.is_successful()
        assert result.error_message is not None
        assert "transform operation failed" in result.error_message.lower()
        assert "nonexistent_table" in result.error_message.lower()

        # Verify table was not created
        assert not real_execution_context.sql_engine.table_exists("error_result")

    def test_comprehensive_observability_data(self, real_execution_context):
        """Test that observability collects comprehensive data."""
        handler = TransformStepHandler()

        step = TransformStep(
            id="observability_test",
            sql="SELECT customer_id, UPPER(name) as name_upper, age * 2 as double_age FROM customers",
            target_table="transformed_customers",
            operation_type="create_table",
        )

        result = handler.execute(step, real_execution_context)

        assert result.is_successful()

        # Verify comprehensive result data
        assert result.performance_metrics is not None
        assert result.output_schema is not None
        assert result.data_lineage is not None
        assert result.execution_duration_ms > 0

        # Verify schema tracking
        expected_columns = {"customer_id", "name_upper", "double_age"}
        actual_columns = set(result.output_schema.keys())
        assert expected_columns.issubset(actual_columns)

        # Verify data lineage
        assert result.data_lineage["target_table"] == "transformed_customers"
        assert result.data_lineage["operation_type"] == "create_table"
