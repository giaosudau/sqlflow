"""Integration tests for Transform Step executor.

These tests verify that the transform step executor works correctly
with real DuckDB engine and actual SQL transformations.
"""

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2.execution.context import create_test_context
from sqlflow.core.executors.v2.steps.definitions import create_step_from_dict
from sqlflow.core.executors.v2.steps.transform import TransformStepExecutor


@pytest.mark.integration
class TestTransformStepIntegration:
    """Integration tests for Transform Step executor."""

    @pytest.fixture(scope="function")
    def real_duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        return DuckDBEngine(":memory:")

    @pytest.fixture(scope="function")
    def execution_context(self, real_duckdb_engine):
        """Create execution context with real DuckDB engine."""
        return create_test_context(engine=real_duckdb_engine)

    @pytest.fixture(scope="function")
    def transform_executor(self):
        """Create transform step executor."""
        return TransformStepExecutor()

    def test_simple_transform_execution(
        self, execution_context, transform_executor, real_duckdb_engine
    ):
        """Test that transform step can execute simple SQL transformations."""
        # Setup: Create a test table
        real_duckdb_engine.execute_query(
            "CREATE TABLE test_source AS SELECT 1 as id, 'test' as name"
        )

        # Create transform step
        step_dict = {
            "id": "test_transform",
            "type": "transform",
            "target_table": "result_table",
            "sql": "SELECT id, name, 'processed' as status FROM test_source",
        }
        step = create_step_from_dict(step_dict)

        # Execute transform
        result = transform_executor.execute(step, execution_context)

        # Verify success
        assert result.success is True

        # Verify rows were affected by checking the table
        count = real_duckdb_engine.execute_query(
            "SELECT COUNT(*) FROM result_table"
        ).fetchone()[0]
        assert count > 0

        # Verify table content
        query_result = real_duckdb_engine.execute_query("SELECT * FROM result_table")
        rows = query_result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1  # id
        assert rows[0][1] == "test"  # name
        assert rows[0][2] == "processed"  # status

    def test_complex_transform_with_aggregation(
        self, execution_context, transform_executor, real_duckdb_engine
    ):
        """Test transform step with aggregation and grouping."""
        # Setup: Create test data
        real_duckdb_engine.execute_query(
            """
            CREATE TABLE sales AS SELECT * FROM VALUES
            (1, 'Product A', 100),
            (2, 'Product A', 150),
            (3, 'Product B', 200),
            (4, 'Product B', 250)
            AS t(id, product, amount)
        """
        )

        # Create aggregation transform
        step_dict = {
            "id": "sales_summary",
            "type": "transform",
            "target_table": "product_totals",
            "sql": """
                SELECT 
                    product,
                    COUNT(*) as sales_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM sales 
                GROUP BY product 
                ORDER BY product
            """,
        }
        step = create_step_from_dict(step_dict)

        # Execute transform
        result = transform_executor.execute(step, execution_context)

        # Verify success
        assert result.success is True

        # Verify rows affected by checking the table
        count = real_duckdb_engine.execute_query(
            "SELECT COUNT(*) FROM product_totals"
        ).fetchone()[0]
        assert count == 2  # Two products

        # Verify table content
        query_result = real_duckdb_engine.execute_query(
            "SELECT * FROM product_totals ORDER BY product"
        )
        rows = query_result.fetchall()

        # Product A: 2 sales, total 250, avg 125
        assert rows[0][0] == "Product A"
        assert rows[0][1] == 2
        assert rows[0][2] == 250
        assert rows[0][3] == 125.0

        # Product B: 2 sales, total 450, avg 225
        assert rows[1][0] == "Product B"
        assert rows[1][1] == 2
        assert rows[1][2] == 450
        assert rows[1][3] == 225.0

    def test_transform_error_handling(self, execution_context, transform_executor):
        """Test that transform step properly handles SQL errors."""
        # Create transform with invalid SQL
        step_dict = {
            "id": "invalid_transform",
            "type": "transform",
            "target_table": "error_table",
            "sql": "SELECT * FROM nonexistent_table",
        }
        step = create_step_from_dict(step_dict)

        # Execute transform (should fail)
        result = transform_executor.execute(step, execution_context)

        # Verify failure
        assert result.success is False
        assert result.step_id == "invalid_transform"
        assert result.error_message is not None
        assert (
            "nonexistent_table" in result.error_message
            or "does not exist" in result.error_message
        )
