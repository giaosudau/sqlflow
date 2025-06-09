"""Integration tests for table UDF execution in SQL FROM clauses.

These tests verify that table UDFs work correctly when used in SQL queries,
particularly in FROM clauses. This would have caught the bug where table UDFs
were returning error messages instead of being executed externally.
"""

import tempfile
from typing import Any, Dict

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.udfs.manager import PythonUDFManager


@pytest.fixture
def table_udf_test_env():
    """Create test environment with table UDFs for SQL execution testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create UDF module - use the default "python_udfs" directory name
        udfs_dir = f"{temp_dir}/python_udfs"
        import os

        os.makedirs(udfs_dir, exist_ok=True)

        # Create __init__.py
        with open(f"{udfs_dir}/__init__.py", "w") as f:
            f.write("")

        # Create test_table_udfs.py with actual table UDFs
        with open(f"{udfs_dir}/test_table_udfs.py", "w") as f:
            f.write(
                '''"""Test table UDFs for SQL execution testing."""

import pandas as pd
from sqlflow.udfs.decorators import python_table_udf


@python_table_udf(
    output_schema={
        "id": "INTEGER",
        "customer_id": "INTEGER",
        "product": "VARCHAR",
        "price": "DOUBLE",
        "quantity": "INTEGER",
        "total": "DOUBLE",
        "tax": "DOUBLE",
        "final_price": "DOUBLE"
    }
)
def add_sales_calculations(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales calculations to a sales DataFrame."""
    result = df.copy()
    result["total"] = result["price"] * result["quantity"]
    result["tax"] = result["total"] * 0.08  # 8% tax
    result["final_price"] = result["total"] + result["tax"]
    return result
'''
            )

        yield {"project_dir": temp_dir, "udfs_dir": udfs_dir}


class TestTableUDFSQLExecution:
    """Test table UDF execution in actual SQL queries."""

    def test_table_udf_in_from_clause_basic(self, table_udf_test_env: Dict[str, Any]):
        """Test basic table UDF execution in FROM clause - this would have caught the bug."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=table_udf_test_env["project_dir"])

        # Discover and register UDFs
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "customer_id": [101, 102, 103],
                "product": ["Widget A", "Widget B", "Widget C"],
                "price": [50.0, 75.0, 100.0],
                "quantity": [2, 1, 3],
            }
        )

        # Register test data
        engine.register_table("sales_data", test_data)

        # THIS IS THE CRITICAL TEST - Execute SQL with table UDF in FROM clause
        # Use the proper UDF query processing path
        query = 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", sales_data)'

        # Process query for UDFs first (this is what LocalExecutor does)
        processed_query = engine.process_query_for_udfs(query, manager.udfs)
        result = engine.execute_query(processed_query)
        df = result.fetchdf()

        # Verify the table UDF was actually executed (not just error messages)
        assert len(df) == 3, "Should return 3 rows"
        assert "total" in df.columns, "Should have 'total' column added by UDF"
        assert "tax" in df.columns, "Should have 'tax' column added by UDF"
        assert (
            "final_price" in df.columns
        ), "Should have 'final_price' column added by UDF"

        # Verify calculations are correct (proves UDF was executed, not error message)
        first_row = df.iloc[0]
        assert first_row["total"] == 100.0, "50.0 * 2 = 100.0"
        assert first_row["tax"] == 8.0, "100.0 * 0.08 = 8.0"
        assert first_row["final_price"] == 108.0, "100.0 + 8.0 = 108.0"

    def test_table_udf_returns_data_not_error_messages(
        self, table_udf_test_env: Dict[str, Any]
    ):
        """Regression test: Ensure table UDFs return actual data, not error messages."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=table_udf_test_env["project_dir"])

        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [1, 2],
                "customer_id": [101, 102],
                "product": ["A", "B"],
                "price": [50.0, 75.0],
                "quantity": [1, 2],
            }
        )

        engine.register_table("test_sales", test_data)

        # Execute table UDF with proper query processing
        query = 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", test_sales)'
        processed_query = engine.process_query_for_udfs(query, manager.udfs)
        result = engine.execute_query(processed_query)
        df = result.fetchdf()

        # CRITICAL: Should NOT contain error messages
        column_names = list(df.columns)
        assert "error_message" not in column_names, "Should not return error messages"
        assert "udf_name" not in column_names, "Should not return error metadata"

        # CRITICAL: Should contain actual processed data
        assert "total" in column_names, "Should contain UDF-calculated columns"
        assert "tax" in column_names, "Should contain UDF-calculated columns"
        assert len(df) == 2, "Should return actual data rows"

        # CRITICAL: Values should be calculated, not default/error values
        assert df.iloc[0]["total"] > 0, "Should have calculated values"
        assert df.iloc[1]["total"] > 0, "Should have calculated values"

    def test_table_udf_external_processing_actually_works(
        self, table_udf_test_env: Dict[str, Any]
    ):
        """Regression test: Verify external processing workaround is actually functioning."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=table_udf_test_env["project_dir"])

        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create specific test data to verify UDF processing
        test_data = pd.DataFrame(
            {
                "id": [1],
                "customer_id": [999],  # Unique value to verify processing
                "product": ["TestProduct"],
                "price": [42.0],  # Specific value to check calculations
                "quantity": [3],
            }
        )

        engine.register_table("specific_test", test_data)

        # Execute table UDF with proper query processing
        query = 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", specific_test)'
        processed_query = engine.process_query_for_udfs(query, manager.udfs)
        result = engine.execute_query(processed_query)
        df = result.fetchdf()

        # Verify the UDF was actually executed with correct calculations
        row = df.iloc[0]

        # These calculations prove the UDF function was called
        expected_total = 42.0 * 3  # 126.0
        expected_tax = expected_total * 0.08  # 10.08
        expected_final = expected_total + expected_tax  # 136.08

        assert row["customer_id"] == 999, "Should preserve original data"
        assert (
            row["total"] == expected_total
        ), f"Total should be {expected_total}, got {row['total']}"
        assert (
            abs(row["tax"] - expected_tax) < 0.01
        ), f"Tax should be ~{expected_tax}, got {row['tax']}"
        assert (
            abs(row["final_price"] - expected_final) < 0.01
        ), f"Final should be ~{expected_final}, got {row['final_price']}"

    def test_table_udf_query_transformation_debug(
        self, table_udf_test_env: Dict[str, Any]
    ):
        """Debug test: Check what happens during query transformation."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=table_udf_test_env["project_dir"])

        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Verify UDF was discovered
        assert (
            len(manager.udfs) > 0
        ), f"Should discover UDFs, found: {list(manager.udfs.keys())}"
        assert (
            "python_udfs.test_table_udfs.add_sales_calculations" in manager.udfs
        ), f"Should find specific UDF, available: {list(manager.udfs.keys())}"

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [1],
                "customer_id": [101],
                "product": ["Widget"],
                "price": [50.0],
                "quantity": [2],
            }
        )

        engine.register_table("debug_test", test_data)

        # Test query transformation
        original_query = 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", debug_test)'
        processed_query = engine.process_query_for_udfs(original_query, manager.udfs)

        # The processed query should be different from the original
        assert (
            processed_query != original_query
        ), f"Query should be transformed. Original: {original_query}, Processed: {processed_query}"

        # The processed query should NOT contain PYTHON_FUNC
        assert (
            "PYTHON_FUNC" not in processed_query
        ), f"Processed query should not contain PYTHON_FUNC: {processed_query}"

        # Execute the processed query
        result = engine.execute_query(processed_query)
        df = result.fetchdf()

        # Verify results
        assert len(df) == 1, "Should return 1 row"
        assert "total" in df.columns, "Should have calculated columns"
