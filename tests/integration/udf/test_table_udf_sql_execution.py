"""Integration tests for table UDF execution in SQL FROM clauses.

These tests verify that table UDFs work correctly when used in SQL queries,
particularly in FROM clauses. This would have caught the bug where table UDFs
were returning error messages instead of being executed externally.
"""

import tempfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest


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

    def test_table_udf_in_from_clause_basic(
        self, table_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ):
        """Test basic table UDF execution in FROM clause - this would have caught the bug."""
        project_dir = table_udf_test_env["project_dir"]

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
        source_path = tmp_path / "sales_data.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "project": "udf_from_clause_test",
            "pipeline": "basic_test",
            "steps": [
                {
                    "type": "load",
                    "name": "sales_data",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "calculated_sales",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", sales_data)',
                },
            ],
        }

        # Execute pipeline
        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success

        # Verify the table UDF was actually executed
        df = coordinator.context.engine.execute_query(
            "SELECT * FROM calculated_sales"
        ).df()

        assert len(df) == 3, "Should return 3 rows"
        assert "total" in df.columns, "Should have 'total' column added by UDF"
        assert "tax" in df.columns, "Should have 'tax' column added by UDF"
        assert (
            "final_price" in df.columns
        ), "Should have 'final_price' column added by UDF"

        # Verify calculations are correct
        first_row = df.iloc[0]
        assert first_row["total"] == 100.0, "50.0 * 2 = 100.0"
        assert first_row["tax"] == 8.0, "100.0 * 0.08 = 8.0"
        assert first_row["final_price"] == 108.0, "100.0 + 8.0 = 108.0"

    def test_table_udf_returns_data_not_error_messages(
        self, table_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ):
        """Regression test: Ensure table UDFs return actual data, not error messages."""
        project_dir = table_udf_test_env["project_dir"]

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
        source_path = tmp_path / "test_sales.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "project": "udf_error_msg_test",
            "pipeline": "regression_test",
            "steps": [
                {
                    "type": "load",
                    "name": "test_sales",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "calculated_sales",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", test_sales)',
                },
            ],
        }

        # Execute pipeline
        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success

        df = coordinator.context.engine.execute_query(
            "SELECT * FROM calculated_sales"
        ).df()

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
        self, table_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ):
        """Regression test: Verify external processing workaround is actually functioning."""
        project_dir = table_udf_test_env["project_dir"]

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
        source_path = tmp_path / "specific_test.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "project": "udf_external_processing_test",
            "pipeline": "regression_test",
            "steps": [
                {
                    "type": "load",
                    "name": "specific_test",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "calculated_data",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", specific_test)',
                },
            ],
        }

        # Execute pipeline
        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success

        df = coordinator.context.engine.execute_query(
            "SELECT * FROM calculated_data"
        ).df()

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

    def test_table_udf_query_transformation_is_correct_e2e(
        self, table_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ):
        """Test that the query transformation for table UDFs works end-to-end."""
        project_dir = table_udf_test_env["project_dir"]

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
        source_path = tmp_path / "debug_test.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "project": "udf_debug_test",
            "pipeline": "transformation_test",
            "steps": [
                {
                    "type": "load",
                    "name": "debug_test",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "final_data",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.test_table_udfs.add_sales_calculations", debug_test)',
                },
            ],
        }

        # Execute pipeline
        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success

        # Verify that the UDF was executed correctly by checking the output
        df = coordinator.context.engine.execute_query("SELECT * FROM final_data").df()
        assert len(df) == 1
        assert "final_price" in df.columns
        assert df.iloc[0]["final_price"] == 108.0
