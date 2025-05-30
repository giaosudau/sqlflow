"""Table UDF Edge Cases and Error Handling Tests.

This module contains focused tests for table UDF edge cases,
error handling, schema validation, and batch processing scenarios.
"""

import numpy as np
import pandas as pd

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.decorators import python_table_udf

logger = get_logger(__name__)


class TestTableUDFEdgeCases:
    """Edge case and error handling tests for table UDFs."""

    def test_edge_case_error_handling(self, duckdb_engine: DuckDBEngine):
        """Test edge cases and error handling scenarios."""

        @python_table_udf(
            output_schema={
                "id": "INTEGER",
                "safe_result": "DOUBLE",
                "status": "VARCHAR",
            }
        )
        def robust_error_handling_udf(df: pd.DataFrame) -> pd.DataFrame:
            """UDF with comprehensive error handling."""
            result = df.copy()

            def safe_calculation(row):
                try:
                    value = pd.to_numeric(row.get("value", 0), errors="coerce")
                    if pd.isna(value) or value == 0:
                        return 0.0, "zero_or_null"
                    elif value < 0:
                        return abs(value), "negative_handled"
                    else:
                        return np.sqrt(value) * 2, "success"
                except Exception:
                    return 0.0, "error_handled"

            # Apply safe calculation
            calculations = result.apply(safe_calculation, axis=1, result_type="expand")
            result["safe_result"] = calculations[0]
            result["status"] = calculations[1]

            return result[["id", "safe_result", "status"]]

        # Test data with edge cases
        edge_case_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6],
                "value": [100, -50, 0, None, "invalid", np.inf],
            }
        )

        # Call table UDF directly as Python function
        result = robust_error_handling_udf(edge_case_data)

        # Validate error handling
        assert len(result) == 6
        assert "safe_result" in result.columns
        assert "status" in result.columns

        # Check specific edge case handling
        status_counts = result["status"].value_counts()
        assert "zero_or_null" in status_counts.index
        assert "negative_handled" in status_counts.index
        assert "success" in status_counts.index

    def test_batch_processing_capabilities(self, duckdb_engine: DuckDBEngine):
        """Test batch processing capabilities with varying sizes."""

        @python_table_udf(
            output_schema={
                "batch_id": "INTEGER",
                "item_count": "INTEGER",
                "avg_value": "DOUBLE",
            }
        )
        def batch_processor_udf(
            df: pd.DataFrame, *, batch_size: int = 100
        ) -> pd.DataFrame:
            """UDF that processes data in configurable batches."""
            results = []

            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]
                batch_result = {
                    "batch_id": i // batch_size + 1,
                    "item_count": len(batch),
                    "avg_value": (
                        batch.get("value", pd.Series(dtype=float)).mean()
                        if len(batch) > 0
                        else 0.0
                    ),
                }
                results.append(batch_result)

            return pd.DataFrame(results)

        # Create test data for batch processing
        batch_test_data = pd.DataFrame(
            {
                "id": range(1, 351),  # 350 rows
                "value": np.random.uniform(10, 100, 350),
            }
        )

        # Test different batch sizes
        for batch_size in [50, 100, 150]:
            result = batch_processor_udf(batch_test_data, batch_size=batch_size)

            expected_batches = (350 + batch_size - 1) // batch_size  # Ceiling division
            assert len(result) == expected_batches
            assert "batch_id" in result.columns
            assert "item_count" in result.columns
            assert "avg_value" in result.columns

    def test_schema_compatibility_validation(self, duckdb_engine: DuckDBEngine):
        """Test schema compatibility validation."""

        @python_table_udf(
            output_schema={
                "customer_id": "INTEGER",
                "normalized_name": "VARCHAR",
                "validation_status": "VARCHAR",
            }
        )
        def schema_validation_udf(df: pd.DataFrame) -> pd.DataFrame:
            """UDF that validates and normalizes input schema."""
            result = pd.DataFrame()

            # Validate required columns exist
            required_columns = ["customer_id", "customer_name"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                # Return error row for missing columns
                return pd.DataFrame(
                    {
                        "customer_id": [0],
                        "normalized_name": [""],
                        "validation_status": [
                            f"Missing columns: {', '.join(missing_columns)}"
                        ],
                    }
                )

            # Process valid data
            result["customer_id"] = df["customer_id"]
            result["normalized_name"] = df["customer_name"].str.strip().str.upper()
            result["validation_status"] = "valid"

            return result

        # Test with valid schema
        valid_data = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "customer_name": ["  john doe  ", "Jane Smith", "bob wilson"],
            }
        )

        result = schema_validation_udf(valid_data)

        assert len(result) == 3
        assert all(result["validation_status"] == "valid")
        assert result.loc[0, "normalized_name"] == "JOHN DOE"

        # Test with invalid schema
        invalid_data = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "wrong_column": ["value1", "value2"],
            }
        )

        result = schema_validation_udf(invalid_data)

        assert len(result) == 1
        assert "Missing columns" in result.loc[0, "validation_status"]

    def test_debugging_and_optimization_capabilities(self, duckdb_engine: DuckDBEngine):
        """Test debugging and optimization capabilities."""

        @python_table_udf(
            output_schema={
                "operation_id": "INTEGER",
                "processing_time_ms": "DOUBLE",
                "optimization_applied": "VARCHAR",
                "result": "DOUBLE",
            }
        )
        def debug_optimization_udf(df: pd.DataFrame) -> pd.DataFrame:
            """UDF with debugging and optimization tracking."""
            import time

            results = []

            for idx, row in df.iterrows():
                start_time = time.time()

                value = row.get("value", 0)

                # Simulate different optimization paths
                if value > 100:
                    # Complex computation path
                    result_value = np.log(value) * np.sqrt(value) + np.sin(value / 10)
                    optimization = "complex_math"
                elif value > 10:
                    # Simple computation path
                    result_value = value * 2 + 5
                    optimization = "simple_math"
                else:
                    # Minimal computation path
                    result_value = value
                    optimization = "passthrough"

                processing_time = (time.time() - start_time) * 1000

                results.append(
                    {
                        "operation_id": int(idx) + 1,
                        "processing_time_ms": processing_time,
                        "optimization_applied": optimization,
                        "result": result_value,
                    }
                )

            return pd.DataFrame(results)

        # Create test data with different value ranges
        debug_data = pd.DataFrame(
            {
                "id": range(1, 21),
                "value": [
                    5,
                    15,
                    150,
                    8,
                    25,
                    200,
                    3,
                    50,
                    300,
                    1,
                    12,
                    175,
                    6,
                    35,
                    250,
                    9,
                    45,
                    180,
                    4,
                    60,
                ],
            }
        )

        result = debug_optimization_udf(debug_data)

        # Validate debugging output
        assert len(result) == 20
        assert "processing_time_ms" in result.columns
        assert "optimization_applied" in result.columns

        # Check that different optimization paths were used
        optimizations = result["optimization_applied"].unique()
        assert "passthrough" in optimizations
        assert "simple_math" in optimizations
        assert "complex_math" in optimizations

    def test_null_and_empty_data_handling(self, duckdb_engine: DuckDBEngine):
        """Test handling of NULL values and empty datasets."""

        @python_table_udf(
            output_schema={
                "id": "INTEGER",
                "processed": "VARCHAR",
                "is_valid": "BOOLEAN",
            }
        )
        def null_safe_udf(df: pd.DataFrame) -> pd.DataFrame:
            """UDF that safely handles NULL and empty data."""
            if df.empty:
                return pd.DataFrame(
                    {
                        "id": [],
                        "processed": [],
                        "is_valid": [],
                    }
                )

            result = df.copy()
            result["processed"] = result.get("text", pd.Series(dtype=str)).fillna(
                "NULL_VALUE"
            )
            result["is_valid"] = ~result.get("text", pd.Series(dtype=str)).isna()

            return result[["id", "processed", "is_valid"]]

        # Test with NULL values
        null_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "text": ["hello", None, "world", None],
            }
        )

        result = null_safe_udf(null_data)

        assert len(result) == 4
        assert result.loc[1, "processed"] == "NULL_VALUE"
        assert result.loc[1, "is_valid"] == False
        assert result.loc[0, "is_valid"] == True

        # Test with empty dataset
        empty_data = pd.DataFrame({"id": [], "text": []})

        result = null_safe_udf(empty_data)

        assert len(result) == 0
        assert list(result.columns) == ["id", "processed", "is_valid"]
