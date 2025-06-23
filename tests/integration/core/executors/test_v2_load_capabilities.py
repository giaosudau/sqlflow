"""V2 Load Capabilities Integration Tests.

This test suite validates that the V2 LoadStepHandler capabilities
work correctly with real data and real operations. These tests
focus on core loading functionality, different modes, and basic
performance characteristics.

No mocking - all tests use real components and real data.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TestV2LoadCapabilities:
    """Test V2 load capabilities with real data and operations."""

    @pytest.fixture
    def load_test_data(self):
        """Create test data for load capability testing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Products data
            products = pd.DataFrame(
                {
                    "product_id": [1, 2, 3, 4, 5],
                    "name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"],
                    "price": [999.99, 29.99, 89.99, 299.99, 199.99],
                    "category": [
                        "Electronics",
                        "Accessories",
                        "Accessories",
                        "Electronics",
                        "Accessories",
                    ],
                }
            )
            products_file = tmp_path / "products.csv"
            products.to_csv(products_file, index=False)

            # Additional products for append testing
            more_products = pd.DataFrame(
                {
                    "product_id": [6, 7],
                    "name": ["Tablet", "Phone"],
                    "price": [599.99, 899.99],
                    "category": ["Electronics", "Electronics"],
                }
            )
            more_products_file = tmp_path / "more_products.csv"
            more_products.to_csv(more_products_file, index=False)

            yield {
                "products_file": products_file,
                "more_products_file": more_products_file,
                "products_data": products,
                "more_products_data": more_products,
            }

    def test_v2_load_replace_mode_basic_functionality(
        self, load_test_data, v2_pipeline_runner
    ):
        """Test V2 REPLACE mode loads data correctly."""
        products_file = load_test_data["products_file"]

        pipeline = [
            {
                "type": "load",
                "id": "v2_replace_test",
                "source": str(products_file),
                "target_table": "products_v2",
                "mode": "REPLACE",
            }
        ]
        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        # Should succeed
        assert result.success is True
        assert len(result.step_results) == 1

        # Verify step executed successfully
        step_result = result.step_results[0]
        assert step_result.success is True
        assert step_result.step_id == "v2_replace_test"
        assert step_result.rows_affected > 0

        logger.info(
            f"V2 REPLACE mode loaded {step_result.rows_affected} products successfully"
        )

    def test_v2_load_append_mode_adds_data(self, load_test_data, v2_pipeline_runner):
        """Test V2 APPEND mode adds data to existing table."""
        products_file = load_test_data["products_file"]
        more_products_file = load_test_data["more_products_file"]

        pipeline = [
            {
                "type": "load",
                "id": "v2_initial_load",
                "source": str(products_file),
                "target_table": "products_append_test",
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "v2_append_load",
                "source": str(more_products_file),
                "target_table": "products_append_test",
                "load_mode": "append",
            },
        ]
        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        # Pipeline should succeed
        assert result.success is True
        assert len(result.step_results) == 2

        # Verify both steps executed successfully
        step_results = result.step_results
        for step_result in step_results:
            assert step_result.success is True
            assert step_result.rows_affected > 0

        # Verify the steps processed the expected number of rows
        initial_rows = step_results[0].rows_affected
        appended_rows = step_results[1].rows_affected
        assert initial_rows == 5
        assert appended_rows == 2

        logger.info(
            f"V2 APPEND mode: {initial_rows} initial + {appended_rows} appended = {initial_rows + appended_rows} total"
        )

    def test_v2_load_multiple_tables_isolation(
        self, load_test_data, v2_pipeline_runner
    ):
        """Test V2 loading multiple tables doesn't interfere with each other."""
        products_file = load_test_data["products_file"]
        more_products_file = load_test_data["more_products_file"]

        pipeline = [
            {
                "type": "load",
                "id": "v2_table1_load",
                "source": str(products_file),
                "target_table": "products_table1",
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "v2_table2_load",
                "source": str(more_products_file),
                "target_table": "products_table2",
                "load_mode": "replace",
            },
        ]
        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        # Pipeline should succeed
        assert result.success is True
        assert len(result.step_results) == 2

        # Verify both steps executed successfully
        step_results = result.step_results
        for step_result in step_results:
            assert step_result.success is True
            assert step_result.rows_affected > 0

        table1_rows = step_results[0].rows_affected
        table2_rows = step_results[1].rows_affected
        assert table1_rows == 5
        assert table2_rows == 2

        logger.info(
            f"V2 table isolation: table1={table1_rows} rows, table2={table2_rows} rows"
        )

    def test_v2_load_with_different_file_types(
        self, load_test_data, v2_pipeline_runner
    ):
        """Test V2 loading with different file formats (if supported)."""
        products_file = load_test_data["products_file"]

        pipeline = [
            {
                "type": "load",
                "id": "v2_csv_test",
                "source": str(products_file),
                "target_table": "csv_test_table",
                "mode": "REPLACE",
            }
        ]
        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        assert result.success is True

        # Verify CSV data loaded
        engine = coordinator.context.engine
        count_result = engine.execute_query("SELECT COUNT(*) FROM csv_test_table").df()
        count = count_result.iloc[0, 0]
        assert count == 5, f"Expected 5 rows, got {count}"

        step_result = result.step_results[0]
        assert step_result.rows_affected == 5

    def test_v2_load_performance_reasonable(self, load_test_data, v2_pipeline_runner):
        """Test that V2 load performance is reasonable."""
        products_file = load_test_data["products_file"]
        import time

        pipeline = [
            {
                "type": "load",
                "id": "v2_performance_test",
                "source": str(products_file),
                "target_table": "performance_test_table",
                "mode": "REPLACE",
            }
        ]

        start_time = time.time()
        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result
        end_time = time.time()

        assert result.success is True
        duration = end_time - start_time
        assert duration < 2  # Should be very fast

        step_result = result.step_results[0]
        assert step_result.rows_affected == 5
        logger.info(f"V2 load performance test took {duration:.2f}s")

    def test_v2_load_error_handling(self, load_test_data, v2_pipeline_runner):
        """Test V2 load error handling with invalid inputs."""
        nonexistent_file = load_test_data["products_file"].parent / "does_not_exist.csv"
        pipeline = [
            {
                "type": "load",
                "id": "v2_error_test",
                "source": str(nonexistent_file),
                "target_table": "error_test_table",
                "mode": "REPLACE",
            }
        ]

        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        assert result.success is False
        assert len(result.step_results) == 1
        step_result = result.step_results[0]
        assert step_result.success is False
        assert "No such file or directory" in step_result.error_message

    def test_v2_load_step_id_tracking(self, load_test_data, v2_pipeline_runner):
        """Test that V2 properly tracks step IDs throughout execution."""
        products_file = load_test_data["products_file"]
        custom_step_id = "custom_product_load_step_123"
        pipeline = [
            {
                "type": "load",
                "id": custom_step_id,
                "source": str(products_file),
                "target_table": "step_id_test_table",
                "mode": "REPLACE",
            }
        ]
        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        assert result.success is True
        assert len(result.step_results) == 1
        step_result = result.step_results[0]
        assert step_result.step_id == custom_step_id

    def test_v2_load_consecutive_operations(self, load_test_data, v2_pipeline_runner):
        """Test V2 consecutive load operations maintain proper state."""
        products_file = load_test_data["products_file"]
        more_products_file = load_test_data["more_products_file"]

        pipeline = [
            {
                "type": "load",
                "id": "consecutive_op_1",
                "source": str(products_file),
                "target_table": "consecutive_table1",
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "consecutive_op_2",
                "source": str(more_products_file),
                "target_table": "consecutive_table2",
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "consecutive_op_3",
                "source": str(products_file),
                "target_table": "consecutive_table1",  # Overwrite first table
                "load_mode": "replace",
            },
        ]

        coordinator = v2_pipeline_runner(pipeline)
        result = coordinator.result

        assert result.success is True
        assert len(result.step_results) == 3
        for step_result in result.step_results:
            assert step_result.success is True

        engine = coordinator.context.engine
        count1 = (
            engine.execute_query("SELECT COUNT(*) FROM consecutive_table1")
            .df()
            .iloc[0, 0]
        )
        count2 = (
            engine.execute_query("SELECT COUNT(*) FROM consecutive_table2")
            .df()
            .iloc[0, 0]
        )
        assert count1 == 5
        assert count2 == 2
        logger.info(
            f"V2 consecutive operations test passed. Final counts: table1={count1}, table2={count2}"
        )
