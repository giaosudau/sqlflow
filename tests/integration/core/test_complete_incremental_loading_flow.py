"""Integration tests for complete incremental loading flow.

Tests the end-to-end behavior of automatic watermark-based incremental loading
from SOURCE definition through LOAD execution with real connectors and state management.
"""

import os
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest

from sqlflow.connectors.connector_engine import ConnectorEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.state.backends import DuckDBStateBackend
from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.parser.ast import LoadStep


class TestCompleteIncrementalLoadingFlow:
    """Test end-to-end incremental loading behavior."""

    @pytest.fixture
    def sample_orders_data(self):
        """Create sample orders data for incremental loading."""
        return pd.DataFrame(
            {
                "order_id": [1001, 1002, 1003, 1004, 1005],
                "customer_id": [101, 102, 103, 104, 105],
                "product_name": [
                    "Widget A",
                    "Widget B",
                    "Widget C",
                    "Widget D",
                    "Widget E",
                ],
                "amount": [25.00, 50.00, 75.00, 100.00, 125.00],
                "updated_at": [
                    "2024-01-15 09:00:00",
                    "2024-01-15 10:30:00",
                    "2024-01-15 12:15:00",
                    "2024-01-16 10:00:00",
                    "2024-01-16 11:00:00",
                ],
                "status": ["shipped", "processing", "pending", "shipped", "pending"],
            }
        )

    @pytest.fixture
    def temp_csv_file(self, sample_orders_data):
        """Create temporary CSV file with orders data."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            sample_orders_data.to_csv(f, index=False)
            temp_path = f.name

        yield temp_path

        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def executor_with_state_management(self):
        """Create executor with real state management components."""
        executor = LocalExecutor()

        # Initialize real state management with in-memory DuckDB
        state_backend = DuckDBStateBackend(executor.duckdb_engine.connection)
        executor.watermark_manager = WatermarkManager(state_backend)
        executor.connector_engine = ConnectorEngine()

        return executor

    def test_initial_incremental_load_processes_all_data(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that initial incremental load processes all data and establishes watermark."""
        # Define incremental SOURCE
        source_step = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "primary_key": ["order_id"],
            "params": {"path": temp_csv_file, "has_header": True},
        }

        # Execute SOURCE definition
        result = executor_with_state_management._execute_source_definition(source_step)
        assert result["status"] == "success"

        # Verify data was stored for LOAD operation
        assert "orders" in executor_with_state_management.table_data
        data_chunk = executor_with_state_management.table_data["orders"]
        assert len(data_chunk) == 5  # All records processed

        # Verify watermark was established
        watermark = (
            executor_with_state_management.watermark_manager.get_source_watermark(
                pipeline="default_pipeline", source="orders", cursor_field="updated_at"
            )
        )
        assert watermark == "2024-01-16 11:00:00"  # Latest timestamp

    def test_subsequent_incremental_load_filters_by_watermark(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that subsequent incremental loads filter data based on watermarks."""
        # First, establish a watermark
        executor_with_state_management.watermark_manager.update_source_watermark(
            pipeline="default_pipeline",
            source="orders",
            cursor_field="updated_at",
            value="2024-01-15 11:00:00",
        )

        # Create SOURCE step with incremental mode
        source_step = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        # Execute incremental SOURCE
        result = executor_with_state_management._execute_source_definition(source_step)
        assert result["status"] == "success"

        # Verify only records after watermark were processed
        data_chunk = executor_with_state_management.table_data["orders"]
        df = data_chunk.pandas_df
        assert len(df) == 3  # Only records after 2024-01-15 11:00:00

        # Verify all returned records are after watermark
        for timestamp in df["updated_at"]:
            assert pd.to_datetime(timestamp) > pd.to_datetime("2024-01-15 11:00:00")

    def test_incremental_load_updates_watermark_after_success(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that watermark is updated after successful incremental load."""
        # Set initial watermark
        initial_watermark = "2024-01-15 10:00:00"
        executor_with_state_management.watermark_manager.update_source_watermark(
            pipeline="default_pipeline",
            source="orders",
            cursor_field="updated_at",
            value=initial_watermark,
        )

        # Execute incremental SOURCE
        source_step = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        result = executor_with_state_management._execute_source_definition(source_step)
        assert result["status"] == "success"

        # Verify watermark was updated to latest processed value
        updated_watermark = (
            executor_with_state_management.watermark_manager.get_source_watermark(
                pipeline="default_pipeline", source="orders", cursor_field="updated_at"
            )
        )

        assert updated_watermark == "2024-01-16 11:00:00"  # Latest timestamp in data
        assert updated_watermark != initial_watermark

    def test_full_refresh_mode_ignores_watermarks(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that full_refresh mode ignores existing watermarks."""
        # Set existing watermark
        executor_with_state_management.watermark_manager.update_source_watermark(
            pipeline="default_pipeline",
            source="orders",
            cursor_field="updated_at",
            value="2024-01-16 10:30:00",
        )

        # Execute SOURCE with full_refresh mode
        source_step = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "full_refresh",  # Should ignore watermarks
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        result = executor_with_state_management._execute_source_definition(source_step)
        assert result["status"] == "success"

        # Verify all data was processed regardless of watermark
        data_chunk = executor_with_state_management.table_data["orders"]
        assert len(data_chunk) == 5  # All records processed

    def test_load_step_works_with_incremental_source_data(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that LOAD step correctly processes incrementally loaded SOURCE data."""
        # First, execute incremental SOURCE
        source_step = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        source_result = executor_with_state_management._execute_source_definition(
            source_step
        )
        assert source_result["status"] == "success"

        # Then execute LOAD step
        load_step = LoadStep(
            table_name="orders_table", source_name="orders", mode="REPLACE"
        )

        load_result = executor_with_state_management.execute_load_step(load_step)
        assert load_result["status"] == "success"

        # Verify table was created in DuckDB
        assert executor_with_state_management.duckdb_engine.table_exists("orders_table")

        # Verify correct number of rows were loaded
        row_count = executor_with_state_management._get_table_row_count("orders_table")
        assert row_count == 5  # All orders for initial load

    def test_error_in_incremental_loading_preserves_watermark_state(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that errors during incremental loading don't corrupt watermark state."""
        # Set initial watermark
        initial_watermark = "2024-01-15 10:00:00"
        executor_with_state_management.watermark_manager.update_source_watermark(
            pipeline="default_pipeline",
            source="orders",
            cursor_field="updated_at",
            value=initial_watermark,
        )

        # Mock connector to raise error during read_incremental
        with patch(
            "sqlflow.connectors.csv_connector.CSVConnector.read_incremental"
        ) as mock_read:
            mock_read.side_effect = Exception("Simulated read error")

            source_step = {
                "id": "source_orders",
                "name": "orders",
                "type": "source_definition",
                "connector_type": "CSV",
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
                "params": {"path": temp_csv_file, "has_header": True},
            }

            result = executor_with_state_management._execute_source_definition(
                source_step
            )
            assert result["status"] == "error"

            # Verify watermark was not modified
            preserved_watermark = (
                executor_with_state_management.watermark_manager.get_source_watermark(
                    pipeline="default_pipeline",
                    source="orders",
                    cursor_field="updated_at",
                )
            )
            assert preserved_watermark == initial_watermark

    def test_incremental_loading_performance_improvement(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that incremental loading shows performance improvement over full refresh."""
        # Measure full refresh performance
        source_step_full = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "full_refresh",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        executor_with_state_management._execute_source_definition(source_step_full)
        full_refresh_rows = len(executor_with_state_management.table_data["orders"])

        # Set watermark to filter most data
        executor_with_state_management.watermark_manager.update_source_watermark(
            pipeline="default_pipeline",
            source="orders",
            cursor_field="updated_at",
            value="2024-01-16 10:30:00",
        )

        # Measure incremental performance
        source_step_inc = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        executor_with_state_management._execute_source_definition(source_step_inc)
        incremental_rows = len(executor_with_state_management.table_data["orders"])

        # Verify incremental processed fewer rows (performance improvement)
        assert incremental_rows < full_refresh_rows
        assert incremental_rows == 1  # Only 1 record after 2024-01-16 10:30:00
        assert full_refresh_rows == 5  # All records

    def test_multiple_sources_maintain_separate_watermarks(
        self, executor_with_state_management, temp_csv_file
    ):
        """Test that multiple sources maintain separate watermark state."""
        # Execute two different sources with incremental loading
        source1_step = {
            "id": "source_orders",
            "name": "orders",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        source2_step = {
            "id": "source_products",
            "name": "products",
            "type": "source_definition",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "params": {"path": temp_csv_file, "has_header": True},
        }

        # Execute both sources
        result1 = executor_with_state_management._execute_source_definition(
            source1_step
        )
        result2 = executor_with_state_management._execute_source_definition(
            source2_step
        )

        assert result1["status"] == "success"
        assert result2["status"] == "success"

        # Verify separate watermarks were established
        watermark1 = (
            executor_with_state_management.watermark_manager.get_source_watermark(
                pipeline="default_pipeline", source="orders", cursor_field="updated_at"
            )
        )

        watermark2 = (
            executor_with_state_management.watermark_manager.get_source_watermark(
                pipeline="default_pipeline",
                source="products",
                cursor_field="updated_at",
            )
        )

        assert watermark1 is not None
        assert watermark2 is not None
        # Both should have same value since same data, but stored separately
        assert watermark1 == watermark2 == "2024-01-16 11:00:00"
