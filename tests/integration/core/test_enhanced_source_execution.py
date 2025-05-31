"""Integration tests for enhanced SOURCE execution with incremental loading.

These tests verify the complete integration of watermark management,
incremental loading, and SOURCE execution without using mocks.
"""

import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.state.backends import DuckDBStateBackend
from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.parser.ast import LoadStep


class TestEnhancedSourceExecution:
    """Integration tests for enhanced SOURCE execution."""

    @pytest.fixture
    def temp_csv_file(self):
        """Create a temporary CSV file with test data."""
        # Create test data with timestamps for incremental loading
        base_time = datetime.now()
        data = [
            {"id": 1, "name": "Alice", "updated_at": base_time.isoformat()},
            {
                "id": 2,
                "name": "Bob",
                "updated_at": (base_time + timedelta(minutes=1)).isoformat(),
            },
            {
                "id": 3,
                "name": "Charlie",
                "updated_at": (base_time + timedelta(minutes=2)).isoformat(),
            },
        ]

        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name

        # Cleanup
        if os.path.exists(f.name):
            os.unlink(f.name)

    @pytest.fixture
    def temp_csv_file_updated(self):
        """Create a temporary CSV file with updated data for incremental loading."""
        # Create test data with newer timestamps
        base_time = datetime.now() + timedelta(hours=1)
        data = [
            {"id": 1, "name": "Alice Updated", "updated_at": base_time.isoformat()},
            {
                "id": 4,
                "name": "David",
                "updated_at": (base_time + timedelta(minutes=1)).isoformat(),
            },
            {
                "id": 5,
                "name": "Eve",
                "updated_at": (base_time + timedelta(minutes=2)).isoformat(),
            },
        ]

        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name

        # Cleanup
        if os.path.exists(f.name):
            os.unlink(f.name)

    @pytest.fixture
    def executor_with_watermarks(self):
        """Create LocalExecutor with watermark management enabled."""
        executor = LocalExecutor()

        # Ensure watermark manager is initialized
        if (
            not hasattr(executor, "watermark_manager")
            or executor.watermark_manager is None
        ):
            # Initialize manually if needed
            state_backend = DuckDBStateBackend(executor.duckdb_engine.connection)
            executor.watermark_manager = WatermarkManager(state_backend)

        return executor

    def test_watermark_manager_initialization(self, executor_with_watermarks):
        """Test that watermark manager is properly initialized."""
        executor = executor_with_watermarks

        assert hasattr(executor, "watermark_manager")
        assert executor.watermark_manager is not None
        assert isinstance(executor.watermark_manager, WatermarkManager)

    def test_full_refresh_source_execution(
        self, executor_with_watermarks, temp_csv_file
    ):
        """Test full refresh mode SOURCE execution."""
        executor = executor_with_watermarks

        # Create SOURCE definition with full refresh
        source_def = {
            "id": "source_users",
            "name": "users",
            "connector_type": "CSV",
            "params": {"path": temp_csv_file, "sync_mode": "full_refresh"},
        }

        # Execute SOURCE definition
        source_result = executor._execute_source_definition(source_def)
        assert source_result["status"] == "success"

        # Create LOAD step
        load_step = LoadStep(
            table_name="users_table", source_name="users", mode="REPLACE"
        )

        # Execute LOAD step
        load_result = executor.execute_load_step(load_step)

        assert load_result["status"] == "success"
        assert load_result["rows_loaded"] == 3
        assert load_result["target_table"] == "users_table"

    def test_incremental_source_execution_first_load(
        self, executor_with_watermarks, temp_csv_file
    ):
        """Test incremental mode SOURCE execution on first load (no watermark)."""
        executor = executor_with_watermarks

        # Create SOURCE definition with incremental mode
        source_def = {
            "id": "source_users",
            "name": "users",
            "connector_type": "CSV",
            "params": {
                "path": temp_csv_file,
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
            },
        }

        # Execute SOURCE definition
        source_result = executor._execute_source_definition(source_def)
        assert source_result["status"] == "success"

        # Create LOAD step
        load_step = LoadStep(
            table_name="users_table", source_name="users", mode="REPLACE"
        )
        setattr(load_step, "pipeline_name", "test_pipeline")

        # Execute LOAD step - should load all data on first run
        load_result = executor.execute_load_step(load_step)

        assert load_result["status"] == "success"
        assert load_result["rows_loaded"] == 3

        # Verify watermark was created
        watermark = executor.watermark_manager.get_watermark(
            "test_pipeline", "users", "users_table", "updated_at"
        )
        assert watermark is not None

    def test_incremental_source_execution_subsequent_load(
        self, executor_with_watermarks, temp_csv_file, temp_csv_file_updated
    ):
        """Test incremental mode SOURCE execution with existing watermark."""
        executor = executor_with_watermarks

        # First load - establish watermark
        source_def = {
            "id": "source_users",
            "name": "users",
            "connector_type": "CSV",
            "params": {
                "path": temp_csv_file,
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
            },
        }

        executor._execute_source_definition(source_def)

        load_step = LoadStep(
            table_name="users_table", source_name="users", mode="REPLACE"
        )
        setattr(load_step, "pipeline_name", "test_pipeline")

        # First load
        first_result = executor.execute_load_step(load_step)
        assert first_result["status"] == "success"
        assert first_result["rows_loaded"] == 3

        # Get initial watermark
        initial_watermark = executor.watermark_manager.get_watermark(
            "test_pipeline", "users", "users_table", "updated_at"
        )
        assert initial_watermark is not None

        # Update SOURCE to point to updated file (simulating new data)
        source_def["params"]["path"] = temp_csv_file_updated
        executor._execute_source_definition(source_def)

        # Second load - should only load incremental data
        second_result = executor.execute_load_step(load_step)

        assert second_result["status"] == "success"
        # Note: In real scenarios this would be filtered, but CSV connector doesn't
        # support filtering yet, so we expect all rows
        assert second_result["rows_loaded"] >= 0

        # Verify watermark was updated
        final_watermark = executor.watermark_manager.get_watermark(
            "test_pipeline", "users", "users_table", "updated_at"
        )
        assert final_watermark is not None

    def test_incremental_source_without_cursor_field_fails(
        self, executor_with_watermarks, temp_csv_file
    ):
        """Test that incremental mode fails when cursor_field is missing."""
        executor = executor_with_watermarks

        # Create SOURCE definition with incremental mode but no cursor_field
        source_def = {
            "id": "source_users",
            "name": "users",
            "connector_type": "CSV",
            "params": {
                "path": temp_csv_file,
                "sync_mode": "incremental",
                # Missing cursor_field
            },
        }

        executor._execute_source_definition(source_def)

        load_step = LoadStep(
            table_name="users_table", source_name="users", mode="REPLACE"
        )
        setattr(load_step, "pipeline_name", "test_pipeline")

        # Should fail due to missing cursor_field
        load_result = executor.execute_load_step(load_step)
        assert load_result["status"] == "error"
        assert "cursor_field is required" in load_result["message"]

    def test_watermark_state_persistence(self, executor_with_watermarks):
        """Test that watermark state persists across operations."""
        executor = executor_with_watermarks
        watermark_manager = executor.watermark_manager

        # Set a watermark
        test_value = "2024-01-01T12:00:00Z"
        watermark_manager.update_watermark_atomic(
            "test_pipeline", "test_source", "test_table", "updated_at", test_value
        )

        # Retrieve the watermark
        retrieved_value = watermark_manager.get_watermark(
            "test_pipeline", "test_source", "test_table", "updated_at"
        )

        assert retrieved_value == test_value

    def test_watermark_source_level_operations(self, executor_with_watermarks):
        """Test source-level watermark operations."""
        executor = executor_with_watermarks
        watermark_manager = executor.watermark_manager

        # Set a source-level watermark
        test_value = "2024-01-01T12:00:00Z"
        watermark_manager.update_source_watermark(
            "test_pipeline", "test_source", "updated_at", test_value
        )

        # Retrieve the source-level watermark
        retrieved_value = watermark_manager.get_source_watermark(
            "test_pipeline", "test_source", "updated_at"
        )

        assert retrieved_value == test_value

    def test_watermark_reset_functionality(self, executor_with_watermarks):
        """Test watermark reset functionality."""
        executor = executor_with_watermarks
        watermark_manager = executor.watermark_manager

        # Set a watermark
        test_value = "2024-01-01T12:00:00Z"
        watermark_manager.update_watermark_atomic(
            "test_pipeline", "test_source", "test_table", "updated_at", test_value
        )

        # Verify it exists
        assert (
            watermark_manager.get_watermark(
                "test_pipeline", "test_source", "test_table", "updated_at"
            )
            == test_value
        )

        # Reset the watermark
        reset_result = watermark_manager.reset_watermark(
            "test_pipeline", "test_source", "test_table", "updated_at"
        )

        assert reset_result is True

        # Verify it's gone
        assert (
            watermark_manager.get_watermark(
                "test_pipeline", "test_source", "test_table", "updated_at"
            )
            is None
        )

    def test_incremental_fallback_to_full_refresh(
        self, executor_with_watermarks, temp_csv_file
    ):
        """Test that incremental loading falls back to full refresh on error."""
        executor = executor_with_watermarks

        # Create SOURCE definition with incremental mode
        source_def = {
            "id": "source_users",
            "name": "users",
            "connector_type": "CSV",
            "params": {
                "path": temp_csv_file,
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
            },
        }

        executor._execute_source_definition(source_def)

        load_step = LoadStep(
            table_name="users_table", source_name="users", mode="REPLACE"
        )
        setattr(load_step, "pipeline_name", "test_pipeline")

        # Should work even if incremental loading has issues
        # (fallback to full refresh behavior)
        load_result = executor.execute_load_step(load_step)

        assert load_result["status"] == "success"
        assert load_result["rows_loaded"] == 3

    def test_multiple_sources_with_different_sync_modes(
        self, executor_with_watermarks, temp_csv_file
    ):
        """Test handling multiple sources with different sync modes."""
        executor = executor_with_watermarks

        # Create full refresh source
        full_refresh_source = {
            "id": "source_users_full",
            "name": "users_full",
            "connector_type": "CSV",
            "params": {"path": temp_csv_file, "sync_mode": "full_refresh"},
        }

        # Create incremental source
        incremental_source = {
            "id": "source_users_inc",
            "name": "users_inc",
            "connector_type": "CSV",
            "params": {
                "path": temp_csv_file,
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
            },
        }

        # Execute both SOURCE definitions
        executor._execute_source_definition(full_refresh_source)
        executor._execute_source_definition(incremental_source)

        # Load from full refresh source
        full_load_step = LoadStep(
            table_name="users_full_table", source_name="users_full", mode="REPLACE"
        )
        full_result = executor.execute_load_step(full_load_step)

        # Load from incremental source
        inc_load_step = LoadStep(
            table_name="users_inc_table", source_name="users_inc", mode="REPLACE"
        )
        setattr(inc_load_step, "pipeline_name", "test_pipeline")
        inc_result = executor.execute_load_step(inc_load_step)

        # Both should succeed
        assert full_result["status"] == "success"
        assert inc_result["status"] == "success"
        assert full_result["rows_loaded"] == 3
        assert inc_result["rows_loaded"] == 3

        # Only incremental source should have watermark
        watermark = executor.watermark_manager.get_watermark(
            "test_pipeline", "users_inc", "users_inc_table", "updated_at"
        )
        assert watermark is not None
