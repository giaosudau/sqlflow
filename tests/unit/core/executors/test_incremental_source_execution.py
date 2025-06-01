"""Unit tests for incremental SOURCE execution in LocalExecutor.

Tests the behavior of automatic watermark-based incremental loading
when SOURCE definitions specify sync_mode='incremental'.
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.state.watermark_manager import WatermarkManager


class TestIncrementalSourceExecution:
    """Test automatic incremental loading behavior in SOURCE execution."""

    @pytest.fixture
    def mock_watermark_manager(self):
        """Create a mock watermark manager."""
        manager = Mock(spec=WatermarkManager)
        manager.get_source_watermark.return_value = None
        manager.update_source_watermark.return_value = None
        return manager

    @pytest.fixture
    def executor_with_watermarks(self, mock_watermark_manager):
        """Create executor with watermark manager."""
        executor = LocalExecutor()
        executor.watermark_manager = mock_watermark_manager
        setattr(executor, "pipeline_name", "test_pipeline")
        return executor

    def test_incremental_source_validates_cursor_field_requirement(
        self, executor_with_watermarks
    ):
        """Test that incremental SOURCE requires cursor_field parameter."""
        step = {
            "id": "source_orders",
            "name": "orders",
            "connector_type": "CSV",
            "sync_mode": "incremental",
            # Missing cursor_field - should raise error
            "params": {"path": "/data/orders.csv"},
        }

        # The actual implementation raises ValueError for missing cursor_field
        with pytest.raises(ValueError) as exc_info:
            executor_with_watermarks._execute_incremental_source_definition(step)

        assert "cursor_field" in str(exc_info.value)
        assert "requires" in str(exc_info.value)

    def test_incremental_source_retrieves_watermark_value(
        self, executor_with_watermarks
    ):
        """Test that incremental SOURCE retrieves last watermark value."""
        mock_connector = Mock()
        mock_connector.supports_incremental.return_value = True
        mock_connector.read_incremental.return_value = [DataChunk(pd.DataFrame())]
        mock_connector.get_cursor_value.return_value = None

        with patch.object(
            executor_with_watermarks,
            "_get_incremental_connector_instance",
            return_value=mock_connector,
        ):
            with patch.object(
                executor_with_watermarks,
                "_extract_object_name_from_step",
                return_value="orders.csv",
            ):
                step = {
                    "id": "source_orders",
                    "name": "orders",
                    "connector_type": "CSV",
                    "sync_mode": "incremental",
                    "cursor_field": "updated_at",
                    "params": {"path": "/data/orders.csv"},
                }

                executor_with_watermarks._execute_incremental_source_definition(step)

                # Verify watermark retrieval was called
                executor_with_watermarks.watermark_manager.get_source_watermark.assert_called_once_with(
                    pipeline="test_pipeline", source="orders", cursor_field="updated_at"
                )

    def test_incremental_source_calls_read_incremental_with_watermark(
        self, executor_with_watermarks
    ):
        """Test that incremental SOURCE calls read_incremental with watermark value."""
        mock_connector = Mock()
        mock_connector.supports_incremental.return_value = True
        mock_connector.read_incremental.return_value = [
            DataChunk(pd.DataFrame({"id": [1], "updated_at": ["2024-01-01"]}))
        ]
        mock_connector.get_cursor_value.return_value = "2024-01-01"

        # Set up watermark manager to return a previous watermark
        executor_with_watermarks.watermark_manager.get_source_watermark.return_value = (
            "2024-01-01 10:00:00"
        )

        with patch.object(
            executor_with_watermarks,
            "_get_incremental_connector_instance",
            return_value=mock_connector,
        ):
            with patch.object(
                executor_with_watermarks,
                "_extract_object_name_from_step",
                return_value="orders.csv",
            ):
                step = {
                    "id": "source_orders",
                    "name": "orders",
                    "connector_type": "CSV",
                    "sync_mode": "incremental",
                    "cursor_field": "updated_at",
                    "params": {"path": "/data/orders.csv"},
                }

                result = (
                    executor_with_watermarks._execute_incremental_source_definition(
                        step
                    )
                )

                # Verify read_incremental was called with watermark value
                mock_connector.read_incremental.assert_called_once_with(
                    object_name="orders.csv",
                    cursor_field="updated_at",
                    cursor_value="2024-01-01 10:00:00",
                    batch_size=10000,
                )

                # Verify the result structure (actual implementation doesn't return connector_instance)
                assert result["status"] == "success"
                assert result["sync_mode"] == "incremental"
                assert result["source_name"] == "orders"
                assert "rows_processed" in result

    def test_incremental_source_updates_watermark_after_successful_read(
        self, executor_with_watermarks
    ):
        """Test that watermark is updated after successful incremental read."""
        test_data = pd.DataFrame(
            {"id": [1, 2, 3], "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"]}
        )

        mock_connector = Mock()
        mock_connector.supports_incremental.return_value = True
        mock_connector.read_incremental.return_value = [DataChunk(test_data)]
        mock_connector.get_cursor_value.return_value = "2024-01-03"  # Latest value

        executor_with_watermarks.watermark_manager.get_source_watermark.return_value = (
            "2024-01-01"
        )

        with patch.object(
            executor_with_watermarks,
            "_get_incremental_connector_instance",
            return_value=mock_connector,
        ):
            with patch.object(
                executor_with_watermarks,
                "_extract_object_name_from_step",
                return_value="orders.csv",
            ):
                step = {
                    "id": "source_orders",
                    "name": "orders",
                    "connector_type": "CSV",
                    "sync_mode": "incremental",
                    "cursor_field": "updated_at",
                    "params": {"path": "/data/orders.csv"},
                }

                result = (
                    executor_with_watermarks._execute_incremental_source_definition(
                        step
                    )
                )

                # Verify watermark update was called with new value
                executor_with_watermarks.watermark_manager.update_source_watermark.assert_called_once_with(
                    pipeline="test_pipeline",
                    source="orders",
                    cursor_field="updated_at",
                    value="2024-01-03",
                )

                assert result["status"] == "success"
                assert result["previous_watermark"] == "2024-01-01"
                assert result["new_watermark"] == "2024-01-03"
                assert result["rows_processed"] == 3

    def test_incremental_source_fallback_when_connector_unsupported(
        self, executor_with_watermarks
    ):
        """Test fallback to traditional source when connector doesn't support incremental."""
        mock_connector = Mock()
        mock_connector.supports_incremental.return_value = False

        with patch.object(
            executor_with_watermarks,
            "_get_incremental_connector_instance",
            return_value=mock_connector,
        ):
            with patch.object(
                executor_with_watermarks,
                "_handle_traditional_source",
                return_value={"status": "success", "fallback": True},
            ) as mock_traditional:
                step = {
                    "id": "source_orders",
                    "name": "orders",
                    "connector_type": "CSV",
                    "sync_mode": "incremental",
                    "cursor_field": "updated_at",
                    "params": {"path": "/data/orders.csv"},
                }

                result = (
                    executor_with_watermarks._execute_incremental_source_definition(
                        step
                    )
                )

                # Verify fallback was called
                mock_traditional.assert_called_once_with(
                    step, "source_orders", "orders"
                )
                assert result["fallback"] is True

    def test_incremental_source_error_handling_preserves_watermarks(
        self, executor_with_watermarks
    ):
        """Test that watermarks are not updated when incremental reading fails."""
        mock_connector = Mock()
        mock_connector.supports_incremental.return_value = True
        mock_connector.read_incremental.side_effect = Exception("Connection failed")

        with patch.object(
            executor_with_watermarks,
            "_get_incremental_connector_instance",
            return_value=mock_connector,
        ):
            with patch.object(
                executor_with_watermarks,
                "_extract_object_name_from_step",
                return_value="orders.csv",
            ):
                step = {
                    "id": "source_orders",
                    "name": "orders",
                    "connector_type": "CSV",
                    "sync_mode": "incremental",
                    "cursor_field": "updated_at",
                    "params": {"path": "/data/orders.csv"},
                }

                result = (
                    executor_with_watermarks._execute_incremental_source_definition(
                        step
                    )
                )

                # Verify error result and no watermark update
                assert result["status"] == "error"
                assert "Connection failed" in result["error"]
                executor_with_watermarks.watermark_manager.update_source_watermark.assert_not_called()

    def test_extract_object_name_from_csv_step(self, executor_with_watermarks):
        """Test extraction of object name for CSV connector."""
        step = {"connector_type": "CSV", "params": {"path": "/data/orders.csv"}}

        result = executor_with_watermarks._extract_object_name_from_step(step)
        assert result == "/data/orders.csv"

    def test_extract_object_name_from_postgres_step(self, executor_with_watermarks):
        """Test extraction of object name for PostgreSQL connector."""
        step = {"connector_type": "POSTGRES", "params": {"table": "orders"}}

        result = executor_with_watermarks._extract_object_name_from_step(step)
        assert result == "orders"

    def test_extract_object_name_fallback_to_common_params(
        self, executor_with_watermarks
    ):
        """Test extraction falls back to common parameter names."""
        step = {"connector_type": "OTHER", "params": {"object_name": "my_object"}}

        result = executor_with_watermarks._extract_object_name_from_step(step)
        assert result == "my_object"
