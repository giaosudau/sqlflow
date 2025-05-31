"""Tests for WatermarkManager."""

from datetime import datetime
from unittest.mock import MagicMock, Mock

import pytest

from sqlflow.core.errors import ConnectorError
from sqlflow.core.state.backends import DuckDBStateBackend
from sqlflow.core.state.watermark_manager import WatermarkManager


class TestWatermarkManager:
    """Test suite for WatermarkManager functionality."""

    @pytest.fixture
    def mock_backend(self):
        """Create a mock state backend for testing."""
        backend = Mock()
        backend.get.return_value = None
        backend.set.return_value = None
        backend.delete.return_value = True
        # Properly mock context manager
        transaction_mock = MagicMock()
        transaction_mock.__enter__ = Mock(return_value=transaction_mock)
        transaction_mock.__exit__ = Mock(return_value=None)
        backend.transaction.return_value = transaction_mock
        return backend

    @pytest.fixture
    def watermark_manager(self, mock_backend):
        """Create a WatermarkManager instance for testing."""
        return WatermarkManager(mock_backend)

    def test_get_state_key(self, watermark_manager):
        """Test state key generation."""
        key = watermark_manager.get_state_key(
            "pipeline1", "source1", "target1", "updated_at"
        )
        assert key == "pipeline1.source1.target1.updated_at"

    def test_get_watermark_exists(self, watermark_manager, mock_backend):
        """Test retrieving an existing watermark."""
        mock_backend.get.return_value = "2024-01-01T00:00:00Z"

        result = watermark_manager.get_watermark(
            "pipeline1", "source1", "target1", "updated_at"
        )

        assert result == "2024-01-01T00:00:00Z"
        mock_backend.get.assert_called_once_with("pipeline1.source1.target1.updated_at")

    def test_get_watermark_not_exists(self, watermark_manager, mock_backend):
        """Test retrieving a non-existent watermark."""
        mock_backend.get.return_value = None

        result = watermark_manager.get_watermark(
            "pipeline1", "source1", "target1", "updated_at"
        )

        assert result is None
        mock_backend.get.assert_called_once_with("pipeline1.source1.target1.updated_at")

    def test_get_watermark_error(self, watermark_manager, mock_backend):
        """Test error handling when retrieving watermark."""
        mock_backend.get.side_effect = Exception("Backend error")

        with pytest.raises(ConnectorError) as exc_info:
            watermark_manager.get_watermark(
                "pipeline1", "source1", "target1", "updated_at"
            )

        assert "Failed to retrieve watermark" in str(exc_info.value)
        assert "watermark_manager" in str(exc_info.value)

    def test_get_source_watermark(self, watermark_manager, mock_backend):
        """Test source-level watermark retrieval."""
        mock_backend.get.return_value = "2024-01-01T00:00:00Z"

        result = watermark_manager.get_source_watermark(
            "pipeline1", "source1", "updated_at"
        )

        assert result == "2024-01-01T00:00:00Z"
        # Should use source name as both source and target
        mock_backend.get.assert_called_once_with("pipeline1.source1.source1.updated_at")

    def test_update_watermark_atomic(self, watermark_manager, mock_backend):
        """Test atomic watermark update."""
        watermark_manager.update_watermark_atomic(
            "pipeline1", "source1", "target1", "updated_at", "2024-01-01T00:00:00Z"
        )

        mock_backend.transaction.assert_called_once()
        mock_backend.set.assert_called_once()

        # Verify the set call includes the correct parameters
        call_args = mock_backend.set.call_args
        assert call_args[0][0] == "pipeline1.source1.target1.updated_at"  # key
        assert call_args[0][1] == "2024-01-01T00:00:00Z"  # value
        assert isinstance(call_args[0][2], datetime)  # timestamp

    def test_update_watermark_atomic_error(self, watermark_manager, mock_backend):
        """Test error handling during atomic watermark update."""
        mock_backend.set.side_effect = Exception("Backend error")

        with pytest.raises(ConnectorError) as exc_info:
            watermark_manager.update_watermark_atomic(
                "pipeline1", "source1", "target1", "updated_at", "2024-01-01T00:00:00Z"
            )

        assert "Failed to update watermark" in str(exc_info.value)

    def test_update_source_watermark(self, watermark_manager, mock_backend):
        """Test source-level watermark update."""
        watermark_manager.update_source_watermark(
            "pipeline1", "source1", "updated_at", "2024-01-01T00:00:00Z"
        )

        mock_backend.transaction.assert_called_once()
        mock_backend.set.assert_called_once()

        # Verify it uses source name as both source and target
        call_args = mock_backend.set.call_args
        assert call_args[0][0] == "pipeline1.source1.source1.updated_at"

    def test_reset_watermark_exists(self, watermark_manager, mock_backend):
        """Test resetting an existing watermark."""
        mock_backend.delete.return_value = True

        result = watermark_manager.reset_watermark(
            "pipeline1", "source1", "target1", "updated_at"
        )

        assert result is True
        mock_backend.delete.assert_called_once_with(
            "pipeline1.source1.target1.updated_at"
        )

    def test_reset_watermark_not_exists(self, watermark_manager, mock_backend):
        """Test resetting a non-existent watermark."""
        mock_backend.delete.return_value = False

        result = watermark_manager.reset_watermark(
            "pipeline1", "source1", "target1", "updated_at"
        )

        assert result is False
        mock_backend.delete.assert_called_once_with(
            "pipeline1.source1.target1.updated_at"
        )

    def test_reset_watermark_error(self, watermark_manager, mock_backend):
        """Test error handling during watermark reset."""
        mock_backend.delete.side_effect = Exception("Backend error")

        with pytest.raises(ConnectorError) as exc_info:
            watermark_manager.reset_watermark(
                "pipeline1", "source1", "target1", "updated_at"
            )

        assert "Failed to reset watermark" in str(exc_info.value)

    def test_list_watermarks(self, watermark_manager):
        """Test watermark listing functionality."""
        # This is currently a placeholder implementation
        result = watermark_manager.list_watermarks("pipeline1")

        assert isinstance(result, list)
        assert len(result) == 0  # Empty for now

    def test_get_execution_history(self, watermark_manager):
        """Test execution history retrieval."""
        # This is currently a placeholder implementation
        result = watermark_manager.get_execution_history(
            "pipeline1", "source1", "target1", "updated_at"
        )

        assert isinstance(result, list)
        assert len(result) == 0  # Empty for now

    def test_close(self, watermark_manager, mock_backend):
        """Test closing the watermark manager."""
        watermark_manager.close()

        mock_backend.close.assert_called_once()

    def test_close_with_error(self, watermark_manager, mock_backend):
        """Test closing with backend error doesn't raise exception."""
        mock_backend.close.side_effect = Exception("Close error")

        # Should not raise exception
        watermark_manager.close()

        mock_backend.close.assert_called_once()


class TestDuckDBStateBackend:
    """Test suite for DuckDBStateBackend functionality."""

    def test_state_backend_creation(self):
        """Test that DuckDBStateBackend can be created."""
        backend = DuckDBStateBackend()
        assert backend is not None
        backend.close()

    def test_set_and_get_simple_value(self):
        """Test setting and getting a simple value."""
        backend = DuckDBStateBackend()

        backend.set("test_key", "test_value")
        result = backend.get("test_key")

        assert result == "test_value"
        backend.close()

    def test_set_and_get_complex_value(self):
        """Test setting and getting a complex value."""
        backend = DuckDBStateBackend()

        complex_value = {"key": "value", "number": 42, "list": [1, 2, 3]}
        backend.set("complex_key", complex_value)
        result = backend.get("complex_key")

        assert result == complex_value
        backend.close()

    def test_get_nonexistent_key(self):
        """Test getting a key that doesn't exist."""
        backend = DuckDBStateBackend()

        result = backend.get("nonexistent_key")

        assert result is None
        backend.close()

    def test_delete_existing_key(self):
        """Test deleting an existing key."""
        backend = DuckDBStateBackend()

        backend.set("delete_me", "value")
        # Verify it exists first
        assert backend.get("delete_me") == "value"

        deleted = backend.delete("delete_me")

        assert deleted is True
        assert backend.get("delete_me") is None
        backend.close()

    def test_delete_nonexistent_key(self):
        """Test deleting a key that doesn't exist."""
        backend = DuckDBStateBackend()

        deleted = backend.delete("nonexistent_key")

        assert deleted is False
        backend.close()

    def test_transaction_commit(self):
        """Test transaction commit behavior."""
        backend = DuckDBStateBackend()

        with backend.transaction():
            backend.set("tx_key", "tx_value")

        # Value should be committed
        result = backend.get("tx_key")
        assert result == "tx_value"
        backend.close()

    def test_transaction_rollback(self):
        """Test transaction rollback behavior."""
        backend = DuckDBStateBackend()

        try:
            with backend.transaction():
                backend.set("rollback_key", "rollback_value")
                raise Exception("Trigger rollback")
        except Exception:
            pass

        # Value should be rolled back
        result = backend.get("rollback_key")
        assert result is None
        backend.close()
