"""Watermark management for incremental loading in SQLFlow.

This module provides the WatermarkManager class which handles atomic watermark
updates for reliable incremental loading without artificial checkpoints.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlflow.core.errors import ConnectorError
from sqlflow.core.state.backends import StateBackend
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class WatermarkManager:
    """Manages watermarks for incremental loading operations.

    Provides atomic watermark updates for reliable incremental loading
    without artificial checkpoints within LOAD operations. Watermarks
    are updated only after successful completion of operations.
    """

    def __init__(self, state_backend: StateBackend):
        """Initialize WatermarkManager.

        Args:
            state_backend: Backend for state persistence
        """
        self.backend = state_backend
        logger.info("WatermarkManager initialized")

    def get_state_key(
        self, pipeline: str, source: str, target: str, column: str
    ) -> str:
        """Generate unique key for source->target->column combination.

        Args:
            pipeline: Pipeline name
            source: Source name
            target: Target table name
            column: Cursor column name

        Returns:
            Unique state key for the combination
        """
        return f"{pipeline}.{source}.{target}.{column}"

    def get_watermark(
        self, pipeline: str, source: str, target: str, column: str
    ) -> Any:
        """Get last processed value for incremental loading.

        Args:
            pipeline: Pipeline name
            source: Source name
            target: Target table name
            column: Cursor column name

        Returns:
            Last processed cursor value or None if no watermark exists
        """
        key = self.get_state_key(pipeline, source, target, column)

        try:
            watermark = self.backend.get(key)
            logger.debug(f"Retrieved watermark for {key}: {watermark}")
            return watermark

        except Exception as e:
            logger.error(f"Failed to get watermark for {key}: {e}")
            raise ConnectorError(
                "watermark_manager", f"Failed to retrieve watermark: {str(e)}"
            ) from e

    def get_source_watermark(
        self, pipeline: str, source: str, cursor_field: str
    ) -> Any:
        """Get watermark for a source at the source level.

        This is a simplified interface when target table is not yet known.

        Args:
            pipeline: Pipeline name
            source: Source name
            cursor_field: Cursor field name

        Returns:
            Last processed cursor value or None if no watermark exists
        """
        # Use source name as target for source-level watermarks
        return self.get_watermark(pipeline, source, source, cursor_field)

    def update_watermark_atomic(
        self, pipeline: str, source: str, target: str, column: str, value: Any
    ) -> None:
        """Update watermark atomically after successful LOAD.

        Args:
            pipeline: Pipeline name
            source: Source name
            target: Target table name
            column: Cursor column name
            value: New cursor value to store
        """
        key = self.get_state_key(pipeline, source, target, column)

        try:
            with self.backend.transaction():
                timestamp = datetime.utcnow()
                self.backend.set(key, value, timestamp)
                logger.info(f"Updated watermark for {key}: {value}")

        except Exception as e:
            logger.error(f"Failed to update watermark for {key}: {e}")
            raise ConnectorError(
                "watermark_manager", f"Failed to update watermark: {str(e)}"
            ) from e

    def update_source_watermark(
        self, pipeline: str, source: str, cursor_field: str, value: Any
    ) -> None:
        """Update watermark for a source at the source level.

        This is a simplified interface for source-level watermark updates.

        Args:
            pipeline: Pipeline name
            source: Source name
            cursor_field: Cursor field name
            value: New cursor value to store
        """
        # Use source name as target for source-level watermarks
        self.update_watermark_atomic(pipeline, source, source, cursor_field, value)

    def reset_watermark(
        self, pipeline: str, source: str, target: str, column: str
    ) -> bool:
        """Reset watermark for debugging/recovery purposes.

        Args:
            pipeline: Pipeline name
            source: Source name
            target: Target table name
            column: Cursor column name

        Returns:
            True if watermark existed and was deleted, False otherwise
        """
        key = self.get_state_key(pipeline, source, target, column)

        try:
            deleted = self.backend.delete(key)
            if deleted:
                logger.info(f"Reset watermark for {key}")
            else:
                logger.debug(f"No watermark found to reset for {key}")
            return deleted

        except Exception as e:
            logger.error(f"Failed to reset watermark for {key}: {e}")
            raise ConnectorError(
                "watermark_manager", f"Failed to reset watermark: {str(e)}"
            ) from e

    def list_watermarks(self, pipeline: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all watermarks, optionally filtered by pipeline.

        Args:
            pipeline: Optional pipeline name to filter by

        Returns:
            List of watermark information dictionaries
        """
        # This is a convenience method that would query the state backend
        # For now, we'll provide a basic implementation
        watermarks = []

        try:
            # Note: This is a simplified implementation
            # In a full implementation, we'd query the backend for all keys
            # and parse them to extract watermark information
            logger.debug(f"Listing watermarks for pipeline: {pipeline}")

            # TODO: Implement full watermark listing when needed
            # This would require additional backend methods to list all keys

            return watermarks

        except Exception as e:
            logger.error(f"Failed to list watermarks: {e}")
            raise ConnectorError(
                "watermark_manager", f"Failed to list watermarks: {str(e)}"
            ) from e

    def get_execution_history(
        self, pipeline: str, source: str, target: str, column: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get execution history for debugging purposes.

        Args:
            pipeline: Pipeline name
            source: Source name
            target: Target table name
            column: Cursor column name
            limit: Maximum number of history entries to return

        Returns:
            List of execution history entries
        """
        # This would query the execution history table in the backend
        # For now, we'll provide a placeholder implementation
        try:
            logger.debug(
                f"Getting execution history for {pipeline}.{source}.{target}.{column}"
            )

            # TODO: Implement execution history retrieval
            # This would require additional backend methods to query history tables

            return []

        except Exception as e:
            logger.error(f"Failed to get execution history: {e}")
            raise ConnectorError(
                "watermark_manager", f"Failed to get execution history: {str(e)}"
            ) from e

    def close(self) -> None:
        """Close the watermark manager and clean up resources."""
        try:
            self.backend.close()
            logger.info("WatermarkManager closed")

        except Exception as e:
            logger.error(f"Error closing WatermarkManager: {e}")
            # Don't raise exception during cleanup
