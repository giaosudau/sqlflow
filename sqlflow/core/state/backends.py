"""State backend implementations for SQLFlow.

This module provides the abstract StateBackend interface and concrete
implementations for state persistence. Currently includes DuckDB backend
for lightweight, self-contained state management.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ContextManager, Optional

import duckdb

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class StateBackend(ABC):
    """Abstract interface for state persistence."""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get value for the given key.

        Args:
            key: The state key to retrieve

        Returns:
            The stored value or None if key doesn't exist
        """

    @abstractmethod
    def set(self, key: str, value: Any, timestamp: Optional[datetime] = None) -> None:
        """Set value for the given key.

        Args:
            key: The state key to store
            value: The value to store
            timestamp: Optional timestamp for the operation
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete the given key.

        Args:
            key: The state key to delete

        Returns:
            True if key existed and was deleted, False otherwise
        """

    @abstractmethod
    def transaction(self) -> ContextManager[Any]:
        """Context manager for atomic transactions."""

    @abstractmethod
    def close(self) -> None:
        """Close the backend and clean up resources."""


class DuckDBStateBackend(StateBackend):
    """DuckDB-based state persistence backend.

    Stores state in DuckDB tables for simplicity and self-containment.
    Provides ACID transactions and efficient key-value operations.
    """

    def __init__(self, connection: Optional[duckdb.DuckDBPyConnection] = None):
        """Initialize DuckDB state backend.

        Args:
            connection: Optional existing DuckDB connection. If None, creates new one.
        """
        self.connection = connection or duckdb.connect()
        self._create_state_tables()
        logger.info("DuckDB state backend initialized")

    def _create_state_tables(self) -> None:
        """Create state management tables if they don't exist."""
        # Watermark state table
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sqlflow_watermarks (
                id INTEGER PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,
                target_table VARCHAR NOT NULL,
                cursor_field VARCHAR NOT NULL,
                cursor_value VARCHAR NOT NULL,  -- JSON for complex types
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_mode VARCHAR NOT NULL,
                UNIQUE(pipeline_name, source_name, target_table, cursor_field)
            )
        """
        )

        # Execution history for debugging
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sqlflow_execution_history (
                id INTEGER PRIMARY KEY,
                watermark_id INTEGER,
                execution_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                execution_end TIMESTAMP,
                rows_processed INTEGER,
                status VARCHAR DEFAULT 'running',
                error_message TEXT,
                FOREIGN KEY(watermark_id) REFERENCES sqlflow_watermarks(id)
            )
        """
        )

        # Generic key-value state table
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sqlflow_state (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL,  -- JSON encoded
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create indexes for performance
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watermarks_pipeline 
            ON sqlflow_watermarks(pipeline_name, source_name)
        """
        )

        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_execution_history_watermark 
            ON sqlflow_execution_history(watermark_id)
        """
        )

        logger.debug("State tables created successfully")

    def get(self, key: str) -> Optional[Any]:
        """Get value for the given key.

        Args:
            key: The state key to retrieve

        Returns:
            The stored value or None if key doesn't exist
        """
        try:
            result = self.connection.execute(
                "SELECT value FROM sqlflow_state WHERE key = ?", [key]
            ).fetchone()

            if result is None:
                return None

            # Deserialize JSON value
            return json.loads(result[0])

        except Exception as e:
            logger.error(f"Failed to get state for key {key}: {e}")
            raise

    def set(self, key: str, value: Any, timestamp: Optional[datetime] = None) -> None:
        """Set value for the given key.

        Args:
            key: The state key to store
            value: The value to store
            timestamp: Optional timestamp for the operation
        """
        try:
            # Serialize value as JSON
            json_value = json.dumps(value, default=str)
            ts = timestamp or datetime.utcnow()

            self.connection.execute(
                """
                INSERT OR REPLACE INTO sqlflow_state (key, value, timestamp)
                VALUES (?, ?, ?)
            """,
                [key, json_value, ts],
            )

            logger.debug(f"Set state for key {key}")

        except Exception as e:
            logger.error(f"Failed to set state for key {key}: {e}")
            raise

    def delete(self, key: str) -> bool:
        """Delete the given key.

        Args:
            key: The state key to delete

        Returns:
            True if key existed and was deleted, False otherwise
        """
        try:
            # First check if key exists
            exists = self.connection.execute(
                "SELECT 1 FROM sqlflow_state WHERE key = ?", [key]
            ).fetchone()

            if not exists:
                return False

            # Delete the key
            self.connection.execute("DELETE FROM sqlflow_state WHERE key = ?", [key])

            logger.debug(f"Deleted state for key {key}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete state for key {key}: {e}")
            raise

    def transaction(self) -> ContextManager[Any]:
        """Context manager for atomic transactions."""
        return DuckDBTransaction(self.connection)

    def close(self) -> None:
        """Close the backend and clean up resources."""
        if self.connection:
            self.connection.close()
            logger.info("DuckDB state backend closed")


class DuckDBTransaction:
    """Context manager for DuckDB transactions."""

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """Initialize transaction context.

        Args:
            connection: DuckDB connection to use for transaction
        """
        self.connection = connection

    def __enter__(self):
        """Begin transaction."""
        self.connection.execute("BEGIN TRANSACTION")
        logger.debug("Started DuckDB transaction")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction, rolling back on error."""
        if exc_type is not None:
            self.connection.execute("ROLLBACK")
            logger.debug("Rolled back DuckDB transaction due to error")
        else:
            self.connection.execute("COMMIT")
            logger.debug("Committed DuckDB transaction")
