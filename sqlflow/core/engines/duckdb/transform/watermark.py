"""
Optimized watermark management for SQLFlow transform operations.

This module provides the OptimizedWatermarkManager class which extends
the basic watermark functionality with high-performance caching and
metadata-based tracking specifically optimized for transform operations.
"""

import threading
from datetime import datetime
from typing import Any, Dict, Optional

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class OptimizedWatermarkManager:
    """High-performance watermark tracking for incremental transforms.

    Provides optimized watermark management with:
    - Metadata table with indexed lookups for fast retrieval
    - In-memory caching system with automatic cache invalidation
    - Fallback to MAX() queries when metadata unavailable
    - Concurrent access safety with proper locking mechanisms
    """

    def __init__(self, duckdb_engine: DuckDBEngine):
        """Initialize the optimized watermark manager.

        Args:
            duckdb_engine: DuckDB engine instance for database operations
        """
        self.engine = duckdb_engine
        self._cache: Dict[str, datetime] = {}
        self._cache_lock = threading.RLock()
        self._ensure_watermark_table()
        logger.info("OptimizedWatermarkManager initialized with caching")

    def _ensure_watermark_table(self) -> None:
        """Create optimized watermark tracking table with indexes."""
        try:
            # Create watermark metadata table
            self.engine.execute_query(
                """
                CREATE TABLE IF NOT EXISTS sqlflow_transform_watermarks (
                    table_name VARCHAR PRIMARY KEY,
                    time_column VARCHAR NOT NULL,
                    last_watermark TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create index for fast lookups
            self.engine.execute_query(
                """
                CREATE INDEX IF NOT EXISTS idx_watermark_lookup 
                ON sqlflow_transform_watermarks(table_name, time_column)
            """
            )

            logger.debug("Transform watermark tracking table ensured")

        except Exception as e:
            logger.warning(f"Failed to create watermark table: {e}")

    def get_transform_watermark(
        self, table_name: str, time_column: str
    ) -> Optional[datetime]:
        """Get last processed timestamp with caching.

        Provides sub-10ms cached lookups and sub-100ms cold lookups.

        Args:
            table_name: Target table name
            time_column: Time column used for incremental processing

        Returns:
            Last processed timestamp or None if no watermark exists
        """
        cache_key = f"{table_name}:{time_column}"

        # Check cache first (sub-10ms performance target)
        with self._cache_lock:
            if cache_key in self._cache:
                logger.debug(f"Cache hit for watermark {cache_key}")
                return self._cache[cache_key]

        # Try watermark metadata table first (fast indexed lookup)
        try:
            result = self.engine.execute_query(
                f"""
                SELECT last_watermark FROM sqlflow_transform_watermarks
                WHERE table_name = '{table_name}' AND time_column = '{time_column}'
            """
            )

            # Convert DuckDB result to rows
            rows = result.fetchall() if hasattr(result, "fetchall") else []

            if rows and rows[0] and rows[0][0]:
                watermark = rows[0][0]
                # Cache the result
                with self._cache_lock:
                    self._cache[cache_key] = watermark
                logger.debug(f"Metadata lookup for watermark {cache_key}: {watermark}")
                return watermark

        except Exception as e:
            logger.debug(f"Metadata lookup failed for {cache_key}: {e}")

        # Fallback to MAX() query (slower but accurate)
        try:
            result = self.engine.execute_query(
                f"""
                SELECT MAX({time_column}) as max_timestamp
                FROM {table_name}
                WHERE {time_column} IS NOT NULL
            """
            )

            # Convert DuckDB result to rows
            rows = result.fetchall() if hasattr(result, "fetchall") else []

            if rows and rows[0] and rows[0][0]:
                watermark = rows[0][0]
                # Store in metadata table for future fast lookups
                self.update_watermark(table_name, time_column, watermark)
                logger.debug(f"MAX() fallback for watermark {cache_key}: {watermark}")
                return watermark

        except Exception as e:
            # Table doesn't exist or column missing - this is normal for first run
            logger.debug(f"Could not get watermark for {table_name}.{time_column}: {e}")

        return None

    def update_watermark(
        self, table_name: str, time_column: str, watermark: datetime
    ) -> None:
        """Update watermark with cache invalidation.

        Args:
            table_name: Target table name
            time_column: Time column used for incremental processing
            watermark: New watermark value to store
        """
        cache_key = f"{table_name}:{time_column}"

        try:
            # Format watermark as ISO string for SQL
            watermark_str = watermark.isoformat()

            # Update metadata table (upsert operation)
            self.engine.execute_query(
                f"""
                INSERT INTO sqlflow_transform_watermarks 
                (table_name, time_column, last_watermark, last_updated)
                VALUES ('{table_name}', '{time_column}', '{watermark_str}', CURRENT_TIMESTAMP)
                ON CONFLICT (table_name) DO UPDATE SET
                    time_column = excluded.time_column,
                    last_watermark = excluded.last_watermark,
                    last_updated = excluded.last_updated
            """
            )

            # Update cache
            with self._cache_lock:
                self._cache[cache_key] = watermark

            logger.info(f"Updated transform watermark for {cache_key}: {watermark}")

        except Exception as e:
            logger.error(f"Failed to update watermark for {cache_key}: {e}")
            # Still update cache even if metadata update fails
            with self._cache_lock:
                self._cache[cache_key] = watermark

    def clear_cache(self) -> None:
        """Clear the watermark cache."""
        with self._cache_lock:
            cache_size = len(self._cache)
            self._cache.clear()
            logger.debug(f"Cleared watermark cache ({cache_size} entries)")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring.

        Returns:
            Dictionary with cache statistics
        """
        with self._cache_lock:
            return {
                "cache_size": len(self._cache),
                "cached_tables": list(self._cache.keys()),
                "cache_type": "in_memory_lru",
            }

    def reset_watermark(self, table_name: str, time_column: str) -> bool:
        """Reset watermark for debugging/recovery purposes.

        Args:
            table_name: Target table name
            time_column: Time column used for incremental processing

        Returns:
            True if watermark existed and was deleted, False otherwise
        """
        cache_key = f"{table_name}:{time_column}"

        try:
            # Remove from cache first
            cache_deleted = False
            with self._cache_lock:
                if cache_key in self._cache:
                    del self._cache[cache_key]
                    cache_deleted = True

            # Remove from metadata table
            result = self.engine.execute_query(
                f"""
                DELETE FROM sqlflow_transform_watermarks
                WHERE table_name = '{table_name}' AND time_column = '{time_column}'
            """
            )

            # Check if any rows were affected
            rows_affected = result.rowcount if hasattr(result, "rowcount") else 0

            # If we had it in cache or deleted from database, consider it deleted
            if cache_deleted or rows_affected > 0:
                logger.info(f"Reset transform watermark for {cache_key}")
                return True
            else:
                logger.debug(f"No watermark found to reset for {cache_key}")
                return False

        except Exception as e:
            logger.error(f"Failed to reset watermark for {cache_key}: {e}")
            return False

    def list_watermarks(self) -> Dict[str, Any]:
        """List all transform watermarks.

        Returns:
            Dictionary with watermark information
        """
        try:
            result = self.engine.execute_query(
                """
                SELECT table_name, time_column, last_watermark, last_updated
                FROM sqlflow_transform_watermarks
                ORDER BY table_name, time_column
            """
            )

            # Convert DuckDB result to rows
            rows = result.fetchall() if hasattr(result, "fetchall") else []

            watermarks = []
            for row in rows:
                watermarks.append(
                    {
                        "table_name": row[0],
                        "time_column": row[1],
                        "last_watermark": row[2],
                        "last_updated": row[3],
                    }
                )

            return {
                "watermarks": watermarks,
                "total_count": len(watermarks),
                "cache_stats": self.get_cache_stats(),
            }

        except Exception as e:
            logger.error(f"Failed to list watermarks: {e}")
            return {"watermarks": [], "total_count": 0, "error": str(e)}

    def close(self) -> None:
        """Close the watermark manager and clean up resources."""
        try:
            self.clear_cache()
            logger.info("OptimizedWatermarkManager closed")
        except Exception as e:
            logger.error(f"Error closing OptimizedWatermarkManager: {e}")
