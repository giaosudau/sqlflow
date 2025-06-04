"""Integration tests for OptimizedWatermarkManager with real DuckDB engine.

These tests provide comprehensive coverage of watermark functionality using real
database operations, replacing the mock-based unit tests that were removed
during the testing standards refactoring.
"""

import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.transform.watermark import OptimizedWatermarkManager


class TestOptimizedWatermarkManagerIntegration:
    """Integration tests using real DuckDB engine."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        yield str(temp_path)
        # Cleanup
        if temp_path.exists():
            temp_path.unlink()

    @pytest.fixture
    def duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        # Use in-memory database to avoid file issues
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def watermark_manager(self, duckdb_engine):
        """Create watermark manager with real DuckDB engine."""
        manager = OptimizedWatermarkManager(duckdb_engine)
        yield manager
        manager.close()

    @pytest.fixture
    def sample_data_table(self, duckdb_engine):
        """Create a sample table with time-based data."""
        table_name = "sample_events"

        # Create table with sample data
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                event_date DATE,
                created_at TIMESTAMP,
                value INTEGER
            )
        """
        )

        # Insert sample data spanning several days
        base_date = datetime(2024, 1, 1)
        for i in range(10):  # Reduced for faster tests
            event_date = base_date + timedelta(days=i % 5)
            created_at = base_date + timedelta(days=i % 5, hours=i % 12)
            duckdb_engine.execute_query(
                f"""
                INSERT INTO {table_name} VALUES ({i}, '{event_date.date()}', '{created_at.isoformat()}', {i * 10})
            """
            )

        return table_name

    def test_watermark_manager_initialization(self, watermark_manager, duckdb_engine):
        """Test watermark manager initialization with real database."""
        # Verify manager is properly initialized
        assert watermark_manager.engine == duckdb_engine
        assert isinstance(watermark_manager._cache, dict)
        assert len(watermark_manager._cache) == 0

        # Verify the watermark table and index were created
        result = duckdb_engine.execute_query(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'sqlflow_transform_watermarks'
        """
        )
        rows = result.fetchall()
        assert rows[0][0] == 1

    def test_watermark_table_creation(self, watermark_manager, duckdb_engine):
        """Test that watermark metadata table is created properly."""
        # Check table exists
        result = duckdb_engine.execute_query(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'sqlflow_transform_watermarks'
        """
        )
        rows = result.fetchall()
        assert rows[0][0] == 1

        # Check table structure
        result = duckdb_engine.execute_query(
            """
            SELECT column_name, data_type FROM information_schema.columns 
            WHERE table_name = 'sqlflow_transform_watermarks'
            ORDER BY column_name
        """
        )

        rows = result.fetchall()
        columns = {row[0]: row[1] for row in rows}
        assert "table_name" in columns
        assert "time_column" in columns
        assert "last_watermark" in columns
        assert "last_updated" in columns

    def test_watermark_lifecycle_full_flow(
        self, watermark_manager, sample_data_table, duckdb_engine
    ):
        """Test complete watermark lifecycle: get → update → get → reset."""
        table_name = sample_data_table
        time_column = "created_at"

        # 1. Initial get (should fallback to MAX() and return actual max value from sample data)
        initial_watermark = watermark_manager.get_transform_watermark(
            table_name, time_column
        )
        assert (
            initial_watermark is not None
        )  # Should find the MAX value from sample data

        # 2. Reset to clear all data, then should be None for non-existent table
        watermark_manager.reset_watermark(table_name, time_column)

        # Test with non-existent table (should be None)
        nonexistent_watermark = watermark_manager.get_transform_watermark(
            "nonexistent_table", time_column
        )
        assert nonexistent_watermark is None

        # 3. Update watermark with a specific value
        test_watermark = datetime(2024, 1, 5, 12, 0, 0)
        watermark_manager.update_watermark(table_name, time_column, test_watermark)

        # 4. Get watermark (should return our value, not the MAX)
        retrieved_watermark = watermark_manager.get_transform_watermark(
            table_name, time_column
        )
        assert retrieved_watermark == test_watermark

        # 5. Verify it's cached (clear cache and get again - should hit metadata)
        watermark_manager.clear_cache()
        retrieved_again = watermark_manager.get_transform_watermark(
            table_name, time_column
        )
        assert retrieved_again == test_watermark

        # 6. Reset watermark
        reset_result = watermark_manager.reset_watermark(table_name, time_column)
        assert reset_result is True

        # 7. Get after reset (should fallback to MAX() again)
        final_watermark = watermark_manager.get_transform_watermark(
            table_name, time_column
        )
        assert (
            final_watermark == initial_watermark
        )  # Should be the same as the initial MAX value

    def test_watermark_cache_miss_metadata_hit(self, watermark_manager, duckdb_engine):
        """Test watermark retrieval with cache miss but metadata hit."""
        table_name = "test_table"
        time_column = "created_at"
        test_time = datetime(2024, 1, 15, 10, 30, 0)

        # Store watermark in metadata first
        watermark_manager.update_watermark(table_name, time_column, test_time)

        # Clear cache to force metadata lookup
        watermark_manager.clear_cache()

        # Retrieve watermark - should hit metadata, not cache
        result = watermark_manager.get_transform_watermark(table_name, time_column)
        assert result == test_time

        # Verify it's now cached
        assert f"{table_name}:{time_column}" in watermark_manager._cache

    def test_max_fallback_functionality(
        self, watermark_manager, sample_data_table, duckdb_engine
    ):
        """Test fallback to MAX() query when no metadata exists."""
        table_name = sample_data_table
        time_column = "created_at"

        # Get expected MAX value directly
        result = duckdb_engine.execute_query(
            f"""
            SELECT MAX({time_column}) FROM {table_name}
        """
        )
        rows = result.fetchall()
        expected_max = rows[0][0]

        # Clear any existing metadata
        watermark_manager.reset_watermark(table_name, time_column)
        watermark_manager.clear_cache()

        # Get watermark - should fallback to MAX() and return the max value
        watermark = watermark_manager.get_transform_watermark(table_name, time_column)
        assert watermark == expected_max

        # Verify the MAX value was stored in metadata for future fast lookups
        cached_watermark = watermark_manager.get_transform_watermark(
            table_name, time_column
        )
        assert cached_watermark == expected_max

    def test_watermark_no_data_scenario(self, watermark_manager, duckdb_engine):
        """Test watermark behavior when table has no data."""
        table_name = "empty_table"
        time_column = "timestamp_col"

        # Create empty table
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                timestamp_col TIMESTAMP
            )
        """
        )

        # Get watermark from empty table
        watermark = watermark_manager.get_transform_watermark(table_name, time_column)
        assert watermark is None

    def test_watermark_update_functionality(self, watermark_manager):
        """Test watermark update with cache management."""
        table_name = "test_table"
        time_column = "created_at"
        test_time = datetime(2024, 1, 15, 10, 30, 0)
        cache_key = f"{table_name}:{time_column}"

        # Update watermark
        watermark_manager.update_watermark(table_name, time_column, test_time)

        # Verify cache was updated
        assert watermark_manager._cache[cache_key] == test_time

        # Verify database was updated by retrieving from fresh manager
        retrieved = watermark_manager.get_transform_watermark(table_name, time_column)
        assert retrieved == test_time

    def test_watermark_update_database_error_resilience(
        self, watermark_manager, duckdb_engine
    ):
        """Test watermark update resilience when database errors occur."""
        table_name = "test_table"
        time_column = "created_at"
        test_time = datetime(2024, 1, 15, 10, 30, 0)
        cache_key = f"{table_name}:{time_column}"

        # Close engine to simulate database error
        duckdb_engine.close()

        # Update should not raise exception but still update cache
        watermark_manager.update_watermark(table_name, time_column, test_time)

        # Cache should still be updated despite database error
        assert watermark_manager._cache[cache_key] == test_time

    def test_reset_watermark_scenarios(self, watermark_manager, duckdb_engine):
        """Test watermark reset for existing and non-existing watermarks."""
        table_name = "test_table"
        time_column = "created_at"
        test_time = datetime(2024, 1, 15, 10, 30, 0)
        cache_key = f"{table_name}:{time_column}"

        # Test reset of existing watermark
        watermark_manager.update_watermark(table_name, time_column, test_time)
        assert cache_key in watermark_manager._cache

        result = watermark_manager.reset_watermark(table_name, time_column)
        assert result is True
        assert cache_key not in watermark_manager._cache

        # Test reset of non-existent watermark
        result = watermark_manager.reset_watermark(
            "nonexistent_table", "nonexistent_col"
        )
        assert result is False

    def test_reset_watermark_error_handling(self, watermark_manager, duckdb_engine):
        """Test error handling during watermark reset."""
        table_name = "test_table"
        time_column = "created_at"

        # Close engine to simulate database error
        duckdb_engine.close()

        # Reset should handle error gracefully
        result = watermark_manager.reset_watermark(table_name, time_column)
        assert result is False

    def test_list_watermarks_functionality(self, watermark_manager, duckdb_engine):
        """Test listing all watermarks."""
        # Add several watermarks
        watermarks_data = [
            ("events", "created_at", datetime(2024, 1, 1, 10, 0, 0)),
            ("orders", "order_date", datetime(2024, 1, 2, 11, 0, 0)),
            ("users", "registered_at", datetime(2024, 1, 3, 12, 0, 0)),
        ]

        for table_name, time_column, watermark in watermarks_data:
            watermark_manager.update_watermark(table_name, time_column, watermark)

        # List all watermarks
        result = watermark_manager.list_watermarks()

        assert result["total_count"] == 3
        assert len(result["watermarks"]) == 3
        assert "cache_stats" in result

        # Verify data
        watermark_tables = [w["table_name"] for w in result["watermarks"]]
        assert "events" in watermark_tables
        assert "orders" in watermark_tables
        assert "users" in watermark_tables

    def test_list_watermarks_error_handling(self, watermark_manager, duckdb_engine):
        """Test error handling during watermark listing."""
        # Close engine to simulate database error
        duckdb_engine.close()

        result = watermark_manager.list_watermarks()

        assert result["total_count"] == 0
        assert result["watermarks"] == []
        assert "error" in result

    def test_caching_performance(self, watermark_manager, sample_data_table):
        """Test caching performance meets sub-10ms requirement."""
        table_name = sample_data_table
        time_column = "created_at"
        test_watermark = datetime(2024, 1, 5, 12, 0, 0)

        # Set up watermark
        watermark_manager.update_watermark(table_name, time_column, test_watermark)

        # Warm up cache with one call
        watermark_manager.get_transform_watermark(table_name, time_column)

        # Measure cached lookups
        start_time = time.time()
        for _ in range(100):  # 100 lookups
            result = watermark_manager.get_transform_watermark(table_name, time_column)
            assert result == test_watermark
        end_time = time.time()

        total_time = end_time - start_time
        avg_time_per_lookup = (total_time / 100) * 1000  # Convert to milliseconds

        # Should be well under 10ms per lookup for cached data
        assert (
            avg_time_per_lookup < 10.0
        ), f"Average lookup time {avg_time_per_lookup:.2f}ms exceeds 10ms target"

    def test_concurrent_access_safety(self, watermark_manager, duckdb_engine):
        """Test thread safety with concurrent operations."""
        table_name = "concurrent_test"
        time_column = "timestamp_col"

        # Create test table and insert data
        self._setup_concurrent_test_table(duckdb_engine, table_name, time_column)

        results = []
        errors = []
        base_time = datetime(2024, 1, 1, 12, 0, 0)

        def worker_thread(worker_id):
            """Worker function for concurrent testing."""
            self._run_concurrent_operations(
                watermark_manager,
                table_name,
                time_column,
                base_time,
                worker_id,
                results,
                errors,
            )

        # Run concurrent operations
        threads = []
        for worker_id in range(3):  # Reduced for faster tests
            thread = threading.Thread(target=worker_thread, args=(worker_id,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Validate results
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        assert len(results) > 0
        for result in results:
            assert result is None or isinstance(result, datetime)

    def _setup_concurrent_test_table(self, duckdb_engine, table_name, time_column):
        """Helper to set up test table for concurrent operations."""
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                timestamp_col TIMESTAMP,
                value INTEGER
            )
        """
        )

        # Insert some data
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        for i in range(10):
            timestamp = base_time + timedelta(minutes=i)
            duckdb_engine.execute_query(
                f"""
                INSERT INTO {table_name} VALUES ({i}, '{timestamp.isoformat()}', {i})
            """
            )

    def _run_concurrent_operations(
        self,
        watermark_manager,
        table_name,
        time_column,
        base_time,
        worker_id,
        results,
        errors,
    ):
        """Helper to run concurrent operations for a single worker."""
        try:
            for i in range(5):  # Reduced for faster tests
                # Mix of operations
                if i % 3 == 0:
                    # Update watermark
                    watermark = base_time + timedelta(minutes=worker_id * 10 + i)
                    watermark_manager.update_watermark(
                        table_name, time_column, watermark
                    )
                elif i % 3 == 1:
                    # Get watermark
                    watermark = watermark_manager.get_transform_watermark(
                        table_name, time_column
                    )
                    results.append(watermark)
                else:
                    # Clear cache
                    watermark_manager.clear_cache()

        except Exception as e:
            errors.append(f"Worker {worker_id}: {e}")

    def test_cache_statistics(self, watermark_manager):
        """Test cache statistics functionality."""
        # Initially empty
        stats = watermark_manager.get_cache_stats()
        assert stats["cache_size"] == 0
        assert stats["cached_tables"] == []
        assert stats["cache_type"] == "in_memory_lru"

        # Add some entries
        watermark_manager.update_watermark("table1", "col1", datetime(2024, 1, 1))
        watermark_manager.update_watermark("table2", "col2", datetime(2024, 1, 2))

        stats = watermark_manager.get_cache_stats()
        assert stats["cache_size"] == 2
        assert "table1:col1" in stats["cached_tables"]
        assert "table2:col2" in stats["cached_tables"]

    def test_multiple_time_columns_per_table(self, watermark_manager, duckdb_engine):
        """Test handling multiple time columns for the same table."""
        table_name = "multi_time_table"

        # Create table with multiple time columns
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                processed_at TIMESTAMP
            )
        """
        )

        # Set different watermarks for different time columns
        watermarks = {
            "created_at": datetime(2024, 1, 1, 10, 0, 0),
            "updated_at": datetime(2024, 1, 2, 11, 0, 0),
            "processed_at": datetime(2024, 1, 3, 12, 0, 0),
        }

        for time_column, watermark in watermarks.items():
            watermark_manager.update_watermark(table_name, time_column, watermark)

        # Verify each watermark is stored and retrieved correctly
        for time_column, expected_watermark in watermarks.items():
            retrieved_watermark = watermark_manager.get_transform_watermark(
                table_name, time_column
            )
            assert retrieved_watermark == expected_watermark

    def test_watermark_persistence_across_manager_instances(self, duckdb_engine):
        """Test that watermarks persist across different manager instances."""
        table_name = "persistence_test"
        time_column = "event_time"
        test_watermark = datetime(2024, 1, 15, 14, 30, 0)

        # Create first manager instance and set watermark
        manager1 = OptimizedWatermarkManager(duckdb_engine)
        manager1.update_watermark(table_name, time_column, test_watermark)
        manager1.close()

        # Create second manager instance and retrieve watermark
        manager2 = OptimizedWatermarkManager(duckdb_engine)
        retrieved_watermark = manager2.get_transform_watermark(table_name, time_column)

        assert retrieved_watermark == test_watermark
        manager2.close()

    def test_error_handling_for_nonexistent_table(self, watermark_manager):
        """Test graceful handling when target table doesn't exist."""
        # Should return None without raising exception
        watermark = watermark_manager.get_transform_watermark(
            "nonexistent_table", "some_column"
        )
        assert watermark is None

        # Update should still work (stores in metadata)
        test_watermark = datetime(2024, 1, 1, 10, 0, 0)
        watermark_manager.update_watermark(
            "nonexistent_table", "some_column", test_watermark
        )

        # Should retrieve from metadata
        retrieved = watermark_manager.get_transform_watermark(
            "nonexistent_table", "some_column"
        )
        assert retrieved == test_watermark

    def test_comprehensive_error_scenarios(self, watermark_manager, duckdb_engine):
        """Test comprehensive error handling scenarios."""
        table_name = "error_test_table"
        time_column = "timestamp_col"

        # Test 1: Invalid column name with real table
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                valid_timestamp TIMESTAMP
            )
        """
        )

        # Should handle invalid column gracefully
        watermark = watermark_manager.get_transform_watermark(
            table_name, "invalid_column"
        )
        assert watermark is None

        # Test 2: Database connection issues during operations
        # Store a watermark first
        test_time = datetime(2024, 1, 1, 10, 0, 0)
        watermark_manager.update_watermark(table_name, time_column, test_time)

        # Close engine
        duckdb_engine.close()

        # Operations should handle errors gracefully
        watermark = watermark_manager.get_transform_watermark(table_name, time_column)
        # Should return cached value or None, not raise exception
        assert watermark is None or isinstance(watermark, datetime)

        # Reset and list should also handle errors gracefully
        reset_result = watermark_manager.reset_watermark(table_name, time_column)
        assert isinstance(reset_result, bool)

        list_result = watermark_manager.list_watermarks()
        assert isinstance(list_result, dict)
        assert "error" in list_result

    def test_watermark_with_different_timestamp_formats(
        self, watermark_manager, duckdb_engine
    ):
        """Test watermark handling with different timestamp formats."""
        table_name = "timestamp_format_test"

        # Create table with different timestamp formats
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                date_col DATE,
                datetime_col TIMESTAMP,
                timestamp_with_tz_col TIMESTAMPTZ
            )
        """
        )

        # Insert test data
        base_date = datetime(2024, 1, 1, 12, 0, 0)
        duckdb_engine.execute_query(
            f"""
            INSERT INTO {table_name} VALUES 
            (1, '{base_date.date()}', '{base_date.isoformat()}', '{base_date.isoformat()}'),
            (2, '{(base_date + timedelta(days=1)).date()}', '{(base_date + timedelta(days=1)).isoformat()}', '{(base_date + timedelta(days=1)).isoformat()}')
        """
        )

        # Test watermarks with different column types
        columns_to_test = ["date_col", "datetime_col", "timestamp_with_tz_col"]

        for column in columns_to_test:
            watermark = watermark_manager.get_transform_watermark(table_name, column)
            assert watermark is not None, f"Failed to get watermark for column {column}"

            # DATE columns return datetime.date, TIMESTAMP columns return datetime.datetime
            if column == "date_col":
                assert isinstance(
                    watermark, (datetime, type(base_date.date()))
                ), f"Watermark for {column} should be datetime or date: {type(watermark)}"
            else:
                assert isinstance(
                    watermark, datetime
                ), f"Watermark for {column} is not a datetime: {type(watermark)}"
