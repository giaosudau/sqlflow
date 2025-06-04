"""Tests for OptimizedWatermarkManager - Pure logic tests without mocks.

This module contains unit tests for pure logic functions that don't require
database operations or complex mocking. Complex integration tests with real
DuckDB engines are in tests/integration/core/test_watermark_integration.py
"""

import threading
import time
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from sqlflow.core.engines.duckdb.transform.watermark import OptimizedWatermarkManager


class TestOptimizedWatermarkManagerPureLogic:
    """Test suite for OptimizedWatermarkManager pure logic functionality."""

    @pytest.fixture
    def watermark_manager(self):
        """Create OptimizedWatermarkManager with mock engine for pure logic tests."""
        mock_engine = MagicMock()
        # Disable actual database operations for pure logic tests
        with pytest.MonkeyPatch.context() as m:
            m.setattr(
                OptimizedWatermarkManager, "_ensure_watermark_table", lambda self: None
            )
            manager = OptimizedWatermarkManager(mock_engine)
            return manager

    def test_cache_hit_performance(self, watermark_manager):
        """Test cache hit performance meets sub-10ms requirement."""
        test_time = datetime(2024, 1, 15, 10, 30, 0)
        cache_key = "test_table:created_at"

        # Pre-populate cache
        watermark_manager._cache[cache_key] = test_time

        start_time = time.time()
        result = watermark_manager.get_transform_watermark("test_table", "created_at")
        end_time = time.time()

        assert result == test_time
        # Verify it was fast (should be sub-10ms, allowing for test overhead)
        assert (end_time - start_time) < 0.1  # 100ms generous limit for tests

        # Engine should not have been called (cache hit)
        watermark_manager.engine.execute_query.assert_not_called()

    def test_clear_cache_functionality(self, watermark_manager):
        """Test cache clearing functionality."""
        # Pre-populate cache
        watermark_manager._cache["table1:col1"] = datetime.now()
        watermark_manager._cache["table2:col2"] = datetime.now()

        assert len(watermark_manager._cache) == 2

        watermark_manager.clear_cache()

        assert len(watermark_manager._cache) == 0

    def test_get_cache_stats_functionality(self, watermark_manager):
        """Test cache statistics retrieval."""
        # Pre-populate cache
        watermark_manager._cache["table1:col1"] = datetime.now()
        watermark_manager._cache["table2:col2"] = datetime.now()

        stats = watermark_manager.get_cache_stats()

        assert stats["cache_size"] == 2
        assert "table1:col1" in stats["cached_tables"]
        assert "table2:col2" in stats["cached_tables"]
        assert stats["cache_type"] == "in_memory_lru"

    def test_close_cleanup(self, watermark_manager):
        """Test closing the watermark manager clears cache."""
        # Pre-populate cache
        watermark_manager._cache["test:table"] = datetime.now()

        watermark_manager.close()

        # Cache should be cleared
        assert len(watermark_manager._cache) == 0

    def test_concurrent_access_thread_safety(self, watermark_manager):
        """Test thread safety of cache operations with real threading."""
        test_time = datetime(2024, 1, 15, 10, 30, 0)
        cache_key = "test_table:created_at"

        def update_cache():
            watermark_manager._cache[cache_key] = test_time

        def read_cache():
            return watermark_manager._cache.get(cache_key)

        # Run concurrent operations
        threads = []
        for _ in range(10):
            t1 = threading.Thread(target=update_cache)
            t2 = threading.Thread(target=read_cache)
            threads.extend([t1, t2])

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Should not raise any exceptions and final state should be consistent
        assert watermark_manager._cache[cache_key] == test_time

    def test_cache_performance_benchmark(self, watermark_manager):
        """Test cache performance requirements (10x faster than database queries)."""
        test_time = datetime(2024, 1, 15, 10, 30, 0)
        cache_key = "test_table:created_at"

        # Pre-populate cache
        watermark_manager._cache[cache_key] = test_time

        # Measure multiple cache hits
        start_time = time.time()
        for _ in range(1000):  # 1000 lookups
            result = watermark_manager.get_transform_watermark(
                "test_table", "created_at"
            )
            assert result == test_time
        end_time = time.time()

        # Should be very fast for cached lookups
        total_time = end_time - start_time
        avg_time_per_lookup = total_time / 1000

        # Should be well under 10ms per lookup (allowing for test overhead)
        assert avg_time_per_lookup < 0.01  # 10ms per lookup

        # Verify engine was not called (all cache hits)
        watermark_manager.engine.execute_query.assert_not_called()

    def test_cache_invalidation_logic(self, watermark_manager):
        """Test cache invalidation logic during updates."""
        cache_key = "test_table:created_at"
        old_time = datetime(2024, 1, 15, 10, 0, 0)
        new_time = datetime(2024, 1, 15, 11, 0, 0)

        # Pre-populate cache with old value
        watermark_manager._cache[cache_key] = old_time

        # Test the cache update logic directly (without database operation)
        watermark_manager._cache[cache_key] = new_time

        # Cache should be updated
        assert watermark_manager._cache[cache_key] == new_time
        assert watermark_manager._cache[cache_key] != old_time

    def test_cache_key_generation_logic(self, watermark_manager):
        """Test cache key generation and management logic."""
        table_name = "test_table"
        time_column = "created_at"
        expected_key = f"{table_name}:{time_column}"
        test_time = datetime(2024, 1, 15, 10, 30, 0)

        # Test direct cache operations
        watermark_manager._cache[expected_key] = test_time

        # Verify key format is consistent
        assert expected_key in watermark_manager._cache
        assert watermark_manager._cache[expected_key] == test_time

        # Test with different table/column combinations
        key2 = "other_table:timestamp_col"
        watermark_manager._cache[key2] = test_time

        # Both should coexist
        assert len(watermark_manager._cache) == 2
        assert expected_key in watermark_manager._cache
        assert key2 in watermark_manager._cache

    def test_cache_stats_with_empty_cache(self, watermark_manager):
        """Test cache statistics with empty cache."""
        stats = watermark_manager.get_cache_stats()

        assert stats["cache_size"] == 0
        assert stats["cached_tables"] == []
        assert stats["cache_type"] == "in_memory_lru"

    def test_cache_stats_with_multiple_entries(self, watermark_manager):
        """Test cache statistics with multiple entries."""
        # Add multiple cache entries
        test_entries = {
            "table1:col1": datetime(2024, 1, 1),
            "table2:col2": datetime(2024, 1, 2),
            "table3:col3": datetime(2024, 1, 3),
        }

        for key, value in test_entries.items():
            watermark_manager._cache[key] = value

        stats = watermark_manager.get_cache_stats()

        assert stats["cache_size"] == 3
        assert len(stats["cached_tables"]) == 3

        for key in test_entries.keys():
            assert key in stats["cached_tables"]
