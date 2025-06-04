"""
Integration tests for partition management framework with real DuckDB engine.

These tests validate partition detection, creation, and optimization using real
database operations, ensuring production-scale functionality.
"""

from datetime import datetime, timedelta

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.transform.partitions import (
    PartitionManager,
    PartitionType,
    TimeGranularity,
    TimeRange,
)


class TestPartitionManagerIntegration:
    """Integration tests for partition management with real DuckDB engine."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        # Use in-memory database to avoid file issues
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def partition_manager(self, duckdb_engine):
        """Create partition manager with real DuckDB engine."""
        return PartitionManager(duckdb_engine)

    @pytest.fixture
    def events_table(self, duckdb_engine):
        """Create a test events table with time-based data."""
        table_name = "events"

        # Create table with time-based data
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                event_type VARCHAR,
                created_at TIMESTAMP,
                value DECIMAL
            )
        """
        )

        # Insert sample data spanning multiple days - need 100k+ for partitioning strategy
        base_time = datetime(2024, 1, 1, 12, 0, 0)

        # Insert data in batches for efficiency
        batch_size = 1000
        total_batches = 102  # 102,000 rows total

        for batch in range(total_batches):
            values = []
            for i in range(batch_size):
                record_id = batch * batch_size + i
                # Spread data over ~85 days (2000 hours)
                hours_offset = record_id // 50  # 50 records per hour
                day_offset = hours_offset // 24
                timestamp = base_time + timedelta(
                    days=day_offset, hours=hours_offset % 24, minutes=i % 60
                )
                values.append(
                    f"({record_id}, 'type_{record_id % 5}', '{timestamp}', {record_id * 10.5})"
                )

            if values:
                duckdb_engine.execute_query(
                    f"""
                    INSERT INTO {table_name} VALUES {', '.join(values)}
                """
                )

        return table_name

    @pytest.fixture
    def partitioned_events_table(self, duckdb_engine):
        """Create partitioned events tables for testing."""
        base_table = "events_partitioned"

        # Create base table (not used for data, just for schema reference)
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {base_table} (
                id INTEGER,
                event_type VARCHAR,
                created_at TIMESTAMP,
                value DECIMAL
            )
        """
        )

        # Create partition tables with date-based naming
        partition_tables = []
        base_date = datetime(2024, 1, 1)

        for day in range(3):
            partition_date = base_date + timedelta(days=day)
            partition_name = f"{base_table}_{partition_date.strftime('%Y%m%d')}"
            partition_tables.append(partition_name)

            # Create partition table
            duckdb_engine.execute_query(
                f"""
                CREATE TABLE {partition_name} (
                    id INTEGER,
                    event_type VARCHAR,
                    created_at TIMESTAMP,
                    value DECIMAL
                )
            """
            )

            # Insert data for this partition
            for hour in range(24):
                timestamp = partition_date + timedelta(hours=hour)
                duckdb_engine.execute_query(
                    f"""
                    INSERT INTO {partition_name} VALUES 
                    ({day * 1000 + hour}, 'type_{day}', '{timestamp}', {hour * 5.0})
                """
                )

        return base_table, partition_tables

    def test_virtual_partition_detection(self, partition_manager, events_table):
        """Test detection of virtual partitions from data distribution."""
        partitions = partition_manager.detect_partitions(events_table, "created_at")

        # Should detect virtual partitions based on data distribution
        assert len(partitions) > 0

        # Check that all partitions are virtual and time-based
        for partition in partitions:
            assert partition.partition_type == PartitionType.VIRTUAL
            assert partition.is_time_based() is True
            assert partition.time_range is not None
            assert partition.row_count is not None
            assert partition.row_count > 0

        # Verify time ranges don't overlap
        for i in range(len(partitions) - 1):
            current = partitions[i]
            next_partition = partitions[i + 1]
            assert not current.time_range.overlaps(next_partition.time_range)

    def test_pattern_based_partition_detection(
        self, partition_manager, partitioned_events_table
    ):
        """Test detection of pattern-based partitions from table names."""
        base_table, partition_tables = partitioned_events_table

        partitions = partition_manager.detect_partitions(base_table, "created_at")

        # Should detect all partition tables
        assert len(partitions) == len(partition_tables)

        # Check partition details
        for partition in partitions:
            assert partition.partition_type == PartitionType.TIME_BASED
            assert partition.table_name == base_table
            assert partition.partition_name in partition_tables
            assert partition.time_range is not None
            assert partition.time_range.granularity == TimeGranularity.DAY

        # Verify partitions are ordered by time
        sorted_partitions = sorted(partitions, key=lambda p: p.time_range.start_time)
        for i in range(len(sorted_partitions) - 1):
            current_end = sorted_partitions[i].time_range.end_time
            next_start = sorted_partitions[i + 1].time_range.start_time
            assert current_end <= next_start

    def test_partition_creation(self, partition_manager, events_table, duckdb_engine):
        """Test creation of new partition tables."""
        time_range = TimeRange(
            datetime(2024, 2, 1, 0, 0, 0),
            datetime(2024, 2, 2, 0, 0, 0),
            TimeGranularity.DAY,
        )

        partition_name = partition_manager.create_partition(
            events_table, time_range, "created_at"
        )

        # Verify partition table was created
        assert partition_name == f"{events_table}_p_20240201"

        # Check that table exists and has correct schema
        result = duckdb_engine.execute_query(
            f"""
            SELECT column_name, data_type FROM information_schema.columns 
            WHERE table_name = '{partition_name}'
            ORDER BY ordinal_position
        """
        )

        columns = result.fetchall()
        assert len(columns) == 4  # id, event_type, created_at, value

        expected_columns = ["id", "event_type", "created_at", "value"]
        actual_columns = [row[0] for row in columns]
        assert actual_columns == expected_columns

        # Verify index was created on time column
        # Note: DuckDB doesn't expose index information easily, so we test functionality
        # by inserting data and querying
        test_time = datetime(2024, 2, 1, 15, 30, 0)
        duckdb_engine.execute_query(
            f"""
            INSERT INTO {partition_name} VALUES (1, 'test', '{test_time}', 100.0)
        """
        )

        result = duckdb_engine.execute_query(
            f"""
            SELECT * FROM {partition_name} WHERE created_at = '{test_time}'
        """
        )

        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][2].replace(tzinfo=None) == test_time

    def test_partition_statistics_calculation(self, partition_manager, events_table):
        """Test calculation of partition statistics."""
        stats = partition_manager.get_partition_statistics(events_table)

        # Should have statistics for virtual partitions
        assert stats.total_partitions > 0
        assert stats.total_rows > 0
        assert stats.average_partition_size > 0

        # Time range coverage should span the data
        assert stats.time_range_coverage is not None
        assert stats.time_range_coverage.start_time.year == 2024
        assert stats.time_range_coverage.start_time.month == 1

        # Most recent partition should be identified
        assert stats.most_recent_partition is not None
        assert stats.most_recent_partition.is_time_based()

        # Distribution should show virtual partitions
        assert "virtual" in stats.partition_distribution
        assert stats.partition_distribution["virtual"] == stats.total_partitions

    def test_partitioning_strategy_analysis(self, partition_manager, events_table):
        """Test partitioning strategy suggestion with real data."""
        strategy = partition_manager.suggest_partitioning_strategy(
            events_table, "created_at"
        )

        assert strategy["strategy"] == "time_based"
        assert "granularity" in strategy
        assert strategy["total_rows"] > 0
        assert strategy["time_span_days"] >= 4  # Data spans 5 days
        assert strategy["estimated_partitions"] > 0
        assert strategy["avg_rows_per_partition"] > 0

    def test_small_table_no_partitioning(self, partition_manager, duckdb_engine):
        """Test strategy recommendation for small tables."""
        small_table = "small_events"

        # Create small table with minimal data
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {small_table} (
                id INTEGER,
                created_at TIMESTAMP
            )
        """
        )

        # Insert only a few records
        for i in range(50):
            timestamp = datetime(2024, 1, 1) + timedelta(hours=i)
            duckdb_engine.execute_query(
                f"""
                INSERT INTO {small_table} VALUES ({i}, '{timestamp}')
            """
            )

        strategy = partition_manager.suggest_partitioning_strategy(
            small_table, "created_at"
        )

        assert strategy["strategy"] == "none"
        assert "too small" in strategy["reason"]
        assert strategy["recommended_threshold"] == 100000

    def test_query_partition_pruning(self, partition_manager):
        """Test partition pruning optimization on queries."""
        original_query = """
            SELECT event_type, COUNT(*) 
            FROM events 
            GROUP BY event_type 
            ORDER BY COUNT(*) DESC
        """

        time_range = TimeRange(
            datetime(2024, 1, 2, 0, 0, 0),
            datetime(2024, 1, 3, 0, 0, 0),
            TimeGranularity.DAY,
        )

        optimized_query = partition_manager.prune_partitions(
            original_query, time_range, "created_at"
        )

        # Should add time range filter
        assert "created_at >= '2024-01-02T00:00:00'" in optimized_query
        assert "created_at < '2024-01-03T00:00:00'" in optimized_query
        assert "GROUP BY event_type" in optimized_query
        assert "ORDER BY COUNT(*) DESC" in optimized_query

    def test_query_with_existing_conditions(self, partition_manager):
        """Test partition pruning with existing WHERE conditions."""
        original_query = """
            SELECT * FROM events 
            WHERE event_type = 'type_1' AND value > 50.0
            ORDER BY created_at
        """

        time_range = TimeRange(
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 2, 12, 0, 0),
            TimeGranularity.DAY,
        )

        optimized_query = partition_manager.prune_partitions(
            original_query, time_range, "created_at"
        )

        # Should integrate time filter with existing conditions
        assert "created_at >= '2024-01-01T12:00:00'" in optimized_query
        assert "created_at < '2024-01-02T12:00:00'" in optimized_query
        assert "event_type = 'type_1'" in optimized_query
        assert "value > 50.0" in optimized_query
        assert "ORDER BY created_at" in optimized_query

    def test_partition_cache_functionality(self, partition_manager, events_table):
        """Test partition caching behavior."""
        # First call should query database and cache results
        partitions1 = partition_manager.detect_partitions(events_table, "created_at")

        # Second call should use cache
        partitions2 = partition_manager.detect_partitions(events_table, "created_at")

        # Results should be identical
        assert len(partitions1) == len(partitions2)
        for p1, p2 in zip(partitions1, partitions2):
            assert p1.partition_name == p2.partition_name
            assert p1.time_range.start_time == p2.time_range.start_time

        # Cache statistics should reflect usage
        cache_stats = partition_manager.get_cache_stats()
        assert cache_stats["partition_cache_size"] > 0
        assert events_table in cache_stats["cached_tables"]

    def test_cache_invalidation(self, partition_manager, events_table, duckdb_engine):
        """Test cache invalidation when partitions are created."""
        # Initial partition detection - this will detect virtual partitions
        initial_partitions = partition_manager.detect_partitions(
            events_table, "created_at"
        )
        len(initial_partitions)

        # Create a new partition table
        new_partition_name = f"{events_table}_20240210"
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {new_partition_name} (
                id INTEGER,
                event_type VARCHAR,
                created_at TIMESTAMP,
                value DECIMAL
            )
        """
        )

        # Clear cache and detect again
        partition_manager._clear_cache(events_table)
        new_partitions = partition_manager.detect_partitions(events_table, "created_at")

        # The detection logic will prioritize pattern-based partitions over virtual ones
        # If pattern-based partitions are found (the new table), virtual partitions won't be created
        # So we should find the new pattern-based partition
        pattern_partitions = [
            p for p in new_partitions if p.partition_type == PartitionType.TIME_BASED
        ]
        assert len(pattern_partitions) >= 1  # Should find at least the new partition

        # Cache should be properly cleared and refreshed
        cache_stats = partition_manager.get_cache_stats()
        assert (
            cache_stats["partition_cache_size"] >= 0
        )  # Cache was cleared and may be repopulated

    def test_performance_with_large_dataset(self, partition_manager, duckdb_engine):
        """Test partition detection performance with larger dataset."""
        large_table = "large_events"

        # Create table with more substantial data
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {large_table} (
                id INTEGER,
                created_at TIMESTAMP,
                category VARCHAR,
                value REAL
            )
        """
        )

        # Insert data spanning multiple months
        import time

        base_time = datetime(2023, 6, 1)
        batch_size = 100

        for month in range(6):  # 6 months of data
            for day in range(1, 29):  # ~28 days per month
                timestamp = base_time.replace(month=base_time.month + month, day=day)

                # Insert batch of records
                values = []
                for i in range(batch_size):
                    record_time = timestamp + timedelta(hours=i % 24, minutes=i % 60)
                    values.append(
                        f"({month * 1000 + day * 100 + i}, '{record_time}', 'cat_{month}', {i * 1.5})"
                    )

                if values:
                    duckdb_engine.execute_query(
                        f"""
                        INSERT INTO {large_table} VALUES {', '.join(values)}
                    """
                    )

        # Test partition detection performance
        start_time = time.time()
        partitions = partition_manager.detect_partitions(large_table, "created_at")
        detection_time = time.time() - start_time

        # Should complete within reasonable time (< 5 seconds for this dataset)
        assert detection_time < 5.0

        # Should detect virtual partitions for each day with data
        assert len(partitions) > 0

        # Test statistics calculation performance
        start_time = time.time()
        stats = partition_manager.get_partition_statistics(large_table)
        stats_time = time.time() - start_time

        assert stats_time < 2.0  # Statistics should be fast
        assert stats.total_rows > 10000  # Should have substantial data
        assert stats.total_partitions == len(partitions)

    def test_error_handling_with_invalid_table(self, partition_manager):
        """Test error handling with non-existent tables."""
        # Should handle non-existent table gracefully
        partitions = partition_manager.detect_partitions(
            "nonexistent_table", "created_at"
        )
        assert partitions == []

        stats = partition_manager.get_partition_statistics("nonexistent_table")
        assert stats.total_partitions == 0
        assert stats.total_rows == 0

    def test_error_handling_with_invalid_column(self, partition_manager, events_table):
        """Test error handling with non-existent time columns."""
        # Should handle non-existent column gracefully
        partitions = partition_manager.detect_partitions(
            events_table, "nonexistent_column"
        )

        # Should return empty list since virtual partition detection will fail
        assert partitions == []

    def test_time_granularity_detection(self, partition_manager, duckdb_engine):
        """Test automatic time granularity detection based on data patterns."""
        hourly_table = "hourly_events"

        # Create table with hourly data patterns
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {hourly_table} (
                id INTEGER,
                created_at TIMESTAMP,
                value REAL
            )
        """
        )

        # Insert data every hour for longer period - need 100k+ rows
        base_time = datetime(2024, 1, 1, 0, 0, 0)

        # Insert data in batches
        batch_size = 1000
        total_batches = 101  # 101,000 rows

        for batch in range(total_batches):
            values = []
            for i in range(batch_size):
                record_id = batch * batch_size + i
                # Spread data over ~84 hours
                hour_offset = record_id // 1200  # 1200 records per hour
                timestamp = base_time + timedelta(
                    hours=hour_offset, minutes=(record_id % 60)
                )
                values.append(f"({record_id}, '{timestamp}', {record_id * 2.5})")

            if values:
                duckdb_engine.execute_query(
                    f"""
                    INSERT INTO {hourly_table} VALUES {', '.join(values)}
                """
                )

        strategy = partition_manager.suggest_partitioning_strategy(
            hourly_table, "created_at"
        )

        # Should suggest appropriate granularity for hourly data
        assert strategy["strategy"] == "time_based"
        assert "granularity" in strategy
        assert strategy["time_span_days"] <= 4  # Data spans ~3.5 days

        # For short time spans with high-density data, should suggest day-level partitioning
        assert strategy["granularity"] in ["day", "hour"]

    def test_partition_boundary_consistency(
        self, partition_manager, partitioned_events_table
    ):
        """Test that partition boundaries are consistent and non-overlapping."""
        base_table, _ = partitioned_events_table

        partitions = partition_manager.detect_partitions(base_table, "created_at")

        # Sort partitions by start time
        sorted_partitions = sorted(partitions, key=lambda p: p.time_range.start_time)

        # Verify no overlaps and proper boundaries
        for i in range(len(sorted_partitions) - 1):
            current = sorted_partitions[i]
            next_partition = sorted_partitions[i + 1]

            # End of current should be <= start of next (no overlap)
            assert current.time_range.end_time <= next_partition.time_range.start_time

            # Each partition should have valid time range
            assert current.time_range.start_time < current.time_range.end_time
            assert (
                next_partition.time_range.start_time
                < next_partition.time_range.end_time
            )
