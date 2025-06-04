"""
Unit tests for partition management framework.

Tests partition detection, time range calculations, and optimization logic
using real DuckDB engine for reliable testing without mocks.
"""

from datetime import datetime, timedelta

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.transform.partitions import (
    PartitionInfo,
    PartitionManager,
    PartitionStatistics,
    PartitionType,
    TimeGranularity,
    TimeRange,
)


class TestTimeRange:
    """Test suite for TimeRange dataclass."""

    def test_time_range_creation(self):
        """Test basic time range creation."""
        start = datetime(2024, 1, 1, 12, 0, 0)
        end = datetime(2024, 1, 2, 12, 0, 0)
        time_range = TimeRange(start, end, TimeGranularity.DAY)

        assert time_range.start_time == start
        assert time_range.end_time == end
        assert time_range.granularity == TimeGranularity.DAY

    def test_invalid_time_range(self):
        """Test invalid time range validation."""
        start = datetime(2024, 1, 2, 12, 0, 0)
        end = datetime(2024, 1, 1, 12, 0, 0)  # End before start

        with pytest.raises(ValueError, match="Invalid time range"):
            TimeRange(start, end, TimeGranularity.DAY)

    def test_time_range_contains(self):
        """Test timestamp containment checking."""
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)
        time_range = TimeRange(start, end, TimeGranularity.DAY)

        # Test various timestamps
        assert time_range.contains(datetime(2024, 1, 1, 12, 0, 0)) is True  # Inside
        assert (
            time_range.contains(datetime(2024, 1, 1, 0, 0, 0)) is True
        )  # Start (inclusive)
        assert (
            time_range.contains(datetime(2024, 1, 2, 0, 0, 0)) is False
        )  # End (exclusive)
        assert time_range.contains(datetime(2023, 12, 31, 23, 0, 0)) is False  # Before
        assert time_range.contains(datetime(2024, 1, 3, 0, 0, 0)) is False  # After

    def test_time_range_overlaps(self):
        """Test time range overlap detection."""
        range1 = TimeRange(
            datetime(2024, 1, 1), datetime(2024, 1, 3), TimeGranularity.DAY
        )
        range2 = TimeRange(
            datetime(2024, 1, 2), datetime(2024, 1, 4), TimeGranularity.DAY
        )
        range3 = TimeRange(
            datetime(2024, 1, 5), datetime(2024, 1, 7), TimeGranularity.DAY
        )

        assert range1.overlaps(range2) is True  # Overlapping
        assert range1.overlaps(range3) is False  # Non-overlapping
        assert range2.overlaps(range3) is False  # Non-overlapping

    def test_partition_name_generation(self):
        """Test partition name generation for different granularities."""
        base_time = datetime(2024, 3, 15, 14, 30, 0)

        test_cases = [
            (TimeGranularity.HOUR, "p_20240315_14"),
            (TimeGranularity.DAY, "p_20240315"),
            (TimeGranularity.MONTH, "p_202403"),
            (TimeGranularity.QUARTER, "p_2024q1"),
            (TimeGranularity.YEAR, "p_2024"),
        ]

        for granularity, expected_name in test_cases:
            time_range = TimeRange(
                base_time, base_time + timedelta(hours=1), granularity
            )
            assert time_range.to_partition_name() == expected_name

    def test_week_partition_name(self):
        """Test week-based partition name generation."""
        # Week 1 of 2024 starts on January 1st (Monday)
        week1_start = datetime(2024, 1, 1)
        time_range = TimeRange(
            week1_start, week1_start + timedelta(days=7), TimeGranularity.WEEK
        )
        assert time_range.to_partition_name() == "p_2024w01"

        # Week 10 of 2024
        week10_start = datetime(2024, 3, 4)  # Approximately week 10
        time_range = TimeRange(
            week10_start, week10_start + timedelta(days=7), TimeGranularity.WEEK
        )
        expected_week = week10_start.isocalendar()[1]
        assert time_range.to_partition_name() == f"p_2024w{expected_week:02d}"


class TestPartitionInfo:
    """Test suite for PartitionInfo dataclass."""

    def test_partition_info_creation(self):
        """Test partition info creation."""
        time_range = TimeRange(
            datetime(2024, 1, 1), datetime(2024, 1, 2), TimeGranularity.DAY
        )

        partition = PartitionInfo(
            table_name="events",
            partition_name="events_p_20240101",
            partition_type=PartitionType.TIME_BASED,
            time_range=time_range,
            column_name="created_at",
            row_count=1000,
        )

        assert partition.table_name == "events"
        assert partition.partition_name == "events_p_20240101"
        assert partition.partition_type == PartitionType.TIME_BASED
        assert partition.is_time_based() is True
        assert partition.row_count == 1000

    def test_non_time_based_partition(self):
        """Test non-time-based partition."""
        partition = PartitionInfo(
            table_name="users",
            partition_name="users_hash_1",
            partition_type=PartitionType.HASH_BASED,
        )

        assert partition.is_time_based() is False


class TestPartitionStatistics:
    """Test suite for PartitionStatistics dataclass."""

    def test_statistics_creation(self):
        """Test partition statistics creation."""
        time_range = TimeRange(
            datetime(2024, 1, 1), datetime(2024, 1, 31), TimeGranularity.MONTH
        )

        stats = PartitionStatistics(
            total_partitions=30,
            total_rows=1000000,
            total_size_bytes=50000000,
            average_partition_size=33333,
            time_range_coverage=time_range,
        )

        assert stats.total_partitions == 30
        assert stats.total_rows == 1000000
        assert stats.average_partition_size == 33333
        assert stats.time_range_coverage == time_range
        assert isinstance(stats.partition_distribution, dict)

    def test_statistics_with_distribution(self):
        """Test statistics with partition distribution."""
        distribution = {"time_based": 25, "hash_based": 5}
        stats = PartitionStatistics(
            total_partitions=30,
            total_rows=1000000,
            total_size_bytes=50000000,
            average_partition_size=33333,
            partition_distribution=distribution,
        )

        assert stats.partition_distribution == distribution


class TestPartitionManager:
    """Test suite for PartitionManager using real DuckDB engine."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def partition_manager(self, duckdb_engine):
        """Create partition manager with real DuckDB engine."""
        return PartitionManager(duckdb_engine)

    @pytest.fixture
    def test_table(self, duckdb_engine):
        """Create a test table with data."""
        table_name = "test_events"

        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                created_at TIMESTAMP,
                event_type VARCHAR,
                value DECIMAL
            )
        """
        )

        # Insert test data - need enough for partitioning strategy (100k+ rows)
        base_time = datetime(2024, 1, 1, 12, 0, 0)

        # Insert data in batches for efficiency
        batch_size = 1000
        total_batches = 101  # 101,000 rows total

        for batch in range(total_batches):
            values = []
            for i in range(batch_size):
                record_id = batch * batch_size + i
                # Spread data over ~90 days
                hours_offset = record_id // 50  # 50 records per hour
                timestamp = base_time + timedelta(hours=hours_offset)
                values.append(
                    f"({record_id}, '{timestamp}', 'type_{record_id % 5}', {record_id * 10.5})"
                )

            if values:
                duckdb_engine.execute_query(
                    f"""
                    INSERT INTO {table_name} VALUES {', '.join(values)}
                """
                )

        return table_name

    @pytest.fixture
    def partitioned_table(self, duckdb_engine):
        """Create partitioned tables for testing."""
        base_table = "partitioned_events"

        # Create base table
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {base_table} (
                id INTEGER,
                created_at TIMESTAMP,
                event_type VARCHAR
            )
        """
        )

        # Create partition tables
        partition_names = []
        for day in range(3):
            date_str = f"2024010{day+1}"
            partition_name = f"{base_table}_{date_str}"
            partition_names.append(partition_name)

            duckdb_engine.execute_query(
                f"""
                CREATE TABLE {partition_name} (
                    id INTEGER,
                    created_at TIMESTAMP,
                    event_type VARCHAR
                )
            """
            )

        return base_table, partition_names

    def test_manager_initialization(self, duckdb_engine):
        """Test partition manager initialization."""
        manager = PartitionManager(duckdb_engine, TimeGranularity.HOUR)

        assert manager.engine == duckdb_engine
        assert manager.default_granularity == TimeGranularity.HOUR
        assert len(manager._partition_cache) == 0
        assert len(manager._statistics_cache) == 0

    def test_virtual_partition_detection(self, partition_manager, test_table):
        """Test virtual partition detection from data distribution."""
        partitions = partition_manager.detect_partitions(test_table, "created_at")

        # Should detect virtual partitions based on data
        assert len(partitions) > 0

        for partition in partitions:
            assert partition.partition_type == PartitionType.VIRTUAL
            assert partition.is_time_based() is True
            assert partition.time_range is not None
            assert partition.row_count is not None

    def test_pattern_based_partition_detection(
        self, partition_manager, partitioned_table
    ):
        """Test pattern-based partition detection."""
        base_table, partition_names = partitioned_table

        partitions = partition_manager.detect_partitions(base_table, "created_at")

        # Should detect partition tables
        assert len(partitions) == len(partition_names)

        for partition in partitions:
            assert partition.partition_type == PartitionType.TIME_BASED
            assert partition.table_name == base_table
            assert partition.partition_name in partition_names

    def test_partition_name_parsing(self, partition_manager):
        """Test partition name parsing logic."""
        test_cases = [
            ("events_20240101", "events", "created_at", True),
            ("users_p_202401", "users", "signup_date", True),
            ("logs_2024", "logs", "timestamp", True),
            ("invalid_table", "events", "created_at", False),
        ]

        for table_full_name, base_table, time_column, should_parse in test_cases:
            result = partition_manager._parse_partition_name(
                table_full_name, base_table, time_column
            )

            if should_parse:
                assert result is not None
                assert result.partition_name == table_full_name
                assert result.table_name == base_table
                assert result.partition_type == PartitionType.TIME_BASED
            else:
                assert result is None

    def test_file_partition_parsing(self, partition_manager):
        """Test file-based partition parsing."""
        test_cases = [
            "/data/events/year=2024/month=01/day=15/file.parquet",
            "/data/events/dt=2024-01-15/file.parquet",
            "/data/events/date=20240115/file.parquet",
            "/data/events/20240115_data.parquet",
        ]

        for file_path in test_cases:
            result = partition_manager._parse_file_partition(
                file_path, "events", "created_at"
            )

            assert result is not None
            assert result.table_name == "events"
            assert result.partition_type == PartitionType.TIME_BASED
            assert result.time_range is not None
            assert result.time_range.start_time.year == 2024
            assert result.time_range.start_time.month == 1
            assert result.time_range.start_time.day == 15

    def test_invalid_file_paths(self, partition_manager):
        """Test file paths without date patterns."""
        invalid_paths = [
            "/data/events/file.parquet",
            "/data/events/random_name.csv",
            "/data/no_date_pattern/data.json",
        ]

        for file_path in invalid_paths:
            result = partition_manager._parse_file_partition(
                file_path, "events", "created_at"
            )
            assert result is None

    def test_partition_creation(self, partition_manager, test_table, duckdb_engine):
        """Test partition table creation."""
        time_range = TimeRange(
            datetime(2024, 2, 1, 0, 0, 0),
            datetime(2024, 2, 2, 0, 0, 0),
            TimeGranularity.DAY,
        )

        partition_name = partition_manager.create_partition(
            test_table, time_range, "created_at"
        )

        # Verify partition was created
        assert partition_name == f"{test_table}_p_20240201"

        # Check table exists with correct schema
        result = duckdb_engine.execute_query(
            f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{partition_name}'
            ORDER BY ordinal_position
        """
        )

        columns = [row[0] for row in result.fetchall()]
        expected_columns = ["id", "created_at", "event_type", "value"]
        assert columns == expected_columns

    def test_query_partition_pruning(self, partition_manager):
        """Test partition pruning query optimization."""
        original_query = "SELECT * FROM events ORDER BY created_at"
        time_range = TimeRange(
            datetime(2024, 1, 1), datetime(2024, 1, 2), TimeGranularity.DAY
        )

        optimized_query = partition_manager.prune_partitions(
            original_query, time_range, "created_at"
        )

        assert "created_at >= '2024-01-01T00:00:00'" in optimized_query
        assert "created_at < '2024-01-02T00:00:00'" in optimized_query
        assert "ORDER BY" in optimized_query

    def test_query_with_existing_where_clause(self, partition_manager):
        """Test partition pruning with existing WHERE clause."""
        original_query = "SELECT * FROM events WHERE status = 'active'"
        time_range = TimeRange(
            datetime(2024, 1, 1), datetime(2024, 1, 2), TimeGranularity.DAY
        )

        optimized_query = partition_manager.prune_partitions(
            original_query, time_range, "created_at"
        )

        assert "WHERE" in optimized_query
        assert "created_at >= '2024-01-01T00:00:00'" in optimized_query
        assert "status = 'active'" in optimized_query

    def test_partition_statistics_calculation(self, partition_manager, test_table):
        """Test partition statistics calculation."""
        stats = partition_manager.get_partition_statistics(test_table)

        # Should have valid statistics
        assert stats.total_partitions > 0
        assert stats.total_rows > 0
        assert stats.average_partition_size > 0

        # Should have time range coverage for virtual partitions
        assert stats.time_range_coverage is not None
        assert stats.most_recent_partition is not None
        assert "virtual" in stats.partition_distribution

    def test_partitioning_strategy_suggestion(self, partition_manager, test_table):
        """Test partitioning strategy suggestion."""
        strategy = partition_manager.suggest_partitioning_strategy(
            test_table, "created_at"
        )

        assert strategy["strategy"] == "time_based"
        assert "granularity" in strategy
        assert strategy["total_rows"] > 0
        assert strategy["time_span_days"] > 0
        assert "estimated_partitions" in strategy

    def test_small_table_strategy(self, partition_manager, duckdb_engine):
        """Test strategy for small tables."""
        small_table = "small_table"

        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {small_table} (
                id INTEGER,
                created_at TIMESTAMP
            )
        """
        )

        # Insert minimal data
        for i in range(10):
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

    def test_cache_functionality(self, partition_manager, test_table):
        """Test partition caching behavior."""
        # First call populates cache
        partitions1 = partition_manager.detect_partitions(test_table, "created_at")

        # Second call uses cache
        partitions2 = partition_manager.detect_partitions(test_table, "created_at")

        # Results should be identical
        assert len(partitions1) == len(partitions2)

        # Cache should contain data
        cache_stats = partition_manager.get_cache_stats()
        assert cache_stats["partition_cache_size"] > 0
        assert test_table in cache_stats["cached_tables"]

    def test_cache_clearing(self, partition_manager, test_table):
        """Test cache clearing functionality."""
        # Populate cache
        partition_manager.detect_partitions(test_table, "created_at")
        partition_manager.get_partition_statistics(test_table)

        # Verify cache has data
        stats_before = partition_manager.get_cache_stats()
        assert stats_before["partition_cache_size"] > 0
        assert stats_before["statistics_cache_size"] > 0

        # Clear specific table cache
        partition_manager._clear_cache(test_table)

        # Cache should be cleared
        stats_after = partition_manager.get_cache_stats()
        assert stats_after["partition_cache_size"] == 0
        assert stats_after["statistics_cache_size"] == 0

    def test_error_handling_with_invalid_table(self, partition_manager):
        """Test error handling with non-existent tables."""
        # Should handle gracefully
        partitions = partition_manager.detect_partitions(
            "nonexistent_table", "created_at"
        )
        assert partitions == []

        stats = partition_manager.get_partition_statistics("nonexistent_table")
        assert stats.total_partitions == 0
        assert stats.total_rows == 0

    def test_time_granularity_edge_cases(self):
        """Test edge cases in time granularity calculations."""
        # Month boundary crossing
        dec_time = datetime(2023, 12, 15)
        time_range = TimeRange(
            dec_time, dec_time + timedelta(days=1), TimeGranularity.MONTH
        )
        partition_name = time_range.to_partition_name()
        assert "202312" in partition_name

        # Year boundary crossing
        year_end = datetime(2023, 12, 31, 23, 59, 59)
        time_range = TimeRange(
            year_end, year_end + timedelta(seconds=1), TimeGranularity.YEAR
        )
        partition_name = time_range.to_partition_name()
        assert "2023" in partition_name
