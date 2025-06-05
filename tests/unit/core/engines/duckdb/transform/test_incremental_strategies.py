"""
Unit tests for incremental loading strategies.

Tests the IncrementalStrategyManager and individual strategy classes
with focus on behavior testing and minimal mocking.
"""

import unittest
from datetime import datetime
from unittest.mock import Mock

from sqlflow.core.engines.duckdb.transform.incremental_strategies import (
    AppendStrategy,
    CDCStrategy,
    ConflictResolution,
    DataSource,
    IncrementalStrategyManager,
    LoadPattern,
    LoadResult,
    LoadStrategy,
    QualityReport,
    SnapshotStrategy,
    UpsertStrategy,
)
from sqlflow.core.engines.duckdb.transform.performance import PerformanceOptimizer
from sqlflow.core.engines.duckdb.transform.watermark import OptimizedWatermarkManager


class TestLoadPattern(unittest.TestCase):
    """Test LoadPattern dataclass and its methods."""

    def test_load_pattern_creation(self):
        """Test LoadPattern can be created with default values."""
        pattern = LoadPattern()

        self.assertEqual(pattern.row_count_estimate, 0)
        self.assertEqual(pattern.change_rate, 0.0)
        self.assertEqual(pattern.insert_rate, 0.0)
        self.assertEqual(pattern.update_rate, 0.0)
        self.assertEqual(pattern.delete_rate, 0.0)
        self.assertEqual(pattern.load_frequency, "daily")
        self.assertFalse(pattern.has_primary_key)
        self.assertFalse(pattern.has_update_timestamp)
        self.assertFalse(pattern.has_delete_flag)
        self.assertFalse(pattern.requires_exact_history)
        self.assertFalse(pattern.allows_duplicates)
        self.assertTrue(pattern.needs_rollback)

    def test_load_pattern_with_custom_values(self):
        """Test LoadPattern with custom values."""
        pattern = LoadPattern(
            row_count_estimate=100000,
            change_rate=0.3,
            insert_rate=0.7,
            update_rate=0.2,
            delete_rate=0.1,
            load_frequency="hourly",
            has_primary_key=True,
            has_update_timestamp=True,
            requires_exact_history=True,
        )

        self.assertEqual(pattern.row_count_estimate, 100000)
        self.assertEqual(pattern.change_rate, 0.3)
        self.assertEqual(pattern.insert_rate, 0.7)
        self.assertEqual(pattern.update_rate, 0.2)
        self.assertEqual(pattern.delete_rate, 0.1)
        self.assertEqual(pattern.load_frequency, "hourly")
        self.assertTrue(pattern.has_primary_key)
        self.assertTrue(pattern.has_update_timestamp)
        self.assertTrue(pattern.requires_exact_history)


class TestDataSource(unittest.TestCase):
    """Test DataSource dataclass."""

    def test_data_source_creation(self):
        """Test DataSource creation with required fields."""
        source = DataSource(
            source_query="SELECT * FROM source_table", table_name="target_table"
        )

        self.assertEqual(source.source_query, "SELECT * FROM source_table")
        self.assertEqual(source.table_name, "target_table")
        self.assertEqual(source.key_columns, [])
        self.assertIsNone(source.time_column)
        self.assertIsNone(source.delete_column)
        self.assertEqual(source.parameters, {})

    def test_data_source_with_all_fields(self):
        """Test DataSource with all fields populated."""
        source = DataSource(
            source_query="SELECT * FROM source WHERE updated_at > @start_date",
            table_name="target_table",
            key_columns=["id", "name"],
            time_column="updated_at",
            delete_column="operation_type",
            parameters={"start_date": "2023-01-01"},
        )

        self.assertEqual(source.key_columns, ["id", "name"])
        self.assertEqual(source.time_column, "updated_at")
        self.assertEqual(source.delete_column, "operation_type")
        self.assertEqual(source.parameters["start_date"], "2023-01-01")


class TestLoadResult(unittest.TestCase):
    """Test LoadResult dataclass and its properties."""

    def test_load_result_creation(self):
        """Test LoadResult creation."""
        result = LoadResult(strategy_used=LoadStrategy.APPEND)

        self.assertEqual(result.strategy_used, LoadStrategy.APPEND)
        self.assertEqual(result.rows_inserted, 0)
        self.assertEqual(result.rows_updated, 0)
        self.assertEqual(result.rows_deleted, 0)
        self.assertEqual(result.execution_time_ms, 0)
        self.assertIsNone(result.watermark_updated)
        self.assertEqual(result.data_quality_score, 1.0)
        self.assertEqual(result.validation_errors, [])
        self.assertIsNone(result.rollback_point)
        self.assertEqual(result.rollback_metadata, {})

    def test_total_rows_affected_property(self):
        """Test total_rows_affected property calculation."""
        result = LoadResult(
            strategy_used=LoadStrategy.UPSERT,
            rows_inserted=100,
            rows_updated=50,
            rows_deleted=10,
        )

        self.assertEqual(result.total_rows_affected, 160)

    def test_success_property(self):
        """Test success property based on validation errors."""
        # Successful result
        success_result = LoadResult(strategy_used=LoadStrategy.APPEND)
        self.assertTrue(success_result.success)

        # Failed result
        failed_result = LoadResult(
            strategy_used=LoadStrategy.APPEND,
            validation_errors=["Connection failed", "Data validation error"],
        )
        self.assertFalse(failed_result.success)


class TestQualityReport(unittest.TestCase):
    """Test QualityReport dataclass and its properties."""

    def test_quality_report_creation(self):
        """Test QualityReport creation with defaults."""
        report = QualityReport()

        self.assertEqual(report.overall_score, 1.0)
        self.assertEqual(report.checks_passed, 0)
        self.assertEqual(report.checks_failed, 0)
        self.assertEqual(report.null_rate, 0.0)
        self.assertEqual(report.duplicate_rate, 0.0)
        self.assertFalse(report.schema_drift_detected)
        self.assertEqual(report.data_freshness_hours, 0.0)
        self.assertEqual(report.validation_details, {})
        self.assertEqual(report.recommendations, [])

    def test_passed_property(self):
        """Test passed property based on score and failed checks."""
        # Passing report
        passing_report = QualityReport(overall_score=0.9, checks_failed=0)
        self.assertTrue(passing_report.passed)

        # Failing report - low score
        low_score_report = QualityReport(overall_score=0.7, checks_failed=0)
        self.assertFalse(low_score_report.passed)

        # Failing report - failed checks
        failed_checks_report = QualityReport(overall_score=0.9, checks_failed=1)
        self.assertFalse(failed_checks_report.passed)


class TestAppendStrategy(unittest.TestCase):
    """Test AppendStrategy behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_engine = Mock()
        self.mock_watermark_manager = Mock(spec=OptimizedWatermarkManager)
        self.mock_performance_optimizer = Mock(spec=PerformanceOptimizer)

        self.strategy = AppendStrategy(
            self.mock_engine,
            self.mock_watermark_manager,
            self.mock_performance_optimizer,
        )

    def test_can_handle_append_suitable_pattern(self):
        """Test can_handle returns True for append-suitable patterns."""
        pattern = LoadPattern(
            insert_rate=0.9,  # Mostly inserts
            update_rate=0.05,  # Few updates
            delete_rate=0.05,  # Few deletes
            allows_duplicates=False,
        )

        self.assertTrue(self.strategy.can_handle(pattern))

    def test_can_handle_rejects_unsuitable_pattern(self):
        """Test can_handle returns False for unsuitable patterns."""
        pattern = LoadPattern(
            insert_rate=0.5,  # Not mostly inserts
            update_rate=0.3,  # Too many updates
            delete_rate=0.2,  # Too many deletes
            allows_duplicates=True,
        )

        self.assertFalse(self.strategy.can_handle(pattern))

    def test_estimate_performance(self):
        """Test performance estimation."""
        pattern = LoadPattern(row_count_estimate=10000)

        estimate = self.strategy.estimate_performance(pattern)

        self.assertEqual(estimate["strategy"], "append")
        self.assertEqual(estimate["estimated_time_ms"], 1000.0)  # 10000 * 0.1
        self.assertEqual(estimate["memory_mb"], 10.0)  # max(10, 10000 * 0.001)
        self.assertEqual(estimate["cpu_intensity"], "low")
        self.assertEqual(estimate["io_pattern"], "sequential_write")

    def test_build_incremental_query_without_watermark(self):
        """Test query building without time column or watermark."""
        base_query = "SELECT * FROM source_table"

        result = self.strategy._build_incremental_query(base_query, None, None)

        self.assertEqual(result, base_query)

    def test_build_incremental_query_with_watermark(self):
        """Test query building with time column and watermark."""
        base_query = "SELECT * FROM source_table"
        time_column = "created_at"
        watermark = datetime(2023, 1, 1, 12, 0, 0)

        result = self.strategy._build_incremental_query(
            base_query, time_column, watermark
        )

        expected = f"{base_query} WHERE \"created_at\" > '{watermark.isoformat()}'"
        self.assertEqual(result, expected)

    def test_build_incremental_query_with_existing_where(self):
        """Test query building when WHERE clause already exists."""
        base_query = "SELECT * FROM source_table WHERE status = 'active'"
        time_column = "created_at"
        watermark = datetime(2023, 1, 1, 12, 0, 0)

        result = self.strategy._build_incremental_query(
            base_query, time_column, watermark
        )

        expected = f"{base_query} AND \"created_at\" > '{watermark.isoformat()}'"
        self.assertEqual(result, expected)

    def test_execute_success(self):
        """Test successful append execution."""
        # Setup mocks
        self.mock_watermark_manager.get_transform_watermark.return_value = None

        mock_result = Mock()
        mock_result.rowcount = 100
        self.mock_engine.execute_query.return_value = mock_result

        self.mock_performance_optimizer.optimize_insert_operation.return_value = (
            "OPTIMIZED SQL",
            True,
        )

        # Create test data
        source = DataSource(
            source_query="SELECT * FROM source_table",
            table_name="target_table",
            time_column="created_at",
        )

        # Execute
        result = self.strategy.execute(source, "target_table")

        # Verify results
        self.assertEqual(result.strategy_used, LoadStrategy.APPEND)
        self.assertEqual(result.rows_inserted, 100)
        self.assertTrue(result.success)
        self.assertGreaterEqual(result.execution_time_ms, 0)

        # Verify calls
        self.mock_watermark_manager.get_transform_watermark.assert_called_once_with(
            "target_table", "created_at"
        )
        self.mock_engine.execute_query.assert_called()
        self.mock_watermark_manager.update_watermark.assert_called()

    def test_execute_failure(self):
        """Test append execution failure handling."""
        # Setup mock to raise exception
        self.mock_engine.execute_query.side_effect = Exception("Database error")

        source = DataSource(
            source_query="SELECT * FROM source_table", table_name="target_table"
        )

        # Execute
        result = self.strategy.execute(source, "target_table")

        # Verify failure handling
        self.assertEqual(result.strategy_used, LoadStrategy.APPEND)
        self.assertFalse(result.success)
        self.assertEqual(len(result.validation_errors), 1)
        self.assertIn("Append strategy failed", result.validation_errors[0])


class TestUpsertStrategy(unittest.TestCase):
    """Test UpsertStrategy behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = Mock()
        self.watermark_manager = Mock()
        self.performance_optimizer = Mock()
        self.strategy = UpsertStrategy(
            self.engine,
            self.watermark_manager,
            self.performance_optimizer,
        )

    def test_can_handle_upsert_suitable_pattern(self):
        """Test can_handle returns True for upsert-suitable patterns."""
        pattern = LoadPattern(
            has_primary_key=True,
            update_rate=0.3,  # Significant updates
            insert_rate=0.2,  # Some inserts
            delete_rate=0.1,  # Low deletes
        )

        self.assertTrue(self.strategy.can_handle(pattern))

    def test_can_handle_rejects_no_primary_key(self):
        """Test can_handle returns False when no primary key."""
        pattern = LoadPattern(has_primary_key=False, update_rate=0.3, insert_rate=0.2)

        self.assertFalse(self.strategy.can_handle(pattern))

    def test_estimate_performance(self):
        """Test upsert performance estimation."""
        pattern = LoadPattern(row_count_estimate=5000)

        estimate = self.strategy.estimate_performance(pattern)

        self.assertEqual(estimate["strategy"], "upsert")
        self.assertEqual(estimate["estimated_time_ms"], 2500.0)  # 5000 * 0.5
        self.assertEqual(estimate["memory_mb"], 50.0)  # max(50, 5000 * 0.002)
        self.assertEqual(estimate["cpu_intensity"], "high")
        self.assertEqual(estimate["io_pattern"], "random_read_write")

    def test_generate_upsert_sql(self):
        """Test upsert SQL generation."""
        key_columns = ["id", "name"]
        conflict_resolution = ConflictResolution.SOURCE_WINS

        sql = self.strategy._generate_upsert_sql(
            "temp_table", "target_table", key_columns, conflict_resolution
        )

        # Verify SQL contains expected patterns - updated to match actual implementation
        self.assertIn("INSERT INTO target_table", sql)
        self.assertIn("NOT EXISTS", sql)
        self.assertIn("src.id = tgt.id AND src.name = tgt.name", sql)
        self.assertIn("UPDATE target_table", sql)
        self.assertIn("FROM temp_table src", sql)

    def test_execute_without_key_columns(self):
        """Test execution fails without key columns."""
        source = DataSource(
            source_query="SELECT * FROM source_table",
            table_name="target_table",
            key_columns=[],  # No key columns
        )

        result = self.strategy.execute(source, "target_table")

        self.assertFalse(result.success)
        self.assertIn(
            "Upsert strategy requires key columns", result.validation_errors[0]
        )


class TestSnapshotStrategy(unittest.TestCase):
    """Test SnapshotStrategy behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_engine = Mock()
        self.mock_watermark_manager = Mock(spec=OptimizedWatermarkManager)
        self.mock_performance_optimizer = Mock(spec=PerformanceOptimizer)

        self.strategy = SnapshotStrategy(
            self.mock_engine,
            self.mock_watermark_manager,
            self.mock_performance_optimizer,
        )

    def test_can_handle_snapshot_suitable_pattern(self):
        """Test can_handle returns True for snapshot-suitable patterns."""
        pattern = LoadPattern(
            row_count_estimate=500000,  # Medium size
            change_rate=0.7,  # High change rate
            requires_exact_history=False,
        )

        self.assertTrue(self.strategy.can_handle(pattern))

    def test_can_handle_rejects_large_table(self):
        """Test can_handle returns False for very large tables."""
        pattern = LoadPattern(row_count_estimate=2000000, change_rate=0.7)  # Too large

        self.assertFalse(self.strategy.can_handle(pattern))

    def test_can_handle_rejects_history_requirement(self):
        """Test can_handle returns False when exact history required."""
        pattern = LoadPattern(
            row_count_estimate=500000,
            change_rate=0.7,
            requires_exact_history=True,  # Not suitable for snapshot
        )

        self.assertFalse(self.strategy.can_handle(pattern))

    def test_estimate_performance(self):
        """Test snapshot performance estimation."""
        pattern = LoadPattern(row_count_estimate=100000)

        estimate = self.strategy.estimate_performance(pattern)

        self.assertEqual(estimate["strategy"], "snapshot")
        self.assertEqual(estimate["estimated_time_ms"], 30000.0)  # 100000 * 0.3
        self.assertEqual(estimate["memory_mb"], 300.0)  # max(100, 100000 * 0.003)
        self.assertEqual(estimate["cpu_intensity"], "medium")
        self.assertEqual(estimate["io_pattern"], "bulk_write")


class TestCDCStrategy(unittest.TestCase):
    """Test CDCStrategy behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_engine = Mock()
        self.mock_watermark_manager = Mock(spec=OptimizedWatermarkManager)
        self.mock_performance_optimizer = Mock(spec=PerformanceOptimizer)

        self.strategy = CDCStrategy(
            self.mock_engine,
            self.mock_watermark_manager,
            self.mock_performance_optimizer,
        )

    def test_can_handle_cdc_suitable_pattern(self):
        """Test can_handle returns True for CDC-suitable patterns."""
        pattern = LoadPattern(
            has_delete_flag=True,
            has_primary_key=True,
            delete_rate=0.1,  # Has delete operations
        )

        self.assertTrue(self.strategy.can_handle(pattern))

    def test_can_handle_rejects_missing_requirements(self):
        """Test can_handle returns False when requirements missing."""
        # Missing delete flag
        pattern1 = LoadPattern(
            has_delete_flag=False, has_primary_key=True, delete_rate=0.1
        )
        self.assertFalse(self.strategy.can_handle(pattern1))

        # Missing primary key
        pattern2 = LoadPattern(
            has_delete_flag=True, has_primary_key=False, delete_rate=0.1
        )
        self.assertFalse(self.strategy.can_handle(pattern2))

        # No delete operations
        pattern3 = LoadPattern(
            has_delete_flag=True, has_primary_key=True, delete_rate=0.0
        )
        self.assertFalse(self.strategy.can_handle(pattern3))

    def test_estimate_performance(self):
        """Test CDC performance estimation."""
        pattern = LoadPattern(row_count_estimate=25000)

        estimate = self.strategy.estimate_performance(pattern)

        self.assertEqual(estimate["strategy"], "cdc")
        self.assertEqual(estimate["estimated_time_ms"], 20000.0)  # 25000 * 0.8
        self.assertEqual(estimate["memory_mb"], 25.0)  # max(20, 25000 * 0.001)
        self.assertEqual(estimate["cpu_intensity"], "medium")
        self.assertEqual(estimate["io_pattern"], "mixed_operations")

    def test_execute_without_delete_column(self):
        """Test execution fails without delete column."""
        source = DataSource(
            source_query="SELECT * FROM source_table",
            table_name="target_table",
            delete_column=None,  # No delete column
        )

        result = self.strategy.execute(source, "target_table")

        self.assertFalse(result.success)
        self.assertIn(
            "CDC strategy requires delete column marker", result.validation_errors[0]
        )


class TestIncrementalStrategyManager(unittest.TestCase):
    """Test IncrementalStrategyManager behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_engine = Mock()
        self.mock_watermark_manager = Mock(spec=OptimizedWatermarkManager)
        self.mock_performance_optimizer = Mock(spec=PerformanceOptimizer)

        self.manager = IncrementalStrategyManager(
            self.mock_engine,
            self.mock_watermark_manager,
            self.mock_performance_optimizer,
        )

    def test_initialization(self):
        """Test manager initialization."""
        self.assertEqual(len(self.manager.strategies), 4)
        self.assertIn(LoadStrategy.APPEND, self.manager.strategies)
        self.assertIn(LoadStrategy.UPSERT, self.manager.strategies)
        self.assertIn(LoadStrategy.SNAPSHOT, self.manager.strategies)
        self.assertIn(LoadStrategy.CDC, self.manager.strategies)

        # Verify strategy weights
        self.assertEqual(self.manager.strategy_weights[LoadStrategy.APPEND], 1.0)
        self.assertEqual(self.manager.strategy_weights[LoadStrategy.UPSERT], 0.7)
        self.assertEqual(self.manager.strategy_weights[LoadStrategy.SNAPSHOT], 0.5)
        self.assertEqual(self.manager.strategy_weights[LoadStrategy.CDC], 0.9)

    def test_select_strategy_append_suitable(self):
        """Test strategy selection for append-suitable pattern."""
        from sqlflow.core.engines.duckdb.load.handlers import TableInfo

        table_info = TableInfo(exists=True, schema={"columns": []})
        pattern = LoadPattern(
            insert_rate=0.9, update_rate=0.05, delete_rate=0.05, allows_duplicates=False
        )

        selected = self.manager.select_strategy(table_info, pattern)

        self.assertEqual(selected, LoadStrategy.APPEND)

    def test_select_strategy_upsert_suitable(self):
        """Test strategy selection for upsert-suitable pattern."""
        from sqlflow.core.engines.duckdb.load.handlers import TableInfo

        table_info = TableInfo(exists=True, schema={"columns": []})
        pattern = LoadPattern(
            has_primary_key=True, insert_rate=0.3, update_rate=0.4, delete_rate=0.1
        )

        selected = self.manager.select_strategy(table_info, pattern)

        self.assertEqual(selected, LoadStrategy.UPSERT)

    def test_select_strategy_no_suitable_fallback(self):
        """Test strategy selection fallback when no strategy suitable."""
        from sqlflow.core.engines.duckdb.load.handlers import TableInfo

        table_info = TableInfo(exists=True, schema={"columns": []})
        pattern = LoadPattern(
            # Pattern that doesn't match any strategy well
            insert_rate=0.1,
            update_rate=0.1,
            delete_rate=0.1,
            has_primary_key=False,
            allows_duplicates=True,
        )

        selected = self.manager.select_strategy(table_info, pattern)

        # Should fallback to APPEND
        self.assertEqual(selected, LoadStrategy.APPEND)

    def test_analyze_load_pattern(self):
        """Test load pattern analysis."""
        # Setup mock engine response
        mock_result = Mock()
        mock_result.fetchall.return_value = [(1000,)]
        self.mock_engine.execute_query.return_value = mock_result

        source = DataSource(
            source_query="SELECT * FROM source",
            table_name="target",
            key_columns=["id"],
            time_column="created_at",
        )

        pattern = self.manager._analyze_load_pattern(source, "target")

        self.assertEqual(pattern.row_count_estimate, 1000)
        self.assertTrue(pattern.has_primary_key)
        self.assertTrue(pattern.has_update_timestamp)
        self.assertFalse(pattern.has_delete_flag)
        self.assertEqual(pattern.change_rate, 0.2)
        self.assertEqual(pattern.insert_rate, 0.8)
        self.assertEqual(pattern.update_rate, 0.2)
        self.assertEqual(pattern.delete_rate, 0.0)

    def test_execute_append_strategy(self):
        """Test direct append strategy execution."""
        source = DataSource(source_query="SELECT * FROM source", table_name="target")

        # Mock the strategy execution
        mock_result = LoadResult(strategy_used=LoadStrategy.APPEND, rows_inserted=50)
        self.manager.strategies[LoadStrategy.APPEND].execute = Mock(
            return_value=mock_result
        )

        result = self.manager.execute_append_strategy(source, "target")

        self.assertEqual(result.strategy_used, LoadStrategy.APPEND)
        self.assertEqual(result.rows_inserted, 50)

    def test_execute_upsert_strategy(self):
        """Test direct upsert strategy execution."""
        source = DataSource(
            source_query="SELECT * FROM source", table_name="target", key_columns=["id"]
        )

        # Mock the strategy execution
        mock_result = LoadResult(strategy_used=LoadStrategy.UPSERT, rows_updated=25)
        self.manager.strategies[LoadStrategy.UPSERT].execute = Mock(
            return_value=mock_result
        )

        result = self.manager.execute_upsert_strategy(
            source, "target", ConflictResolution.LATEST_WINS
        )

        self.assertEqual(result.strategy_used, LoadStrategy.UPSERT)
        self.assertEqual(result.rows_updated, 25)

    def test_rollback_incremental_load_success(self):
        """Test successful rollback of incremental load."""
        load_result = LoadResult(
            strategy_used=LoadStrategy.SNAPSHOT, rollback_point="backup_table_123"
        )

        # Mock successful rollback
        self.mock_engine.execute_query.return_value = Mock()

        success = self.manager.rollback_incremental_load(load_result, "target_table")

        self.assertTrue(success)

        # Verify rollback SQL calls
        expected_calls = [
            unittest.mock.call("DELETE FROM target_table"),
            unittest.mock.call(
                "INSERT INTO target_table SELECT * FROM backup_table_123"
            ),
            unittest.mock.call("DROP TABLE backup_table_123"),
        ]
        self.mock_engine.execute_query.assert_has_calls(expected_calls)

    def test_rollback_incremental_load_no_rollback_point(self):
        """Test rollback failure when no rollback point available."""
        load_result = LoadResult(strategy_used=LoadStrategy.APPEND, rollback_point=None)

        success = self.manager.rollback_incremental_load(load_result, "target_table")

        self.assertFalse(success)
        self.mock_engine.execute_query.assert_not_called()

    def test_rollback_incremental_load_failure(self):
        """Test rollback failure handling."""
        load_result = LoadResult(
            strategy_used=LoadStrategy.SNAPSHOT, rollback_point="backup_table_123"
        )

        # Mock rollback failure
        self.mock_engine.execute_query.side_effect = Exception("Rollback failed")

        success = self.manager.rollback_incremental_load(load_result, "target_table")

        self.assertFalse(success)


if __name__ == "__main__":
    unittest.main()
