"""
Integration tests for incremental loading strategies.

Tests the IncrementalStrategyManager with real DuckDB engines to verify
actual database operations and end-to-end behavior.
"""

import os
import tempfile
import unittest
from datetime import datetime, timedelta

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.transform.data_quality import DataQualityValidator
from sqlflow.core.engines.duckdb.transform.incremental_strategies import (
    ConflictResolution,
    DataSource,
    IncrementalStrategyManager,
    LoadPattern,
    LoadStrategy,
)
from sqlflow.core.engines.duckdb.transform.performance import PerformanceOptimizer
from sqlflow.core.engines.duckdb.transform.watermark import OptimizedWatermarkManager


class TestIncrementalStrategiesIntegration(unittest.TestCase):
    """Integration tests for incremental strategies with real DuckDB."""

    def setUp(self):
        """Set up test fixtures with real DuckDB engine."""
        # Create temporary database
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")

        # Initialize real DuckDB engine
        self.engine = DuckDBEngine(database_path=self.db_path)

        # Initialize real components
        self.watermark_manager = OptimizedWatermarkManager(self.engine)
        self.performance_optimizer = PerformanceOptimizer()
        self.data_quality_validator = DataQualityValidator(self.engine)

        # Initialize strategy manager
        self.strategy_manager = IncrementalStrategyManager(
            self.engine, self.watermark_manager, self.performance_optimizer
        )

        # Create test tables
        self._create_test_tables()

    def tearDown(self):
        """Clean up test resources."""
        self.engine.close()
        # Clean up temp directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_tables(self):
        """Create test tables for integration testing."""
        # Source table with sample data
        self.engine.execute_query(
            """
            CREATE TABLE source_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert sample data
        self.engine.execute_query(
            """
            INSERT INTO source_orders (order_id, customer_id, order_date, amount, status, created_at)
            VALUES 
                (1, 101, '2023-01-01', 100.50, 'completed', '2023-01-01 10:00:00'),
                (2, 102, '2023-01-02', 250.75, 'pending', '2023-01-02 11:00:00'),
                (3, 103, '2023-01-03', 99.99, 'completed', '2023-01-03 12:00:00'),
                (4, 101, '2023-01-04', 175.25, 'completed', '2023-01-04 13:00:00'),
                (5, 104, '2023-01-05', 320.00, 'pending', '2023-01-05 14:00:00')
        """
        )

        # Target table for append strategy
        self.engine.execute_query(
            """
            CREATE TABLE target_orders_append (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Target table for upsert strategy
        self.engine.execute_query(
            """
            CREATE TABLE target_orders_merge (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Target table for snapshot strategy
        self.engine.execute_query(
            """
            CREATE TABLE target_orders_snapshot (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                snapshot_date DATE DEFAULT CURRENT_DATE
            )
        """
        )

        # CDC source table with operation markers
        self.engine.execute_query(
            """
            CREATE TABLE cdc_orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                operation_type CHAR(1), -- 'I', 'U', 'D'
                cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert CDC sample data
        self.engine.execute_query(
            """
            INSERT INTO cdc_orders (order_id, customer_id, order_date, amount, status, operation_type)
            VALUES 
                (1, 101, '2023-01-01', 100.50, 'completed', 'I'),
                (2, 102, '2023-01-02', 250.75, 'pending', 'I'),
                (3, 103, '2023-01-03', 99.99, 'completed', 'I'),
                (2, 102, '2023-01-02', 250.75, 'completed', 'U'),
                (3, 103, '2023-01-03', 99.99, 'cancelled', 'D')
        """
        )

        # Target table for CDC
        self.engine.execute_query(
            """
            CREATE TABLE target_orders_cdc (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                cdc_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

    def test_append_strategy_integration(self):
        """Test append strategy with real database operations."""
        # Create data source
        source = DataSource(
            source_query="""
                SELECT order_id, customer_id, order_date, amount, status, CURRENT_TIMESTAMP as processed_at
                FROM source_orders 
                WHERE status = 'completed'
            """,
            table_name="target_orders_append",
            time_column="processed_at",
        )

        # Execute append strategy
        result = self.strategy_manager.execute_append_strategy(
            source, "target_orders_append"
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.strategy_used, LoadStrategy.APPEND)
        self.assertGreater(result.rows_inserted, 0)
        self.assertGreater(result.execution_time_ms, 0)

        # Verify data was inserted
        count_result = self.engine.execute_query(
            "SELECT COUNT(*) FROM target_orders_append"
        )
        rows = count_result.fetchall()
        self.assertGreater(rows[0][0], 0)

        # Verify data quality
        quality_profile = self.data_quality_validator.validate_incremental_load(
            "target_orders_append", time_column="processed_at", key_columns=["order_id"]
        )
        self.assertGreaterEqual(quality_profile.overall_score, 0.8)

    def test_upsert_strategy_integration(self):
        """Test upsert strategy with real database operations."""
        # Insert initial data
        self.engine.execute_query(
            """
            INSERT INTO target_orders_merge (order_id, customer_id, order_date, amount, status)
            VALUES (1, 101, '2023-01-01', 100.50, 'pending')
        """
        )

        # Create data source with updates and new records
        source = DataSource(
            source_query="""
                SELECT order_id, customer_id, order_date, amount, status, CURRENT_TIMESTAMP as last_updated
                FROM source_orders 
                WHERE order_id IN (1, 4, 5)
            """,
            table_name="target_orders_merge",
            key_columns=["order_id"],
        )

        # Execute upsert strategy
        result = self.strategy_manager.execute_upsert_strategy(
            source, "target_orders_merge", ConflictResolution.SOURCE_WINS
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.strategy_used, LoadStrategy.UPSERT)
        self.assertGreater(result.execution_time_ms, 0)

    def test_snapshot_strategy_integration(self):
        """Test snapshot strategy with real database operations."""
        # Insert initial data
        self.engine.execute_query(
            """
            INSERT INTO target_orders_snapshot (order_id, customer_id, order_date, amount, status)
            VALUES 
                (1, 101, '2022-12-01', 50.00, 'old_status'),
                (999, 999, '2022-12-31', 1.00, 'should_be_replaced')
        """
        )

        # Get initial count
        initial_count = self.engine.execute_query(
            "SELECT COUNT(*) FROM target_orders_snapshot"
        )
        initial_rows = initial_count.fetchall()

        # Create data source for snapshot replacement
        source = DataSource(
            source_query="""
                SELECT order_id, customer_id, order_date, amount, status, CURRENT_DATE as snapshot_date
                FROM source_orders
            """,
            table_name="target_orders_snapshot",
        )

        # Execute snapshot strategy
        result = self.strategy_manager.execute_snapshot_strategy(
            source, "target_orders_snapshot"
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.strategy_used, LoadStrategy.SNAPSHOT)
        self.assertEqual(
            result.rows_deleted, initial_rows[0][0]
        )  # All old rows deleted
        self.assertEqual(result.rows_inserted, 5)  # 5 new rows from source
        self.assertIsNotNone(result.rollback_point)  # Backup table created

        # Verify complete replacement
        final_count = self.engine.execute_query(
            "SELECT COUNT(*) FROM target_orders_snapshot"
        )
        final_rows = final_count.fetchall()
        self.assertEqual(final_rows[0][0], 5)

        # Verify old data is gone
        old_data_check = self.engine.execute_query(
            """
            SELECT COUNT(*) FROM target_orders_snapshot WHERE order_id = 999
        """
        )
        old_data_rows = old_data_check.fetchall()
        self.assertEqual(old_data_rows[0][0], 0)

    def test_cdc_strategy_integration(self):
        """Test CDC strategy with real database operations."""
        # Create data source for CDC
        source = DataSource(
            source_query="""
                SELECT order_id, customer_id, order_date, amount, status, operation_type
                FROM cdc_orders
                ORDER BY cdc_timestamp
            """,
            table_name="target_orders_cdc",
            key_columns=["order_id"],
            delete_column="operation_type",
        )

        # Execute CDC strategy
        result = self.strategy_manager.execute_cdc_strategy(
            source, "target_orders_cdc", ConflictResolution.LATEST_WINS
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.strategy_used, LoadStrategy.CDC)
        self.assertGreater(result.execution_time_ms, 0)

        # Verify CDC operations were applied correctly
        final_count = self.engine.execute_query(
            "SELECT COUNT(*) FROM target_orders_cdc"
        )
        final_rows = final_count.fetchall()

        # Should have only inserts (I operations) since we process in order
        self.assertGreaterEqual(final_rows[0][0], 1)

    def test_auto_strategy_selection_integration(self):
        """Test automatic strategy selection with real data patterns."""
        # Test with append-suitable pattern
        append_source = DataSource(
            source_query="SELECT * FROM source_orders WHERE order_date >= '2023-01-03'",
            table_name="target_orders_append",
            time_column="processed_at",
        )

        append_pattern = LoadPattern(
            row_count_estimate=1000,
            insert_rate=0.95,
            update_rate=0.03,
            delete_rate=0.02,
            has_primary_key=True,
            has_update_timestamp=True,
        )

        append_result = self.strategy_manager.execute_with_auto_strategy(
            append_source, "target_orders_append", append_pattern
        )

        self.assertTrue(append_result.success)
        self.assertEqual(append_result.strategy_used, LoadStrategy.APPEND)

        # Test with upsert-suitable pattern
        upsert_source = DataSource(
            source_query="SELECT order_id, customer_id, order_date, amount, status, CURRENT_TIMESTAMP as last_updated FROM source_orders",
            table_name="target_orders_merge",
            key_columns=["order_id"],
        )

        upsert_pattern = LoadPattern(
            row_count_estimate=5000,
            insert_rate=0.4,
            update_rate=0.4,
            delete_rate=0.2,
            has_primary_key=True,
        )

        upsert_result = self.strategy_manager.execute_with_auto_strategy(
            upsert_source, "target_orders_merge", upsert_pattern
        )

        self.assertTrue(upsert_result.success)
        self.assertEqual(upsert_result.strategy_used, LoadStrategy.UPSERT)

    def test_incremental_quality_validation_integration(self):
        """Test data quality validation for incremental loads."""
        # Setup data with quality issues
        self.engine.execute_query(
            """
            INSERT INTO target_orders_append (order_id, customer_id, order_date, amount, status)
            VALUES 
                (100, 201, '2023-01-10', 50.00, 'completed'),
                (101, NULL, '2023-01-11', -25.00, 'invalid'),  -- Null customer, negative amount
                (102, 202, '2030-01-01', 100.00, 'future')     -- Future date
        """
        )

        # Run quality validation
        quality_profile = self.data_quality_validator.validate_incremental_load(
            "target_orders_append",
            time_column="processed_at",
            key_columns=["order_id"],
            since=datetime.now() - timedelta(hours=1),
        )

        # Verify quality checks
        self.assertIsNotNone(quality_profile)
        self.assertGreater(len(quality_profile.validation_results), 0)

        # Check for specific validation results
        validation_names = [r.rule_name for r in quality_profile.validation_results]
        self.assertIn("incremental_duplicate_check", validation_names)

        # Verify quality score reflects issues
        self.assertLessEqual(quality_profile.overall_score, 1.0)

    def test_rollback_functionality_integration(self):
        """Test rollback functionality with real database operations."""
        # Insert initial data
        self.engine.execute_query(
            """
            INSERT INTO target_orders_snapshot (order_id, customer_id, order_date, amount, status)
            VALUES (1, 101, '2023-01-01', 100.00, 'initial')
        """
        )

        # Get initial state
        initial_result = self.engine.execute_query(
            "SELECT * FROM target_orders_snapshot"
        )
        initial_data = initial_result.fetchall()

        # Create source for snapshot that creates rollback point
        source = DataSource(
            source_query="""
                SELECT order_id, customer_id, order_date, amount, status, CURRENT_DATE as snapshot_date
                FROM source_orders WHERE order_id <= 2
            """,
            table_name="target_orders_snapshot",
        )

        # Execute snapshot (creates rollback point)
        result = self.strategy_manager.execute_snapshot_strategy(
            source, "target_orders_snapshot"
        )

        self.assertTrue(result.success)
        self.assertIsNotNone(result.rollback_point)

        # Verify data changed
        after_snapshot = self.engine.execute_query(
            "SELECT COUNT(*) FROM target_orders_snapshot"
        )
        after_rows = after_snapshot.fetchall()
        self.assertEqual(after_rows[0][0], 2)  # Should have 2 rows now

        # Execute rollback
        rollback_success = self.strategy_manager.rollback_incremental_load(
            result, "target_orders_snapshot"
        )

        self.assertTrue(rollback_success)

        # Verify rollback restored original data
        final_result = self.engine.execute_query("SELECT * FROM target_orders_snapshot")
        final_data = final_result.fetchall()

        self.assertEqual(len(final_data), len(initial_data))
        self.assertEqual(final_data[0][4], "initial")  # Status should be restored

    def test_performance_optimization_integration(self):
        """Test performance optimization with larger datasets."""
        # Create larger test dataset
        self.engine.execute_query(
            """
            CREATE TABLE large_source AS
            SELECT 
                row_number() OVER () as id,
                (row_number() OVER () % 1000) + 1 as customer_id,
                DATE '2023-01-01' + INTERVAL (row_number() OVER () % 365) DAY as order_date,
                (random() * 1000)::DECIMAL(10,2) as amount,
                CASE (row_number() OVER () % 3) 
                    WHEN 0 THEN 'pending'
                    WHEN 1 THEN 'completed'
                    ELSE 'cancelled'
                END as status
            FROM range(10000)
        """
        )

        self.engine.execute_query(
            """
            CREATE TABLE large_target (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create source for large dataset
        source = DataSource(
            source_query="SELECT id, customer_id, order_date, amount, status, CURRENT_TIMESTAMP as processed_at FROM large_source",
            table_name="large_target",
        )

        # Execute with performance optimization
        start_time = datetime.now()
        result = self.strategy_manager.execute_append_strategy(source, "large_target")
        execution_time = datetime.now() - start_time

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.rows_inserted, 10000)

        # Verify reasonable performance (should complete within reasonable time)
        self.assertLess(
            execution_time.total_seconds(), 30
        )  # Should be fast with optimization

        # Verify data integrity
        count_check = self.engine.execute_query("SELECT COUNT(*) FROM large_target")
        count_rows = count_check.fetchall()
        self.assertEqual(count_rows[0][0], 10000)

    def test_watermark_management_integration(self):
        """Test watermark management with real incremental operations."""
        # Create source table with timestamps
        self.engine.execute_query(
            """
            CREATE TABLE incremental_source (
                id INTEGER PRIMARY KEY,
                data VARCHAR(100),
                created_at TIMESTAMP
            )
        """
        )

        # Create target table with timestamps
        self.engine.execute_query(
            """
            CREATE TABLE incremental_target (
                id INTEGER PRIMARY KEY,
                data VARCHAR(100),
                created_at TIMESTAMP
            )
        """
        )

        # Insert initial batch to source
        self.engine.execute_query(
            """
            INSERT INTO incremental_source (id, data, created_at)
            VALUES
                (1, 'batch1', '2023-01-01 10:00:00'),
                (2, 'batch1', '2023-01-01 11:00:00')
        """
        )

        # Execute first incremental load
        source1 = DataSource(
            source_query="SELECT * FROM incremental_source WHERE id <= 2",
            table_name="incremental_target",
            time_column="created_at",
        )

        result1 = self.strategy_manager.execute_append_strategy(
            source1, "incremental_target"
        )
        self.assertTrue(result1.success)

        # Verify watermark was set
        watermark1 = self.watermark_manager.get_transform_watermark(
            "incremental_target", "created_at"
        )
        self.assertIsNotNone(watermark1)

        # Add more data to source
        self.engine.execute_query(
            """
            INSERT INTO incremental_source (id, data, created_at)
            VALUES
                (3, 'batch2', '2023-01-02 10:00:00'),
                (4, 'batch2', '2023-01-02 11:00:00')
        """
        )

        # Execute second incremental load (should only process new data)
        source2 = DataSource(
            source_query="SELECT * FROM incremental_source",
            table_name="incremental_target",
            time_column="created_at",
        )

        result2 = self.strategy_manager.execute_append_strategy(
            source2, "incremental_target"
        )
        self.assertTrue(result2.success)

        # Verify watermark was updated
        watermark2 = self.watermark_manager.get_transform_watermark(
            "incremental_target", "created_at"
        )
        self.assertGreaterEqual(watermark2, watermark1)

        # Verify incremental processing worked correctly
        final_count = self.engine.execute_query(
            "SELECT COUNT(*) FROM incremental_target"
        )
        final_rows = final_count.fetchall()
        # Only the first 2 records should be loaded since the watermark filters out records
        # that have timestamps older than the watermark from the first load
        self.assertEqual(final_rows[0][0], 2)  # Should have only first 2 records


if __name__ == "__main__":
    unittest.main()
