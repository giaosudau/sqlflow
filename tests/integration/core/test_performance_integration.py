"""Integration tests for PerformanceOptimizer with real transform operations."""

import time
from datetime import datetime

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.transform.handlers import TransformModeHandlerFactory
from sqlflow.core.engines.duckdb.transform.performance import PerformanceOptimizer
from sqlflow.parser.ast import SQLBlockStep


class TestPerformanceOptimizerIntegration:
    """Integration tests for performance optimization framework."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def performance_optimizer(self):
        """Create a performance optimizer for testing."""
        return PerformanceOptimizer()

    @pytest.fixture
    def sample_table(self, duckdb_engine):
        """Create a sample table with data for performance testing."""
        table_name = "performance_test_events"

        # Create table
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                event_date DATE,
                created_at TIMESTAMP,
                category VARCHAR,
                value DECIMAL(10,2)
            )
        """
        )

        # Insert sample data (larger dataset for performance testing)
        base_date = datetime(2024, 1, 1)
        for i in range(1000):  # 1000 rows for performance testing
            event_date = base_date.date()
            created_at = base_date
            category = f"category_{i % 10}"
            value = i * 10.5

            duckdb_engine.execute_query(
                f"""
                INSERT INTO {table_name} VALUES (
                    {i}, 
                    '{event_date}', 
                    '{created_at.isoformat()}', 
                    '{category}', 
                    {value}
                )
            """
            )

        return table_name

    def test_bulk_operation_detection(self, performance_optimizer):
        """Test bulk operation threshold detection."""
        # Small operation - should not use bulk
        assert not performance_optimizer.should_use_bulk_operation(5000)

        # Large operation - should use bulk
        assert performance_optimizer.should_use_bulk_operation(15000)

        # Exactly at threshold
        assert performance_optimizer.should_use_bulk_operation(10000)

    def test_insert_optimization(self, performance_optimizer):
        """Test INSERT operation optimization."""
        original_sql = "INSERT INTO test_table SELECT * FROM source_table"

        # Small dataset - no optimization
        optimized_sql, was_optimized = performance_optimizer.optimize_insert_operation(
            original_sql, 5000
        )
        assert not was_optimized
        assert optimized_sql == original_sql

        # Large dataset - should be optimized
        optimized_sql, was_optimized = performance_optimizer.optimize_insert_operation(
            original_sql, 15000
        )
        assert was_optimized
        assert "USE_BULK_INSERT" in optimized_sql

    def test_delete_optimization(self, performance_optimizer):
        """Test DELETE operation optimization."""
        original_sql = "DELETE FROM test_table WHERE date >= '2024-01-01'"

        optimized_sql, was_optimized = performance_optimizer.optimize_delete_operation(
            original_sql, "test_table"
        )

        assert was_optimized
        assert "Optimized DELETE" in optimized_sql
        assert original_sql in optimized_sql

    def test_merge_optimization(self, performance_optimizer):
        """Test MERGE operation optimization."""
        original_sql = "MERGE INTO target USING source ON target.id = source.id"

        # Large dataset - should be optimized
        optimized_sql, was_optimized = performance_optimizer.optimize_merge_operation(
            original_sql, 15000
        )

        assert was_optimized
        assert "Optimized MERGE" in optimized_sql
        assert "15000 rows" in optimized_sql

    def test_columnar_access_optimization(self, performance_optimizer):
        """Test columnar storage access optimization."""
        original_sql = "SELECT id, name, value FROM large_table"
        columns_used = ["id", "name", "value"]

        optimized_sql, was_optimized = performance_optimizer.optimize_columnar_access(
            original_sql, columns_used
        )

        assert was_optimized
        assert "Columnar optimization" in optimized_sql
        assert str(columns_used) in optimized_sql

    def test_memory_estimation(self, performance_optimizer):
        """Test memory usage estimation."""
        # Test with different row counts
        memory_1k = performance_optimizer.estimate_memory_usage(1000)
        memory_10k = performance_optimizer.estimate_memory_usage(10000)
        memory_100k = performance_optimizer.estimate_memory_usage(100000)

        # Memory should scale roughly linearly
        assert memory_10k > memory_1k
        assert memory_100k > memory_10k

        # Should be reasonable estimates (2MB overhead factor)
        assert memory_1k < 10  # Less than 10MB for 1k rows
        assert memory_100k < 1000  # Less than 1GB for 100k rows

    def test_memory_constraints_check(self, performance_optimizer):
        """Test memory constraint checking."""
        # Small operation - should be within limits
        small_check = performance_optimizer.check_memory_constraints(10000)
        assert small_check["within_limits"]
        assert small_check["recommendation"] == "proceed"

        # Very large operation - should exceed limits
        large_check = performance_optimizer.check_memory_constraints(5000000)  # 5M rows
        assert not large_check["within_limits"]
        assert large_check["recommendation"] == "consider_batching"

    def test_performance_monitoring_context(self, performance_optimizer):
        """Test performance monitoring context manager."""
        estimated_rows = 1000

        # Test monitoring context
        with performance_optimizer.monitor_performance(
            "TEST_OPERATION", estimated_rows
        ):
            # Simulate some work
            time.sleep(0.1)

        # Check that metrics were recorded
        metrics = performance_optimizer.metrics.get_summary()
        assert metrics["operation_count"] == 1
        assert metrics["total_rows_processed"] == estimated_rows
        assert metrics["total_execution_time"] >= 0.1
        assert metrics["throughput_rows_per_sec"] > 0

    def test_optimization_recommendations(self, performance_optimizer):
        """Test optimization recommendation system."""
        # Test with large dataset that needs optimization
        sql = "INSERT INTO large_table SELECT * FROM source"
        estimated_rows = 50000

        recommendations = performance_optimizer.get_optimization_recommendations(
            sql, estimated_rows
        )

        assert "recommendations" in recommendations
        assert "optimizations" in recommendations
        assert "memory_analysis" in recommendations

        # Should recommend bulk operation for large dataset
        assert recommendations["optimizations"]["bulk_eligible"]

    def test_performance_report_generation(self, performance_optimizer):
        """Test comprehensive performance report."""
        # Add some mock operations
        performance_optimizer.metrics.record_operation("INSERT", 0.5, 1000, 50.0, True)
        performance_optimizer.metrics.record_operation("DELETE", 0.2, 500, 25.0, False)

        report = performance_optimizer.get_performance_report()

        assert "metrics" in report
        assert "thresholds" in report
        assert "optimization_enabled" in report
        assert "recent_operations" in report

        # Check metrics
        metrics = report["metrics"]
        assert metrics["operation_count"] == 2
        assert metrics["total_rows_processed"] == 1500
        assert metrics["optimized_queries"] == 1

    def test_integration_with_transform_handlers(self, duckdb_engine, sample_table):
        """Test integration with actual transform handlers."""
        # Create an incremental transform handler
        handler = TransformModeHandlerFactory.create("INCREMENTAL", duckdb_engine)

        # Verify it has the performance optimizer
        assert hasattr(handler, "performance_optimizer")
        assert isinstance(handler.performance_optimizer, PerformanceOptimizer)

        # Create a transform step
        transform_step = SQLBlockStep(
            table_name=sample_table,
            sql_query="SELECT * FROM performance_test_events WHERE created_at BETWEEN @start_date AND @end_date",
            mode="INCREMENTAL",
            time_column="created_at",
            lookback="1 DAY",
        )

        # Generate SQL with performance optimization
        sql_statements, parameters = handler.generate_sql_with_params(transform_step)

        # Should have transaction boundaries
        assert len(sql_statements) == 4
        assert sql_statements[0] == "BEGIN TRANSACTION;"
        assert sql_statements[3] == "COMMIT;"

        # Should have optimization comments for larger datasets
        delete_sql = sql_statements[1]
        insert_sql = sql_statements[2]

        # The delete operation should be optimized
        assert "DELETE FROM" in delete_sql

        # The insert operation should contain the substituted query and optimization hints
        assert "INSERT" in insert_sql
        assert "performance_test_events" in insert_sql
        assert "$start_date" in insert_sql  # Should have parameter substitution
        assert "$end_date" in insert_sql

        # Should have optimization hints for bulk operations
        if "USE_BULK_INSERT" in insert_sql:
            # Performance optimization was applied
            assert "/*+ USE_BULK_INSERT */" in insert_sql

    def test_performance_optimizer_disable_enable(self, performance_optimizer):
        """Test enabling/disabling performance optimization."""
        # Initially enabled
        assert performance_optimizer.optimization_enabled

        # Disable optimization
        performance_optimizer.enable_optimization(False)
        assert not performance_optimizer.optimization_enabled

        # Operations should not be optimized when disabled
        sql = "INSERT INTO test_table SELECT * FROM source"
        optimized_sql, was_optimized = performance_optimizer.optimize_insert_operation(
            sql, 15000
        )
        assert not was_optimized

        # Re-enable optimization
        performance_optimizer.enable_optimization(True)
        assert performance_optimizer.optimization_enabled

        # Operations should be optimized again
        optimized_sql, was_optimized = performance_optimizer.optimize_insert_operation(
            sql, 15000
        )
        assert was_optimized

    def test_metrics_reset(self, performance_optimizer):
        """Test metrics reset functionality."""
        # Add some operations
        performance_optimizer.metrics.record_operation("TEST", 1.0, 100, 10.0, True)
        performance_optimizer.metrics.record_operation("TEST", 2.0, 200, 20.0, False)

        # Verify metrics exist
        summary = performance_optimizer.metrics.get_summary()
        assert summary["operation_count"] == 2

        # Reset metrics
        performance_optimizer.reset_metrics()

        # Verify metrics are cleared
        summary = performance_optimizer.metrics.get_summary()
        assert summary["operation_count"] == 0
        assert summary["total_execution_time"] == 0.0

    def test_real_performance_benchmark(
        self, performance_optimizer, duckdb_engine, sample_table
    ):
        """Test actual performance with real data operations."""
        # Test performance monitoring with real operation
        with performance_optimizer.monitor_performance("BENCHMARK_SELECT", 1000):
            result = duckdb_engine.execute_query(
                f"""
                SELECT category, COUNT(*), AVG(value) 
                FROM {sample_table} 
                GROUP BY category
            """
            )
            rows = result.fetchall()
            assert len(rows) > 0  # Should have results

        # Check that performance was monitored
        metrics = performance_optimizer.metrics.get_summary()
        assert metrics["operation_count"] == 1
        assert metrics["throughput_rows_per_sec"] > 0

        # Throughput should be reasonable (at least 1000 rows/sec for this simple operation)
        assert metrics["throughput_rows_per_sec"] >= 1000
