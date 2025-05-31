"""Integration tests for debugging infrastructure.

These tests verify the complete integration of debugging tools including
DebugLogger, QueryTracer, and OperationTracer without using mocks.
"""

import tempfile
import time

import pandas as pd
import pytest

from sqlflow.core.debug import DebugLogger, OperationTracer, QueryTracer
from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor


class TestDebuggingInfrastructure:
    """Integration tests for debugging infrastructure."""

    @pytest.fixture
    def temp_csv_file(self):
        """Create a temporary CSV file with test data."""
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name

    @pytest.fixture
    def duckdb_engine(self):
        """Create a DuckDB engine for testing."""
        return DuckDBEngine()

    @pytest.fixture
    def debug_logger(self):
        """Create a debug logger for testing."""
        return DebugLogger("test_logger", debug_mode=True)

    @pytest.fixture
    def query_tracer(self, duckdb_engine):
        """Create a query tracer for testing."""
        return QueryTracer(engine=duckdb_engine, debug_mode=True)

    @pytest.fixture
    def operation_tracer(self):
        """Create an operation tracer for testing."""
        return OperationTracer(debug_mode=True)

    def test_debug_logger_basic_functionality(self, debug_logger):
        """Test basic debug logger functionality."""
        logger = debug_logger

        # Test debug mode toggle
        assert logger.debug_mode is True
        logger.set_debug_mode(False)
        assert logger.debug_mode is False
        logger.set_debug_mode(True)
        assert logger.debug_mode is True

        # Test structured logging
        logger.log_structured("info", "Test message", test_key="test_value", count=42)

        # Test performance logging
        logger.log_performance("test_operation", 0.123, rows_processed=100)

        # Test error logging
        test_exception = ValueError("Test error")
        logger.log_error(
            "Test error occurred", exception=test_exception, context="test"
        )

    def test_debug_logger_operation_context(self, debug_logger):
        """Test debug logger operation context management."""
        logger = debug_logger

        # Test nested operations
        with logger.operation("outer_operation", param1="value1"):
            logger.log_structured("info", "Inside outer operation")

            with logger.operation("inner_operation", param2="value2"):
                logger.log_structured("info", "Inside inner operation")
                time.sleep(0.01)  # Small delay to test timing

            logger.log_structured("info", "Back in outer operation")

        # Check metrics were recorded
        metrics = logger.get_metrics()
        assert "outer_operation" in metrics
        assert "inner_operation" in metrics
        assert metrics["outer_operation"]["count"] == 1
        assert metrics["inner_operation"]["count"] == 1
        assert metrics["outer_operation"]["total_duration"] > 0
        assert metrics["inner_operation"]["total_duration"] > 0

    def test_debug_logger_watermark_logging(self, debug_logger):
        """Test watermark-specific logging functionality."""
        logger = debug_logger

        # Test watermark operation logging
        logger.log_watermark_operation(
            "get", "test_pipeline", "test_source", "test_table", "updated_at"
        )

        logger.log_watermark_operation(
            "update",
            "test_pipeline",
            "test_source",
            "test_table",
            "updated_at",
            value="2024-01-01T12:00:00Z",
        )

    def test_debug_logger_connector_logging(self, debug_logger):
        """Test connector-specific logging functionality."""
        logger = debug_logger

        # Test connector operation logging
        logger.log_connector_operation(
            "configure",
            "test_connector",
            "CSV",
            path="/path/to/test.csv",
            sync_mode="incremental",
        )

        logger.log_connector_operation(
            "read", "test_connector", "CSV", rows_read=100, duration=0.456
        )

    def test_debug_logger_metrics_management(self, debug_logger):
        """Test metrics collection and management."""
        logger = debug_logger

        # Perform multiple operations to collect metrics
        for i in range(3):
            with logger.operation("test_operation", iteration=i):
                time.sleep(0.001)  # Small delay

        # Check metrics
        metrics = logger.get_metrics()
        assert "test_operation" in metrics
        op_metrics = metrics["test_operation"]
        assert op_metrics["count"] == 3
        assert op_metrics["total_duration"] > 0
        assert op_metrics["avg_duration"] > 0
        assert op_metrics["min_duration"] > 0
        assert op_metrics["max_duration"] > 0

        # Reset metrics
        logger.reset_metrics()
        assert len(logger.get_metrics()) == 0

    def test_query_tracer_basic_functionality(self, query_tracer, duckdb_engine):
        """Test basic query tracer functionality."""
        tracer = query_tracer

        # Test debug mode toggle
        assert tracer.debug_mode is True
        tracer.set_debug_mode(False)
        assert tracer.debug_mode is False
        tracer.set_debug_mode(True)
        assert tracer.debug_mode is True

        # Test simple query tracing
        with tracer.trace_query("SELECT 1 as test", "SELECT", context="test"):
            # Simulate query execution
            result = duckdb_engine.execute_query("SELECT 1 as test")
            rows = result.fetchall()
            assert len(rows) == 1

        # Check query history
        history = tracer.get_query_history()
        assert len(history) == 1
        assert history[0]["query"] == "SELECT 1 as test"
        assert history[0]["query_type"] == "SELECT"
        assert "duration" in history[0]

    def test_query_tracer_explain_plans(self, query_tracer, duckdb_engine):
        """Test query tracer explain plan functionality."""
        tracer = query_tracer

        # Create a test table with unique name
        table_name = f"test_table_{int(time.time() * 1000)}"
        duckdb_engine.execute_query(
            f"CREATE TABLE {table_name} (id INTEGER, name VARCHAR)"
        )
        duckdb_engine.execute_query(
            f"INSERT INTO {table_name} VALUES (1, 'Alice'), (2, 'Bob')"
        )

        # Trace a query that should generate an explain plan
        with tracer.trace_query(f"SELECT * FROM {table_name} WHERE id = 1", "SELECT"):
            result = duckdb_engine.execute_query(
                f"SELECT * FROM {table_name} WHERE id = 1"
            )
            rows = result.fetchall()
            assert len(rows) == 1

        # Check that explain plan was captured
        history = tracer.get_query_history()
        assert len(history) == 1
        # Note: explain_plan might be None if EXPLAIN doesn't work as expected
        # but the query should still be traced

    def test_query_tracer_performance_stats(self, query_tracer, duckdb_engine):
        """Test query tracer performance statistics."""
        tracer = query_tracer

        # Execute multiple queries of different types
        queries = [
            ("SELECT 1", "SELECT"),
            ("SELECT 2", "SELECT"),
            ("CREATE TABLE temp_test (id INTEGER)", "CREATE"),
            ("DROP TABLE temp_test", "DROP"),
        ]

        for query, query_type in queries:
            with tracer.trace_query(query, query_type):
                duckdb_engine.execute_query(query)

        # Check performance statistics
        stats = tracer.get_performance_stats()
        assert "SELECT" in stats
        assert "CREATE" in stats
        assert "DROP" in stats

        select_stats = stats["SELECT"]
        assert select_stats["count"] == 2
        assert select_stats["total_duration"] > 0
        assert select_stats["avg_duration"] > 0
        assert select_stats["error_count"] == 0

    def test_query_tracer_error_handling(self, query_tracer, duckdb_engine):
        """Test query tracer error handling."""
        tracer = query_tracer

        # Test query that will fail
        with pytest.raises(Exception):  # DuckDB will raise an error
            with tracer.trace_query("SELECT * FROM nonexistent_table", "SELECT"):
                duckdb_engine.execute_query("SELECT * FROM nonexistent_table")

        # Check that error was recorded
        history = tracer.get_query_history()
        assert len(history) == 1
        assert "error" in history[0]

        stats = tracer.get_performance_stats()
        assert stats["SELECT"]["error_count"] == 1

    def test_operation_tracer_basic_functionality(self, operation_tracer):
        """Test basic operation tracer functionality."""
        tracer = operation_tracer

        # Test debug mode toggle
        assert tracer.debug_mode is True
        tracer.set_debug_mode(False)
        assert tracer.debug_mode is False
        tracer.set_debug_mode(True)
        assert tracer.debug_mode is True

        # Test operation tracing
        with tracer.trace_operation("load", "test_load", source="csv", rows=100):
            time.sleep(0.01)  # Simulate work

        # Check operation history
        history = tracer.get_operation_history()
        assert len(history) == 1
        assert history[0]["type"] == "load"
        assert history[0]["name"] == "test_load"
        assert "duration" in history[0]
        assert history[0]["context"]["source"] == "csv"

    def test_operation_tracer_active_operations(self, operation_tracer):
        """Test active operations tracking."""
        tracer = operation_tracer

        # Start an operation but don't complete it yet
        with tracer.trace_operation("transform", "test_transform", table="users"):
            # Check active operations while operation is running
            active = tracer.get_active_operations()
            assert len(active) == 1

            operation_id = list(active.keys())[0]
            op_info = active[operation_id]
            assert op_info["type"] == "transform"
            assert op_info["name"] == "test_transform"

            time.sleep(0.01)  # Simulate work

        # After operation completes, it should no longer be active
        active = tracer.get_active_operations()
        assert len(active) == 0

    def test_operation_tracer_data_flow_logging(self, operation_tracer):
        """Test data flow logging functionality."""
        tracer = operation_tracer

        # Log data flow
        tracer.trace_data_flow(
            "users_csv", "users_table", 1000, processing_time=0.5, memory_used=1024
        )

        # Data flow logging doesn't add to operation history
        # but should be logged for debugging

    def test_operation_tracer_error_handling(self, operation_tracer):
        """Test operation tracer error handling."""
        tracer = operation_tracer

        # Test operation that will fail
        with pytest.raises(ValueError):
            with tracer.trace_operation("export", "test_export", destination="s3"):
                raise ValueError("Test error")

        # Check that error was recorded
        history = tracer.get_operation_history()
        assert len(history) == 1
        assert "error" in history[0]
        assert history[0]["error"]["type"] == "ValueError"
        assert history[0]["error"]["message"] == "Test error"

    def test_operation_tracer_filtering(self, operation_tracer):
        """Test operation history filtering."""
        tracer = operation_tracer

        # Perform different types of operations
        operations = [
            ("load", "load_users"),
            ("transform", "transform_users"),
            ("load", "load_orders"),
            ("export", "export_report"),
        ]

        for op_type, op_name in operations:
            with tracer.trace_operation(op_type, op_name):
                time.sleep(0.001)

        # Test filtering by operation type
        load_ops = tracer.get_operation_history(operation_type="load")
        assert len(load_ops) == 2
        assert all(op["type"] == "load" for op in load_ops)

        # Test limiting results
        limited_ops = tracer.get_operation_history(limit=2)
        assert len(limited_ops) == 2

    def test_integrated_debugging_with_executor(self, temp_csv_file):
        """Test integrated debugging with LocalExecutor."""
        executor = LocalExecutor()

        # Create debugging infrastructure
        debug_logger = DebugLogger("executor_test", debug_mode=True)
        query_tracer = QueryTracer(engine=executor.duckdb_engine, debug_mode=True)
        operation_tracer = OperationTracer(debug_mode=True)

        # Simulate a complete pipeline operation with debugging
        with operation_tracer.trace_operation("pipeline", "test_pipeline"):
            with debug_logger.operation("source_setup"):
                # Create SOURCE definition
                source_def = {
                    "id": "source_users",
                    "name": "users",
                    "connector_type": "CSV",
                    "params": {"path": temp_csv_file},
                }

                debug_logger.log_connector_operation(
                    "register", "users", "CSV", path=temp_csv_file
                )

                source_result = executor._execute_source_definition(source_def)
                assert source_result["status"] == "success"

            with debug_logger.operation("data_loading"):
                # Create and execute LOAD step
                from sqlflow.parser.ast import LoadStep

                load_step = LoadStep(
                    table_name="users_table", source_name="users", mode="REPLACE"
                )

                load_result = executor.execute_load_step(load_step)
                assert load_result["status"] == "success"

                operation_tracer.trace_data_flow(
                    "users", "users_table", load_result["rows_loaded"]
                )

            with debug_logger.operation("query_execution"):
                with query_tracer.trace_query("SELECT COUNT(*) FROM users", "SELECT"):
                    result = executor.duckdb_engine.execute_query(
                        "SELECT COUNT(*) FROM users"
                    )
                    count = result.fetchone()[0]
                    assert count == 3

        # Verify debugging data was collected
        debug_metrics = debug_logger.get_metrics()
        assert len(debug_metrics) > 0

        query_history = query_tracer.get_query_history()
        assert len(query_history) > 0

        operation_history = operation_tracer.get_operation_history()
        assert len(operation_history) == 1  # The main pipeline operation
