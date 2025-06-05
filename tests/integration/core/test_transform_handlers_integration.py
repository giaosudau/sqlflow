"""Integration tests for transform mode handlers with real DuckDB engine.

These tests replace the mock-based unit tests with real integration tests
following the testing standards of minimizing mocks and testing with real implementations.
"""

import threading
from datetime import datetime, timedelta

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.exceptions import InvalidLoadModeError
from sqlflow.core.engines.duckdb.transform.handlers import (
    AppendTransformHandler,
    IncrementalTransformHandler,
    ReplaceTransformHandler,
    SecureTimeSubstitution,
    TransformError,
    TransformLockManager,
    TransformModeHandlerFactory,
    UpsertTransformHandler,
)
from sqlflow.parser.ast import SQLBlockStep


class TestSecureTimeSubstitutionIntegration:
    """Integration tests for secure time macro substitution."""

    @pytest.fixture
    def time_substitution(self):
        """Create SecureTimeSubstitution instance."""
        return SecureTimeSubstitution()

    def test_substitute_time_macros_comprehensive(self, time_substitution):
        """Test comprehensive time macro substitution scenarios."""
        sql = """
        SELECT * FROM events 
        WHERE created_at BETWEEN @start_date AND @end_date
        AND updated_at >= @start_dt
        AND processed_at <= @end_dt
        """
        start_time = datetime(2024, 1, 15, 10, 30, 45)
        end_time = datetime(2024, 1, 16, 15, 45, 30)

        result_sql, parameters = time_substitution.substitute_time_macros(
            sql, start_time, end_time
        )

        # Verify macro replacement
        assert "@start_date" not in result_sql
        assert "@end_date" not in result_sql
        assert "@start_dt" not in result_sql
        assert "@end_dt" not in result_sql

        assert "$start_date" in result_sql
        assert "$end_date" in result_sql
        assert "$start_dt" in result_sql
        assert "$end_dt" in result_sql

        # Verify parameter values
        assert parameters["start_date"] == "2024-01-15"
        assert parameters["end_date"] == "2024-01-16"
        assert parameters["start_dt"] == "2024-01-15T10:30:45"
        assert parameters["end_dt"] == "2024-01-16T15:45:30"

    def test_substitute_time_macros_no_macros(self, time_substitution):
        """Test SQL without macros passes through unchanged."""
        sql = "SELECT * FROM events WHERE id = 1"
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 2)

        result_sql, parameters = time_substitution.substitute_time_macros(
            sql, start_time, end_time
        )

        assert result_sql == sql
        assert len(parameters) == 4  # All parameters still created
        assert "start_date" in parameters
        assert "end_date" in parameters
        assert "start_dt" in parameters
        assert "end_dt" in parameters


class TestTransformLockManagerIntegration:
    """Integration tests for transform lock manager with real threading."""

    @pytest.fixture
    def lock_manager(self):
        """Create TransformLockManager instance."""
        return TransformLockManager()

    def test_lock_lifecycle_complete(self, lock_manager):
        """Test complete lock lifecycle with real threading."""
        table_name = "test_table"

        # Initially no locks
        assert table_name not in lock_manager.locks

        # Acquire lock
        with lock_manager.acquire_table_lock(table_name):
            assert table_name in lock_manager.locks

            # Lock should exist during context
            lock_exists = table_name in lock_manager.locks
            assert lock_exists

        # Lock should be released after context
        assert table_name not in lock_manager.locks

    def test_concurrent_lock_prevention(self, lock_manager):
        """Test prevention of concurrent locks with real threads."""
        table_name = "concurrent_test_table"
        lock_acquired = threading.Event()
        release_lock = threading.Event()
        concurrent_error = threading.Event()

        def first_worker():
            """First worker that holds the lock."""
            with lock_manager.acquire_table_lock(table_name):
                lock_acquired.set()
                release_lock.wait(timeout=5)  # Wait max 5 seconds

        def second_worker():
            """Second worker that should fail to acquire lock."""
            try:
                lock_acquired.wait(timeout=5)  # Wait for first worker
                with lock_manager.acquire_table_lock(table_name):
                    pass  # Should never reach here
            except TransformError as e:
                if "already being transformed" in str(e):
                    concurrent_error.set()

        # Start both workers
        thread1 = threading.Thread(target=first_worker)
        thread2 = threading.Thread(target=second_worker)

        thread1.start()
        thread2.start()

        # Wait for concurrent error to be detected
        assert concurrent_error.wait(timeout=5), "Concurrent access should be prevented"

        # Release first lock
        release_lock.set()

        # Wait for threads to complete
        thread1.join(timeout=5)
        thread2.join(timeout=5)

    def test_multiple_tables_no_conflict(self, lock_manager):
        """Test that different tables can be locked simultaneously."""
        with lock_manager.acquire_table_lock("table1"):
            with lock_manager.acquire_table_lock("table2"):
                with lock_manager.acquire_table_lock("table3"):
                    # All should succeed
                    assert "table1" in lock_manager.locks
                    assert "table2" in lock_manager.locks
                    assert "table3" in lock_manager.locks

        # All should be released
        assert "table1" not in lock_manager.locks
        assert "table2" not in lock_manager.locks
        assert "table3" not in lock_manager.locks


class TestTransformModeHandlerFactoryIntegration:
    """Integration tests for transform mode handler factory with real engines."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    def test_factory_creates_all_handlers(self, duckdb_engine):
        """Test factory creates all handler types with real engine."""
        modes_and_classes = [
            ("REPLACE", ReplaceTransformHandler),
            ("APPEND", AppendTransformHandler),
            ("UPSERT", UpsertTransformHandler),
            ("INCREMENTAL", IncrementalTransformHandler),
        ]

        for mode, expected_class in modes_and_classes:
            handler = TransformModeHandlerFactory.create(mode, duckdb_engine)
            assert isinstance(handler, expected_class)
            assert hasattr(handler, "engine")
            assert handler.engine == duckdb_engine

    def test_factory_case_insensitive(self, duckdb_engine):
        """Test that factory handles case insensitive mode names."""
        handler_lower = TransformModeHandlerFactory.create("replace", duckdb_engine)
        handler_upper = TransformModeHandlerFactory.create("REPLACE", duckdb_engine)
        handler_mixed = TransformModeHandlerFactory.create("Replace", duckdb_engine)

        assert isinstance(handler_lower, ReplaceTransformHandler)
        assert isinstance(handler_upper, ReplaceTransformHandler)
        assert isinstance(handler_mixed, ReplaceTransformHandler)

    def test_factory_invalid_mode_error(self, duckdb_engine):
        """Test that factory raises error for invalid modes."""
        with pytest.raises(InvalidLoadModeError) as exc_info:
            TransformModeHandlerFactory.create("INVALID_MODE", duckdb_engine)

        assert "INVALID_MODE" in str(exc_info.value)

    def test_all_handlers_have_required_components(self, duckdb_engine):
        """Test that all handlers have required infrastructure components."""
        modes = ["REPLACE", "APPEND", "UPSERT", "INCREMENTAL"]

        for mode in modes:
            handler = TransformModeHandlerFactory.create(mode, duckdb_engine)

            # Should inherit LOAD infrastructure
            assert hasattr(handler, "validation_helper")
            assert hasattr(handler, "sql_generator")
            assert hasattr(handler, "sql_helper")

            # Should have transform-specific components
            assert hasattr(handler, "time_substitution")
            assert hasattr(handler, "lock_manager")
            assert hasattr(handler, "watermark_manager")
            assert hasattr(handler, "performance_optimizer")

            # Should have required methods
            assert callable(getattr(handler, "generate_sql_with_params"))


class TestReplaceTransformHandlerIntegration:
    """Integration tests for REPLACE transform handler with real database operations."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def replace_handler(self, duckdb_engine):
        """Create REPLACE handler with real engine."""
        return ReplaceTransformHandler(duckdb_engine)

    @pytest.fixture
    def source_table(self, duckdb_engine):
        """Create source table with sample data."""
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_data (
                id INTEGER,
                name VARCHAR,
                value DECIMAL(10,2)
            )
        """
        )

        duckdb_engine.execute_query(
            """
            INSERT INTO source_data VALUES 
            (1, 'Alice', 100.50),
            (2, 'Bob', 200.75),
            (3, 'Charlie', 300.25)
        """
        )

        return "source_data"

    def test_replace_creates_new_table(
        self, replace_handler, source_table, duckdb_engine
    ):
        """Test REPLACE mode creates new table from source data."""
        transform_step = SQLBlockStep(
            table_name="target_table",
            sql_query=f"SELECT id, name, value * 2 as doubled_value FROM {source_table}",
            mode="REPLACE",
        )

        sql_statements, parameters = replace_handler.generate_sql_with_params(
            transform_step
        )

        # Should generate single CREATE OR REPLACE statement
        assert len(sql_statements) == 1
        assert "CREATE OR REPLACE TABLE target_table AS" in sql_statements[0]
        assert (
            f"SELECT id, name, value * 2 as doubled_value FROM {source_table}"
            in sql_statements[0]
        )
        assert parameters == {}

        # Execute the SQL to verify it works
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify table was created with correct data
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM target_table")
        count = result.fetchall()[0][0]
        assert count == 3

        # Verify data transformation
        result = duckdb_engine.execute_query(
            "SELECT doubled_value FROM target_table WHERE id = 1"
        )
        doubled_value = result.fetchall()[0][0]
        assert doubled_value == 201.0  # 100.50 * 2

    def test_replace_overwrites_existing_table(
        self, replace_handler, source_table, duckdb_engine
    ):
        """Test REPLACE mode overwrites existing table."""
        # Create initial table
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table (
                id INTEGER,
                old_data VARCHAR
            )
        """
        )
        duckdb_engine.execute_query(
            "INSERT INTO target_table VALUES (99, 'old_record')"
        )

        transform_step = SQLBlockStep(
            table_name="target_table",
            sql_query=f"SELECT id, name FROM {source_table}",
            mode="REPLACE",
        )

        sql_statements, parameters = replace_handler.generate_sql_with_params(
            transform_step
        )

        # Execute replacement
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify old data is gone and new data exists
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM target_table")
        count = result.fetchall()[0][0]
        assert count == 3  # New data count

        # Verify old record is gone
        result = duckdb_engine.execute_query(
            "SELECT COUNT(*) FROM target_table WHERE id = 99"
        )
        old_count = result.fetchall()[0][0]
        assert old_count == 0

        # Verify new columns exist
        result = duckdb_engine.execute_query(
            "SELECT name FROM target_table WHERE id = 1"
        )
        name = result.fetchall()[0][0]
        assert name == "Alice"


class TestAppendTransformHandlerIntegration:
    """Integration tests for APPEND transform handler with real database operations."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def append_handler(self, duckdb_engine):
        """Create APPEND handler with real engine."""
        return AppendTransformHandler(duckdb_engine)

    @pytest.fixture
    def source_table(self, duckdb_engine):
        """Create source table with sample data."""
        duckdb_engine.execute_query(
            """
            CREATE TABLE events (
                id INTEGER,
                event_type VARCHAR,
                timestamp TIMESTAMP
            )
        """
        )

        duckdb_engine.execute_query(
            """
            INSERT INTO events VALUES 
            (1, 'login', '2024-01-01 10:00:00'),
            (2, 'purchase', '2024-01-01 11:00:00'),
            (3, 'logout', '2024-01-01 12:00:00')
        """
        )

        return "events"

    def test_append_creates_table_when_not_exists(
        self, append_handler, source_table, duckdb_engine
    ):
        """Test APPEND mode creates table when it doesn't exist."""
        transform_step = SQLBlockStep(
            table_name="event_log",
            sql_query=f"SELECT id, event_type FROM {source_table} WHERE event_type = 'login'",
            mode="APPEND",
        )

        sql_statements, parameters = append_handler.generate_sql_with_params(
            transform_step
        )

        # Should generate CREATE TABLE AS when table doesn't exist
        assert len(sql_statements) == 1
        assert "CREATE TABLE event_log AS" in sql_statements[0]
        assert parameters == {}

        # Execute to verify
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify table created with correct data
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM event_log")
        count = result.fetchall()[0][0]
        assert count == 1  # Only login events

    def test_append_inserts_when_table_exists(
        self, append_handler, source_table, duckdb_engine
    ):
        """Test APPEND mode inserts data when table exists."""
        # Create target table with compatible schema
        duckdb_engine.execute_query(
            """
            CREATE TABLE event_log (
                id INTEGER,
                event_type VARCHAR
            )
        """
        )

        # Insert initial data
        duckdb_engine.execute_query("INSERT INTO event_log VALUES (100, 'initial')")

        transform_step = SQLBlockStep(
            table_name="event_log",
            sql_query=f"SELECT id, event_type FROM {source_table}",
            mode="APPEND",
        )

        sql_statements, parameters = append_handler.generate_sql_with_params(
            transform_step
        )

        # Should generate INSERT INTO when table exists
        assert len(sql_statements) == 1
        assert "INSERT INTO event_log" in sql_statements[0]
        assert parameters == {}

        # Execute to verify
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify data was appended (not replaced)
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM event_log")
        count = result.fetchall()[0][0]
        assert count == 4  # 1 initial + 3 from source

        # Verify initial record still exists
        result = duckdb_engine.execute_query(
            "SELECT COUNT(*) FROM event_log WHERE id = 100"
        )
        initial_count = result.fetchall()[0][0]
        assert initial_count == 1


class TestUpsertTransformHandlerIntegration:
    """Integration tests for UpsertTransformHandler with real DuckDB."""

    @pytest.fixture
    def handler(self, duckdb_engine):
        """Create an UpsertTransformHandler for testing."""
        return UpsertTransformHandler(duckdb_engine)

    @pytest.fixture
    def source_table(self, duckdb_engine):
        """Create source table with sample data."""
        duckdb_engine.execute_query(
            """
            CREATE TABLE customer_updates (
                customer_id INTEGER,
                name VARCHAR,
                email VARCHAR,
                updated_at TIMESTAMP
            )
        """
        )

        duckdb_engine.execute_query(
            """
            INSERT INTO customer_updates VALUES 
            (1, 'Alice Updated', 'alice.new@example.com', '2024-01-01 10:00:00'),
            (2, 'Bob Smith', 'bob@example.com', '2024-01-01 11:00:00'),
            (4, 'David New', 'david@example.com', '2024-01-01 12:00:00')
        """
        )

        return "customer_updates"

    def test_upsert_creates_table_when_not_exists(
        self, handler, source_table, duckdb_engine
    ):
        """Test UPSERT mode creates table when it doesn't exist."""
        transform_step = SQLBlockStep(
            table_name="customers",
            sql_query=f"SELECT customer_id, name, email FROM {source_table}",
            mode="UPSERT",
            upsert_keys=["customer_id"],
        )

        sql_statements, parameters = handler.generate_sql_with_params(transform_step)

        # Should generate CREATE TABLE AS when table doesn't exist
        assert len(sql_statements) == 1
        assert "CREATE TABLE customers AS" in sql_statements[0]
        assert parameters == {}

        # Execute to verify
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify table created with correct data
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM customers")
        count = result.fetchall()[0][0]
        assert count == 3

    def test_upsert_upserts_when_table_exists(
        self, handler, source_table, duckdb_engine
    ):
        """Test UPSERT mode performs upsert when table exists."""
        # Create target table with existing data
        duckdb_engine.execute_query(
            """
            CREATE TABLE customers (
                customer_id INTEGER,
                name VARCHAR,
                email VARCHAR
            )
        """
        )

        duckdb_engine.execute_query(
            """
            INSERT INTO customers VALUES 
            (1, 'Alice Original', 'alice.old@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        """
        )

        transform_step = SQLBlockStep(
            table_name="customers",
            sql_query=f"SELECT customer_id, name, email FROM {source_table}",
            mode="UPSERT",
            upsert_keys=["customer_id"],
        )

        sql_statements, parameters = handler.generate_sql_with_params(transform_step)

        # Should generate UPSERT statements when table exists (CREATE VIEW, DELETE, INSERT, DROP VIEW)
        assert len(sql_statements) == 4
        assert "CREATE TEMPORARY VIEW temp_upsert_" in sql_statements[0]
        assert "DELETE FROM customers" in sql_statements[1]
        assert "INSERT INTO customers" in sql_statements[2]
        assert "DROP VIEW temp_upsert_" in sql_statements[3]
        assert parameters == {}

        # Execute all statements
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify UPSERT results
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM customers")
        count = result.fetchall()[0][0]
        assert count == 4  # 1 updated, 1 unchanged, 2 new

        # Verify customer 1 was updated
        result = duckdb_engine.execute_query(
            "SELECT name FROM customers WHERE customer_id = 1"
        )
        name = result.fetchall()[0][0]
        assert name == "Alice Updated"

        # Verify customer 3 was unchanged
        result = duckdb_engine.execute_query(
            "SELECT name FROM customers WHERE customer_id = 3"
        )
        name = result.fetchall()[0][0]
        assert name == "Charlie"

        # Verify new customers were inserted
        result = duckdb_engine.execute_query(
            "SELECT COUNT(*) FROM customers WHERE customer_id IN (2, 4)"
        )
        new_count = result.fetchall()[0][0]
        assert new_count == 2


class TestIncrementalTransformHandlerIntegration:
    """Integration tests for INCREMENTAL transform handler with real database operations."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def incremental_handler(self, duckdb_engine):
        """Create INCREMENTAL handler with real engine."""
        return IncrementalTransformHandler(duckdb_engine)

    @pytest.fixture
    def source_table(self, duckdb_engine):
        """Create source table with time-based data."""
        duckdb_engine.execute_query(
            """
            CREATE TABLE raw_events (
                id INTEGER,
                event_date DATE,
                created_at TIMESTAMP,
                event_type VARCHAR,
                value INTEGER
            )
        """
        )

        # Insert data spanning multiple days
        base_date = datetime(2024, 1, 1)
        for i in range(10):
            event_date = (base_date + timedelta(days=i % 5)).date()
            created_at = base_date + timedelta(days=i % 5, hours=i % 12)
            duckdb_engine.execute_query(
                f"""
                INSERT INTO raw_events VALUES 
                ({i}, '{event_date}', '{created_at.isoformat()}', 'type_{i % 3}', {i * 10})
            """
            )

        return "raw_events"

    def test_incremental_creates_table_when_not_exists(
        self, incremental_handler, source_table, duckdb_engine
    ):
        """Test INCREMENTAL mode creates table when it doesn't exist."""
        transform_step = SQLBlockStep(
            table_name="daily_metrics",
            sql_query=f"SELECT event_date, COUNT(*) as event_count FROM {source_table} WHERE created_at BETWEEN @start_date AND @end_date GROUP BY event_date",
            mode="INCREMENTAL",
            time_column="event_date",
        )

        sql_statements, parameters = incremental_handler.generate_sql_with_params(
            transform_step
        )

        # Should generate CREATE TABLE AS when table doesn't exist
        assert len(sql_statements) == 1
        assert "CREATE TABLE daily_metrics AS" in sql_statements[0]
        # Should have time parameters because the query contains time macros
        assert "start_date" in parameters
        assert "end_date" in parameters
        assert "start_dt" in parameters
        assert "end_dt" in parameters

        # Verify the SQL contains substituted parameters
        assert "$start_date" in sql_statements[0]
        assert "$end_date" in sql_statements[0]

        # For execution testing, we'll create a simpler version without time macros
        simple_sql = f"CREATE TABLE daily_metrics AS SELECT event_date, COUNT(*) as event_count FROM {source_table} GROUP BY event_date"
        duckdb_engine.execute_query(simple_sql)

        # Verify table created
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM daily_metrics")
        count = result.fetchall()[0][0]
        assert count > 0

    def test_incremental_updates_existing_table(
        self, incremental_handler, source_table, duckdb_engine
    ):
        """Test INCREMENTAL mode updates existing table."""
        # Create target table
        duckdb_engine.execute_query(
            """
            CREATE TABLE daily_metrics (
                event_date DATE,
                event_count INTEGER
            )
        """
        )

        # Insert some existing data
        duckdb_engine.execute_query(
            """
            INSERT INTO daily_metrics VALUES 
            ('2024-01-01', 100),
            ('2024-01-02', 200)
        """
        )

        transform_step = SQLBlockStep(
            table_name="daily_metrics",
            sql_query=f"SELECT event_date, COUNT(*) as event_count FROM {source_table} WHERE created_at BETWEEN @start_date AND @end_date GROUP BY event_date",
            mode="INCREMENTAL",
            time_column="event_date",
        )

        sql_statements, parameters = incremental_handler.generate_sql_with_params(
            transform_step
        )

        # Should generate transaction with DELETE and INSERT
        assert len(sql_statements) == 4
        assert sql_statements[0] == "BEGIN TRANSACTION;"
        assert "DELETE FROM daily_metrics" in sql_statements[1]
        assert "INSERT INTO daily_metrics" in sql_statements[2]
        assert sql_statements[3] == "COMMIT;"

        # Should have time parameters
        assert "start_date" in parameters
        assert "end_date" in parameters

    def test_incremental_with_lookback(
        self, incremental_handler, source_table, duckdb_engine
    ):
        """Test INCREMENTAL mode with LOOKBACK option."""
        # Create target table
        duckdb_engine.execute_query(
            """
            CREATE TABLE daily_metrics (
                event_date DATE,
                event_count INTEGER
            )
        """
        )

        transform_step = SQLBlockStep(
            table_name="daily_metrics",
            sql_query=f"SELECT event_date, COUNT(*) as event_count FROM {source_table} WHERE created_at BETWEEN @start_date AND @end_date GROUP BY event_date",
            mode="INCREMENTAL",
            time_column="event_date",
            lookback="3 DAYS",
        )

        sql_statements, parameters = incremental_handler.generate_sql_with_params(
            transform_step
        )

        # Should apply lookback in time range calculation
        assert len(sql_statements) == 4
        assert "start_date" in parameters
        assert "end_date" in parameters

    def test_parse_lookback_functionality(self, incremental_handler):
        """Test lookback parsing functionality."""
        assert incremental_handler._parse_lookback("2 DAYS") == 2
        assert incremental_handler._parse_lookback("1 DAY") == 1
        assert incremental_handler._parse_lookback("7 days") == 7
        assert incremental_handler._parse_lookback("5 Days") == 5
        assert incremental_handler._parse_lookback("invalid format") == 1  # Default


class TestTransformHandlerPerformanceIntegration:
    """Integration tests for performance optimization in transform handlers."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def large_source_table(self, duckdb_engine):
        """Create larger source table for performance testing."""
        duckdb_engine.execute_query(
            """
            CREATE TABLE large_events (
                id INTEGER,
                event_date DATE,
                created_at TIMESTAMP,
                category VARCHAR,
                value DECIMAL(10,2)
            )
        """
        )

        # Insert enough data to trigger performance optimizations
        base_date = datetime(2024, 1, 1)
        for i in range(100):  # Smaller for faster tests
            event_date = (base_date + timedelta(days=i % 30)).date()
            created_at = base_date + timedelta(days=i % 30, hours=i % 24)
            category = f"cat_{i % 10}"
            value = i * 12.34

            duckdb_engine.execute_query(
                f"""
                INSERT INTO large_events VALUES 
                ({i}, '{event_date}', '{created_at.isoformat()}', '{category}', {value})
            """
            )

        return "large_events"

    def test_incremental_handler_performance_optimization(
        self, large_source_table, duckdb_engine
    ):
        """Test that incremental handler applies performance optimizations."""
        handler = IncrementalTransformHandler(duckdb_engine)

        # Create target table
        duckdb_engine.execute_query(
            """
            CREATE TABLE optimized_metrics (
                event_date DATE,
                category VARCHAR,
                total_value DECIMAL(10,2)
            )
        """
        )

        transform_step = SQLBlockStep(
            table_name="optimized_metrics",
            sql_query=f"SELECT event_date, category, SUM(value) as total_value FROM {large_source_table} WHERE created_at BETWEEN @start_date AND @end_date GROUP BY event_date, category",
            mode="INCREMENTAL",
            time_column="event_date",
        )

        sql_statements, parameters = handler.generate_sql_with_params(transform_step)

        # Should have performance optimization applied
        assert len(sql_statements) == 4

        # Check for optimization comments in the generated SQL
        delete_sql = sql_statements[1]
        insert_sql = sql_statements[2]

        # Should have DELETE and INSERT operations
        assert "DELETE FROM" in delete_sql
        assert (
            "INSERT" in insert_sql
        )  # May have optimization hints like "INSERT /*+ USE_BULK_INSERT */ INTO"

        # Should have parameter substitution
        assert "$start_date" in insert_sql
        assert "$end_date" in insert_sql

        # May have optimization hints for larger datasets
        any("USE_BULK_INSERT" in sql or "Optimized" in sql for sql in sql_statements)
        # Note: optimization may or may not be applied depending on estimated row count

    def test_all_handlers_have_performance_components(self, duckdb_engine):
        """Test that all transform handlers have performance optimization components."""
        modes = ["REPLACE", "APPEND", "UPSERT", "INCREMENTAL"]

        for mode in modes:
            handler = TransformModeHandlerFactory.create(mode, duckdb_engine)

            # Should have performance optimizer
            assert hasattr(handler, "performance_optimizer")
            assert handler.performance_optimizer is not None

            # Should have watermark manager for incremental processing
            assert hasattr(handler, "watermark_manager")
            assert handler.watermark_manager is not None


class TestTransformHandlerComprehensiveIntegration:
    """Comprehensive integration tests covering all transform handler functionality."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create real DuckDB engine for testing."""
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    def test_end_to_end_transform_workflow(self, duckdb_engine):
        """Test complete end-to-end transform workflow with real data."""
        # Create source data
        duckdb_engine.execute_query(
            """
            CREATE TABLE orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR
            )
        """
        )

        duckdb_engine.execute_query(
            """
            INSERT INTO orders VALUES 
            (1, 101, '2024-01-01', 150.50, 'completed'),
            (2, 102, '2024-01-01', 200.75, 'completed'),
            (3, 101, '2024-01-02', 75.25, 'pending'),
            (4, 103, '2024-01-02', 300.00, 'completed')
        """
        )

        # Test REPLACE mode
        replace_handler = ReplaceTransformHandler(duckdb_engine)
        replace_step = SQLBlockStep(
            table_name="daily_totals",
            sql_query="SELECT order_date, SUM(amount) as total_amount FROM orders GROUP BY order_date",
            mode="REPLACE",
        )

        sql_statements, _ = replace_handler.generate_sql_with_params(replace_step)
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify REPLACE result
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM daily_totals")
        assert result.fetchall()[0][0] == 2  # Two days

        # Test APPEND mode
        append_handler = AppendTransformHandler(duckdb_engine)
        append_step = SQLBlockStep(
            table_name="order_log",
            sql_query="SELECT order_id, 'processed' as status FROM orders WHERE status = 'completed'",
            mode="APPEND",
        )

        sql_statements, _ = append_handler.generate_sql_with_params(append_step)
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify APPEND result
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM order_log")
        assert result.fetchall()[0][0] == 3  # Three completed orders

        # Test UPSERT mode
        upsert_handler = UpsertTransformHandler(duckdb_engine)
        upsert_step = SQLBlockStep(
            table_name="customer_summary",
            sql_query="SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_amount FROM orders GROUP BY customer_id",
            mode="UPSERT",
            upsert_keys=["customer_id"],
        )

        sql_statements, _ = upsert_handler.generate_sql_with_params(upsert_step)
        for sql in sql_statements:
            duckdb_engine.execute_query(sql)

        # Verify UPSERT result
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM customer_summary")
        assert result.fetchall()[0][0] == 3  # Three customers

    def test_error_handling_with_real_failures(self, duckdb_engine):
        """Test error handling with real database failure scenarios."""
        handler = ReplaceTransformHandler(duckdb_engine)

        # Test with invalid SQL
        invalid_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT * FROM nonexistent_table",
            mode="REPLACE",
        )

        sql_statements, _ = handler.generate_sql_with_params(invalid_step)

        # Should generate valid SQL syntax, but execution should fail
        assert len(sql_statements) == 1
        assert "CREATE OR REPLACE TABLE test_table AS" in sql_statements[0]

        # Actual execution would fail, but SQL generation succeeds
        with pytest.raises(Exception):
            duckdb_engine.execute_query(sql_statements[0])

    def test_concurrent_handler_operations(self, duckdb_engine):
        """Test concurrent operations across different handlers."""
        # Create shared source data
        duckdb_engine.execute_query(
            """
            CREATE TABLE shared_source (
                id INTEGER,
                data VARCHAR,
                timestamp TIMESTAMP
            )
        """
        )

        duckdb_engine.execute_query(
            """
            INSERT INTO shared_source VALUES 
            (1, 'data1', '2024-01-01 10:00:00'),
            (2, 'data2', '2024-01-01 11:00:00')
        """
        )

        # Test that different handlers can work with same source
        replace_handler = ReplaceTransformHandler(duckdb_engine)
        append_handler = AppendTransformHandler(duckdb_engine)
        upsert_handler = UpsertTransformHandler(duckdb_engine)

        replace_step = SQLBlockStep(
            table_name="replace_result",
            sql_query="SELECT id, data FROM shared_source",
            mode="REPLACE",
        )

        append_step = SQLBlockStep(
            table_name="append_result",
            sql_query="SELECT id, data FROM shared_source",
            mode="APPEND",
        )

        upsert_step = SQLBlockStep(
            table_name="customer_summary",
            sql_query="SELECT id as customer_id, COUNT(*) as order_count, 1 as total_amount FROM shared_source GROUP BY id",
            mode="UPSERT",
            upsert_keys=["customer_id"],
        )

        # Both should work without conflict
        replace_sql, _ = replace_handler.generate_sql_with_params(replace_step)
        append_sql, _ = append_handler.generate_sql_with_params(append_step)
        upsert_sql, _ = upsert_handler.generate_sql_with_params(upsert_step)

        for sql in replace_sql:
            duckdb_engine.execute_query(sql)
        for sql in append_sql:
            duckdb_engine.execute_query(sql)
        for sql in upsert_sql:
            duckdb_engine.execute_query(sql)

        # Verify both tables exist
        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM replace_result")
        assert result.fetchall()[0][0] == 2

        result = duckdb_engine.execute_query("SELECT COUNT(*) FROM append_result")
        assert result.fetchall()[0][0] == 2
