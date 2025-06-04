"""Tests for transform mode handlers."""

import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from sqlflow.core.engines.duckdb.exceptions import InvalidLoadModeError
from sqlflow.core.engines.duckdb.load.handlers import TableInfo
from sqlflow.core.engines.duckdb.transform.handlers import (
    AppendTransformHandler,
    IncrementalTransformHandler,
    MergeTransformHandler,
    ReplaceTransformHandler,
    SecureTimeSubstitution,
    TransformError,
    TransformLockManager,
    TransformModeHandlerFactory,
)
from sqlflow.parser.ast import SQLBlockStep


class TestSecureTimeSubstitution:
    """Test secure time macro substitution."""

    def setup_method(self):
        """Set up test fixtures."""
        self.substitution = SecureTimeSubstitution()

    def test_substitute_time_macros_basic(self):
        """Test basic time macro substitution."""
        sql = "SELECT * FROM events WHERE date BETWEEN @start_date AND @end_date"
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 2)

        result_sql, parameters = self.substitution.substitute_time_macros(
            sql, start_time, end_time
        )

        assert "$start_date" in result_sql
        assert "$end_date" in result_sql
        assert "@start_date" not in result_sql
        assert "@end_date" not in result_sql

        assert parameters["start_date"] == "2023-01-01"
        assert parameters["end_date"] == "2023-01-02"
        assert "start_dt" in parameters
        assert "end_dt" in parameters

    def test_substitute_time_macros_datetime(self):
        """Test datetime macro substitution."""
        sql = "SELECT * FROM events WHERE timestamp BETWEEN @start_dt AND @end_dt"
        start_time = datetime(2023, 1, 1, 10, 30)
        end_time = datetime(2023, 1, 2, 15, 45)

        result_sql, parameters = self.substitution.substitute_time_macros(
            sql, start_time, end_time
        )

        assert "$start_dt" in result_sql
        assert "$end_dt" in result_sql
        assert parameters["start_dt"] == "2023-01-01T10:30:00"
        assert parameters["end_dt"] == "2023-01-02T15:45:00"

    def test_substitute_time_macros_no_macros(self):
        """Test SQL without macros."""
        sql = "SELECT * FROM events WHERE id = 1"
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 2)

        result_sql, parameters = self.substitution.substitute_time_macros(
            sql, start_time, end_time
        )

        assert result_sql == sql
        assert len(parameters) == 4  # All parameters still created


class TestTransformLockManager:
    """Test transform lock manager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.lock_manager = TransformLockManager()

    def test_acquire_lock_success(self):
        """Test successful lock acquisition."""
        with self.lock_manager.acquire_table_lock("test_table"):
            assert "test_table" in self.lock_manager.locks

        # Lock should be released after context
        assert "test_table" not in self.lock_manager.locks

    def test_acquire_lock_concurrent_error(self):
        """Test error when trying to acquire concurrent lock."""
        # Acquire lock in one thread
        lock_acquired = threading.Event()
        release_lock = threading.Event()

        def acquire_lock():
            with self.lock_manager.acquire_table_lock("test_table"):
                lock_acquired.set()
                release_lock.wait()

        thread = threading.Thread(target=acquire_lock)
        thread.start()
        lock_acquired.wait()

        # Try to acquire the same lock - should fail
        with pytest.raises(TransformError) as exc_info:
            with self.lock_manager.acquire_table_lock("test_table"):
                pass

        assert "already being transformed" in str(exc_info.value)

        # Release the first lock
        release_lock.set()
        thread.join()

    def test_different_tables_no_conflict(self):
        """Test that different tables don't conflict."""
        with self.lock_manager.acquire_table_lock("table1"):
            with self.lock_manager.acquire_table_lock("table2"):
                assert "table1" in self.lock_manager.locks
                assert "table2" in self.lock_manager.locks


class TestTransformModeHandlerFactory:
    """Test transform mode handler factory."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = MagicMock()

    def test_create_replace_handler(self):
        """Test creating REPLACE handler."""
        handler = TransformModeHandlerFactory.create("REPLACE", self.mock_engine)
        assert isinstance(handler, ReplaceTransformHandler)

    def test_create_append_handler(self):
        """Test creating APPEND handler."""
        handler = TransformModeHandlerFactory.create("APPEND", self.mock_engine)
        assert isinstance(handler, AppendTransformHandler)

    def test_create_merge_handler(self):
        """Test creating MERGE handler."""
        handler = TransformModeHandlerFactory.create("MERGE", self.mock_engine)
        assert isinstance(handler, MergeTransformHandler)

    def test_create_incremental_handler(self):
        """Test creating INCREMENTAL handler."""
        handler = TransformModeHandlerFactory.create("INCREMENTAL", self.mock_engine)
        assert isinstance(handler, IncrementalTransformHandler)

    def test_create_case_insensitive(self):
        """Test that mode is case insensitive."""
        handler = TransformModeHandlerFactory.create("replace", self.mock_engine)
        assert isinstance(handler, ReplaceTransformHandler)

    def test_create_invalid_mode(self):
        """Test error for invalid mode."""
        with pytest.raises(InvalidLoadModeError):
            TransformModeHandlerFactory.create("INVALID", self.mock_engine)


class TestReplaceTransformHandler:
    """Test REPLACE transform handler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = MagicMock()
        self.handler = ReplaceTransformHandler(self.mock_engine)

    def test_generate_sql_with_params(self):
        """Test SQL generation for REPLACE mode."""
        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT id, name FROM source_table",
            mode="REPLACE",
        )

        sql_statements, parameters = self.handler.generate_sql_with_params(
            transform_step
        )

        assert len(sql_statements) == 1
        assert "CREATE OR REPLACE TABLE test_table AS" in sql_statements[0]
        assert "SELECT id, name FROM source_table" in sql_statements[0]
        assert parameters == {}

    def test_inheritance_from_load_handler(self):
        """Test that handler inherits LOAD infrastructure."""
        assert hasattr(self.handler, "validation_helper")
        assert hasattr(self.handler, "sql_generator")
        assert hasattr(self.handler, "time_substitution")
        assert hasattr(self.handler, "lock_manager")


class TestAppendTransformHandler:
    """Test APPEND transform handler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = MagicMock()
        self.handler = AppendTransformHandler(self.mock_engine)

    def test_generate_sql_table_exists(self):
        """Test SQL generation when table exists."""
        # Mock table exists
        table_info = TableInfo(exists=True)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT id, name FROM source_table",
            mode="APPEND",
        )

        sql_statements, parameters = self.handler.generate_sql_with_params(
            transform_step
        )

        assert len(sql_statements) == 3
        assert "CREATE OR REPLACE VIEW temp_transform_" in sql_statements[0]
        assert (
            "INSERT INTO test_table SELECT * FROM temp_transform_" in sql_statements[1]
        )
        assert "DROP VIEW temp_transform_" in sql_statements[2]
        assert parameters == {}

    def test_generate_sql_table_not_exists(self):
        """Test SQL generation when table doesn't exist."""
        # Mock table doesn't exist
        table_info = TableInfo(exists=False)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT id, name FROM source_table",
            mode="APPEND",
        )

        sql_statements, parameters = self.handler.generate_sql_with_params(
            transform_step
        )

        assert len(sql_statements) == 1
        assert "CREATE TABLE test_table AS" in sql_statements[0]
        assert "SELECT id, name FROM source_table" in sql_statements[0]
        assert parameters == {}


class TestMergeTransformHandler:
    """Test MERGE transform handler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = MagicMock()
        self.handler = MergeTransformHandler(self.mock_engine)

    def test_generate_sql_table_exists(self):
        """Test SQL generation when table exists."""
        # Mock table exists
        table_info = TableInfo(exists=True)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT id, name FROM source_table",
            mode="MERGE",
            merge_keys=["id"],
        )

        sql_statements, parameters = self.handler.generate_sql_with_params(
            transform_step
        )

        assert len(sql_statements) == 3
        assert "CREATE OR REPLACE TABLE temp_merge_" in sql_statements[0]
        assert "INSERT OR REPLACE INTO test_table" in sql_statements[1]
        assert "DROP TABLE temp_merge_" in sql_statements[2]
        assert parameters == {}

    def test_generate_sql_table_not_exists(self):
        """Test SQL generation when table doesn't exist."""
        # Mock table doesn't exist
        table_info = TableInfo(exists=False)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT id, name FROM source_table",
            mode="MERGE",
            merge_keys=["id"],
        )

        sql_statements, parameters = self.handler.generate_sql_with_params(
            transform_step
        )

        assert len(sql_statements) == 1
        assert "CREATE TABLE test_table AS" in sql_statements[0]
        assert parameters == {}


class TestIncrementalTransformHandler:
    """Test INCREMENTAL transform handler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = MagicMock()
        self.handler = IncrementalTransformHandler(self.mock_engine)

    def test_generate_sql_table_exists(self):
        """Test SQL generation when table exists."""
        # Mock table exists
        table_info = TableInfo(exists=True)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT * FROM events WHERE date BETWEEN @start_date AND @end_date",
            mode="INCREMENTAL",
            time_column="date",
        )

        with patch(
            "sqlflow.core.engines.duckdb.transform.handlers.datetime"
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2023, 1, 2)
            mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sql_statements, parameters = self.handler.generate_sql_with_params(
                transform_step
            )

        assert len(sql_statements) == 4
        assert sql_statements[0] == "BEGIN TRANSACTION;"
        assert "DELETE FROM test_table" in sql_statements[1]
        assert "WHERE date >=" in sql_statements[1]
        assert "INSERT INTO test_table" in sql_statements[2]
        assert sql_statements[3] == "COMMIT;"

        # Check parameters contain time values
        assert "start_date" in parameters
        assert "end_date" in parameters

    def test_generate_sql_table_not_exists(self):
        """Test SQL generation when table doesn't exist."""
        # Mock table doesn't exist
        table_info = TableInfo(exists=False)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT * FROM events WHERE date BETWEEN @start_date AND @end_date",
            mode="INCREMENTAL",
            time_column="date",
        )

        sql_statements, parameters = self.handler.generate_sql_with_params(
            transform_step
        )

        assert len(sql_statements) == 1
        assert "CREATE TABLE test_table AS" in sql_statements[0]
        assert "start_date" in parameters

    def test_parse_lookback(self):
        """Test lookback parsing."""
        assert self.handler._parse_lookback("2 DAYS") == 2
        assert self.handler._parse_lookback("1 DAY") == 1
        assert self.handler._parse_lookback("7 days") == 7
        assert self.handler._parse_lookback("invalid") == 1  # Default

    def test_generate_sql_with_lookback(self):
        """Test SQL generation with lookback."""
        # Mock table exists
        table_info = TableInfo(exists=True)
        self.handler.validation_helper.get_table_info = MagicMock(
            return_value=table_info
        )

        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT * FROM events WHERE date BETWEEN @start_date AND @end_date",
            mode="INCREMENTAL",
            time_column="date",
            lookback="2 DAYS",
        )

        with patch(
            "sqlflow.core.engines.duckdb.transform.handlers.datetime"
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2023, 1, 3)
            mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sql_statements, parameters = self.handler.generate_sql_with_params(
                transform_step
            )

        # Should have lookback applied (2 days before the default 1 day = 3 days total)
        assert len(sql_statements) == 4
        assert "start_date" in parameters


class TestTransformModeHandlerIntegration:
    """Integration tests for transform handlers."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = MagicMock()

    def test_all_modes_have_secure_substitution(self):
        """Test that all handlers have secure time substitution."""
        modes = ["REPLACE", "APPEND", "MERGE", "INCREMENTAL"]

        for mode in modes:
            handler = TransformModeHandlerFactory.create(mode, self.mock_engine)
            assert hasattr(handler, "time_substitution")
            assert isinstance(handler.time_substitution, SecureTimeSubstitution)

    def test_all_modes_have_lock_manager(self):
        """Test that all handlers have lock manager."""
        modes = ["REPLACE", "APPEND", "MERGE", "INCREMENTAL"]

        for mode in modes:
            handler = TransformModeHandlerFactory.create(mode, self.mock_engine)
            assert hasattr(handler, "lock_manager")
            assert isinstance(handler.lock_manager, TransformLockManager)

    def test_all_modes_inherit_load_infrastructure(self):
        """Test that all handlers inherit LOAD infrastructure."""
        modes = ["REPLACE", "APPEND", "MERGE", "INCREMENTAL"]

        for mode in modes:
            handler = TransformModeHandlerFactory.create(mode, self.mock_engine)
            assert hasattr(handler, "validation_helper")
            assert hasattr(handler, "sql_generator")
            assert hasattr(handler, "sql_helper")

    def test_performance_monitoring_hooks(self):
        """Test that handlers have performance monitoring capabilities."""
        handler = TransformModeHandlerFactory.create("REPLACE", self.mock_engine)

        # Should have logger for performance monitoring
        assert hasattr(handler, "engine")

        # Should be able to call generate_sql_with_params without error
        transform_step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="REPLACE",
        )

        sql_statements, parameters = handler.generate_sql_with_params(transform_step)
        assert isinstance(sql_statements, list)
        assert isinstance(parameters, dict)
