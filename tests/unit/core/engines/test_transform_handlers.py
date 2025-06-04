"""Tests for transform mode handlers - Pure logic tests without mocks.

This module contains unit tests for pure logic functions that don't require
database operations or complex mocking. Complex integration tests with real
implementations are in tests/integration/core/test_transform_handlers_integration.py
"""

import threading
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from sqlflow.core.engines.duckdb.exceptions import InvalidLoadModeError
from sqlflow.core.engines.duckdb.transform.handlers import (
    IncrementalTransformHandler,
    SecureTimeSubstitution,
    TransformError,
    TransformLockManager,
    TransformModeHandlerFactory,
)


class TestSecureTimeSubstitution:
    """Test secure time macro substitution - pure logic, no mocks needed."""

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
    """Test transform lock manager - real threading logic, no mocks needed."""

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
    """Test transform mode handler factory - simple instantiation tests."""

    def test_create_case_insensitive(self):
        """Test that mode is case insensitive."""
        mock_engine = MagicMock()
        handler = TransformModeHandlerFactory.create("replace", mock_engine)
        assert handler.__class__.__name__ == "ReplaceTransformHandler"

    def test_create_invalid_mode(self):
        """Test error for invalid mode."""
        mock_engine = MagicMock()
        with pytest.raises(InvalidLoadModeError):
            TransformModeHandlerFactory.create("INVALID", mock_engine)


class TestIncrementalTransformHandlerPureLogic:
    """Test pure logic methods in IncrementalTransformHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        mock_engine = MagicMock()
        self.handler = IncrementalTransformHandler(mock_engine)

    def test_parse_lookback(self):
        """Test lookback parsing - pure string parsing logic."""
        assert self.handler._parse_lookback("2 DAYS") == 2
        assert self.handler._parse_lookback("1 DAY") == 1
        assert self.handler._parse_lookback("7 days") == 7
        assert self.handler._parse_lookback("5 Days") == 5
        assert self.handler._parse_lookback("invalid") == 1  # Default
        assert self.handler._parse_lookback("") == 1  # Default
        assert self.handler._parse_lookback("not a number DAYS") == 1  # Default
