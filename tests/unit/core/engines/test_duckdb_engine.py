"""Unit tests for the refactored DuckDB engine."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from sqlflow.core.engines.duckdb.constants import DuckDBConstants
from sqlflow.core.engines.duckdb.engine import (
    DuckDBEngine,
    ExecutionStats,
    UDFExecutionContext,
)
from sqlflow.core.engines.duckdb.exceptions import UDFRegistrationError


class TestExecutionStats(unittest.TestCase):
    """Test the ExecutionStats helper class."""

    def setUp(self):
        """Set up test environment."""
        self.stats = ExecutionStats()

    def test_initial_state(self):
        """Test initial state of ExecutionStats."""
        self.assertEqual(self.stats.query_count, 0)
        self.assertEqual(self.stats.udf_executions, 0)
        self.assertEqual(self.stats.udf_errors, 0)
        self.assertIsNone(self.stats.last_error)
        self.assertEqual(self.stats.query_times, [])

    def test_record_query(self):
        """Test recording query execution."""
        duration = 1.5
        self.stats.record_query(duration)

        self.assertEqual(self.stats.query_count, 1)
        self.assertEqual(self.stats.query_times, [duration])

    def test_record_udf_execution_success(self):
        """Test recording successful UDF execution."""
        self.stats.record_udf_execution(True)

        self.assertEqual(self.stats.udf_executions, 1)
        self.assertEqual(self.stats.udf_errors, 0)
        self.assertIsNone(self.stats.last_error)

    def test_record_udf_execution_failure(self):
        """Test recording failed UDF execution."""
        error = Exception("Test error")
        self.stats.record_udf_execution(False, error)

        self.assertEqual(self.stats.udf_executions, 1)
        self.assertEqual(self.stats.udf_errors, 1)
        self.assertEqual(self.stats.last_error, error)

    def test_get_avg_query_time(self):
        """Test average query time calculation."""
        # No queries recorded
        self.assertEqual(self.stats.get_avg_query_time(), 0.0)

        # Multiple queries
        self.stats.record_query(1.0)
        self.stats.record_query(2.0)
        self.stats.record_query(3.0)

        self.assertEqual(self.stats.get_avg_query_time(), 2.0)

    def test_get_summary(self):
        """Test getting summary statistics."""
        self.stats.record_query(1.5)
        self.stats.record_udf_execution(True)
        error = Exception("Test error")
        self.stats.record_udf_execution(False, error)

        summary = self.stats.get_summary()

        expected = {
            "query_count": 1,
            "udf_executions": 2,
            "udf_errors": 1,
            "avg_query_time": 1.5,
            "last_error": "Test error",
        }

        self.assertEqual(summary, expected)


class TestUDFExecutionContext(unittest.TestCase):
    """Test the UDFExecutionContext class."""

    def setUp(self):
        """Set up test environment."""
        self.engine = MagicMock()
        self.udf_name = "test_udf"

    def test_successful_execution(self):
        """Test successful UDF execution context."""
        with UDFExecutionContext(self.engine, self.udf_name) as ctx:
            self.assertIsNotNone(ctx.start_time)
            # Simulate some work

    def test_failed_execution(self):
        """Test failed UDF execution context."""
        test_error = ValueError("Test error")

        with self.assertRaises(ValueError):
            with UDFExecutionContext(self.engine, self.udf_name):
                raise test_error

    @patch("time.time", side_effect=[1000.0, 1001.5])  # Mock start and end times
    def test_timing_calculation(self, mock_time):
        """Test that timing is calculated correctly."""
        with UDFExecutionContext(self.engine, self.udf_name) as ctx:
            # Verify start time was recorded
            self.assertEqual(ctx.start_time, 1000.0)


class TestDuckDBEngine(unittest.TestCase):
    """Test the main DuckDB engine class."""

    def setUp(self):
        """Set up test environment."""
        with patch("sqlflow.core.engines.duckdb.engine.duckdb") as self.mock_duckdb:
            # Mock DuckDB connection
            self.mock_connection = MagicMock()
            self.mock_duckdb.connect.return_value = self.mock_connection

            # Create engine with in-memory database
            self.engine = DuckDBEngine(":memory:")

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "engine"):
            self.engine.close()

    def test_initialization_memory_database(self):
        """Test engine initialization with in-memory database."""
        self.assertEqual(self.engine.database_path, ":memory:")
        self.assertFalse(self.engine.is_persistent)
        self.assertIsNotNone(self.engine.transaction_manager)
        self.assertIsNotNone(self.engine.stats)
        self.assertEqual(self.engine.variables, {})
        self.assertEqual(self.engine.registered_udfs, {})

    def test_initialization_file_database(self):
        """Test engine initialization with file database using real DuckDB connection."""
        # Create a temporary file path without creating the file
        # This allows DuckDB to create a proper database file
        with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
            db_path = f.name
        # File is now deleted, leaving just the path

        try:
            engine = DuckDBEngine(db_path)
            self.assertEqual(engine.database_path, db_path)
            self.assertTrue(engine.is_persistent)

            # Test that we can actually execute a query on the real connection
            result = engine.execute_query("SELECT 1 as test_col")
            self.assertIsNotNone(result)

            engine.close()
        finally:
            try:
                os.unlink(db_path)
            except (OSError, PermissionError):
                pass

    def test_initialization_default_database(self):
        """Test engine initialization with default database path using real DuckDB connection."""
        # Test with None to trigger default path logic
        engine = DuckDBEngine(None)

        try:
            self.assertEqual(
                engine.database_path, DuckDBConstants.DEFAULT_DATABASE_PATH
            )
            self.assertTrue(engine.is_persistent)

            # Test that we can actually execute a query on the real connection
            result = engine.execute_query("SELECT 1 as default_test")
            self.assertIsNotNone(result)

        finally:
            engine.close()
            # Clean up the created database file
            try:
                if os.path.exists(DuckDBConstants.DEFAULT_DATABASE_PATH):
                    os.unlink(DuckDBConstants.DEFAULT_DATABASE_PATH)
                # Also clean up the directory if it was created
                db_dir = os.path.dirname(DuckDBConstants.DEFAULT_DATABASE_PATH)
                if os.path.exists(db_dir) and not os.listdir(db_dir):
                    os.rmdir(db_dir)
            except (OSError, PermissionError):
                pass

    def test_connection_fallback_to_memory(self):
        """Test fallback to in-memory database when file connection fails."""
        # Create a path that should cause connection to fail (e.g., invalid directory)
        invalid_path = "/invalid/path/that/does/not/exist/test.db"

        engine = DuckDBEngine(invalid_path)

        try:
            # After fallback, it should be using in-memory database
            self.assertEqual(engine.database_path, ":memory:")
            self.assertFalse(engine.is_persistent)

            # Test that we can actually execute a query on the real in-memory connection
            result = engine.execute_query("SELECT 1 as fallback_test")
            self.assertIsNotNone(result)

        finally:
            engine.close()

    def test_execute_query(self):
        """Test query execution."""
        query = "SELECT 1"
        mock_result = MagicMock()
        self.mock_connection.execute.return_value = mock_result

        with patch("time.time", side_effect=[1000.0, 1001.0]):
            result = self.engine.execute_query(query)

        # The mock connection gets called during initialization too, so check that execute was called
        self.mock_connection.execute.assert_called_with(query)
        self.assertEqual(result, mock_result)
        self.assertEqual(self.engine.stats.query_count, 1)
        self.assertEqual(self.engine.stats.query_times, [1.0])

    def test_execute_query_failure(self):
        """Test query execution failure."""
        query = "INVALID SQL"
        self.mock_connection.execute.side_effect = Exception("SQL error")

        with self.assertRaises(Exception):
            self.engine.execute_query(query)

    def test_register_python_udf(self):
        """Test Python UDF registration."""

        def test_udf(x):
            return x * 2

        with patch.object(self.engine, "connection", self.mock_connection):
            # Mock the UDF handler factory and handler
            with patch(
                "sqlflow.core.engines.duckdb.engine.UDFHandlerFactory"
            ) as mock_factory:
                mock_handler = MagicMock()
                mock_factory.create.return_value = mock_handler

                self.engine.register_python_udf("test_func", test_udf)

                mock_factory.create.assert_called_once_with(test_udf)
                mock_handler.register.assert_called_once_with(
                    "test_func", test_udf, self.mock_connection
                )
                self.assertIn("test_func", self.engine.registered_udfs)

    def test_register_python_udf_failure(self):
        """Test Python UDF registration failure."""

        def test_udf(x):
            return x * 2

        with patch.object(self.engine, "connection", self.mock_connection):
            with patch(
                "sqlflow.core.engines.duckdb.engine.UDFHandlerFactory"
            ) as mock_factory:
                mock_factory.create.side_effect = Exception("Registration error")

                with self.assertRaises(UDFRegistrationError):
                    self.engine.register_python_udf("test_func", test_udf)

    def test_register_table(self):
        """Test table registration."""
        mock_data = MagicMock()
        mock_data.columns = ["col1", "col2"]

        with patch.object(self.engine, "transaction_manager") as mock_tm:
            self.engine.register_table("test_table", mock_data)

            # Should use transaction manager
            mock_tm.__enter__.assert_called_once()
            mock_tm.__exit__.assert_called_once()

    def test_register_table_without_transaction(self):
        """Test table registration without transaction management."""
        mock_data = MagicMock()
        mock_data.columns = ["col1", "col2"]

        self.engine.register_table("test_table", mock_data, manage_transaction=False)

        self.mock_connection.register.assert_called_once_with("test_table", mock_data)

    def test_table_exists_true(self):
        """Test table_exists method when table exists."""
        mock_result = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_result.fetchdf.return_value = mock_df
        self.mock_connection.execute.return_value = mock_result

        result = self.engine.table_exists("test_table")

        self.assertTrue(result)

    def test_table_exists_false(self):
        """Test table_exists method when table doesn't exist."""
        mock_result = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=0)
        mock_result.fetchdf.return_value = mock_df
        self.mock_connection.execute.return_value = mock_result

        result = self.engine.table_exists("test_table")

        self.assertFalse(result)

    def test_table_exists_fallback(self):
        """Test table_exists method fallback to direct query."""
        # First approach fails, fallback succeeds
        self.mock_connection.execute.side_effect = [
            Exception("Information schema error"),
            MagicMock(),  # Direct query succeeds
        ]

        result = self.engine.table_exists("test_table")

        self.assertTrue(result)
        # Note: The actual call count may vary due to initialization queries
        # Just verify that execute was called multiple times for fallback
        self.assertGreaterEqual(self.mock_connection.execute.call_count, 2)

    def test_table_exists_all_methods_fail(self):
        """Test table_exists when all methods fail."""
        self.mock_connection.execute.side_effect = Exception("All methods fail")

        result = self.engine.table_exists("test_table")

        self.assertFalse(result)

    def test_get_table_schema(self):
        """Test getting table schema."""
        mock_result = MagicMock()
        mock_df = MagicMock()
        mock_df.to_dict.return_value = [
            {"name": "col1", "type": "INTEGER"},
            {"name": "col2", "type": "VARCHAR"},
        ]
        mock_result.fetchdf.return_value = mock_df
        self.mock_connection.execute.return_value = mock_result

        schema = self.engine.get_table_schema("test_table")

        expected = {"col1": "INTEGER", "col2": "VARCHAR"}
        self.assertEqual(schema, expected)

    def test_get_table_schema_fallback(self):
        """Test getting table schema with fallback to DESCRIBE."""
        # PRAGMA fails, DESCRIBE succeeds
        mock_result = MagicMock()
        mock_df = MagicMock()
        mock_df.to_dict.return_value = [
            {"column_name": "col1", "column_type": "INTEGER"},
            {"column_name": "col2", "column_type": "VARCHAR"},
        ]
        mock_result.fetchdf.return_value = mock_df

        self.mock_connection.execute.side_effect = [
            Exception("PRAGMA failed"),
            mock_result,
        ]

        schema = self.engine.get_table_schema("test_table")

        expected = {"col1": "INTEGER", "col2": "VARCHAR"}
        self.assertEqual(schema, expected)

    def test_generate_load_sql_basic_functionality(self):
        """Test basic load SQL generation functionality (now covered in integration tests)."""
        # This test is now redundant with integration tests that use real DuckDB
        # Keeping minimal test to verify method exists and doesn't crash
        load_step = MagicMock()
        load_step.table_name = "target_table"
        load_step.source_name = "source_table"
        load_step.mode = "REPLACE"
        load_step.upsert_keys = []

        # Should not crash and should return some SQL
        sql = self.engine.generate_load_sql(load_step)
        self.assertIsInstance(sql, str)
        self.assertGreater(len(sql), 0)

    def test_configure_engine(self):
        """Test engine configuration."""
        config = {"memory_limit": "1GB"}
        profile_variables = {"env": "production", "region": "us-east"}

        self.engine.configure(config, profile_variables)

        # Variables should be registered
        self.assertEqual(self.engine.get_variable("env"), "production")
        self.assertEqual(self.engine.get_variable("region"), "us-east")

    def test_supports_feature(self):
        """Test feature support checking."""
        self.assertTrue(self.engine.supports_feature("python_udfs"))
        self.assertTrue(self.engine.supports_feature("arrow"))
        self.assertTrue(self.engine.supports_feature("json"))
        self.assertTrue(self.engine.supports_feature("upsert"))
        self.assertTrue(self.engine.supports_feature("window_functions"))
        self.assertTrue(self.engine.supports_feature("ctes"))
        self.assertFalse(self.engine.supports_feature("unsupported_feature"))

    def test_validate_schema_compatibility_table_not_exists(self):
        """Test schema compatibility when target table doesn't exist."""
        source_schema = {"col1": "INTEGER", "col2": "VARCHAR"}

        with patch.object(self.engine, "table_exists", return_value=False):
            result = self.engine.validate_schema_compatibility(
                "target_table", source_schema
            )
            self.assertTrue(result)

    def test_validate_schema_compatibility_compatible(self):
        """Test schema compatibility with compatible schemas."""
        source_schema = {"col1": "INTEGER", "col2": "VARCHAR"}
        target_schema = {"col1": "INTEGER", "col2": "VARCHAR", "col3": "DATE"}

        with patch.object(self.engine, "table_exists", return_value=True):
            with patch.object(
                self.engine, "get_table_schema", return_value=target_schema
            ):
                result = self.engine.validate_schema_compatibility(
                    "target_table", source_schema
                )
                self.assertTrue(result)

    def test_validate_schema_compatibility_missing_column(self):
        """Test schema compatibility with missing column."""
        source_schema = {"col1": "INTEGER", "missing_col": "VARCHAR"}
        target_schema = {"col1": "INTEGER", "col2": "VARCHAR"}

        with patch.object(self.engine, "table_exists", return_value=True):
            with patch.object(
                self.engine, "get_table_schema", return_value=target_schema
            ):
                with self.assertRaises(ValueError) as cm:
                    self.engine.validate_schema_compatibility(
                        "target_table", source_schema
                    )
                self.assertIn("missing_col", str(cm.exception))

    def test_validate_schema_compatibility_incompatible_types(self):
        """Test schema compatibility with incompatible types."""
        source_schema = {"col1": "VARCHAR", "col2": "VARCHAR"}
        target_schema = {"col1": "INTEGER", "col2": "VARCHAR"}

        with patch.object(self.engine, "table_exists", return_value=True):
            with patch.object(
                self.engine, "get_table_schema", return_value=target_schema
            ):
                with self.assertRaises(ValueError) as cm:
                    self.engine.validate_schema_compatibility(
                        "target_table", source_schema
                    )
                self.assertIn("incompatible types", str(cm.exception))

    def test_validate_upsert_keys_success(self):
        """Test successful upsert key validation."""
        source_schema = {"user_id": "INTEGER", "name": "VARCHAR"}
        target_schema = {"user_id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
        upsert_keys = ["user_id"]

        with patch.object(self.engine, "table_exists", return_value=True):
            with patch.object(
                self.engine,
                "get_table_schema",
                side_effect=[source_schema, target_schema],
            ):
                result = self.engine.validate_upsert_keys(
                    "target_table", "source_table", upsert_keys
                )
                self.assertTrue(result)

    def test_validate_upsert_keys_empty_keys(self):
        """Test upsert key validation with empty keys."""
        with self.assertRaises(ValueError) as cm:
            self.engine.validate_upsert_keys("target_table", "source_table", [])
        self.assertIn("at least one upsert key", str(cm.exception))

    def test_validate_upsert_keys_missing_in_source(self):
        """Test upsert key validation with key missing in source."""
        source_schema = {"name": "VARCHAR"}
        target_schema = {"user_id": "INTEGER", "name": "VARCHAR"}
        upsert_keys = ["user_id"]

        with patch.object(self.engine, "table_exists", return_value=True):
            with patch.object(
                self.engine,
                "get_table_schema",
                side_effect=[source_schema, target_schema],
            ):
                with self.assertRaises(ValueError) as cm:
                    self.engine.validate_upsert_keys(
                        "target_table", "source_table", upsert_keys
                    )
                self.assertIn("does not exist in source", str(cm.exception))

    def test_get_stats(self):
        """Test getting execution statistics."""
        self.engine.stats.record_query(1.5)
        self.engine.stats.record_udf_execution(True)

        stats = self.engine.get_stats()

        self.assertEqual(stats["query_count"], 1)
        self.assertEqual(stats["udf_executions"], 1)
        self.assertEqual(stats["avg_query_time"], 1.5)

    def test_reset_stats(self):
        """Test resetting execution statistics."""
        self.engine.stats.record_query(1.5)
        self.engine.reset_stats()

        stats = self.engine.get_stats()
        self.assertEqual(stats["query_count"], 0)

    def test_close_connection(self):
        """Test closing the database connection."""
        self.engine.close()

        self.mock_connection.close.assert_called_once()
        self.assertIsNone(self.engine._connection)


if __name__ == "__main__":
    unittest.main()
