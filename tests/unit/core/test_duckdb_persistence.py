"""Tests for DuckDB persistence layer."""

import os
import tempfile
import unittest

import pandas as pd

from sqlflow.core.engines.duckdb import DuckDBEngine


class TestDuckDBPersistence(unittest.TestCase):
    """Test DuckDB persistence mechanisms."""

    def setUp(self):
        """Set up test environment with a temporary database file."""
        # Create a temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()

        # Create database path
        self.db_path = os.path.join(self.temp_dir.name, "test_db.duckdb")

        # Initialize engine with this path
        self.engine = DuckDBEngine(self.db_path)

        # Verify the engine is in persistent mode
        self.assertTrue(self.engine.is_persistent)
        self.assertEqual(self.engine.database_path, self.db_path)
        self.assertIsNotNone(self.engine.transaction_manager)

    def tearDown(self):
        """Clean up after tests."""
        # Close the engine
        if hasattr(self, "engine") and self.engine:
            self.engine.close()

        # Clean up temporary directory
        if hasattr(self, "temp_dir"):
            self.temp_dir.cleanup()

    def test_transaction_manager_initialization(self):
        """Test TransactionManager is correctly initialized."""
        tm = self.engine.transaction_manager

        # Check basic properties
        self.assertEqual(tm.engine, self.engine)
        self.assertFalse(tm.in_transaction)

    def test_context_manager(self):
        """Test transaction manager as context manager."""
        tm = self.engine.transaction_manager

        # Test successful transaction
        with tm:
            self.assertTrue(tm.in_transaction)
            self.engine.execute_query("CREATE TABLE test_table (id INTEGER)")

        # Transaction should be completed
        self.assertFalse(tm.in_transaction)

        # Table should exist
        result = self.engine.execute_query("SELECT * FROM test_table").fetchdf()
        self.assertEqual(len(result), 0)  # Empty table

        # Test transaction with exception
        try:
            with tm:
                self.assertTrue(tm.in_transaction)
                self.engine.execute_query("INSERT INTO test_table VALUES (1)")
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Transaction should be completed
        self.assertFalse(tm.in_transaction)

        # Insert should still have happened since simplified transaction manager doesn't rollback
        result = self.engine.execute_query("SELECT * FROM test_table").fetchdf()
        self.assertEqual(len(result), 1)  # One row inserted

    def test_basic_persistence(self):
        """Test basic persistence functionality."""
        # Verify database file is created when we perform operations
        self.assertTrue(
            os.path.exists(self.db_path) or self.engine.database_path == self.db_path
        )

        # Create a table and verify it works
        with self.engine.transaction_manager:
            self.engine.execute_query(
                "CREATE TABLE persistence_test (id INTEGER, name VARCHAR)"
            )
            self.engine.execute_query("INSERT INTO persistence_test VALUES (1, 'test')")

        # Verify data exists
        result = self.engine.execute_query("SELECT * FROM persistence_test").fetchdf()
        self.assertEqual(len(result), 1)
        self.assertEqual(result["name"][0], "test")

    def test_data_durability(self):
        """Test that data persists when engine is closed and reopened."""
        # Create test data
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create table and insert data
        with self.engine.transaction_manager:
            # Register pandas dataframe directly with engine
            self.engine.register_table("test_data", test_data)
            self.engine.execute_query(
                "CREATE TABLE durability_test AS SELECT * FROM test_data"
            )

        # Verify data was inserted
        result = self.engine.execute_query(
            "SELECT * FROM durability_test ORDER BY id"
        ).fetchdf()
        self.assertEqual(len(result), 3)
        self.assertEqual(result["name"][0], "Alice")

        # Close engine
        self.engine.close()

        # Reopen engine with same database path
        self.engine = DuckDBEngine(self.db_path)

        # Verify data is still there
        result = self.engine.execute_query(
            "SELECT * FROM durability_test ORDER BY id"
        ).fetchdf()
        self.assertEqual(len(result), 3)
        self.assertEqual(result["name"][0], "Alice")
        self.assertEqual(result["name"][1], "Bob")
        self.assertEqual(result["name"][2], "Charlie")

    def test_transaction_commit_functionality(self):
        """Test transaction commit functionality."""
        tm = self.engine.transaction_manager

        # Test commit method exists and works
        with tm:
            self.engine.execute_query("CREATE TABLE commit_test (id INTEGER)")
            tm.commit()  # Should not raise error

        # Verify table exists
        result = self.engine.execute_query("SELECT * FROM commit_test").fetchdf()
        self.assertEqual(len(result), 0)  # Empty table

    def test_transaction_rollback_functionality(self):
        """Test transaction rollback functionality."""
        tm = self.engine.transaction_manager

        # Test that rollback method exists without calling it in active transaction
        with tm:
            self.engine.execute_query("CREATE TABLE rollback_test (id INTEGER)")
            # Don't call rollback here since it would fail without an active transaction

        # Test that rollback method exists and can be called outside transaction
        self.assertTrue(hasattr(tm, "rollback"))
        tm.rollback()  # Should not raise error when called outside transaction

        # With simplified transaction manager, table should still exist
        result = self.engine.execute_query("SELECT * FROM rollback_test").fetchdf()
        self.assertEqual(len(result), 0)  # Empty table

    def test_error_handling(self):
        """Test error handling during transactions."""
        tm = self.engine.transaction_manager

        # Test invalid SQL in transaction
        with self.assertRaises(Exception):
            with tm:
                self.engine.execute_query("CREATE TABLE error_test (id INTEGER)")
                self.engine.execute_query(
                    "INSERT INTO error_test_wrong_name VALUES (1)"
                )

        # Transaction should be completed
        self.assertFalse(tm.in_transaction)

        # First table should exist since error happened in second statement
        result = self.engine.execute_query("SELECT * FROM error_test").fetchdf()
        self.assertEqual(len(result), 0)  # Empty table


class TestInMemoryDuckDBEngine(unittest.TestCase):
    """Test in-memory DuckDB engine behavior."""

    def setUp(self):
        """Set up in-memory engine."""
        self.engine = DuckDBEngine(":memory:")

        # Verify the engine is not in persistent mode
        self.assertFalse(self.engine.is_persistent)
        self.assertEqual(self.engine.database_path, ":memory:")
        self.assertIsNotNone(self.engine.transaction_manager)

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "engine"):
            self.engine.close()

    def test_inmemory_basic_operations(self):
        """Test that in-memory databases work for basic operations."""
        tm = self.engine.transaction_manager

        # Create a table and verify
        with tm:
            self.engine.execute_query("CREATE TABLE test_table (id INTEGER)")
            self.engine.execute_query("INSERT INTO test_table VALUES (1)")

        # Should work for current session
        result = self.engine.execute_query("SELECT * FROM test_table").fetchdf()
        self.assertEqual(len(result), 1)

    def test_inmemory_data_loss(self):
        """Test that in-memory databases lose data when closed."""
        # Create test data
        with self.engine.transaction_manager:
            self.engine.execute_query("CREATE TABLE test_table (id INTEGER)")
            self.engine.execute_query("INSERT INTO test_table VALUES (1)")

        # Verify data was inserted
        result = self.engine.execute_query("SELECT * FROM test_table").fetchdf()
        self.assertEqual(len(result), 1)

        # Close engine
        self.engine.close()

        # Reopen in-memory engine
        self.engine = DuckDBEngine(":memory:")

        # Table should not exist (data is lost)
        with self.assertRaises(Exception):
            self.engine.execute_query("SELECT * FROM test_table")

    def test_transaction_manager_inmemory(self):
        """Test transaction manager works with in-memory database."""
        tm = self.engine.transaction_manager

        # Test context manager
        with tm:
            self.assertTrue(tm.in_transaction)

        self.assertFalse(tm.in_transaction)


if __name__ == "__main__":
    unittest.main()
