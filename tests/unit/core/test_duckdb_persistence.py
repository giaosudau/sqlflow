import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from sqlflow.core.engines.duckdb_engine import (
    DuckDBEngine,
)


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

        # Check properties
        self.assertEqual(tm.database_path, self.db_path)
        self.assertTrue(tm.is_persistent)
        self.assertFalse(tm.transaction_active)
        self.assertTrue(tm.auto_checkpoint)

    def test_transaction_operations(self):
        """Test basic transaction operations."""
        tm = self.engine.transaction_manager

        # Test begin
        tm.begin()
        self.assertTrue(tm.transaction_active)

        # Test commit
        tm.commit()
        self.assertFalse(tm.transaction_active)

        # Test rollback
        tm.begin()
        self.assertTrue(tm.transaction_active)
        tm.rollback()
        self.assertFalse(tm.transaction_active)

    def test_context_manager(self):
        """Test transaction manager as context manager."""
        tm = self.engine.transaction_manager

        # Test successful transaction
        with tm:
            self.assertTrue(tm.transaction_active)
            self.engine.execute_query("CREATE TABLE test_table (id INTEGER)")

        # Transaction should be committed
        self.assertFalse(tm.transaction_active)

        # Table should exist
        result = self.engine.execute_query("SELECT * FROM test_table").fetchdf()
        self.assertEqual(len(result), 0)  # Empty table

        # Test transaction with exception
        try:
            with tm:
                self.assertTrue(tm.transaction_active)
                self.engine.execute_query("INSERT INTO test_table VALUES (1)")
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Transaction should be rolled back
        self.assertFalse(tm.transaction_active)

        # Table should be empty
        result = self.engine.execute_query("SELECT * FROM test_table").fetchdf()
        self.assertEqual(len(result), 0)  # Empty table

    def test_persistence_verification(self):
        """Test persistence verification functionality."""
        tm = self.engine.transaction_manager

        # Verify database file exists
        self.assertTrue(os.path.exists(self.db_path))

        # Manually create and write to the database file to ensure it exists with content
        with open(self.db_path, "wb") as f:
            f.write(b"TESTDATA")

        # Verify persistence
        self.assertTrue(tm.verify_persistence())

        # Create a table and verify it persists
        with tm:
            self.engine.execute_query(
                "CREATE TABLE persistence_test (id INTEGER, name VARCHAR)"
            )
            self.engine.execute_query("INSERT INTO persistence_test VALUES (1, 'test')")

        # Check file size increased
        file_size = os.path.getsize(self.db_path)
        self.assertGreater(file_size, 0)

    def test_data_durability(self):
        """Test that data persists when engine is closed and reopened."""
        # Create test data
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create table and insert data
        with self.engine.transaction_manager:
            self.engine.connection.register("test_data", test_data)
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

    def test_checkpoint_after_commit(self):
        """Test checkpoint is executed after commit."""
        # Mock the execute_query method instead of connection.execute
        with patch.object(self.engine, "execute_query") as mock_execute_query:
            # Start a transaction
            self.engine.transaction_manager.begin()

            # Perform a commit which should trigger a checkpoint
            self.engine.transaction_manager.commit()

            # Check that CHECKPOINT was called
            mock_execute_query.assert_called_with("CHECKPOINT")

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

        # Transaction should be rolled back
        self.assertFalse(tm.transaction_active)

        # Table should not exist due to rollback
        with self.assertRaises(Exception):
            self.engine.execute_query("SELECT * FROM error_test")


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

    def test_inmemory_persistence_verification(self):
        """Test that in-memory databases correctly report no persistence."""
        tm = self.engine.transaction_manager

        # Verify persistence returns False for in-memory database
        self.assertFalse(tm.verify_persistence())

        # Create a table and verify
        with tm:
            self.engine.execute_query("CREATE TABLE test_table (id INTEGER)")
            self.engine.execute_query("INSERT INTO test_table VALUES (1)")

        # Should still report no persistence
        self.assertFalse(tm.verify_persistence())

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

    def test_checkpoint_skipped_for_inmemory(self):
        """Test checkpoint is skipped for in-memory databases."""
        # Mock the execute_query method
        with patch.object(self.engine, "execute_query") as mock_execute_query:
            # Start and commit a transaction
            tm = self.engine.transaction_manager
            tm.begin()
            tm.commit()

            # CHECKPOINT should not be called for in-memory database
            for call in mock_execute_query.call_args_list:
                self.assertNotEqual(call[0][0], "CHECKPOINT")


if __name__ == "__main__":
    unittest.main()
