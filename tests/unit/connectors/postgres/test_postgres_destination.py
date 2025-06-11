import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from sqlflow.connectors.postgres.destination import PostgresDestination


class TestPostgresDestination(unittest.TestCase):
    @patch("sqlflow.connectors.postgres.destination.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_write_success_small_dataset(self, mock_to_sql, mock_create_engine):
        """Test successful write to PostgreSQL with small dataset (direct write)."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        config = {"table_name": "test_table"}
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        connector = PostgresDestination(config=config)
        connector.write(df)

        mock_create_engine.assert_called_once()
        # Verify that to_sql was called on the DataFrame
        mock_to_sql.assert_called_once_with(
            "test_table",
            mock_connection,
            schema="public",
            if_exists="replace",
            index=False,
            method="multi",
        )

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_write_success_large_dataset_copy(self, mock_create_engine):
        """Test successful write to PostgreSQL with large dataset using COPY."""
        # Mock the engine and raw connection for COPY
        mock_engine = MagicMock()
        mock_raw_connection = MagicMock()
        mock_cursor = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.raw_connection.return_value = mock_raw_connection
        mock_raw_connection.cursor.return_value.__enter__.return_value = mock_cursor

        config = {"table_name": "test_table"}
        # Create large dataset that will trigger COPY
        large_data = {
            "id": list(range(15000)),
            "name": [f"user_{i}" for i in range(15000)],
            "value": [i * 1.5 for i in range(15000)],
        }
        df = pd.DataFrame(large_data)

        connector = PostgresDestination(config=config)
        connector.write(df, mode="replace")

        mock_create_engine.assert_called_once()
        # Verify COPY was used
        mock_cursor.copy_expert.assert_called_once()
        mock_raw_connection.commit.assert_called()

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_write_success_chunked(self, mock_to_sql, mock_create_engine):
        """Test successful write to PostgreSQL with chunked writing."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        config = {"table_name": "test_table"}
        # Create dataset with complex schema that won't use COPY but will use chunking
        complex_data = {
            "id": list(range(8000)),
            "large_text": ["x" * 1500] * 8000,  # Large text to make it complex
            "value": [i * 1.5 for i in range(8000)],
        }
        df = pd.DataFrame(complex_data)

        connector = PostgresDestination(config=config)
        connector.write(df, mode="append")

        mock_create_engine.assert_called_once()
        # Should use chunked writing (multiple to_sql calls)
        self.assertGreater(mock_to_sql.call_count, 1)

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_write_success_upsert(self, mock_create_engine):
        """Test successful upsert operation using temporary table strategy."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = MagicMock()

        config = {"table_name": "test_table"}
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["alice", "bob", "charlie"],
                "value": [100, 200, 300],
            }
        )

        connector = PostgresDestination(config=config)
        connector.write(df, mode="upsert", keys=["id"])

        mock_create_engine.assert_called_once()
        # Should create and drop temporary table
        mock_connection.execute.assert_called()

    def test_missing_table_name_config(self):
        """Test that an error is raised if 'table_name' is not in destination config."""
        with self.assertRaises(ValueError):
            PostgresDestination(config={})

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_table_complexity_estimation(self, mock_create_engine):
        """Test table complexity estimation for write optimization."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresDestination(config={"table_name": "test"})

        # Test simple table
        simple_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        complexity = connector._estimate_table_complexity(simple_df)
        self.assertEqual(complexity, "simple")

        # Test medium complexity table (many columns)
        medium_df = pd.DataFrame({f"col_{i}": [1, 2, 3] for i in range(25)})
        complexity = connector._estimate_table_complexity(medium_df)
        self.assertEqual(complexity, "medium")

        # Test complex table (large text)
        complex_df = pd.DataFrame(
            {"id": [1], "large_text": ["x" * 2000]}  # Large text content
        )
        complexity = connector._estimate_table_complexity(complex_df)
        self.assertEqual(complexity, "complex")

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_batch_size_optimization(self, mock_create_engine):
        """Test batch size optimization based on data characteristics."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresDestination(config={"table_name": "test"})

        # Test simple table gets larger batch size
        simple_df = pd.DataFrame({"id": list(range(50000)), "name": ["test"] * 50000})
        batch_size = connector._get_optimal_batch_size(simple_df)
        self.assertGreater(
            batch_size, 10000
        )  # Should use larger batch for simple tables

        # Test complex table gets smaller batch size
        complex_df = pd.DataFrame(
            {"id": list(range(50000)), "large_text": ["x" * 2000] * 50000}
        )
        batch_size = connector._get_optimal_batch_size(complex_df)
        self.assertLess(batch_size, 5000)  # Should use smaller batch for complex tables

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_copy_strategy_detection(self, mock_create_engine):
        """Test COPY strategy detection logic."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresDestination(config={"table_name": "test"})

        # Large dataset with replace mode should use COPY
        large_df = pd.DataFrame({"id": list(range(15000)), "name": ["test"] * 15000})
        should_copy = connector._should_use_copy(large_df, "replace")
        self.assertTrue(should_copy)

        # Small dataset should not use COPY
        small_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        should_copy = connector._should_use_copy(small_df, "replace")
        self.assertFalse(should_copy)

        # Medium dataset with simple schema should use COPY
        medium_simple_df = pd.DataFrame(
            {"id": list(range(8000)), "name": ["test"] * 8000}
        )
        should_copy = connector._should_use_copy(medium_simple_df, "replace")
        self.assertTrue(should_copy)

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_connection_pool_optimization(self, mock_create_engine):
        """Test connection pool optimization for write operations."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresDestination(config={"table_name": "test"})

        # Access engine to trigger pool creation
        _ = connector.engine

        # Verify create_engine was called with pool configuration
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args

        # Check that pool configuration was included
        self.assertIn("pool_size", call_args[1])
        self.assertIn("max_overflow", call_args[1])
        self.assertIn("poolclass", call_args[1])

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_optimized_connect_args(self, mock_create_engine):
        """Test optimized connection arguments for write performance."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresDestination(config={"table_name": "test"})

        # Access engine to trigger connection arg setup
        _ = connector.engine

        # Verify create_engine was called with optimized connect_args
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args

        # Check that connect_args were included
        self.assertIn("connect_args", call_args[1])
        connect_args = call_args[1]["connect_args"]
        self.assertIn("application_name", connect_args)
        self.assertIn("options", connect_args)
        # Verify write-optimized PostgreSQL settings
        self.assertIn("synchronous_commit=off", connect_args["options"])

    @patch("sqlflow.connectors.postgres.destination.create_engine")
    def test_write_failure_handling(self, mock_create_engine):
        """Test write failure handling and error propagation."""
        # Mock the engine to raise an exception during write
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("Connection failed")

        config = {"table_name": "test_table"}
        df = pd.DataFrame({"a": [1, 2, 3]})
        connector = PostgresDestination(config=config)

        with self.assertRaises(Exception):
            connector.write(df)

        mock_create_engine.assert_called_once()


if __name__ == "__main__":
    unittest.main()
