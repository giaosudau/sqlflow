import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from sqlflow.connectors.postgres.source import PostgresSource


class TestPostgresSource(unittest.TestCase):
    @patch("sqlflow.connectors.postgres.source.create_engine")
    @patch("pandas.read_sql_query")
    def test_read_success_simple_query(self, mock_read_sql_query, mock_create_engine):
        """Test successful read from PostgreSQL with simple query (non-cursor path)."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        # Use a simple query that won't trigger server-side cursor
        config = {"query": "SELECT id FROM test"}
        connector = PostgresSource(config=config)

        # Mock pandas chunked iterator
        mock_df = pd.DataFrame({"id": [1, 2, 3]})
        mock_read_sql_query.return_value = iter([mock_df])

        # Consume the generator to trigger the call
        chunks = list(connector.read(options=config))

        self.assertEqual(len(chunks), 1)
        mock_create_engine.assert_called_once()
        mock_read_sql_query.assert_called_once()

    @patch("sqlflow.connectors.postgres.source.create_engine")
    @patch("pandas.read_sql_query")
    def test_read_success_cursor_query(self, mock_read_sql_query, mock_create_engine):
        """Test successful read from PostgreSQL with aggregation query (cursor path)."""
        # Mock the engine and raw connection for server-side cursor
        mock_engine = MagicMock()
        mock_raw_connection = MagicMock()
        mock_cursor = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.raw_connection.return_value = mock_raw_connection
        mock_raw_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock cursor behavior
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchmany.side_effect = [
            [(1, "test1"), (2, "test2")],  # First batch
            [],  # End of results
        ]

        # Use an aggregation query that will trigger server-side cursor
        config = {"query": "SELECT COUNT(*) FROM test"}
        connector = PostgresSource(config=config)

        # Consume the generator to trigger the call
        chunks = list(connector.read(options=config))

        self.assertEqual(len(chunks), 1)
        mock_create_engine.assert_called_once()
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchmany.assert_called()

    def test_missing_query_config(self):
        """Test that an error is raised if 'query' is not in source config."""
        with self.assertRaises(ValueError):
            PostgresSource(config={})

    @patch("sqlflow.connectors.postgres.source.create_engine")
    def test_read_failure(self, mock_create_engine):
        """Test read failure from PostgreSQL."""
        # Mock the engine to raise an exception
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("Connection failed")

        # Use simple query to avoid server-side cursor path
        config = {"query": "SELECT id FROM test"}
        connector = PostgresSource(config=config)
        with self.assertRaises(Exception):
            list(connector.read(options=config))

        mock_create_engine.assert_called_once()

    @patch("sqlflow.connectors.postgres.source.create_engine")
    def test_query_complexity_estimation(self, mock_create_engine):
        """Test query complexity estimation and batch size optimization."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresSource(config={"query": "SELECT id FROM test"})

        # Test simple query
        complexity = connector._estimate_query_complexity("SELECT id FROM users")
        self.assertEqual(complexity, "simple")

        # Test medium complexity query
        complexity = connector._estimate_query_complexity(
            "SELECT id FROM users JOIN orders ON users.id = orders.user_id"
        )
        self.assertEqual(complexity, "medium")

        # Test complex query
        complexity = connector._estimate_query_complexity(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.id HAVING count(*) > 5"
        )
        self.assertEqual(complexity, "complex")

    @patch("sqlflow.connectors.postgres.source.create_engine")
    def test_server_side_cursor_detection(self, mock_create_engine):
        """Test server-side cursor detection logic."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresSource(config={"query": "SELECT id FROM test"})

        # Test queries that should use server-side cursor in production
        # (Note: simple SELECT * doesn't trigger cursor in test environments)
        self.assertTrue(
            connector._use_server_side_cursor("SELECT COUNT(*) FROM large_table")
        )
        self.assertTrue(
            connector._use_server_side_cursor("SELECT SUM(amount) FROM transactions")
        )
        self.assertTrue(
            connector._use_server_side_cursor(
                "SELECT id, name FROM users WHERE id IN (SELECT user_id FROM orders GROUP BY user_id HAVING count(*) > 5)"
            )
        )

        # Test queries that should not use server-side cursor
        self.assertFalse(connector._use_server_side_cursor("SELECT id FROM users"))
        self.assertFalse(
            connector._use_server_side_cursor("SELECT name FROM products WHERE id = 1")
        )
        # Simple SELECT * should not use cursor in test environments
        self.assertFalse(connector._use_server_side_cursor("SELECT * FROM small_table"))


if __name__ == "__main__":
    unittest.main()
