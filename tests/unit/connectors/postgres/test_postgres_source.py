import unittest
from unittest.mock import MagicMock, patch

from sqlflow.connectors.postgres.source import PostgresSource


class TestPostgresSource(unittest.TestCase):
    @patch("sqlflow.connectors.postgres.source.create_engine")
    @patch("pandas.read_sql_query")
    def test_read_success(self, mock_read_sql_query, mock_create_engine):
        """Test successful read from PostgreSQL."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        config = {"query": "SELECT * FROM test"}
        connector = PostgresSource(config=config)
        # We need to consume the generator to trigger the call
        _ = list(connector.read(options=config))

        mock_create_engine.assert_called_once()
        mock_read_sql_query.assert_called_once()

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

        config = {"query": "SELECT * FROM test"}
        connector = PostgresSource(config=config)
        with self.assertRaises(Exception):
            list(connector.read(options=config))

        mock_create_engine.assert_called_once()


if __name__ == "__main__":
    unittest.main()
