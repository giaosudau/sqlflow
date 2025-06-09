import unittest
from unittest.mock import MagicMock, patch

from sqlflow.connectors.postgres.source import PostgresSource


class TestPostgresSource(unittest.TestCase):
    @patch("psycopg2.connect")
    @patch("pandas.read_sql_query")
    def test_read_success(self, mock_read_sql_query, mock_connect):
        """Test successful read from PostgreSQL."""
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        config = {"query": "SELECT * FROM test"}
        connector = PostgresSource(config=config)
        connector.read()
        mock_connect.assert_called_once_with(**config)
        mock_read_sql_query.assert_called_once_with("SELECT * FROM test", mock_conn)

    def test_missing_query_config(self):
        """Test that an error is raised if 'query' is not in source config."""
        with self.assertRaises(ValueError):
            PostgresSource(config={})


if __name__ == "__main__":
    unittest.main()
