import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from sqlflow.connectors.postgres.destination import PostgresDestination


class TestPostgresDestination(unittest.TestCase):
    @patch("psycopg2.connect")
    @patch("pandas.DataFrame.to_sql")
    def test_write_success(self, mock_to_sql, mock_connect):
        """Test successful write to PostgreSQL."""
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        config = {"table_name": "test_table"}
        df = pd.DataFrame({"a": [1]})
        connector = PostgresDestination(config=config)
        connector.write(df)
        mock_connect.assert_called_once_with(**config)
        mock_to_sql.assert_called_once_with(
            "test_table", mock_conn, if_exists="replace", index=False
        )

    def test_missing_table_name_config(self):
        """Test that an error is raised if 'table_name' is not in destination config."""
        with self.assertRaises(ValueError):
            PostgresDestination(config={})


if __name__ == "__main__":
    unittest.main()
