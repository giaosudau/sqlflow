import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from sqlflow.connectors.postgres.destination import PostgresDestination


class TestPostgresDestination(unittest.TestCase):
    @patch("sqlflow.connectors.postgres.destination.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_write_success(self, mock_to_sql, mock_create_engine):
        """Test successful write to PostgreSQL."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        config = {"table_name": "test_table"}
        df = pd.DataFrame({"a": [1]})
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
        )

    def test_missing_table_name_config(self):
        """Test that an error is raised if 'table_name' is not in destination config."""
        with self.assertRaises(ValueError):
            PostgresDestination(config={})


if __name__ == "__main__":
    unittest.main()
