import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from sqlflow.connectors.google_sheets.google_sheets_connector import (
    GoogleSheetsConnector,
)


class TestGoogleSheetsConnector(unittest.TestCase):
    @patch(
        "sqlflow.connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._initialize_service"
    )
    def test_read_success(self, mock_initialize_service):
        """Test successful read from Google Sheets."""
        # Mock the service returned by _initialize_service
        mock_service = MagicMock()
        mock_initialize_service.return_value = mock_service

        # Mock the API response
        mock_values = MagicMock()
        mock_get = MagicMock()
        mock_get.execute.return_value = {
            "values": [["header1", "header2"], ["data1", "data2"]]
        }
        mock_values.get.return_value = mock_get
        mock_service.spreadsheets.return_value.values.return_value = mock_values

        config = {
            "credentials_file": "dummy_creds.json",
            "spreadsheet_id": "dummy_id",
            "sheet_name": "Sheet1",
        }
        connector = GoogleSheetsConnector(config)
        df = connector.read()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1, 2))
        self.assertEqual(list(df.columns), ["header1", "header2"])

    def test_missing_config(self):
        """Test that an error is raised if config is missing."""
        with self.assertRaises(ValueError):
            GoogleSheetsConnector(config={})


if __name__ == "__main__":
    unittest.main()
