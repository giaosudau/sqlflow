"""Tests for Google Sheets source connector."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.google_sheets.source import GoogleSheetsSource


class TestGoogleSheetsSource(unittest.TestCase):
    """Test cases for GoogleSheetsSource."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "credentials_file": "dummy_creds.json",
            "spreadsheet_id": "dummy_id",
            "sheet_name": "Sheet1",
        }

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_configure_success(self, mock_credentials, mock_build):
        """Test successful configuration."""
        mock_credentials.from_service_account_file.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        connector = GoogleSheetsSource()
        connector.configure(self.config)

        self.assertEqual(connector.credentials_file, "dummy_creds.json")
        self.assertEqual(connector.spreadsheet_id, "dummy_id")
        self.assertEqual(connector.sheet_name, "Sheet1")
        self.assertTrue(connector.has_header)

    def test_configure_missing_params(self):
        """Test configuration with missing required parameters."""
        connector = GoogleSheetsSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({})

        self.assertIn("required", str(context.exception))

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_test_connection_success(self, mock_credentials, mock_build):
        """Test successful connection test."""
        # Mock the service and its methods
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock spreadsheet metadata response
        mock_service.spreadsheets().get().execute.return_value = {
            "properties": {"title": "Test Spreadsheet"},
            "sheets": [
                {"properties": {"title": "Sheet1"}},
                {"properties": {"title": "Sheet2"}},
            ],
        }

        connector = GoogleSheetsSource(self.config)
        result = connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertTrue(result.success)
        self.assertIn("Successfully connected", result.message)

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_test_connection_sheet_not_found(self, mock_credentials, mock_build):
        """Test connection test when sheet is not found."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock spreadsheet metadata response without the target sheet
        mock_service.spreadsheets().get().execute.return_value = {
            "properties": {"title": "Test Spreadsheet"},
            "sheets": [
                {"properties": {"title": "Sheet2"}},
                {"properties": {"title": "Sheet3"}},
            ],
        }

        connector = GoogleSheetsSource(self.config)
        result = connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertFalse(result.success)
        self.assertIn("not found", result.message)

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_discover(self, mock_credentials, mock_build):
        """Test discovering available sheets."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock spreadsheet metadata response
        mock_service.spreadsheets().get().execute.return_value = {
            "sheets": [
                {"properties": {"title": "Sheet1"}},
                {"properties": {"title": "Sheet2"}},
                {"properties": {"title": "Data"}},
            ]
        }

        connector = GoogleSheetsSource(self.config)
        sheets = connector.discover()

        self.assertEqual(sheets, ["Sheet1", "Sheet2", "Data"])

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_read_success(self, mock_credentials, mock_build):
        """Test successful read from Google Sheets."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock the API response
        mock_service.spreadsheets().values().get().execute.return_value = {
            "values": [["header1", "header2"], ["data1", "data2"], ["data3", "data4"]]
        }

        connector = GoogleSheetsSource(self.config)
        df = connector.read()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 2))
        self.assertEqual(list(df.columns), ["header1", "header2"])
        self.assertEqual(df.iloc[0, 0], "data1")

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_read_with_columns_filter(self, mock_credentials, mock_build):
        """Test reading with column filtering."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock the API response
        mock_service.spreadsheets().values().get().execute.return_value = {
            "values": [["col1", "col2", "col3"], ["val1", "val2", "val3"]]
        }

        connector = GoogleSheetsSource(self.config)
        df = connector.read(columns=["col1", "col3"])

        self.assertEqual(list(df.columns), ["col1", "col3"])
        self.assertEqual(df.shape, (1, 2))

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_read_empty_sheet(self, mock_credentials, mock_build):
        """Test reading from an empty sheet."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock empty response
        mock_service.spreadsheets().values().get().execute.return_value = {"values": []}

        connector = GoogleSheetsSource(self.config)
        df = connector.read()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_read_no_header(self, mock_credentials, mock_build):
        """Test reading without header row."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = MagicMock()

        # Mock the API response
        mock_service.spreadsheets().values().get().execute.return_value = {
            "values": [["data1", "data2"], ["data3", "data4"]]
        }

        config = self.config.copy()
        config["has_header"] = False

        connector = GoogleSheetsSource(config)
        df = connector.read()

        self.assertEqual(df.shape, (2, 2))
        # Should use default column names when no header
        self.assertNotEqual(list(df.columns), ["data1", "data2"])

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_supports_incremental(self, mock_credentials, mock_build):
        """Test that incremental loading is supported."""
        mock_credentials.from_service_account_file.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        connector = GoogleSheetsSource(self.config)
        self.assertTrue(connector.supports_incremental())

    @patch("sqlflow.connectors.google_sheets.source.build")
    @patch("sqlflow.connectors.google_sheets.source.Credentials")
    def test_get_cursor_value(self, mock_credentials, mock_build):
        """Test getting cursor value from data chunk."""
        mock_credentials.from_service_account_file.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        from sqlflow.connectors.data_chunk import DataChunk

        # Create test data with a cursor field
        test_data = pd.DataFrame(
            {"id": [1, 2, 3], "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03"]}
        )

        chunk = DataChunk(test_data)
        connector = GoogleSheetsSource(self.config)

        cursor_value = connector.get_cursor_value(chunk, "timestamp")
        self.assertEqual(cursor_value, "2023-01-03")


if __name__ == "__main__":
    unittest.main()
