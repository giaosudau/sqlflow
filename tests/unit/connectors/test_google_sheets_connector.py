"""Tests for Google Sheets connector."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.google_sheets_connector import GoogleSheetsConnector
from sqlflow.core.errors import ConnectorError


@pytest.fixture
def google_sheets_connector():
    """Create a Google Sheets connector instance."""
    return GoogleSheetsConnector()


@pytest.fixture
def mock_sheets_service():
    """Create a mock Google Sheets service."""
    mock_service = MagicMock()
    mock_spreadsheets = MagicMock()
    mock_values = MagicMock()
    mock_get = MagicMock()
    mock_update = MagicMock()

    # Configure the mocks to handle method chaining
    mock_service.spreadsheets.return_value = mock_spreadsheets
    mock_spreadsheets.get.return_value = mock_get
    mock_spreadsheets.values.return_value = mock_values
    mock_values.get.return_value = mock_get
    mock_values.update.return_value = mock_update

    # Mock execute() calls
    mock_get.execute.return_value = {
        "properties": {"title": "Test Spreadsheet"},
        "values": [
            ["id", "name", "value"],
            ["1", "Alice", "10.5"],
            ["2", "Bob", "20.0"],
            ["3", "Charlie", "30.0"],
        ],
    }
    mock_update.execute.return_value = {"updatedRange": "Sheet1!A1:C4"}

    return mock_service


@pytest.fixture
def mock_credentials():
    """Create mock credentials."""
    mock_creds = MagicMock()
    if hasattr(mock_creds, "universe_domain"):
        mock_creds.universe_domain = "googleapis.com"

    # Set up mock for authorize method
    mock_http = MagicMock()
    mock_creds.authorize.return_value = mock_http

    # Set up the mock HTTP request to return a proper response
    mock_http.request.return_value = (
        MagicMock(status=200),  # Response
        b'{"success": true}',  # Content
    )

    return mock_creds


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "credentials_file": "path/to/credentials.json",
        "spreadsheet_id": "test-spreadsheet-id",
        "sheet_name": "Sheet1",
        "range": "A1:D100",
        "has_header": True,
    }


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.0, 30.0],
    }
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    return DataChunk(table)


def test_google_sheets_connector_init(google_sheets_connector):
    """Test Google Sheets connector initialization."""
    assert google_sheets_connector.state == ConnectorState.CREATED
    assert google_sheets_connector.credentials_file is None
    assert google_sheets_connector.spreadsheet_id is None
    assert google_sheets_connector.sheet_name is None
    assert google_sheets_connector.range is None
    assert google_sheets_connector.has_header is True
    assert google_sheets_connector.service is None


def test_google_sheets_connector_configure(google_sheets_connector, sample_config):
    """Test configuring Google Sheets connector."""
    google_sheets_connector.configure(sample_config)
    assert google_sheets_connector.state == ConnectorState.CONFIGURED
    assert google_sheets_connector.credentials_file == "path/to/credentials.json"
    assert google_sheets_connector.spreadsheet_id == "test-spreadsheet-id"
    assert google_sheets_connector.sheet_name == "Sheet1"
    assert google_sheets_connector.range == "A1:D100"
    assert google_sheets_connector.has_header is True

    # Test missing required fields
    with pytest.raises(ConnectorError, match="Credentials file is required"):
        google_sheets_connector.configure({})

    with pytest.raises(ConnectorError, match="Spreadsheet ID is required"):
        google_sheets_connector.configure(
            {"credentials_file": "path/to/credentials.json"}
        )


@patch("sqlflow.connectors.google_sheets_connector.build")
@patch("sqlflow.connectors.google_sheets_connector.Credentials")
def test_google_sheets_connector_test_connection(
    mock_credentials_class,
    mock_build,
    google_sheets_connector,
    sample_config,
    mock_sheets_service,
    mock_credentials,
):
    """Test connection testing for Google Sheets connector."""
    # Setup mocks
    mock_credentials_class.from_service_account_file.return_value = mock_credentials
    mock_build.return_value = mock_sheets_service

    # Configure and test connection
    google_sheets_connector.configure(sample_config)
    result = google_sheets_connector.test_connection()

    # Verify results
    assert result.success is True
    assert google_sheets_connector.state == ConnectorState.READY

    # Verify service initialization
    mock_credentials_class.from_service_account_file.assert_called_once_with(
        "path/to/credentials.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    mock_build.assert_called_once()

    # Test connection failure scenario
    mock_sheets_service.spreadsheets.return_value.get.return_value.execute.side_effect = Exception(
        "Connection failed"
    )
    result = google_sheets_connector.test_connection()
    assert result.success is False
    assert "Connection failed" in result.message
    assert google_sheets_connector.state == ConnectorState.ERROR


@patch("sqlflow.connectors.google_sheets_connector.build")
@patch("sqlflow.connectors.google_sheets_connector.Credentials")
def test_google_sheets_connector_read(
    mock_credentials_class,
    mock_build,
    google_sheets_connector,
    sample_config,
    mock_sheets_service,
    mock_credentials,
):
    """Test reading data from Google Sheets."""
    # Setup mocks
    mock_credentials_class.from_service_account_file.return_value = mock_credentials
    mock_build.return_value = mock_sheets_service

    # Configure connector
    google_sheets_connector.configure(sample_config)

    # Setup mock return value for the get values call
    mock_sheets_service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": [
            ["id", "name", "value"],
            ["1", "Alice", "10.5"],
            ["2", "Bob", "20.0"],
            ["3", "Charlie", "30.0"],
        ]
    }

    # Call the read method
    chunks = list(google_sheets_connector.read("Sheet1"))

    # Verify results
    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 3)
    assert list(df.columns) == ["id", "name", "value"]

    # Verify API calls were made correctly
    mock_build.assert_called_once()
    mock_sheets_service.spreadsheets.assert_called()
    mock_sheets_service.spreadsheets.return_value.values.assert_called()
    mock_sheets_service.spreadsheets.return_value.values.return_value.get.assert_called()


@patch("sqlflow.connectors.google_sheets_connector.build")
@patch("sqlflow.connectors.google_sheets_connector.Credentials")
def test_google_sheets_connector_write(
    mock_credentials_class,
    mock_build,
    google_sheets_connector,
    sample_config,
    mock_sheets_service,
    mock_credentials,
    sample_data,
):
    """Test writing data to Google Sheets."""
    # Setup mocks
    mock_credentials_class.from_service_account_file.return_value = mock_credentials
    mock_build.return_value = mock_sheets_service

    # Configure connector
    google_sheets_connector.configure(sample_config)

    # Call the write method
    google_sheets_connector.write("Sheet1", sample_data)

    # Verify API calls
    mock_build.assert_called_once()
    mock_sheets_service.spreadsheets.assert_called()
    mock_sheets_service.spreadsheets.return_value.values.assert_called()
    mock_sheets_service.spreadsheets.return_value.values.return_value.update.assert_called()

    # Verify update parameters
    update_call = (
        mock_sheets_service.spreadsheets.return_value.values.return_value.update
    )
    update_call.assert_called_once()
    args, kwargs = update_call.call_args
    assert kwargs["spreadsheetId"] == "test-spreadsheet-id"
    assert kwargs["range"] == "Sheet1!A1"
    assert kwargs["valueInputOption"] == "RAW"
    assert len(kwargs["body"]["values"]) == 4  # Header + 3 rows


def test_google_sheets_connector_close(google_sheets_connector, sample_config):
    """Test closing Google Sheets connector."""
    google_sheets_connector.configure(sample_config)
    google_sheets_connector.close()
    assert google_sheets_connector.service is None
