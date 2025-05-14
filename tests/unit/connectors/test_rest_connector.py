"""Tests for REST export connector."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from sqlflow.sqlflow.connectors.data_chunk import DataChunk
from sqlflow.sqlflow.connectors.rest_connector import (
    RESTExportConnector,
)
from sqlflow.sqlflow.core.errors import ConnectorError


@pytest.fixture
def rest_connector():
    """Create a REST connector for testing."""
    connector = RESTExportConnector()
    connector.configure(
        {
            "base_url": "https://api.example.com",
            "auth_method": "api_key",
            "auth_params": {
                "key_name": "X-API-Key",
                "key_value": "test-key",
                "location": "header",
            },
            "timeout": 5,
            "max_retries": 2,
            "batch_size": 10,
        }
    )
    return connector


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}
    )
    return DataChunk(df)


@patch("requests.post")
def test_rest_connector_write(mock_post, rest_connector, sample_data):
    """Test writing data to a REST API."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    rest_connector.write("/users", sample_data)

    call_args = mock_post.call_args
    assert call_args[0][0] == "https://api.example.com/users"

    headers = call_args[1]["headers"]
    assert headers["X-API-Key"] == "test-key"
    assert headers["Content-Type"] == "application/json"

    data = json.loads(call_args[1]["data"])
    assert len(data) == 3
    assert data[0]["name"] == "Alice"
    assert data[1]["name"] == "Bob"
    assert data[2]["name"] == "Charlie"


@patch("requests.get")
def test_rest_connector_test_connection(mock_get, rest_connector):
    """Test connection to a REST API."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = rest_connector.test_connection()

    mock_get.assert_called_once_with(
        "https://api.example.com",
        headers=rest_connector.headers,
        auth=None,
        timeout=5,
        **rest_connector.request_params,
    )

    assert result.success is True


@patch("requests.post")
def test_rest_connector_write_retry(mock_post, rest_connector, sample_data):
    """Test retry logic when writing data to a REST API."""
    error_response = MagicMock()
    error_response.raise_for_status.side_effect = requests.exceptions.RequestException(
        "Connection error"
    )

    success_response = MagicMock()
    success_response.status_code = 200

    mock_post.side_effect = [error_response, success_response]

    rest_connector.write("/users", sample_data)

    assert mock_post.call_count == 2


@patch("requests.post")
def test_rest_connector_streaming_mode(mock_post, sample_data):
    """Test streaming mode for the REST connector."""
    connector = RESTExportConnector()
    connector.configure(
        {
            "base_url": "https://api.example.com",
            "streaming_mode": True,
            "batch_size": 10,
        }
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    connector.write("/users", sample_data)

    assert mock_post.call_count == 3

    for i, call in enumerate(mock_post.call_args_list):
        data = json.loads(call[1]["data"])
        assert isinstance(data, dict)  # Single record, not a list
        assert data["name"] in ["Alice", "Bob", "Charlie"]


def test_rest_connector_missing_url():
    """Test error handling when base_url is missing."""
    connector = RESTExportConnector()

    with pytest.raises(ConnectorError):
        connector.configure({})


def test_rest_connector_invalid_auth():
    """Test error handling for invalid auth method."""
    connector = RESTExportConnector()

    with pytest.raises(ConnectorError):
        connector.configure(
            {"base_url": "https://api.example.com", "auth_method": "invalid_method"}
        )


@pytest.fixture
def basic_auth_connector():
    """Create a REST connector with basic auth for testing."""
    connector = RESTExportConnector()
    connector.configure(
        {
            "base_url": "https://api.example.com",
            "auth_method": "basic",
            "auth_params": {"username": "testuser", "password": "testpass"},
        }
    )
    return connector


@patch("requests.get")
def test_basic_auth(mock_get, basic_auth_connector):
    """Test basic authentication."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    basic_auth_connector.test_connection()

    call_args = mock_get.call_args
    assert call_args[1]["auth"] is not None


@pytest.fixture
def bearer_auth_connector():
    """Create a REST connector with bearer auth for testing."""
    connector = RESTExportConnector()
    connector.configure(
        {
            "base_url": "https://api.example.com",
            "auth_method": "bearer",
            "auth_params": {"token": "test-token"},
        }
    )
    return connector


@patch("requests.get")
def test_bearer_auth(mock_get, bearer_auth_connector):
    """Test bearer token authentication."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    bearer_auth_connector.test_connection()

    call_args = mock_get.call_args
    headers = call_args[1]["headers"]
    assert headers["Authorization"] == "Bearer test-token"


@pytest.fixture
def oauth_auth_connector():
    """Create a REST connector with OAuth auth for testing."""
    connector = RESTExportConnector()
    connector.configure(
        {
            "base_url": "https://api.example.com",
            "auth_method": "oauth",
            "auth_params": {"token": "test-oauth-token", "token_type": "OAuth"},
        }
    )
    return connector


@patch("requests.get")
def test_oauth_auth(mock_get, oauth_auth_connector):
    """Test OAuth authentication."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    oauth_auth_connector.test_connection()

    call_args = mock_get.call_args
    headers = call_args[1]["headers"]
    assert headers["Authorization"] == "OAuth test-oauth-token"


@patch("requests.get")
def test_connection_error(mock_get):
    """Test handling of connection errors."""
    connector = RESTExportConnector()
    connector.configure({"base_url": "https://api.example.com"})

    mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

    result = connector.test_connection()

    assert result.success is False
    assert "Connection refused" in result.message
    assert connector.state.name == "ERROR"
