"""Tests for PostgreSQL placeholder connector."""

import pytest

# Skip all tests in this file if psycopg2 is available
# The placeholder connector is only used when psycopg2 is not installed
try:
    pass

    pytest.skip(
        "psycopg2 is available, using real PostgreSQL connector instead of placeholder",
        allow_module_level=True,
    )
except ImportError:
    pass

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.postgres_placeholder import PostgresPlaceholderConnector
from sqlflow.core.errors import ConnectorError


class TestPostgresPlaceholderConnector:
    """Test PostgreSQL placeholder connector functionality."""

    def test_initialization(self):
        """Test connector initialization."""
        connector = PostgresPlaceholderConnector()
        assert connector.state == ConnectorState.ERROR

    def test_configure_raises_error(self):
        """Test that configure always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            connector.configure({"host": "localhost", "port": 5432})

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)
        assert "pip install psycopg2-binary" in str(exc_info.value)

    def test_test_connection_returns_failure(self):
        """Test that test_connection always returns failure."""
        connector = PostgresPlaceholderConnector()

        result = connector.test_connection()

        assert result.success is False
        assert "PostgreSQL connector requires psycopg2 package" in result.message
        assert "pip install psycopg2-binary" in result.message

    def test_discover_raises_error(self):
        """Test that discover always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            connector.discover()

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)
        assert "pip install psycopg2-binary" in str(exc_info.value)

    def test_get_schema_raises_error(self):
        """Test that get_schema always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            connector.get_schema("test_table")

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)
        assert "pip install psycopg2-binary" in str(exc_info.value)

    def test_read_raises_error(self):
        """Test that read always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            list(connector.read("test_table"))

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)
        assert "pip install psycopg2-binary" in str(exc_info.value)

    def test_read_with_columns_raises_error(self):
        """Test that read with columns always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            list(connector.read("test_table", columns=["id", "name"]))

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)

    def test_read_with_filters_raises_error(self):
        """Test that read with filters always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            list(connector.read("test_table", filters={"active": True}))

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)

    def test_read_with_batch_size_raises_error(self):
        """Test that read with custom batch size always raises an error."""
        connector = PostgresPlaceholderConnector()

        with pytest.raises(ConnectorError) as exc_info:
            list(connector.read("test_table", batch_size=1000))

        assert "PostgreSQL connector requires psycopg2 package" in str(exc_info.value)

    def test_error_message_consistency(self):
        """Test that all methods return consistent error messages."""
        connector = PostgresPlaceholderConnector()
        expected_message = "PostgreSQL connector requires psycopg2 package. Please install it with: pip install psycopg2-binary"

        # Test configure error message
        with pytest.raises(
            ConnectorError, match="PostgreSQL connector requires psycopg2 package"
        ):
            connector.configure({})

        # Test test_connection message
        result = connector.test_connection()
        assert expected_message in result.message

        # Test discover error message
        with pytest.raises(
            ConnectorError, match="PostgreSQL connector requires psycopg2 package"
        ):
            connector.discover()

        # Test get_schema error message
        with pytest.raises(
            ConnectorError, match="PostgreSQL connector requires psycopg2 package"
        ):
            connector.get_schema("table")

        # Test read error message
        with pytest.raises(
            ConnectorError, match="PostgreSQL connector requires psycopg2 package"
        ):
            list(connector.read("table"))

    def test_connector_state_remains_error(self):
        """Test that connector state remains ERROR throughout operations."""
        connector = PostgresPlaceholderConnector()

        # Initial state should be ERROR
        assert connector.state == ConnectorState.ERROR

        # State should remain ERROR after failed operations
        try:
            connector.configure({})
        except ConnectorError:
            pass

        assert connector.state == ConnectorState.ERROR

        # Test connection should not change state
        connector.test_connection()
        assert connector.state == ConnectorState.ERROR
