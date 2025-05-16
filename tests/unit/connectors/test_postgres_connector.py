"""Tests for PostgreSQL connector."""

from unittest.mock import MagicMock, call, patch

import pytest
from psycopg2.extensions import connection, cursor

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.postgres_connector import (
    PostgresConnectionParams,
    PostgresConnector,
)
from sqlflow.core.errors import ConnectorError


@pytest.fixture
def postgres_connector():
    """Create a PostgreSQL connector instance."""
    return PostgresConnector()


@pytest.fixture
def mock_connection():
    """Create a mock PostgreSQL connection."""
    mock_conn = MagicMock(spec=connection)
    mock_cur = MagicMock(spec=cursor)
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "host": "localhost",
        "port": 5432,
        "dbname": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "connect_timeout": 5,
        "application_name": "sqlflow_test",
        "min_connections": 1,
        "max_connections": 3,
    }


def test_postgres_connection_params():
    """Test PostgreSQL connection parameters."""
    params = PostgresConnectionParams(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="test_user",
        password="test_pass",
    )
    assert params.host == "localhost"
    assert params.port == 5432
    assert params.dbname == "test_db"
    assert params.user == "test_user"
    assert params.password == "test_pass"
    assert params.connect_timeout == 10  # Default
    assert params.application_name == "sqlflow"  # Default
    assert params.min_connections == 1  # Default
    assert params.max_connections == 5  # Default


def test_postgres_connector_init(postgres_connector):
    """Test PostgreSQL connector initialization."""
    assert postgres_connector.state == ConnectorState.CREATED
    assert postgres_connector.params is None
    assert postgres_connector.connection_pool is None


def test_postgres_connector_configure(postgres_connector, sample_config):
    """Test configuring PostgreSQL connector."""
    postgres_connector.configure(sample_config)
    assert postgres_connector.state == ConnectorState.CONFIGURED
    assert postgres_connector.params is not None
    assert postgres_connector.params.host == "localhost"
    assert postgres_connector.params.port == 5432
    assert postgres_connector.params.dbname == "test_db"

    # Test missing required fields
    with pytest.raises(ConnectorError, match="Host is required"):
        postgres_connector.configure({"port": 5432})

    with pytest.raises(ConnectorError, match="Database name is required"):
        postgres_connector.configure({"host": "localhost"})


@patch("psycopg2.connect")
def test_postgres_connector_test_connection(
    mock_connect, postgres_connector, sample_config, mock_connection
):
    """Test connection testing for PostgreSQL connector."""
    mock_conn, mock_cur = mock_connection
    mock_connect.return_value = mock_conn

    postgres_connector.configure(sample_config)
    result = postgres_connector.test_connection()

    assert result.success is True
    assert postgres_connector.state == ConnectorState.READY

    # Verify connection attempts - one for test, one for pool
    assert mock_connect.call_count == 2

    # Verify first connection attempt (test)
    first_call = mock_connect.call_args_list[0]
    assert first_call == call(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="test_user",
        password="test_pass",
        connect_timeout=5,
        application_name="sqlflow_test",
    )

    # Verify second connection attempt (pool)
    second_call = mock_connect.call_args_list[1]
    assert second_call == call(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="test_user",
        password="test_pass",
        connect_timeout=5,
        application_name="sqlflow_test",
    )

    # Verify SELECT 1 was executed
    mock_cur.execute.assert_called_once_with("SELECT 1")

    # Test connection failure
    mock_connect.side_effect = Exception("Connection failed")
    result = postgres_connector.test_connection()
    assert result.success is False
    assert "Connection failed" in result.message
    assert postgres_connector.state == ConnectorState.ERROR


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_connector_discover(
    mock_pool, postgres_connector, sample_config, mock_connection
):
    """Test discovery for PostgreSQL connector."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    mock_cur.fetchall.return_value = [("table1",), ("table2",)]

    postgres_connector.configure(sample_config)
    tables = postgres_connector.discover()

    assert tables == ["table1", "table2"]
    mock_cur.execute.assert_called_with(
        """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                    """
    )

    # Test discovery failure
    mock_cur.execute.side_effect = Exception("Discovery failed")
    with pytest.raises(ConnectorError, match="Discovery failed"):
        postgres_connector.discover()


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_connector_get_schema(
    mock_pool, postgres_connector, sample_config, mock_connection
):
    """Test schema retrieval for PostgreSQL connector."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    mock_cur.fetchall.return_value = [
        ("id", "integer"),
        ("name", "character varying"),
        ("value", "numeric"),
    ]

    postgres_connector.configure(sample_config)
    schema = postgres_connector.get_schema("test_table")

    assert len(schema.arrow_schema) == 3
    fields = schema.arrow_schema.names
    assert fields == ["id", "name", "value"]

    # Verify type mapping
    assert str(schema.arrow_schema.field("id").type) == "int64"
    assert str(schema.arrow_schema.field("name").type) == "string"
    assert (
        str(schema.arrow_schema.field("value").type) == "double"
    )  # PostgreSQL numeric maps to double


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_connector_read(
    mock_pool, postgres_connector, sample_config, mock_connection
):
    """Test reading data from PostgreSQL connector."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    # Mock cursor fetchmany to return some data
    mock_cur.fetchmany.side_effect = [
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        [],  # End of data
    ]

    postgres_connector.configure(sample_config)
    chunks = list(postgres_connector.read("test_table", columns=["id", "name"]))

    assert len(chunks) == 1  # One chunk of data
    chunk = chunks[0]
    df = chunk.pandas_df
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]

    # Verify query building
    expected_query = 'SELECT "id", "name" FROM "test_table"'
    mock_cur.execute.assert_called_with(expected_query, [])

    # Reset mock for next test
    mock_cur.fetchmany.side_effect = [
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        [],  # End of data
    ]

    # Test with filters
    chunks = list(
        postgres_connector.read(
            "test_table",
            columns=["id", "name"],
            filters={"id": 1, "name": ["Alice", "Bob"]},
        )
    )

    # Verify filter handling
    mock_cur.execute.assert_called_with(
        'SELECT "id", "name" FROM "test_table" WHERE "id" = %s AND "name" IN (%s, %s)',
        [1, "Alice", "Bob"],
    )


def test_postgres_connector_close(postgres_connector, sample_config):
    """Test closing PostgreSQL connector."""
    mock_pool = MagicMock()
    postgres_connector.configure(sample_config)
    postgres_connector.connection_pool = mock_pool

    postgres_connector.close()
    mock_pool.closeall.assert_called_once()
    assert postgres_connector.connection_pool is None
