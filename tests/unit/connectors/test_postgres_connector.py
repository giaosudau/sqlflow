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
        database="test_db",
        username="test_user",
        password="test_pass",
    )
    assert params.host == "localhost"
    assert params.port == 5432
    assert params.database == "test_db"
    assert params.username == "test_user"
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
    assert postgres_connector.params.database == "test_db"

    # Test missing required fields
    with pytest.raises(
        Exception
    ):  # Should catch ParameterError for missing database/dbname
        postgres_connector.configure({"host": "localhost"})

    with pytest.raises(
        Exception
    ):  # Should catch ParameterError for missing user/username
        postgres_connector.configure({"host": "localhost", "dbname": "test_db"})


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
    assert mock_connect.call_count >= 1

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
        sslmode="prefer",
    )

    # Verify version query was executed
    mock_cur.execute.assert_any_call("SELECT version()")

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
                    WHERE table_schema = %s
                    ORDER BY table_name
                    """,
        ("public",),
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


def test_postgres_connector_backward_compatibility(postgres_connector):
    """Test backward compatibility with old parameter names (dbname, user)."""
    old_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "test_db",  # Old parameter name
        "user": "test_user",  # Old parameter name
        "password": "test_pass",
    }

    postgres_connector.configure(old_config)
    assert postgres_connector.state == ConnectorState.CONFIGURED
    assert postgres_connector.params is not None
    assert (
        postgres_connector.params.database == "test_db"
    )  # Should be normalized to new name
    assert (
        postgres_connector.params.username == "test_user"
    )  # Should be normalized to new name


def test_postgres_connector_industry_standard_params(postgres_connector):
    """Test industry-standard parameter names (database, username)."""
    new_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",  # New industry-standard name
        "username": "test_user",  # New industry-standard name
        "password": "test_pass",
    }

    postgres_connector.configure(new_config)
    assert postgres_connector.state == ConnectorState.CONFIGURED
    assert postgres_connector.params is not None
    assert postgres_connector.params.database == "test_db"
    assert postgres_connector.params.username == "test_user"


def test_postgres_connector_parameter_precedence(postgres_connector):
    """Test that new parameter names take precedence over old ones when both are provided."""
    mixed_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "old_db",  # Old parameter name
        "database": "new_db",  # New parameter name (should take precedence)
        "user": "old_user",  # Old parameter name
        "username": "new_user",  # New parameter name (should take precedence)
        "password": "test_pass",
    }

    postgres_connector.configure(mixed_config)
    assert postgres_connector.state == ConnectorState.CONFIGURED
    assert postgres_connector.params is not None
    assert postgres_connector.params.database == "new_db"  # New parameter should win
    assert postgres_connector.params.username == "new_user"  # New parameter should win


def test_postgres_connector_parameter_validation_errors(postgres_connector):
    """Test parameter validation errors for missing required parameters."""
    # Missing database/dbname
    with pytest.raises(Exception) as exc_info:
        postgres_connector.configure(
            {"host": "localhost", "username": "user", "password": "pass"}
        )
    assert "database" in str(exc_info.value) or "dbname" in str(exc_info.value)

    # Missing username/user
    with pytest.raises(Exception) as exc_info:
        postgres_connector.configure(
            {"host": "localhost", "database": "db", "password": "pass"}
        )
    assert "username" in str(exc_info.value) or "user" in str(exc_info.value)


def test_postgres_connector_integer_parameter_conversion(postgres_connector):
    """Test that string integer parameters are converted to integers properly."""
    config_with_strings = {
        "host": "localhost",
        "port": "5432",  # String integer
        "database": "test_db",
        "username": "test_user",
        "password": "test_pass",
        "connect_timeout": "10",  # String integer
        "min_connections": "2",  # String integer
        "max_connections": "8",  # String integer
    }

    postgres_connector.configure(config_with_strings)
    assert postgres_connector.state == ConnectorState.CONFIGURED
    assert postgres_connector.params is not None
    assert postgres_connector.params.port == 5432  # Should be integer
    assert postgres_connector.params.connect_timeout == 10  # Should be integer
    assert postgres_connector.params.min_connections == 2  # Should be integer
    assert postgres_connector.params.max_connections == 8  # Should be integer


def test_postgres_connector_incremental_loading_validation(postgres_connector):
    """Test incremental loading parameter validation."""
    # Test valid incremental configuration
    incremental_config = {
        "host": "localhost",
        "database": "test_db",
        "username": "test_user",
        "password": "test_pass",
        "sync_mode": "incremental",
        "cursor_field": "updated_at",
        "table": "test_table",
    }

    postgres_connector.configure(incremental_config)
    assert postgres_connector.state == ConnectorState.CONFIGURED
    assert postgres_connector.params.sync_mode == "incremental"
    assert postgres_connector.params.cursor_field == "updated_at"

    # Test incremental mode missing cursor_field
    with pytest.raises(Exception) as exc_info:
        postgres_connector.configure(
            {
                "host": "localhost",
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
                "sync_mode": "incremental",
                # Missing cursor_field
            }
        )
    assert "cursor_field" in str(exc_info.value)

    # Test incremental mode missing table and query
    with pytest.raises(Exception) as exc_info:
        postgres_connector.configure(
            {
                "host": "localhost",
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
                # Missing both table and query
            }
        )
    assert "table" in str(exc_info.value) or "query" in str(exc_info.value)


def test_postgres_connector_supports_incremental(postgres_connector):
    """Test that PostgreSQL connector supports incremental loading."""
    assert postgres_connector.supports_incremental() is True
