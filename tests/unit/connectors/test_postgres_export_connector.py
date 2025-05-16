"""Tests for PostgreSQL export connector."""

from unittest.mock import MagicMock, call, patch

import pandas as pd
import pyarrow as pa
import pytest
from psycopg2.extensions import connection, cursor

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.postgres_export_connector import PostgresExportConnector
from sqlflow.core.errors import ConnectorError


@pytest.fixture
def postgres_export_connector():
    """Create a PostgreSQL export connector instance."""
    return PostgresExportConnector()


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
        "target_table": "test_table",
        "batch_size": 500,
        "write_mode": "append",
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


def test_postgres_export_connector_init(postgres_export_connector):
    """Test PostgreSQL export connector initialization."""
    assert postgres_export_connector.state == ConnectorState.CREATED
    assert postgres_export_connector.params is None
    assert postgres_export_connector.connection_pool is None
    assert postgres_export_connector.batch_size == 1000
    assert postgres_export_connector.write_mode == "append"
    assert postgres_export_connector.upsert_keys is None
    assert postgres_export_connector.target_table is None


def test_postgres_export_connector_configure(postgres_export_connector, sample_config):
    """Test configuring PostgreSQL export connector."""
    postgres_export_connector.configure(sample_config)
    assert postgres_export_connector.state == ConnectorState.CONFIGURED
    assert postgres_export_connector.params is not None
    assert postgres_export_connector.params.host == "localhost"
    assert postgres_export_connector.target_table == "test_table"
    assert postgres_export_connector.batch_size == 500
    assert postgres_export_connector.write_mode == "append"

    # Test missing required fields
    with pytest.raises(ConnectorError, match="Host is required"):
        postgres_export_connector.configure({"target_table": "test"})

    with pytest.raises(ConnectorError, match="target_table is required"):
        postgres_export_connector.configure({"host": "localhost", "dbname": "test"})

    # Test invalid write mode
    with pytest.raises(ConnectorError, match="Invalid write mode"):
        postgres_export_connector.configure({**sample_config, "write_mode": "invalid"})

    # Test upsert mode without keys
    with pytest.raises(ConnectorError, match="upsert_keys is required"):
        postgres_export_connector.configure({**sample_config, "write_mode": "upsert"})


@patch("psycopg2.connect")
def test_postgres_export_connector_test_connection(
    mock_connect, postgres_export_connector, sample_config, mock_connection
):
    """Test connection testing for PostgreSQL export connector."""
    mock_conn, mock_cur = mock_connection
    mock_connect.return_value = mock_conn

    postgres_export_connector.configure(sample_config)
    result = postgres_export_connector.test_connection()

    assert result.success is True
    assert postgres_export_connector.state == ConnectorState.READY

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
        connect_timeout=10,  # Default value from PostgresConnectionParams
        application_name="sqlflow",  # Default value from PostgresConnectionParams
    )

    # Verify second connection attempt (pool)
    second_call = mock_connect.call_args_list[1]
    assert second_call == call(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="test_user",
        password="test_pass",
        connect_timeout=10,  # Default value from PostgresConnectionParams
        application_name="sqlflow",  # Default value from PostgresConnectionParams
    )

    # Verify SELECT 1 was executed
    mock_cur.execute.assert_called_once_with("SELECT 1")

    # Test connection failure
    mock_connect.side_effect = Exception("Connection failed")
    result = postgres_export_connector.test_connection()
    assert result.success is False
    assert "Connection failed" in result.message
    assert postgres_export_connector.state == ConnectorState.ERROR


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_export_connector_write_append(
    mock_pool, postgres_export_connector, sample_config, mock_connection, sample_data
):
    """Test writing data in append mode."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    postgres_export_connector.configure(sample_config)
    postgres_export_connector.write(sample_data)

    # Verify table creation check
    mock_cur.execute.assert_any_call(
        """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
                """,
        ("test_table",),
    )

    # Verify COPY command
    mock_cur.copy_expert.assert_called_once()
    copy_command = mock_cur.copy_expert.call_args[0][0]
    assert 'COPY "test_table"' in copy_command
    assert "FROM STDIN" in copy_command


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_export_connector_write_overwrite(
    mock_pool, postgres_export_connector, sample_config, mock_connection, sample_data
):
    """Test writing data in overwrite mode."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    config = {**sample_config, "write_mode": "overwrite"}
    postgres_export_connector.configure(config)
    postgres_export_connector.write(sample_data)

    # Verify truncate command
    mock_cur.execute.assert_any_call('TRUNCATE TABLE "test_table"')


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_export_connector_write_upsert(
    mock_pool, postgres_export_connector, sample_config, mock_connection, sample_data
):
    """Test writing data in upsert mode."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    config = {**sample_config, "write_mode": "upsert", "upsert_keys": ["id"]}
    postgres_export_connector.configure(config)
    postgres_export_connector.write(sample_data)

    # Verify temp table creation
    mock_cur.execute.assert_any_call(
        'CREATE TEMP TABLE "temp_test_table" (LIKE "test_table")'
    )

    # Verify upsert query
    mock_cur.execute.assert_any_call(
        """
            INSERT INTO "test_table" 
            SELECT * FROM "temp_test_table"
            ON CONFLICT ("id") 
            DO UPDATE SET "name" = EXCLUDED."name", "value" = EXCLUDED."value"
            """
    )


@patch("psycopg2.pool.ThreadedConnectionPool")
def test_postgres_export_connector_write_multiple_chunks(
    mock_pool, postgres_export_connector, sample_config, mock_connection, sample_data
):
    """Test writing multiple data chunks."""
    mock_conn, mock_cur = mock_connection
    mock_pool.return_value = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    postgres_export_connector.configure(sample_config)
    postgres_export_connector.write([sample_data, sample_data])

    # Verify COPY command called twice
    assert mock_cur.copy_expert.call_count == 2


def test_postgres_export_connector_close(postgres_export_connector, sample_config):
    """Test closing PostgreSQL export connector."""
    mock_pool = MagicMock()
    postgres_export_connector.configure(sample_config)
    postgres_export_connector.connection_pool = mock_pool

    postgres_export_connector.close()
    mock_pool.closeall.assert_called_once()
    assert postgres_export_connector.connection_pool is None
