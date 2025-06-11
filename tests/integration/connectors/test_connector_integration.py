"""Integration tests for connector framework."""

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests_mock
from sqlalchemy import create_engine

from sqlflow.connectors.csv.destination import CSVDestination
from sqlflow.connectors.csv.source import CSVSource
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.google_sheets.source import GoogleSheetsSource
from sqlflow.connectors.in_memory import IN_MEMORY_DATA_STORE
from sqlflow.connectors.in_memory.in_memory_connector import (
    InMemoryDestination,
    InMemorySource,
)
from sqlflow.connectors.parquet.destination import ParquetDestination
from sqlflow.connectors.postgres.destination import PostgresDestination
from sqlflow.connectors.postgres.source import PostgresSource
from sqlflow.connectors.rest.source import RestSource


def test_csv_to_parquet_conversion(sample_csv_file, temp_dir):
    """Test converting data from CSV to Parquet via connectors."""
    # Use new architecture - configure after instantiation
    csv_connector = CSVSource()
    csv_connector.configure({"path": sample_csv_file})

    output_path = os.path.join(temp_dir, "output.parquet")
    parquet_connector = ParquetDestination(config={"path": output_path})

    # Read data using new connector interface
    data = csv_connector.read()

    # Write data using destination connector
    parquet_connector.write(data)

    # Verify data integrity
    csv_data = pd.read_csv(sample_csv_file)
    parquet_data = pq.read_table(output_path).to_pandas()

    # Normalize data for comparison (handle type differences)
    csv_data_str = csv_data.astype(str)
    parquet_data_str = parquet_data.astype(str)

    for df in [csv_data_str, parquet_data_str]:
        for col in df.columns:
            df[col] = (
                df[col]
                .str.lower()
                .where(df[col].isin(["true", "false", "True", "False"]), df[col])
            )

    pd.testing.assert_frame_equal(
        csv_data_str.reset_index(drop=True),
        parquet_data_str.reset_index(drop=True),
    )


def test_data_chunk_conversion():
    """Test DataChunk conversion between Arrow and pandas."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    }

    arrow_table = pa.Table.from_pydict(data)
    chunk_from_arrow = DataChunk(arrow_table)

    pandas_df = pd.DataFrame(data)
    chunk_from_pandas = DataChunk(pandas_df)

    assert chunk_from_arrow.arrow_table.equals(chunk_from_pandas.arrow_table)
    pd.testing.assert_frame_equal(
        chunk_from_arrow.pandas_df.reset_index(drop=True),
        chunk_from_pandas.pandas_df.reset_index(drop=True),
    )


def test_csv_source_with_new_interface(sample_csv_file):
    """Test CSV source using new standardized interface."""
    connector = CSVSource()

    # Test configuration
    connector.configure({"path": sample_csv_file, "has_header": True})

    # Test connection
    result = connector.test_connection()
    assert result.success, f"Connection test failed: {result.message}"

    # Test discovery
    discovered = connector.discover()
    assert len(discovered) == 1
    assert discovered[0] == sample_csv_file

    # Test schema retrieval
    schema = connector.get_schema(sample_csv_file)
    assert schema is not None
    assert len(schema.arrow_schema) > 0

    # Test data reading
    data = connector.read()
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 0


def test_csv_destination_integration(temp_dir):
    """Test CSV destination integration."""
    test_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100.0, 200.0, 300.0],
        }
    )

    output_path = os.path.join(temp_dir, "test_output.csv")

    # Write data using destination connector
    destination = CSVDestination(config={"path": output_path})
    destination.write(test_data, options={"index": False})

    # Verify file was created
    assert os.path.exists(output_path)

    # Read back and verify data integrity
    read_data = pd.read_csv(output_path)
    pd.testing.assert_frame_equal(test_data, read_data)


def test_in_memory_connector_integration():
    """Test full read/write cycle with InMemory connector."""
    # 1. Setup - Create a DataFrame and put it in the in-memory store
    table_name = "test_in_memory_table"
    test_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": ["A", "B", "C"],
        }
    )
    IN_MEMORY_DATA_STORE[table_name] = test_data

    # 2. Source - Read data from the in-memory store
    source_config = {"table_name": table_name}
    source = InMemorySource(config=source_config)
    read_df = pd.concat(chunk.pandas_df for chunk in source.read())

    pd.testing.assert_frame_equal(test_data, read_df)

    # 3. Destination - Write data back to a different in-memory table
    dest_table_name = "test_in_memory_dest"
    dest_config = {"table_name": dest_table_name}
    destination = InMemoryDestination(config=dest_config)
    destination.write(read_df)

    # 4. Verify - Check that the data was written correctly
    assert dest_table_name in IN_MEMORY_DATA_STORE
    written_df = IN_MEMORY_DATA_STORE[dest_table_name]
    pd.testing.assert_frame_equal(read_df, written_df)

    # 5. Cleanup
    del IN_MEMORY_DATA_STORE[table_name]
    del IN_MEMORY_DATA_STORE[dest_table_name]


@pytest.fixture(scope="module")
def postgres_connection_config():
    """Provides connection details for a test PostgreSQL database."""
    # Assumes a PostgreSQL container is running and accessible.
    # Replace with your actual test test_db connection details.
    return {
        "user": "user",
        "password": "password",
        "host": "localhost",
        "port": "5432",
        "dbname": "test_db",
    }


@pytest.fixture(scope="module")
def postgres_engine(postgres_connection_config):
    """Creates a SQLAlchemy engine for the test database."""
    conn_uri = (
        f"postgresql+psycopg2://{postgres_connection_config['user']}:{postgres_connection_config['password']}"
        f"@{postgres_connection_config['host']}:{postgres_connection_config['port']}/{postgres_connection_config['dbname']}"
    )
    engine = create_engine(conn_uri)
    yield engine
    engine.dispose()


@pytest.mark.skip(reason="Requires a running PostgreSQL instance")
def test_postgres_connector_integration(postgres_engine, postgres_connection_config):
    """Test full read/write cycle with the PostgreSQL connector."""
    table_name = "test_postgres_table"
    schema = "public"
    test_data = pd.DataFrame(
        {
            "id": [10, 20, 30],
            "name": ["David", "Eve", "Frank"],
        }
    )

    # 1. Destination - Write data to PostgreSQL
    dest_config = postgres_connection_config
    destination = PostgresDestination(config=dest_config)
    write_options = {
        "table_name": table_name,
        "schema": schema,
        "if_exists": "replace",
    }
    destination.write(test_data, options=write_options)

    # Verify write with a direct query
    with postgres_engine.connect() as conn:
        persisted_data = pd.read_sql_table(table_name, conn, schema=schema)
    pd.testing.assert_frame_equal(test_data, persisted_data, check_dtype=False)

    # 2. Source - Read data from PostgreSQL
    source_config = postgres_connection_config
    source = PostgresSource(config=source_config)

    # Read by table_name
    read_options_table = {"table_name": table_name, "schema": schema}
    read_df_table = source.read(options=read_options_table)
    pd.testing.assert_frame_equal(test_data, read_df_table, check_dtype=False)

    # Read by query
    read_options_query = {"query": f"SELECT * FROM {schema}.{table_name} ORDER BY id"}
    read_df_query = source.read(options=read_options_query)
    pd.testing.assert_frame_equal(test_data, read_df_query, check_dtype=False)

    # 3. Cleanup
    with postgres_engine.connect() as conn:
        conn.execute(f"DROP TABLE {schema}.{table_name}")


@patch("sqlflow.connectors.google_sheets.source.GoogleSheetsSource._initialize_service")
def test_google_sheets_connector_integration(mock_init_service):
    """Test the Google Sheets connector using a mock."""
    # 1. Setup Mock
    mock_service = MagicMock()
    mock_spreadsheets = MagicMock()
    mock_values = MagicMock()
    mock_service.spreadsheets.return_value = mock_spreadsheets
    mock_spreadsheets.values.return_value = mock_values

    header = ["id", "product", "sales"]
    data_rows = [
        [1, "Laptop", 1200],
        [2, "Mouse", 50],
    ]
    mock_values.get().execute.return_value = {"values": [header] + data_rows}

    # 2. Configure and Read
    config = {
        "credentials_file": "/path/to/mock/creds.json",
        "spreadsheet_id": "mock_spreadsheet_id",
        "sheet_name": "Q1 Sales",
    }
    source = GoogleSheetsSource(config=config)
    # Manually set the service since we're mocking initialization
    source.service = mock_service
    read_df = source.read()

    # 3. Verify
    expected_df = pd.DataFrame(data_rows, columns=header)
    pd.testing.assert_frame_equal(read_df, expected_df)

    # Verify that the service initialization was called
    mock_init_service.assert_called_once()
    # Verify that the correct API methods were called
    mock_spreadsheets.values().get.assert_any_call(
        spreadsheetId=config["spreadsheet_id"],
        range=config["sheet_name"],
    )


def test_rest_connector_integration():
    """Test the generic REST connector with a mock API."""
    api_url = "http://api.test.com/data"
    test_data = {"results": [{"id": 1, "value": "A"}, {"id": 2, "value": "B"}]}

    with requests_mock.Mocker() as m:
        # 1. Setup Mock
        m.get(api_url, json=test_data)

        # 2. Configure and Read
        config = {"url": api_url, "data_path": "results"}
        source = RestSource(config=config)
        read_df = next(source.read()).pandas_df

    # 3. Verify
    expected_df = pd.json_normalize(test_data["results"])
    pd.testing.assert_frame_equal(read_df, expected_df)
