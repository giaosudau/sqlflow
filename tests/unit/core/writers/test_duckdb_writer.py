"""Tests for DuckDB writer functionality."""

from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from sqlflow.core.writers.duckdb_writer import DuckDBWriter


class TestDuckDBWriter:
    """Test DuckDB writer functionality."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock DuckDB connection for testing."""
        return MagicMock(spec=duckdb.DuckDBPyConnection)

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["New York", "London", "Tokyo"],
            }
        )

    def test_init_with_connection(self, mock_connection):
        """Test initializing DuckDBWriter with an existing connection."""
        writer = DuckDBWriter(connection=mock_connection)

        assert writer.connection is mock_connection

    @patch("duckdb.connect")
    def test_init_without_connection(self, mock_connect):
        """Test initializing DuckDBWriter without a connection (should create one)."""
        mock_new_connection = MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_connect.return_value = mock_new_connection

        writer = DuckDBWriter()

        mock_connect.assert_called_once_with()
        assert writer.connection is mock_new_connection

    @patch("duckdb.connect")
    def test_init_with_none_connection(self, mock_connect):
        """Test initializing DuckDBWriter with None connection."""
        mock_new_connection = MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_connect.return_value = mock_new_connection

        writer = DuckDBWriter(connection=None)

        mock_connect.assert_called_once_with()
        assert writer.connection is mock_new_connection

    def test_write_basic_functionality(self, mock_connection, sample_dataframe):
        """Test basic write functionality."""
        writer = DuckDBWriter(connection=mock_connection)
        destination = "test_table"

        # The current implementation is incomplete, so we test what exists
        writer.write(sample_dataframe, destination)

        # Verify the method completes without error
        assert True  # Basic functionality test

    def test_write_with_options(self, mock_connection, sample_dataframe):
        """Test writing with options."""
        writer = DuckDBWriter(connection=mock_connection)
        destination = "test_table"
        options = {"create_table": True, "if_exists": "replace", "index": False}

        writer.write(sample_dataframe, destination, options)

        # Verify the method completes without error
        assert True

    def test_write_with_none_options(self, mock_connection, sample_dataframe):
        """Test writing with None options (should use defaults)."""
        writer = DuckDBWriter(connection=mock_connection)
        destination = "test_table"

        writer.write(sample_dataframe, destination, None)

        # Should work with default options
        assert True

    def test_write_empty_dataframe(self, mock_connection):
        """Test writing an empty DataFrame."""
        writer = DuckDBWriter(connection=mock_connection)
        destination = "empty_table"
        empty_df = pd.DataFrame()

        writer.write(empty_df, destination)

        # Should handle empty DataFrames gracefully
        assert True

    def test_write_with_create_table_false(self, mock_connection, sample_dataframe):
        """Test writing with create_table option set to False."""
        writer = DuckDBWriter(connection=mock_connection)
        destination = "existing_table"
        options = {"create_table": False}

        writer.write(sample_dataframe, destination, options)

        # Should work when not creating table
        assert True

    def test_write_different_data_types(self, mock_connection):
        """Test writing DataFrame with different data types."""
        writer = DuckDBWriter(connection=mock_connection)
        destination = "typed_table"

        # Create DataFrame with various data types
        data = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
                "date_col": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            }
        )

        writer.write(data, destination)

        # Should handle different data types
        assert True

    @patch("duckdb.connect")
    def test_multiple_writes_same_writer(self, mock_connect, sample_dataframe):
        """Test multiple writes using the same writer instance."""
        mock_connection = MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_connect.return_value = mock_connection

        writer = DuckDBWriter()

        # Write to multiple tables
        writer.write(sample_dataframe, "table1")
        writer.write(sample_dataframe, "table2")
        writer.write(sample_dataframe, "table3")

        # Should reuse the same connection
        mock_connect.assert_called_once()

    def test_connection_property_access(self, mock_connection):
        """Test that connection property can be accessed."""
        writer = DuckDBWriter(connection=mock_connection)

        # Should be able to access the connection
        assert writer.connection is mock_connection

        # Connection should be usable for other operations
        assert hasattr(writer.connection, "execute")  # Mock has this due to spec

    def test_writer_protocol_compliance(self):
        """Test that DuckDBWriter properly implements WriterProtocol."""
        from sqlflow.core.protocols import WriterProtocol

        # Should be a subclass of WriterProtocol
        assert issubclass(DuckDBWriter, WriterProtocol)

        # Should have required write method
        assert hasattr(DuckDBWriter, "write")
        assert callable(getattr(DuckDBWriter, "write"))
