"""Unit tests for the DuckDB writer."""

import os
import tempfile
import uuid

import duckdb
import pandas as pd
import pytest

from sqlflow.core.writers.duckdb_writer import DuckDBWriter


class TestDuckDBWriter:
    """Tests for the DuckDBWriter class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.gettempdir()
        db_path = os.path.join(temp_dir, f"sqlflow_test_{uuid.uuid4()}.db")

        yield db_path

        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def connection(self, temp_db_path):
        """Create a DuckDB connection."""
        conn = duckdb.connect(temp_db_path)
        yield conn
        conn.close()

    @pytest.fixture
    def writer(self, connection):
        """Create a DuckDBWriter instance."""
        return DuckDBWriter(connection=connection)

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )

    def test_init(self, connection):
        """Test initializing a DuckDBWriter."""
        writer = DuckDBWriter(connection=connection)
        assert writer.connection is connection

    def test_write_create_table(self, writer, sample_data, connection):
        """Test writing data with table creation."""
        writer.write(sample_data, "test_table")

        result = connection.execute("SELECT * FROM test_table ORDER BY id").fetchdf()

        assert len(result) == 3
        assert list(result["id"]) == [1, 2, 3]
        assert list(result["name"]) == ["Alice", "Bob", "Charlie"]
        assert list(result["age"]) == [25, 30, 35]

    def test_write_append_mode(self, writer, sample_data, connection):
        """Test writing data in append mode."""
        writer.write(sample_data, "append_test")

        new_data = pd.DataFrame(
            {"id": [4, 5], "name": ["Dave", "Eve"], "age": [40, 45]}
        )

        writer.write(new_data, "append_test", options={"mode": "append"})

        result = connection.execute("SELECT * FROM append_test ORDER BY id").fetchdf()

        assert len(result) == 5
        assert list(result["id"]) == [1, 2, 3, 4, 5]
        assert list(result["name"]) == ["Alice", "Bob", "Charlie", "Dave", "Eve"]
        assert list(result["age"]) == [25, 30, 35, 40, 45]

    def test_write_overwrite_mode(self, writer, sample_data, connection):
        """Test writing data in overwrite mode."""
        writer.write(sample_data, "overwrite_test")

        new_data = pd.DataFrame(
            {"id": [4, 5], "name": ["Dave", "Eve"], "age": [40, 45]}
        )

        writer.write(new_data, "overwrite_test", options={"mode": "overwrite"})

        result = connection.execute(
            "SELECT * FROM overwrite_test ORDER BY id"
        ).fetchdf()

        assert len(result) == 2
        assert list(result["id"]) == [4, 5]
        assert list(result["name"]) == ["Dave", "Eve"]
        assert list(result["age"]) == [40, 45]

    def test_no_create_table_option(self, writer, sample_data, connection):
        """Test writing data without creating a table."""
        connection.execute(
            "CREATE TABLE no_create_test (id INTEGER, name VARCHAR, age INTEGER)"
        )

        writer.write(sample_data, "no_create_test", options={"create_table": False})

        result = connection.execute(
            "SELECT * FROM no_create_test ORDER BY id"
        ).fetchdf()

        assert len(result) == 3
        assert list(result["id"]) == [1, 2, 3]
        assert list(result["name"]) == ["Alice", "Bob", "Charlie"]
        assert list(result["age"]) == [25, 30, 35]
