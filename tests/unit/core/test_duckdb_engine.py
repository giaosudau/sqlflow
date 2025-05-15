"""Unit tests for the DuckDB engine."""

import os
import tempfile
import uuid

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine


class TestDuckDBEngine:
    """Tests for the DuckDBEngine class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.gettempdir()
        db_path = os.path.join(temp_dir, f"sqlflow_test_{uuid.uuid4()}.db")

        yield db_path

        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def engine(self, temp_db_path):
        """Create a DuckDBEngine instance."""
        engine = DuckDBEngine(database_path=temp_db_path)
        yield engine
        engine.close()

    def test_init(self, temp_db_path):
        """Test initializing a DuckDBEngine."""
        engine = DuckDBEngine(database_path=temp_db_path)

        assert engine.database_path == temp_db_path
        assert engine.connection is not None

        engine.close()

    def test_execute_query(self, engine):
        """Test executing a query."""
        result = engine.execute_query("SELECT 1 AS value")

        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1

    def test_register_table(self, engine):
        """Test registering a table."""
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        engine.register_table("test_table", data)

        result = engine.execute_query("SELECT * FROM test_table ORDER BY id")
        rows = result.fetchall()

        assert len(rows) == 3
        assert [row[0] for row in rows] == [1, 2, 3]
        assert [row[1] for row in rows] == ["Alice", "Bob", "Charlie"]

    def test_register_arrow(self, engine):
        """Test registering an Arrow table."""
        data = pa.Table.from_pandas(
            pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        )

        engine.register_arrow("test_arrow_table", data)

        result = engine.execute_query("SELECT * FROM test_arrow_table ORDER BY id")
        rows = result.fetchall()

        assert len(rows) == 3
        assert [row[0] for row in rows] == [1, 2, 3]
        assert [row[1] for row in rows] == ["Alice", "Bob", "Charlie"]

    def test_supports_feature(self, engine):
        """Test checking feature support."""
        assert engine.supports_feature("arrow") is True
        assert engine.supports_feature("pandas") is True
        assert engine.supports_feature("python_funcs") is False
        assert engine.supports_feature("nonexistent_feature") is False

    def test_table_exists(self, engine):
        """Test checking if a table exists."""
        data = pd.DataFrame({"id": [1, 2, 3]})
        engine.register_table("exists_test", data)

        assert engine.table_exists("exists_test") is True
        assert engine.table_exists("nonexistent_table") is False

    def test_register_python_func_not_implemented(self, engine):
        """Test that register_python_func raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            engine.register_python_func("test_func", lambda x: x + 1)

    def test_variable_registration(self, engine):
        """Test variable registration and substitution."""
        engine.register_variable("test_var", "test_value")
        engine.register_variable("number", 42)

        assert engine.get_variable("test_var") == "test_value"
        assert engine.get_variable("number") == 42

        template = "SELECT * FROM table WHERE value = ${test_var} AND id = ${number}"
        result = engine.substitute_variables(template)
        expected = "SELECT * FROM table WHERE value = test_value AND id = 42"
        assert result == expected
