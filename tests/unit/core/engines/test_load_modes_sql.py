"""Tests for SQL generation for different load modes."""

from unittest.mock import MagicMock

import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.parser.ast import LoadStep


@pytest.fixture
def duckdb_engine():
    """Create a DuckDBEngine instance for testing."""
    engine = DuckDBEngine(":memory:")
    # Mock the connection to avoid actual DuckDB operations
    engine.connection = MagicMock()
    return engine


def test_generate_sql_for_replace_mode(duckdb_engine):
    """Test SQL generation for REPLACE mode."""
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="REPLACE",
        line_number=1,
    )

    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a schema
    duckdb_engine.get_table_schema = MagicMock(
        return_value={"id": "INTEGER", "name": "VARCHAR"}
    )

    sql = duckdb_engine.generate_load_sql(load_step)

    # REPLACE mode should use CREATE OR REPLACE TABLE
    assert "CREATE OR REPLACE TABLE users_table AS SELECT * FROM users_source" in sql


def test_generate_sql_for_append_mode(duckdb_engine):
    """Test SQL generation for APPEND mode."""
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="APPEND",
        line_number=1,
    )

    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a schema
    duckdb_engine.get_table_schema = MagicMock(
        return_value={"id": "INTEGER", "name": "VARCHAR"}
    )

    sql = duckdb_engine.generate_load_sql(load_step)

    # APPEND mode should use INSERT INTO
    assert "INSERT INTO users_table SELECT * FROM users_source" in sql


def test_generate_sql_for_merge_mode(duckdb_engine):
    """Test SQL generation for MERGE mode."""
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="MERGE",
        merge_keys=["user_id"],
        line_number=1,
    )

    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a schema
    schema = {"user_id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
    duckdb_engine.get_table_schema = MagicMock(return_value=schema)

    sql = duckdb_engine.generate_load_sql(load_step)

    # MERGE mode should use a more complex SQL pattern with MERGE INTO or equivalent
    assert "MERGE INTO" in sql
    assert "users_table" in sql
    assert "users_source" in sql
    assert "user_id" in sql


def test_generate_sql_for_merge_mode_multiple_keys(duckdb_engine):
    """Test SQL generation for MERGE mode with multiple merge keys."""
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="MERGE",
        merge_keys=["user_id", "timestamp"],
        line_number=1,
    )

    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a schema
    schema = {"user_id": "INTEGER", "timestamp": "TIMESTAMP", "name": "VARCHAR"}
    duckdb_engine.get_table_schema = MagicMock(return_value=schema)

    sql = duckdb_engine.generate_load_sql(load_step)

    # Should include both merge keys in the ON clause
    assert "user_id" in sql
    assert "timestamp" in sql
    assert "AND" in sql  # Should join multiple keys with AND


def test_table_creation_for_replace_mode(duckdb_engine):
    """Test that REPLACE mode creates the table if it doesn't exist."""
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="REPLACE",
        line_number=1,
    )

    # Mock table_exists to return False (table doesn't exist)
    duckdb_engine.table_exists = MagicMock(return_value=False)

    sql = duckdb_engine.generate_load_sql(load_step)

    # Should create the table without checking existence
    assert "CREATE TABLE users_table AS SELECT * FROM users_source" in sql


def test_table_creation_for_append_and_merge_modes(duckdb_engine):
    """Test that APPEND and MERGE modes create the table if it doesn't exist."""
    # Test APPEND
    append_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="APPEND",
        line_number=1,
    )

    # Mock table_exists to return False (table doesn't exist)
    duckdb_engine.table_exists = MagicMock(return_value=False)

    sql_append = duckdb_engine.generate_load_sql(append_step)

    # Should create the table first before appending
    assert "CREATE TABLE" in sql_append

    # Test MERGE
    merge_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="MERGE",
        merge_keys=["user_id"],
        line_number=1,
    )

    sql_merge = duckdb_engine.generate_load_sql(merge_step)

    # Should create the table first before merging
    assert "CREATE TABLE" in sql_merge


def test_schema_validation(duckdb_engine):
    """Test that schema validation is called for APPEND and MERGE modes."""
    # Mock validate_schema_compatibility to track calls
    duckdb_engine.validate_schema_compatibility = MagicMock(return_value=True)

    # Create a load step with APPEND mode
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="APPEND",
        line_number=1,
    )

    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return some schema
    duckdb_engine.get_table_schema = MagicMock(
        return_value={"id": "INTEGER", "name": "VARCHAR"}
    )

    # Generate SQL for APPEND mode
    duckdb_engine.generate_load_sql(load_step)

    # Verify validate_schema_compatibility was called
    duckdb_engine.validate_schema_compatibility.assert_called_once()


def test_invalid_mode_raises_error(duckdb_engine):
    """Test that an invalid mode raises an error."""
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="INVALID_MODE",  # Invalid mode
        line_number=1,
    )

    with pytest.raises(ValueError, match="Invalid load mode"):
        duckdb_engine.generate_load_sql(load_step)
