"""Tests for schema compatibility validation."""

from unittest.mock import MagicMock

import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine


@pytest.fixture
def duckdb_engine():
    """Create a DuckDBEngine instance for testing."""
    engine = DuckDBEngine(":memory:")
    # Mock the connection to avoid actual DuckDB operations
    engine.connection = MagicMock()
    return engine


def test_schema_compatibility_validation_success(duckdb_engine):
    """Test schema compatibility validation with compatible schemas."""
    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a target schema
    target_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "email": "VARCHAR",
        "date_joined": "DATE",
    }
    duckdb_engine.get_table_schema = MagicMock(return_value=target_schema)

    # Source schema with compatible types
    source_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "email": "VARCHAR",
        "date_joined": "DATE",
    }

    # This should not raise an exception
    result = duckdb_engine.validate_schema_compatibility("users", source_schema)
    assert result is True


def test_schema_compatibility_validation_type_normalization(duckdb_engine):
    """Test that schema compatibility normalizes similar types correctly."""
    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a target schema
    target_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "value": "DOUBLE",
        "active": "BOOLEAN",
    }
    duckdb_engine.get_table_schema = MagicMock(return_value=target_schema)

    # Source schema with differently named but compatible types
    source_schema = {"id": "INT", "name": "TEXT", "value": "FLOAT", "active": "BOOL"}

    # This should not raise an exception due to type normalization
    result = duckdb_engine.validate_schema_compatibility("users", source_schema)
    assert result is True


def test_schema_compatibility_validation_missing_column(duckdb_engine):
    """Test schema compatibility validation with a missing column in target."""
    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a target schema
    target_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
    }
    duckdb_engine.get_table_schema = MagicMock(return_value=target_schema)

    # Source schema with an extra column not in target
    source_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "email": "VARCHAR",  # This column doesn't exist in target
    }

    # This should raise a ValueError
    with pytest.raises(
        ValueError, match="Column 'email' in source does not exist in target"
    ):
        duckdb_engine.validate_schema_compatibility("users", source_schema)


def test_schema_compatibility_validation_incompatible_type(duckdb_engine):
    """Test schema compatibility validation with incompatible column types."""
    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a target schema
    target_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "active": "BOOLEAN",
    }
    duckdb_engine.get_table_schema = MagicMock(return_value=target_schema)

    # Source schema with incompatible type for active
    source_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "active": "INTEGER",  # Incompatible with BOOLEAN
    }

    # This should raise a ValueError
    with pytest.raises(ValueError, match="Column 'active' has incompatible types"):
        duckdb_engine.validate_schema_compatibility("users", source_schema)


def test_schema_compatibility_validation_nonexistent_table(duckdb_engine):
    """Test schema compatibility validation with a nonexistent target table."""
    # Mock table_exists to return False (table doesn't exist)
    duckdb_engine.table_exists = MagicMock(return_value=False)

    # Any source schema should be considered compatible
    source_schema = {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}

    # This should not raise an exception
    result = duckdb_engine.validate_schema_compatibility("new_table", source_schema)
    assert result is True


def test_schema_compatibility_validation_subset(duckdb_engine):
    """Test schema compatibility validation with source being a subset of target."""
    # Mock table_exists to return True (table exists)
    duckdb_engine.table_exists = MagicMock(return_value=True)

    # Mock get_table_schema to return a target schema
    target_schema = {
        "id": "INTEGER",
        "name": "VARCHAR",
        "email": "VARCHAR",
        "date_joined": "DATE",
        "last_login": "TIMESTAMP",
    }
    duckdb_engine.get_table_schema = MagicMock(return_value=target_schema)

    # Source schema is a subset of target
    source_schema = {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}

    # This should not raise an exception
    result = duckdb_engine.validate_schema_compatibility("users", source_schema)
    assert result is True


def test_compatibility_between_specific_types(duckdb_engine):
    """Test compatibility between specific type pairs."""
    # Test specific type compatibility directly
    # INTEGER family
    assert duckdb_engine._are_types_compatible("INTEGER", "INT") is True
    assert duckdb_engine._are_types_compatible("INT", "BIGINT") is True
    assert duckdb_engine._are_types_compatible("SMALLINT", "INTEGER") is True

    # STRING family
    assert duckdb_engine._are_types_compatible("VARCHAR", "TEXT") is True
    assert duckdb_engine._are_types_compatible("VARCHAR(255)", "TEXT") is True
    assert duckdb_engine._are_types_compatible("CHAR(10)", "VARCHAR") is True
    assert duckdb_engine._are_types_compatible("STRING", "TEXT") is True

    # FLOAT family
    assert duckdb_engine._are_types_compatible("FLOAT", "DOUBLE") is True
    assert duckdb_engine._are_types_compatible("NUMERIC", "DECIMAL") is True
    assert duckdb_engine._are_types_compatible("DECIMAL(10,2)", "DOUBLE") is True

    # BOOLEAN
    assert duckdb_engine._are_types_compatible("BOOLEAN", "BOOL") is True

    # DATE and TIME
    assert duckdb_engine._are_types_compatible("DATE", "DATE") is True
    assert duckdb_engine._are_types_compatible("TIME", "TIME") is True
    assert duckdb_engine._are_types_compatible("TIMESTAMP", "TIMESTAMP") is True

    # Incompatible types
    assert duckdb_engine._are_types_compatible("INTEGER", "VARCHAR") is False
    assert duckdb_engine._are_types_compatible("FLOAT", "BOOLEAN") is False
    assert duckdb_engine._are_types_compatible("DATE", "TIMESTAMP") is False
