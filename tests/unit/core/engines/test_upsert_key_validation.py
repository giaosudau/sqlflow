"""Tests for upsert key validation in DuckDBEngine."""

from unittest.mock import MagicMock

import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine


@pytest.fixture
def mock_engine():
    """Create a mocked DuckDBEngine instance."""
    engine = DuckDBEngine(":memory:")
    engine.table_exists = MagicMock()
    engine.get_table_schema = MagicMock()
    engine._are_types_compatible = MagicMock(return_value=True)
    return engine


def test_validate_upsert_keys_basic(mock_engine):
    """Test basic upsert key validation with valid keys."""
    # Setup
    mock_engine.table_exists.return_value = True
    mock_engine.get_table_schema.side_effect = [
        {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"},  # source schema
        {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"},  # target schema
    ]

    # Execute
    result = mock_engine.validate_upsert_keys("target_table", "source_name", ["id"])

    # Assert
    assert result is True
    mock_engine.table_exists.assert_called_once_with("target_table")
    assert mock_engine.get_table_schema.call_count == 2
    mock_engine._are_types_compatible.assert_called_once()


def test_validate_upsert_keys_multiple(mock_engine):
    """Test validation with multiple upsert keys."""
    # Setup
    mock_engine.table_exists.return_value = True
    mock_engine.get_table_schema.side_effect = [
        {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"},  # source schema
        {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"},  # target schema
    ]

    # Execute
    result = mock_engine.validate_upsert_keys(
        "target_table", "source_name", ["id", "email"]
    )

    # Assert
    assert result is True
    mock_engine.table_exists.assert_called_once_with("target_table")
    assert mock_engine.get_table_schema.call_count == 2
    assert mock_engine._are_types_compatible.call_count == 2


def test_validate_upsert_keys_missing_from_source(mock_engine):
    """Test validation with key missing from source."""
    # Setup
    mock_engine.table_exists.return_value = True
    mock_engine.get_table_schema.side_effect = [
        {"id": "INTEGER", "name": "VARCHAR"},  # source schema missing email
        {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"},  # target schema
    ]

    # Execute and Assert
    with pytest.raises(ValueError) as excinfo:
        mock_engine.validate_upsert_keys("target_table", "source_name", ["id", "email"])

    assert "Upsert key 'email' does not exist in source" in str(excinfo.value)


def test_validate_upsert_keys_missing_from_target(mock_engine):
    """Test validation with key missing from target."""
    # Setup
    mock_engine.table_exists.return_value = True
    mock_engine.get_table_schema.side_effect = [
        {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"},  # source schema
        {"id": "INTEGER", "name": "VARCHAR"},  # target schema missing email
    ]

    # Execute and Assert
    with pytest.raises(ValueError) as excinfo:
        mock_engine.validate_upsert_keys("target_table", "source_name", ["id", "email"])

    assert "Upsert key 'email' does not exist in target table" in str(excinfo.value)


def test_validate_upsert_keys_incompatible_types(mock_engine):
    """Test validation with incompatible key types."""
    # Setup
    mock_engine.table_exists.return_value = True
    mock_engine.get_table_schema.side_effect = [
        {"id": "INTEGER", "name": "VARCHAR"},  # source schema
        {"id": "VARCHAR", "name": "VARCHAR"},  # target schema with incompatible id type
    ]
    mock_engine._are_types_compatible.return_value = False

    # Execute and Assert
    with pytest.raises(ValueError) as excinfo:
        mock_engine.validate_upsert_keys("target_table", "source_name", ["id"])

    assert "Upsert key 'id' has incompatible types" in str(excinfo.value)
    assert "Upsert keys must have compatible types" in str(excinfo.value)


def test_validate_upsert_keys_no_keys(mock_engine):
    """Test validation with empty upsert keys list."""
    # Execute and Assert
    with pytest.raises(ValueError) as excinfo:
        mock_engine.validate_upsert_keys("target_table", "source_name", [])

    assert "UPSERT operation requires at least one upsert key" in str(excinfo.value)


def test_validate_upsert_keys_target_not_exists(mock_engine):
    """Test validation when target table doesn't exist."""
    # Setup
    mock_engine.table_exists.return_value = False

    # Execute
    result = mock_engine.validate_upsert_keys("target_table", "source_name", ["id"])

    # Assert
    assert result is True
    # No schema validation should happen
    mock_engine.get_table_schema.assert_not_called()
