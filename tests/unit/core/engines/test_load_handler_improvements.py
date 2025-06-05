"""Tests for load handler improvements including custom exceptions and optimizations."""

from unittest.mock import Mock

import pytest

from sqlflow.core.engines.duckdb.exceptions import (
    InvalidLoadModeError,
    SchemaValidationError,
    UpsertKeyValidationError,
)
from sqlflow.core.engines.duckdb.load.handlers import (
    AppendLoadHandler,
    LoadModeHandlerFactory,
    LoadStep,
    ReplaceLoadHandler,
    TableInfo,
    UpsertLoadHandler,
    ValidationHelper,
)


class TestCustomExceptions:
    """Test cases for custom exception classes."""

    def test_invalid_load_mode_error_contains_mode_and_valid_modes(self):
        """Test that InvalidLoadModeError contains mode and valid modes information."""
        invalid_mode = "INVALID"
        valid_modes = ["REPLACE", "APPEND", "UPSERT"]

        error = InvalidLoadModeError(invalid_mode, valid_modes)

        assert error.mode == invalid_mode
        assert error.valid_modes == valid_modes
        assert "Invalid load mode: INVALID" in str(error)
        assert "REPLACE, APPEND, UPSERT" in str(error)

    def test_upsert_key_validation_error_attributes(self):
        """Test that UpsertKeyValidationError contains table name and upsert keys."""
        table_name = "test_table"
        upsert_keys = ["id", "name"]
        message = "Upsert keys validation failed"

        error = UpsertKeyValidationError(message, table_name, upsert_keys)

        assert error.table_name == table_name
        assert error.upsert_keys == upsert_keys
        assert str(error) == message

    def test_schema_validation_error_with_schemas(self):
        """Test that SchemaValidationError contains source and target schemas."""
        source_schema = {"id": "INTEGER", "name": "VARCHAR"}
        target_schema = {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
        message = "Schema mismatch"

        error = SchemaValidationError(message, source_schema, target_schema)

        assert error.source_schema == source_schema
        assert error.target_schema == target_schema
        assert message in str(error)


class TestTableInfo:
    """Test cases for TableInfo class."""

    def test_table_info_creation_with_existing_table(self):
        """Test TableInfo creation for existing table."""
        schema = {"id": "INTEGER", "name": "VARCHAR"}

        table_info = TableInfo(exists=True, schema=schema)

        assert table_info.exists is True
        assert table_info.schema == schema

    def test_table_info_creation_with_nonexistent_table(self):
        """Test TableInfo creation for nonexistent table."""
        table_info = TableInfo(exists=False)

        assert table_info.exists is False
        assert table_info.schema is None


class TestValidationHelper:
    """Test cases for ValidationHelper class."""

    def test_get_table_info_existing_table(self):
        """Test get_table_info for existing table."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        mock_engine.get_table_schema.return_value = {"id": "INTEGER"}

        helper = ValidationHelper(mock_engine)
        table_info = helper.get_table_info("test_table")

        assert table_info.exists is True
        assert table_info.schema == {"id": "INTEGER"}
        mock_engine.table_exists.assert_called_once_with("test_table")
        mock_engine.get_table_schema.assert_called_once_with("test_table")

    def test_get_table_info_nonexistent_table(self):
        """Test get_table_info for nonexistent table."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False

        helper = ValidationHelper(mock_engine)
        table_info = helper.get_table_info("test_table")

        assert table_info.exists is False
        assert table_info.schema is None
        mock_engine.table_exists.assert_called_once_with("test_table")
        mock_engine.get_table_schema.assert_not_called()

    def test_validate_schema_and_upsert_keys_success(self):
        """Test successful validation for APPEND mode."""
        mock_engine = Mock()
        mock_engine.get_table_schema.return_value = {"id": "INTEGER", "name": "VARCHAR"}
        mock_engine.validate_schema_compatibility.return_value = None

        helper = ValidationHelper(mock_engine)
        load_step = LoadStep("target_table", "source_table", "APPEND")
        table_info = TableInfo(exists=True, schema={"id": "INTEGER"})

        source_schema = helper.validate_schema_and_upsert_keys(load_step, table_info)

        assert source_schema == {"id": "INTEGER", "name": "VARCHAR"}
        mock_engine.validate_schema_compatibility.assert_called_once()

    def test_validate_schema_and_upsert_keys_upsert_mode_success(self):
        """Test successful validation for UPSERT mode with upsert keys (formerly MERGE)."""
        mock_engine = Mock()
        mock_engine.get_table_schema.return_value = {"id": "INTEGER", "name": "VARCHAR"}
        mock_engine.validate_schema_compatibility.return_value = None
        mock_engine.validate_upsert_keys.return_value = None

        helper = ValidationHelper(mock_engine)
        load_step = LoadStep("target_table", "source_table", "UPSERT", ["id"])
        table_info = TableInfo(exists=True, schema={"id": "INTEGER"})

        source_schema = helper.validate_schema_and_upsert_keys(load_step, table_info)

        assert source_schema == {"id": "INTEGER", "name": "VARCHAR"}
        mock_engine.validate_upsert_keys.assert_called_once_with(
            "target_table", "source_table", ["id"]
        )

    def test_validate_schema_and_upsert_keys_missing_upsert_keys(self):
        """Test validation failure when upsert keys are missing for UPSERT mode."""
        mock_engine = Mock()

        helper = ValidationHelper(mock_engine)
        load_step = LoadStep("target_table", "source_table", "UPSERT")  # No upsert keys
        table_info = TableInfo(exists=True)

        with pytest.raises(UpsertKeyValidationError) as exc_info:
            helper.validate_schema_and_upsert_keys(load_step, table_info)

        assert "UPSERT mode requires upsert keys" in str(exc_info.value)
        assert exc_info.value.table_name == "target_table"

    def test_validate_schema_and_upsert_keys_schema_validation_failure(self):
        """Test schema validation failure wrapping."""
        mock_engine = Mock()
        mock_engine.get_table_schema.side_effect = [
            {"id": "INTEGER"},  # source schema
            {"id": "VARCHAR"},  # target schema (called by SchemaValidator)
        ]
        mock_engine.validate_schema_compatibility.side_effect = Exception(
            "Schema mismatch"
        )

        helper = ValidationHelper(mock_engine)
        load_step = LoadStep("target_table", "source_table", "APPEND")
        table_info = TableInfo(exists=True, schema={"id": "VARCHAR"})

        with pytest.raises(SchemaValidationError) as exc_info:
            helper.validate_schema_and_upsert_keys(load_step, table_info)

        assert "Schema validation failed for table target_table" in str(exc_info.value)
        assert exc_info.value.source_schema == {"id": "INTEGER"}
        assert exc_info.value.target_schema == {"id": "VARCHAR"}

    def test_validate_schema_and_upsert_keys_upsert_key_validation_failure(self):
        """Test upsert key validation failure wrapping."""
        mock_engine = Mock()
        mock_engine.get_table_schema.return_value = {"id": "INTEGER"}
        mock_engine.validate_schema_compatibility.return_value = None
        mock_engine.validate_upsert_keys.side_effect = Exception("Key not found")

        helper = ValidationHelper(mock_engine)
        load_step = LoadStep("target_table", "source_table", "UPSERT", ["invalid_key"])
        table_info = TableInfo(exists=True)

        with pytest.raises(UpsertKeyValidationError) as exc_info:
            helper.validate_schema_and_upsert_keys(load_step, table_info)

        assert "Upsert key validation failed for table target_table" in str(
            exc_info.value
        )
        assert exc_info.value.table_name == "target_table"
        assert exc_info.value.upsert_keys == ["invalid_key"]


class TestLoadHandlerOptimizations:
    """Test cases for load handler performance optimizations."""

    def test_replace_handler_optimized_table_existence_check(self):
        """Test that ReplaceLoadHandler uses optimized table existence check."""
        mock_engine = Mock()
        handler = ReplaceLoadHandler(mock_engine)
        load_step = LoadStep("target_table", "source_table", "REPLACE")

        # Mock the validation helper's get_table_info method
        handler.validation_helper.get_table_info = Mock()
        handler.validation_helper.get_table_info.return_value = TableInfo(exists=True)

        sql = handler.generate_sql(load_step)

        # Should call get_table_info once instead of multiple table_exists calls
        handler.validation_helper.get_table_info.assert_called_once_with("target_table")
        assert "CREATE OR REPLACE TABLE" in sql

    def test_append_handler_uses_shared_validation(self):
        """Test that AppendLoadHandler uses shared validation logic."""
        mock_engine = Mock()
        handler = AppendLoadHandler(mock_engine)
        load_step = LoadStep("target_table", "source_table", "APPEND")

        # Mock the validation helper methods
        table_info = TableInfo(exists=True, schema={"id": "INTEGER"})
        handler.validation_helper.get_table_info = Mock(return_value=table_info)
        handler.validation_helper.validate_for_load_mode = Mock(
            return_value={"id": "INTEGER", "name": "VARCHAR"}
        )

        sql = handler.generate_sql(load_step)

        # Should use shared validation instead of duplicated logic
        handler.validation_helper.validate_for_load_mode.assert_called_once_with(
            load_step, table_info
        )
        assert "INSERT INTO" in sql

    def test_upsert_handler_uses_shared_validation(self):
        """Test that UpsertLoadHandler uses shared validation infrastructure."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        mock_engine.get_table_schema.return_value = {"id": "INTEGER"}
        mock_engine.validate_schema_compatibility.return_value = None
        mock_engine.validate_upsert_keys.return_value = None

        handler = UpsertLoadHandler(mock_engine)
        load_step = LoadStep("test_table", "test_source", "UPSERT", ["id"])

        # This should use the shared validation helper
        sql = handler.generate_sql(load_step)

        # Verify shared validation was called
        mock_engine.table_exists.assert_called_once_with("test_table")
        mock_engine.get_table_schema.assert_called()
        mock_engine.validate_schema_compatibility.assert_called_once()
        mock_engine.validate_upsert_keys.assert_called_once_with(
            "test_table", "test_source", ["id"]
        )

        # Should generate UPSERT SQL with transaction safety and direct table references
        assert "BEGIN TRANSACTION" in sql
        assert "UPDATE" in sql and "INSERT INTO" in sql
        assert "COMMIT" in sql
        # Modern implementation uses direct table references instead of temporary views
        assert 'FROM "test_source" AS source' in sql


class TestLoadModeHandlerFactory:
    """Test cases for LoadModeHandlerFactory with custom exceptions."""

    def test_factory_raises_custom_exception_for_invalid_mode(self):
        """Test that factory raises InvalidLoadModeError instead of ValueError."""
        mock_engine = Mock()

        with pytest.raises(InvalidLoadModeError) as exc_info:
            LoadModeHandlerFactory.create("INVALID_MODE", mock_engine)

        assert exc_info.value.mode == "INVALID_MODE"
        assert "REPLACE" in exc_info.value.valid_modes
        assert "APPEND" in exc_info.value.valid_modes
        assert "UPSERT" in exc_info.value.valid_modes

    def test_factory_creates_correct_handlers(self):
        """Test that factory creates correct handler types."""
        mock_engine = Mock()

        replace_handler = LoadModeHandlerFactory.create("REPLACE", mock_engine)
        append_handler = LoadModeHandlerFactory.create("APPEND", mock_engine)
        upsert_handler = LoadModeHandlerFactory.create("UPSERT", mock_engine)

        assert isinstance(replace_handler, ReplaceLoadHandler)
        assert isinstance(append_handler, AppendLoadHandler)
        assert isinstance(upsert_handler, UpsertLoadHandler)

    def test_factory_handles_case_insensitive_modes(self):
        """Test that factory handles case insensitive mode names."""
        mock_engine = Mock()

        # Test lowercase
        handler1 = LoadModeHandlerFactory.create("replace", mock_engine)
        handler2 = LoadModeHandlerFactory.create("append", mock_engine)
        handler3 = LoadModeHandlerFactory.create("upsert", mock_engine)

        assert isinstance(handler1, ReplaceLoadHandler)
        assert isinstance(handler2, AppendLoadHandler)
        assert isinstance(handler3, UpsertLoadHandler)

        # Test uppercase
        handler3 = LoadModeHandlerFactory.create("UPSERT", mock_engine)
        assert isinstance(handler3, UpsertLoadHandler)


class TestIntegrationOptimizations:
    """Integration tests for the optimization improvements."""

    def test_reduced_database_calls_for_existing_table(self):
        """Test that database calls are minimized for existing tables."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        mock_engine.get_table_schema.return_value = {"id": "INTEGER"}
        mock_engine.validate_schema_compatibility.return_value = None

        handler = AppendLoadHandler(mock_engine)
        load_step = LoadStep("target_table", "source_table", "APPEND")

        sql = handler.generate_sql(load_step)

        # Verify optimal number of calls
        assert mock_engine.table_exists.call_count == 1  # Once in get_table_info
        assert (
            mock_engine.get_table_schema.call_count == 2
        )  # Once for target, once for source
        assert "INSERT INTO" in sql

    def test_no_unnecessary_schema_calls_for_new_table(self):
        """Test that unnecessary schema calls are avoided for new tables."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False

        handler = ReplaceLoadHandler(mock_engine)
        load_step = LoadStep("target_table", "source_table", "REPLACE")

        sql = handler.generate_sql(load_step)

        # Should not call get_table_schema for non-existent target table
        assert mock_engine.table_exists.call_count == 1
        mock_engine.get_table_schema.assert_not_called()
        assert "CREATE TABLE" in sql
