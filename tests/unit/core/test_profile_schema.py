"""Unit tests for SQLFlow ProfileSchemaValidator."""

import pytest

from sqlflow.core.profile_schema import (
    ProfileSchemaValidator,
    SchemaValidationError,
    SchemaValidationResult,
)


class TestSchemaValidationError:
    """Test SchemaValidationError class."""

    def test_str_basic(self):
        """Test basic string representation."""
        error = SchemaValidationError(path="test.path", message="Test message")

        assert str(error) == "test.path: Test message"

    def test_str_with_types(self):
        """Test string representation with type information."""
        error = SchemaValidationError(
            path="test.path",
            message="Type mismatch",
            expected_type="str",
            actual_type="int",
        )

        assert str(error) == "test.path: Type mismatch (expected str, got int)"

    def test_str_with_line_number(self):
        """Test string representation with line number."""
        error = SchemaValidationError(
            path="test.path", message="Syntax error", line_number=42
        )

        assert str(error) == "test.path: Syntax error at line 42"

    def test_str_complete(self):
        """Test string representation with all fields."""
        error = SchemaValidationError(
            path="test.path",
            message="Complete error",
            expected_type="dict",
            actual_type="list",
            line_number=10,
        )

        assert (
            str(error)
            == "test.path: Complete error (expected dict, got list) at line 10"
        )


class TestSchemaValidationResult:
    """Test SchemaValidationResult class."""

    def test_bool_valid(self):
        """Test boolean conversion for valid result."""
        result = SchemaValidationResult(is_valid=True, errors=[], warnings=[])

        assert bool(result) is True

    def test_bool_invalid(self):
        """Test boolean conversion for invalid result."""
        result = SchemaValidationResult(
            is_valid=False, errors=[SchemaValidationError("path", "error")], warnings=[]
        )

        assert bool(result) is False

    def test_get_error_summary_no_errors(self):
        """Test error summary with no errors."""
        result = SchemaValidationResult(is_valid=True, errors=[], warnings=[])

        assert result.get_error_summary() == "No validation errors"

    def test_get_error_summary_with_errors(self):
        """Test error summary with multiple errors."""
        errors = [
            SchemaValidationError("path1", "Error 1"),
            SchemaValidationError("path2", "Error 2"),
        ]
        result = SchemaValidationResult(is_valid=False, errors=errors, warnings=[])

        summary = result.get_error_summary()
        assert "path1: Error 1" in summary
        assert "path2: Error 2" in summary


class TestProfileSchemaValidator:
    """Test ProfileSchemaValidator class."""

    @pytest.fixture
    def validator(self):
        """Create ProfileSchemaValidator instance."""
        return ProfileSchemaValidator()

    @pytest.fixture
    def valid_profile(self):
        """Sample valid profile for testing."""
        return {
            "version": "1.0",
            "variables": {"db_host": "localhost", "db_port": "5432"},
            "connectors": {
                "csv_default": {
                    "type": "csv",
                    "params": {"has_header": True, "delimiter": ","},
                },
                "postgres_main": {
                    "type": "postgres",
                    "params": {"host": "${db_host}", "port": "${db_port}"},
                },
            },
            "engines": {"duckdb": {"mode": "memory"}},
        }

    def test_init(self, validator):
        """Test validator initialization."""
        assert validator.CURRENT_VERSION == "1.0"
        assert "csv" in validator.CONNECTOR_SCHEMAS
        assert "postgres" in validator.CONNECTOR_SCHEMAS
        assert "s3" in validator.CONNECTOR_SCHEMAS
        assert "rest" in validator.CONNECTOR_SCHEMAS

    def test_validate_profile_valid(self, validator, valid_profile):
        """Test validation of valid profile."""
        result = validator.validate_profile(valid_profile)

        assert result.is_valid
        assert len(result.errors) == 0
        assert result.schema_version == "1.0"

    def test_validate_profile_not_dict(self, validator):
        """Test validation of non-dictionary profile."""
        result = validator.validate_profile("not a dict")

        assert not result.is_valid
        assert len(result.errors) == 1
        assert result.errors[0].path == "root"
        assert "dictionary" in result.errors[0].message.lower()

    def test_validate_version_missing(self, validator):
        """Test validation with missing version."""
        profile = {"connectors": {}}
        result = validator.validate_profile(profile)

        assert result.is_valid
        assert any("version not specified" in warning for warning in result.warnings)
        assert result.schema_version == "1.0"  # Default

    def test_validate_version_invalid_type(self, validator):
        """Test validation with invalid version type."""
        profile = {"version": 1.0}  # Should be string
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("version" in error.path.lower() for error in result.errors)

    def test_validate_version_unknown(self, validator):
        """Test validation with unknown version."""
        profile = {"version": "2.0"}
        result = validator.validate_profile(profile)

        assert result.is_valid  # Still valid, just warning
        assert any("Unknown profile version" in warning for warning in result.warnings)

    def test_validate_variables_invalid_type(self, validator):
        """Test validation with invalid variables type."""
        profile = {"variables": "not a dict"}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("variables" in error.path.lower() for error in result.errors)

    def test_validate_variables_invalid_name(self, validator):
        """Test validation with invalid variable name."""
        profile = {
            "variables": {
                "123invalid": "value",  # Invalid identifier
                "valid_name": "value",
            }
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("123invalid" in error.path for error in result.errors)

    def test_validate_variables_invalid_value_type(self, validator):
        """Test validation with invalid variable value type."""
        profile = {
            "variables": {"valid_name": {"complex": "object"}}  # Should be simple type
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("valid_name" in error.path for error in result.errors)

    def test_validate_connectors_missing(self, validator):
        """Test validation with missing connectors section."""
        profile = {"version": "1.0"}
        result = validator.validate_profile(profile)

        assert result.is_valid
        assert any("No connectors section" in warning for warning in result.warnings)

    def test_validate_connectors_invalid_type(self, validator):
        """Test validation with invalid connectors type."""
        profile = {"connectors": "not a dict"}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("connectors" in error.path.lower() for error in result.errors)

    def test_validate_connectors_empty(self, validator):
        """Test validation with empty connectors section."""
        profile = {"connectors": {}}
        result = validator.validate_profile(profile)

        assert result.is_valid
        assert any("empty" in warning.lower() for warning in result.warnings)

    def test_validate_connector_not_dict(self, validator):
        """Test validation with connector that's not a dictionary."""
        profile = {"connectors": {"bad_connector": "not a dict"}}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("bad_connector" in error.path for error in result.errors)

    def test_validate_connector_missing_type(self, validator):
        """Test validation with connector missing type field."""
        profile = {"connectors": {"no_type": {"params": {}}}}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("type" in error.path for error in result.errors)

    def test_validate_connector_invalid_type_field(self, validator):
        """Test validation with invalid connector type field."""
        profile = {"connectors": {"bad_type": {"type": 123}}}  # Should be string
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("type" in error.path for error in result.errors)

    def test_validate_connector_invalid_params(self, validator):
        """Test validation with invalid params type."""
        profile = {
            "connectors": {"bad_params": {"type": "csv", "params": "not a dict"}}
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("params" in error.path for error in result.errors)

    def test_validate_connector_unknown_type(self, validator):
        """Test validation with unknown connector type."""
        profile = {
            "connectors": {"unknown_connector": {"type": "unknown_type", "params": {}}}
        }
        result = validator.validate_profile(profile)

        assert result.is_valid  # Valid but with warning
        assert any("Unknown connector type" in warning for warning in result.warnings)

    def test_validate_connector_csv_params(self, validator):
        """Test validation of CSV connector parameters."""
        profile = {
            "connectors": {
                "csv_test": {
                    "type": "csv",
                    "params": {
                        "has_header": "true",  # Should be boolean
                        "delimiter": ",",  # Valid
                        "unknown_param": "value",  # Unknown parameter
                    },
                }
            }
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid  # Invalid due to wrong type
        assert any("has_header" in error.path for error in result.errors)
        assert any("Unknown parameter" in warning for warning in result.warnings)

    def test_validate_connector_postgres_params(self, validator):
        """Test validation of PostgreSQL connector parameters."""
        profile = {
            "connectors": {
                "pg_test": {
                    "type": "postgres",
                    "params": {
                        "host": "localhost",
                        "port": 5432,  # Can be int
                        "database": "test_db",
                        "connect_timeout": "30",  # Should be int
                    },
                }
            }
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("connect_timeout" in error.path for error in result.errors)

    def test_validate_engines_invalid_type(self, validator):
        """Test validation with invalid engines type."""
        profile = {"engines": "not a dict"}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("engines" in error.path.lower() for error in result.errors)

    def test_validate_duckdb_engine_invalid_type(self, validator):
        """Test validation with invalid DuckDB engine type."""
        profile = {"engines": {"duckdb": "not a dict"}}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("duckdb" in error.path.lower() for error in result.errors)

    def test_validate_duckdb_mode_invalid_type(self, validator):
        """Test validation with invalid DuckDB mode type."""
        profile = {"engines": {"duckdb": {"mode": 123}}}  # Should be string
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("mode" in error.path for error in result.errors)

    def test_validate_duckdb_mode_invalid_value(self, validator):
        """Test validation with invalid DuckDB mode value."""
        profile = {"engines": {"duckdb": {"mode": "invalid_mode"}}}
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("Invalid DuckDB mode" in error.message for error in result.errors)

    def test_validate_duckdb_persistent_missing_path(self, validator):
        """Test validation with persistent mode missing path."""
        profile = {
            "engines": {
                "duckdb": {
                    "mode": "persistent"
                    # Missing path
                }
            }
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("path is required" in error.message for error in result.errors)

    def test_validate_duckdb_persistent_invalid_path_type(self, validator):
        """Test validation with persistent mode invalid path type."""
        profile = {
            "engines": {
                "duckdb": {"mode": "persistent", "path": 123}  # Should be string
            }
        }
        result = validator.validate_profile(profile)

        assert not result.is_valid
        assert any("path must be a string" in error.message for error in result.errors)

    def test_unknown_top_level_keys(self, validator):
        """Test validation with unknown top-level keys."""
        profile = {"version": "1.0", "connectors": {}, "unknown_section": "value"}
        result = validator.validate_profile(profile)

        assert result.is_valid  # Valid but with warning
        assert any(
            "Unknown key 'unknown_section'" in warning for warning in result.warnings
        )

    def test_unknown_connector_keys(self, validator):
        """Test validation with unknown connector keys."""
        profile = {
            "connectors": {
                "test_connector": {"type": "csv", "params": {}, "unknown_key": "value"}
            }
        }
        result = validator.validate_profile(profile)

        assert result.is_valid  # Valid but with warning
        assert any(
            "Unknown key 'unknown_key'" in warning for warning in result.warnings
        )

    def test_get_schema_documentation(self, validator):
        """Test schema documentation generation."""
        docs = validator.get_schema_documentation()

        assert "SQLFlow Profile Schema" in docs
        assert "v1.0" in docs
        assert "csv" in docs
        assert "postgres" in docs
        assert "s3" in docs
        assert "rest" in docs
        assert "Structure" in docs
        assert "version:" in docs
        assert "connectors:" in docs

    def test_field_type_checking(self, validator):
        """Test field type checking helper method."""
        # Test single type
        assert validator._check_field_type("string", str)
        assert not validator._check_field_type(123, str)

        # Test tuple of types
        assert validator._check_field_type(123, (int, str))
        assert validator._check_field_type("123", (int, str))
        assert not validator._check_field_type([], (int, str))

    def test_format_type(self, validator):
        """Test type formatting for error messages."""
        assert validator._format_type(str) == "str"
        assert validator._format_type((int, str)) == "int|str"

    def test_variable_name_validation(self, validator):
        """Test variable name validation."""
        # Valid names
        assert validator._is_valid_variable_name("valid_name")
        assert validator._is_valid_variable_name("_private")
        assert validator._is_valid_variable_name("name123")

        # Invalid names
        assert not validator._is_valid_variable_name("123invalid")
        assert not validator._is_valid_variable_name("invalid-name")
        assert not validator._is_valid_variable_name("invalid.name")
        assert not validator._is_valid_variable_name("invalid space")
