"""Tests for step validation functions."""

from sqlflow.core.executors.v2.validation.step_validation import (
    ValidationResult,
    validate_export_step,
    validate_load_step,
    validate_pipeline,
    validate_source_definition_step,
    validate_step,
    validate_step_id,
    validate_step_type,
    validate_transform_step,
    validate_variables,
)


class TestValidationResult:
    """Test immutable ValidationResult."""

    def test_valid_result(self):
        """Test creating valid result."""
        result = ValidationResult(is_valid=True)
        assert result.is_valid
        assert result.errors == []
        assert result.warnings == []

    def test_add_error_creates_new_result(self):
        """Test that adding error creates new immutable result."""
        result1 = ValidationResult(is_valid=True)
        result2 = result1.add_error("field", "message")

        assert result1.is_valid  # Original unchanged
        assert not result2.is_valid  # New result is invalid
        assert len(result2.errors) == 1
        assert result2.errors[0].field == "field"
        assert result2.errors[0].message == "message"

    def test_add_warning_creates_new_result(self):
        """Test that adding warning creates new immutable result."""
        result1 = ValidationResult(is_valid=True)
        result2 = result1.add_warning("warning message")

        assert result1.warnings == []  # Original unchanged
        assert result2.warnings == ["warning message"]
        assert result2.is_valid  # Still valid


class TestValidateStepType:
    """Test step type validation."""

    def test_valid_step_types(self):
        """Test all valid step types."""
        valid_types = ["load", "transform", "export", "source_definition"]

        for step_type in valid_types:
            step = {"type": step_type}
            result = validate_step_type(step)
            assert result.is_valid

    def test_invalid_step_type(self):
        """Test invalid step type."""
        step = {"type": "invalid_type"}
        result = validate_step_type(step)

        assert not result.is_valid
        assert len(result.errors) == 1
        assert "Invalid step type" in result.errors[0].message

    def test_missing_type_field(self):
        """Test missing type field."""
        step = {"id": "test"}
        result = validate_step_type(step)

        assert not result.is_valid
        assert result.errors[0].field == "type"

    def test_non_dict_step(self):
        """Test non-dictionary step."""
        result = validate_step_type("not a dict")

        assert not result.is_valid
        assert result.errors[0].field == "step"


class TestValidateStepId:
    """Test step ID validation."""

    def test_valid_step_id(self):
        """Test valid step ID."""
        step = {"id": "valid_step_id"}
        result = validate_step_id(step)
        assert result.is_valid

    def test_missing_id_field(self):
        """Test missing ID field."""
        step = {"type": "load"}
        result = validate_step_id(step)

        assert not result.is_valid
        assert result.errors[0].field == "id"

    def test_non_string_id(self):
        """Test non-string ID."""
        step = {"id": 123}
        result = validate_step_id(step)

        assert not result.is_valid
        assert "must be a string" in result.errors[0].message

    def test_empty_id(self):
        """Test empty ID."""
        step = {"id": "   "}
        result = validate_step_id(step)

        assert not result.is_valid
        assert "cannot be empty" in result.errors[0].message

    def test_special_characters_warning(self):
        """Test that special characters generate warning."""
        step = {"id": "step@with#special$chars"}
        result = validate_step_id(step)

        assert result.is_valid  # Still valid but has warning
        assert len(result.warnings) == 1
        assert "special characters" in result.warnings[0]


class TestValidateLoadStep:
    """Test load step validation."""

    def test_valid_load_step(self):
        """Test valid load step."""
        step = {
            "type": "load",
            "id": "load_users",
            "source": "/data/users.csv",
            "target_table": "users",
        }
        result = validate_load_step(step)
        assert result.is_valid

    def test_load_step_with_source_name(self):
        """Test load step with source_name instead of source."""
        step = {
            "type": "load",
            "id": "load_users",
            "source_name": "users_source",
            "target_table": "users",
        }
        result = validate_load_step(step)
        assert result.is_valid

    def test_load_step_missing_source(self):
        """Test load step missing both source and source_name."""
        step = {"type": "load", "id": "load_users", "target_table": "users"}
        result = validate_load_step(step)

        assert not result.is_valid
        assert any("source" in error.field for error in result.errors)

    def test_load_step_missing_target_table(self):
        """Test load step missing target table."""
        step = {"type": "load", "id": "load_users", "source": "/data/users.csv"}
        result = validate_load_step(step)

        assert not result.is_valid
        assert any("target_table" in error.field for error in result.errors)

    def test_invalid_load_mode(self):
        """Test invalid load mode."""
        step = {
            "type": "load",
            "id": "load_users",
            "source": "/data/users.csv",
            "target_table": "users",
            "load_mode": "invalid_mode",
        }
        result = validate_load_step(step)

        assert not result.is_valid
        assert any("load_mode" in error.field for error in result.errors)


class TestValidateTransformStep:
    """Test transform step validation."""

    def test_valid_transform_step(self):
        """Test valid transform step."""
        step = {
            "type": "transform",
            "id": "transform_users",
            "query": "SELECT * FROM users WHERE active = true",
            "target_table": "active_users",
        }
        result = validate_transform_step(step)
        assert result.is_valid

    def test_transform_step_missing_query(self):
        """Test transform step missing query."""
        step = {
            "type": "transform",
            "id": "transform_users",
            "target_table": "active_users",
        }
        result = validate_transform_step(step)

        assert not result.is_valid
        assert any("query" in error.field for error in result.errors)

    def test_transform_step_non_string_query(self):
        """Test transform step with non-string query."""
        step = {
            "type": "transform",
            "id": "transform_users",
            "query": 123,
            "target_table": "active_users",
        }
        result = validate_transform_step(step)

        assert not result.is_valid
        assert any("query" in error.field for error in result.errors)

    def test_transform_step_empty_query(self):
        """Test transform step with empty query."""
        step = {
            "type": "transform",
            "id": "transform_users",
            "query": "   ",
            "target_table": "active_users",
        }
        result = validate_transform_step(step)

        assert not result.is_valid
        assert any("query" in error.field for error in result.errors)


class TestValidateExportStep:
    """Test export step validation."""

    def test_valid_export_step(self):
        """Test valid export step."""
        step = {
            "type": "export",
            "id": "export_users",
            "source_table": "users",
            "destination": "/output/users.csv",
        }
        result = validate_export_step(step)
        assert result.is_valid

    def test_export_step_missing_source_table(self):
        """Test export step missing source table."""
        step = {
            "type": "export",
            "id": "export_users",
            "destination": "/output/users.csv",
        }
        result = validate_export_step(step)

        assert not result.is_valid
        assert any("source_table" in error.field for error in result.errors)

    def test_export_step_missing_destination(self):
        """Test export step missing destination."""
        step = {"type": "export", "id": "export_users", "source_table": "users"}
        result = validate_export_step(step)

        assert not result.is_valid
        assert any("destination" in error.field for error in result.errors)


class TestValidateSourceDefinitionStep:
    """Test source definition step validation."""

    def test_valid_source_definition_step(self):
        """Test valid source definition step."""
        step = {
            "type": "source_definition",
            "id": "define_users",
            "name": "users_source",
            "source_connector_type": "csv",
        }
        result = validate_source_definition_step(step)
        assert result.is_valid

    def test_source_definition_missing_name(self):
        """Test source definition missing name."""
        step = {
            "type": "source_definition",
            "id": "define_users",
            "source_connector_type": "csv",
        }
        result = validate_source_definition_step(step)

        assert not result.is_valid
        assert any("name" in error.field for error in result.errors)

    def test_source_definition_missing_connector_type(self):
        """Test source definition missing connector type."""
        step = {
            "type": "source_definition",
            "id": "define_users",
            "name": "users_source",
        }
        result = validate_source_definition_step(step)

        assert not result.is_valid
        assert any("source_connector_type" in error.field for error in result.errors)


class TestValidateStep:
    """Test general step validation."""

    def test_valid_steps_all_types(self):
        """Test validation works for all step types."""
        steps = [
            {
                "type": "load",
                "id": "load_step",
                "source": "test.csv",
                "target_table": "test",
            },
            {
                "type": "transform",
                "id": "transform_step",
                "query": "SELECT * FROM test",
                "target_table": "transformed",
            },
            {
                "type": "export",
                "id": "export_step",
                "source_table": "transformed",
                "destination": "output.csv",
            },
            {
                "type": "source_definition",
                "id": "source_step",
                "name": "test_source",
                "source_connector_type": "csv",
            },
        ]

        for step in steps:
            result = validate_step(step)
            assert result.is_valid, f"Step {step['type']} failed validation"

    def test_unknown_step_type(self):
        """Test unknown step type."""
        step = {"type": "unknown", "id": "test"}
        result = validate_step(step)

        assert not result.is_valid
        assert any("Invalid step type" in error.message for error in result.errors)


class TestValidatePipeline:
    """Test pipeline validation."""

    def test_valid_pipeline(self):
        """Test valid pipeline."""
        pipeline = [
            {
                "type": "load",
                "id": "load_step",
                "source": "test.csv",
                "target_table": "test",
            },
            {
                "type": "transform",
                "id": "transform_step",
                "query": "SELECT * FROM test",
                "target_table": "transformed",
            },
        ]
        result = validate_pipeline(pipeline)
        assert result.is_valid

    def test_empty_pipeline_warning(self):
        """Test empty pipeline generates warning."""
        result = validate_pipeline([])

        assert result.is_valid  # Empty is valid but warned
        assert len(result.warnings) == 1
        assert "empty" in result.warnings[0].lower()

    def test_non_list_pipeline(self):
        """Test non-list pipeline."""
        result = validate_pipeline("not a list")

        assert not result.is_valid
        assert result.errors[0].field == "pipeline"

    def test_duplicate_step_ids(self):
        """Test duplicate step IDs."""
        pipeline = [
            {
                "type": "load",
                "id": "duplicate_id",
                "source": "test1.csv",
                "target_table": "test1",
            },
            {
                "type": "load",
                "id": "duplicate_id",
                "source": "test2.csv",
                "target_table": "test2",
            },
        ]
        result = validate_pipeline(pipeline)

        assert not result.is_valid
        assert any("Duplicate step ID" in error.message for error in result.errors)


class TestValidateVariables:
    """Test variables validation."""

    def test_valid_variables(self):
        """Test valid variables."""
        variables = {"table": "users", "limit": 100}
        result = validate_variables(variables)
        assert result.is_valid

    def test_none_variables(self):
        """Test None variables is valid."""
        result = validate_variables(None)
        assert result.is_valid

    def test_non_dict_variables(self):
        """Test non-dict variables."""
        result = validate_variables("not a dict")

        assert not result.is_valid
        assert result.errors[0].field == "variables"

    def test_non_string_keys(self):
        """Test variables with non-string keys."""
        variables = {123: "value", "valid": "value"}
        result = validate_variables(variables)

        assert not result.is_valid
        assert any("must be strings" in error.message for error in result.errors)
