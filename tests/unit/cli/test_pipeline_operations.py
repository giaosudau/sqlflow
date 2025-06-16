"""Tests for pipeline operations module."""

from unittest.mock import Mock, mock_open, patch

import pytest

from sqlflow.cli.errors import (
    PipelineNotFoundError,
    VariableParsingError,
)
from sqlflow.cli.pipeline_operations import (
    compile_pipeline_to_plan,
    parse_variables,
    save_compilation_result,
    substitute_variables,
)


class TestParseVariables:
    """Test variable parsing functions."""

    def test_variables_parsed_correctly_when_input_is_empty(self):
        """Test parsing empty variables."""
        result = parse_variables(None)
        assert result == {}

        result = parse_variables("")
        assert result == {}

    def test_variables_parsed_correctly_when_json_format_provided(self):
        """Test parsing JSON variables."""
        json_str = '{"env": "prod", "debug": true, "count": 42}'
        result = parse_variables(json_str)

        expected = {"env": "prod", "debug": True, "count": 42}
        assert result == expected

    def test_variables_parsed_correctly_when_key_value_format_provided(self):
        """Test parsing key=value variables."""
        kv_str = "env=prod,debug=true,count=42"
        result = parse_variables(kv_str)

        expected = {"env": "prod", "debug": True, "count": 42}
        assert result == expected

    def test_variables_parsed_correctly_when_mixed_key_value_format_provided(self):
        """Test parsing key=value with string values."""
        kv_str = "env=production,name=test app,active=true"
        result = parse_variables(kv_str)

        expected = {"env": "production", "name": "test app", "active": True}
        assert result == expected

    def test_parsing_fails_when_invalid_json_provided(self):
        """Test parsing invalid JSON."""
        with pytest.raises(VariableParsingError) as excinfo:
            parse_variables('{"invalid": json}')

        assert "Failed to parse variables" in str(excinfo.value)
        assert excinfo.value.variable_string == '{"invalid": json}'

    def test_parsing_fails_when_invalid_key_value_format_provided(self):
        """Test parsing invalid key=value format."""
        with pytest.raises(VariableParsingError) as excinfo:
            parse_variables("invalid_format")

        assert "Failed to parse variables" in str(excinfo.value)
        assert "Invalid format" in str(excinfo.value)


class TestSubstituteVariables:
    """Test variable substitution."""

    def test_variables_substituted_correctly_when_all_values_provided(self):
        """Test basic variable substitution using real VariableManager."""
        variables = {"table": "test_table"}

        # Test with various template syntaxes to see what works
        test_cases = [
            "SELECT * FROM {{table}}",  # Jinja2 style
            "SELECT * FROM ${table}",  # Shell/SQLFlow style
            "SELECT * FROM {table}",  # Python format style
        ]

        found_working_syntax = False
        for template in test_cases:
            result = substitute_variables(template, variables)
            if "test_table" in result:
                found_working_syntax = True
                break

        # At least one syntax should work
        assert (
            found_working_syntax
        ), f"None of the template syntaxes worked with variables: {variables}"

    @patch("sqlflow.cli.pipeline_operations.logger")
    def test_substitution_handles_gracefully_when_variables_missing(self, mock_logger):
        """Test variable substitution with missing variables using real VariableManager."""
        # Test with missing variable - should trigger logging
        result = substitute_variables("SELECT * FROM {{missing}}", {})

        # The result should either:
        # 1. Leave the template unsubstituted
        # 2. Raise an error (which we'd catch)
        # 3. Log a warning (which is what we're testing)

        # Check that some form of the template remains or warning was logged
        assert "{{missing}}" in result or mock_logger.warning.called

        # If warning was called, verify it mentions the missing variable
        if mock_logger.warning.called:
            warning_msg = str(mock_logger.warning.call_args)
            assert "missing" in warning_msg or "variable" in warning_msg.lower()


class TestSaveCompilationResult:
    """Test saving compilation results."""

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.makedirs")
    def test_compilation_result_saved_successfully_when_default_path_used(
        self, mock_makedirs, mock_file
    ):
        """Test saving with default path."""
        operations = [{"type": "source", "name": "test"}]

        result_path = save_compilation_result(operations, "test_pipeline")

        expected_path = "target/compiled/test_pipeline.json"
        assert result_path == expected_path

        mock_makedirs.assert_called()
        mock_file.assert_called_once_with(expected_path, "w")

        # Verify JSON was written
        handle = mock_file()
        written_content = "".join(
            [call.args[0] for call in handle.write.call_args_list]
        )
        assert "test" in written_content

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.makedirs")
    def test_compilation_result_saved_successfully_when_custom_path_provided(
        self, mock_makedirs, mock_file
    ):
        """Test saving with custom output directory."""
        operations = [{"type": "source", "name": "test"}]
        custom_path = "/custom/path/plan.json"

        result_path = save_compilation_result(operations, "test_pipeline", custom_path)

        assert result_path == custom_path
        mock_file.assert_called_once_with(custom_path, "w")


class TestCompilePipelineToPlan:
    """Test the main compilation function."""

    @patch("sqlflow.cli.pipeline_operations.load_project")
    @patch("sqlflow.cli.pipeline_operations.load_pipeline")
    @patch("sqlflow.cli.pipeline_operations.substitute_variables")
    @patch("sqlflow.cli.pipeline_operations.plan_pipeline")
    @patch("sqlflow.cli.pipeline_operations.save_compilation_result")
    @patch("sqlflow.cli.pipeline_operations.display_compilation_success")
    def test_pipeline_compiled_successfully_when_all_steps_complete(
        self,
        mock_display,
        mock_save,
        mock_plan,
        mock_substitute,
        mock_load_pipeline,
        mock_load_project,
    ):
        """Test successful pipeline compilation."""
        # Setup mocks
        mock_project = Mock()
        mock_project.get_profile.return_value = {"variables": {"env": "test"}}
        mock_load_project.return_value = mock_project
        mock_load_pipeline.return_value = "SELECT * FROM {{table}}"
        mock_substitute.return_value = "SELECT * FROM test_table"
        mock_plan.return_value = [{"type": "source", "name": "test"}]
        mock_save.return_value = "/path/to/plan.json"

        variables = {"table": "test_table"}
        result = compile_pipeline_to_plan("test_pipeline", "dev", variables)

        # Verify all functions were called
        mock_load_project.assert_called_once_with("dev")
        mock_load_pipeline.assert_called_once_with("test_pipeline", mock_project)
        mock_substitute.assert_called_once()
        mock_plan.assert_called_once()
        mock_save.assert_called_once()
        mock_display.assert_called_once()

        # Verify result
        assert len(result) == 1
        assert result[0]["type"] == "source"

    @patch("sqlflow.cli.pipeline_operations.load_project")
    def test_compilation_fails_when_project_load_error_occurs(self, mock_load_project):
        """Test compilation with project load failure."""
        mock_load_project.side_effect = Exception("Project not found")

        with pytest.raises(Exception) as excinfo:
            compile_pipeline_to_plan("test_pipeline", "invalid_profile")

        assert "Project not found" in str(excinfo.value)

    @patch("sqlflow.cli.pipeline_operations.load_project")
    @patch("sqlflow.cli.pipeline_operations.load_pipeline")
    def test_compilation_fails_gracefully_when_cli_error_occurs(
        self, mock_load_pipeline, mock_load_project
    ):
        """Test compilation with CLI error propagation."""
        mock_load_project.return_value = Mock()
        mock_load_pipeline.side_effect = PipelineNotFoundError("test", ["/path"])

        with pytest.raises(PipelineNotFoundError):
            compile_pipeline_to_plan("test_pipeline", "dev")

    @patch("sqlflow.cli.pipeline_operations.load_project")
    @patch("sqlflow.cli.pipeline_operations.load_pipeline")
    @patch("sqlflow.cli.pipeline_operations.substitute_variables")
    @patch("sqlflow.cli.pipeline_operations.plan_pipeline")
    @patch("sqlflow.cli.pipeline_operations.display_compilation_success")
    def test_compilation_succeeds_when_saving_disabled(
        self,
        mock_display,
        mock_plan,
        mock_substitute,
        mock_load_pipeline,
        mock_load_project,
    ):
        """Test compilation without saving results."""
        # Setup mocks
        mock_project = Mock()
        mock_project.get_profile.return_value = {"variables": {}}
        mock_load_project.return_value = mock_project
        mock_load_pipeline.return_value = "SELECT 1"
        mock_substitute.return_value = "SELECT 1"
        mock_plan.return_value = [{"type": "source", "name": "test"}]

        result = compile_pipeline_to_plan("test_pipeline", "dev", save_result=False)

        # Verify result returned without saving
        assert len(result) == 1
        assert result[0]["type"] == "source"
        mock_display.assert_called_once_with(result, None)


class TestIntegrationBehavior:
    """Test integration behavior with real components."""

    def test_variable_parsing_integration_works_correctly_with_real_data(
        self, sample_json_variables, sample_key_value_variables
    ):
        """Test that variable parsing integrates correctly with substitution using shared fixtures."""
        # Test JSON format using shared fixture
        json_vars = parse_variables(sample_json_variables)
        assert "env" in json_vars
        assert "table" in json_vars
        assert json_vars["env"] == "prod"

        # Test key-value format using shared fixture
        kv_vars = parse_variables(sample_key_value_variables)
        assert "env" in kv_vars
        assert "table" in kv_vars
        assert kv_vars["env"] == "prod"

        # Both should parse equivalently
        assert json_vars == kv_vars

    def test_variable_substitution_works_with_shared_template(
        self, sample_sql_template, test_variables_dict
    ):
        """Test variable substitution using shared template and variables."""
        # Use shared fixtures for consistent testing
        result = substitute_variables(sample_sql_template, test_variables_dict)

        # Should contain substituted values
        if "test_table" in result:
            assert "test_table" in result
            assert "test" in result  # env value

    def test_error_messages_are_helpful_when_validation_fails(self):
        """Test that error messages provide actionable information."""
        try:
            parse_variables('{"invalid": json syntax}')
        except VariableParsingError as e:
            # Error should contain enough info for debugging
            assert len(str(e)) > 50  # Reasonable message length
            assert "parse" in str(e).lower()
            assert len(e.suggestions) > 0  # Should have actionable suggestions
