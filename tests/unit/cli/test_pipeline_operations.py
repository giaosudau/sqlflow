"""Tests for pipeline operations module."""

from unittest.mock import Mock, mock_open, patch

import pytest

from sqlflow.cli.errors import (
    PipelineCompilationError,
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

    def test_parse_empty_variables(self):
        """Test parsing empty variables."""
        result = parse_variables(None)
        assert result == {}

        result = parse_variables("")
        assert result == {}

    def test_parse_json_variables(self):
        """Test parsing JSON variables."""
        json_str = '{"env": "prod", "debug": true, "count": 42}'
        result = parse_variables(json_str)

        expected = {"env": "prod", "debug": True, "count": 42}
        assert result == expected

    def test_parse_key_value_variables(self):
        """Test parsing key=value variables."""
        kv_str = "env=prod,debug=true,count=42"
        result = parse_variables(kv_str)

        expected = {"env": "prod", "debug": True, "count": 42}
        assert result == expected

    def test_parse_mixed_key_value_variables(self):
        """Test parsing key=value with string values."""
        kv_str = "env=production,name=test app,active=true"
        result = parse_variables(kv_str)

        expected = {"env": "production", "name": "test app", "active": True}
        assert result == expected

    def test_parse_invalid_json(self):
        """Test parsing invalid JSON."""
        with pytest.raises(VariableParsingError) as excinfo:
            parse_variables('{"invalid": json}')

        assert "Failed to parse variables" in str(excinfo.value)
        assert excinfo.value.variable_string == '{"invalid": json}'

    def test_parse_invalid_key_value(self):
        """Test parsing invalid key=value format."""
        with pytest.raises(VariableParsingError) as excinfo:
            parse_variables("invalid_format")

        assert "Failed to parse variables" in str(excinfo.value)
        assert "Invalid format" in str(excinfo.value)


class TestSubstituteVariables:
    """Test variable substitution."""

    @patch("sqlflow.core.variables.manager.VariableManager")
    @patch("sqlflow.core.variables.manager.VariableConfig")
    def test_substitute_variables_basic(self, mock_config, mock_manager):
        """Test basic variable substitution."""
        # Setup mocks
        mock_manager_instance = Mock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.substitute.return_value = "SELECT * FROM test_table"
        mock_validation_result = Mock()
        mock_validation_result.missing_variables = []
        mock_manager_instance.validate.return_value = mock_validation_result

        variables = {"table": "test_table"}
        result = substitute_variables("SELECT * FROM {{table}}", variables)

        assert result == "SELECT * FROM test_table"
        mock_config.assert_called_once()
        mock_manager_instance.substitute.assert_called_once()

    @patch("sqlflow.core.variables.manager.VariableManager")
    @patch("sqlflow.core.variables.manager.VariableConfig")
    @patch("sqlflow.cli.pipeline_operations.logger")
    def test_substitute_variables_with_missing(
        self, mock_logger, mock_config, mock_manager
    ):
        """Test variable substitution with missing variables."""
        # Setup mocks
        mock_manager_instance = Mock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.substitute.return_value = "SELECT * FROM {{missing}}"
        mock_validation_result = Mock()
        mock_validation_result.missing_variables = ["missing"]
        mock_manager_instance.validate.return_value = mock_validation_result

        substitute_variables("SELECT * FROM {{missing}}", {})

        mock_logger.warning.assert_called_once()
        assert "missing" in mock_logger.warning.call_args[0][0]


class TestSaveCompilationResult:
    """Test saving compilation results."""

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.makedirs")
    def test_save_compilation_result_default_path(self, mock_makedirs, mock_file):
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
    def test_save_compilation_result_custom_path(self, mock_makedirs, mock_file):
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
    def test_compile_pipeline_success(
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
    def test_compile_pipeline_project_load_failure(self, mock_load_project):
        """Test compilation with project load failure."""
        mock_load_project.side_effect = Exception("Project not found")

        with pytest.raises(PipelineCompilationError) as excinfo:
            compile_pipeline_to_plan("test_pipeline")

        assert "test_pipeline" in str(excinfo.value)

    @patch("sqlflow.cli.pipeline_operations.load_project")
    @patch("sqlflow.cli.pipeline_operations.load_pipeline")
    def test_compile_pipeline_with_cli_error(
        self, mock_load_pipeline, mock_load_project
    ):
        """Test compilation with CLI-specific error."""
        mock_load_project.return_value = Mock()
        mock_load_pipeline.side_effect = PipelineNotFoundError("test", ["/path"])

        with pytest.raises(PipelineNotFoundError):
            compile_pipeline_to_plan("test_pipeline")

    @patch("sqlflow.cli.pipeline_operations.load_project")
    @patch("sqlflow.cli.pipeline_operations.load_pipeline")
    @patch("sqlflow.cli.pipeline_operations.substitute_variables")
    @patch("sqlflow.cli.pipeline_operations.plan_pipeline")
    @patch("sqlflow.cli.pipeline_operations.display_compilation_success")
    def test_compile_pipeline_with_no_save(
        self,
        mock_display,
        mock_plan,
        mock_substitute,
        mock_load_pipeline,
        mock_load_project,
    ):
        """Test compilation without saving plan."""
        # Setup mocks
        mock_project = Mock()
        mock_project.get_profile.return_value = {}
        mock_load_project.return_value = mock_project
        mock_load_pipeline.return_value = "SELECT 1"
        mock_substitute.return_value = "SELECT 1"
        mock_plan.return_value = []

        result = compile_pipeline_to_plan("test", save_plan=False)

        # Verify functions were called
        mock_load_project.assert_called_once()
        mock_load_pipeline.assert_called_once()
        mock_substitute.assert_called_once()
        mock_plan.assert_called_once()
        mock_display.assert_called_once()


class TestIntegrationBehavior:
    """Test integration behavior of functions."""

    def test_variable_parsing_integration(self):
        """Test that variable parsing works with actual JSON and key-value formats."""
        # Test JSON format
        json_vars = parse_variables('{"env": "prod", "debug": false}')
        assert json_vars["env"] == "prod"
        assert json_vars["debug"] is False

        # Test key-value format
        kv_vars = parse_variables("env=prod,debug=false")
        assert kv_vars["env"] == "prod"
        assert kv_vars["debug"] is False

        # Results should be equivalent
        assert json_vars == kv_vars

    def test_error_message_quality(self):
        """Test that error messages are helpful."""
        with pytest.raises(VariableParsingError) as excinfo:
            parse_variables('{"incomplete": json')

        error = excinfo.value
        assert error.variable_string == '{"incomplete": json'
        assert (
            "expecting" in error.parse_error.lower()
        )  # JSON error contains "expecting"
        assert len(error.suggestions) > 0
        assert "example" in error.suggestions[0].lower()
