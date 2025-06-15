"""Tests for CLI display functions.

This module tests the Rich display functions following the technical design's
behavioral testing approach - focusing on outcomes users care about.
"""

import io
from unittest.mock import patch

from rich.console import Console

from sqlflow.cli.display import (
    display_compilation_error,
    display_compilation_success,
    display_error,
    display_error_with_panel,
    display_execution_error,
    display_execution_progress,
    display_execution_success,
    display_info,
    display_json_error,
    display_pipeline_list,
    display_pipeline_not_found_error,
    display_pipeline_validation_error,
    display_profile_not_found_error,
    display_success,
    display_variable_parsing_error,
    display_warning,
)
from sqlflow.cli.errors import (
    PipelineCompilationError,
    PipelineExecutionError,
    PipelineNotFoundError,
    PipelineValidationError,
    ProfileNotFoundError,
    VariableParsingError,
)


class TestDisplayFunctions:
    """Test Rich display functions for behavioral outcomes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.console_output = io.StringIO()
        self.console = Console(file=self.console_output, width=80, legacy_windows=False)

    def get_output(self) -> str:
        """Get console output as string."""
        return self.console_output.getvalue()

    @patch("sqlflow.cli.display.console")
    def test_display_compilation_success_shows_green_checkmark_and_table(
        self, mock_console
    ):
        """Test that compilation success displays green checkmark and formatted table."""
        # Given: Successful compilation parameters
        pipeline_name = "test_pipeline"
        profile = "production"
        operation_count = 5
        output_path = "/path/to/output"

        # When: Displaying compilation success
        display_compilation_success(
            pipeline_name, profile, operation_count, output_path
        )

        # Then: Should show success indicator and table with all information
        call_args = [call[0][0] for call in mock_console.print.call_args_list]

        # Check success message with green checkmark
        assert any(
            "✅" in str(arg) and "Pipeline compiled successfully" in str(arg)
            for arg in call_args
        )

        # Check that console.print was called multiple times (for table)
        assert mock_console.print.call_count >= 2

    @patch("sqlflow.cli.display.console")
    def test_display_pipeline_not_found_error_shows_suggestions(self, mock_console):
        """Test that pipeline not found error shows suggestions when available."""
        # Given: Pipeline not found error with suggestions
        error = PipelineNotFoundError(
            pipeline_name="missing_pipeline",
            search_paths=["/path/1", "/path/2"],
            available_pipelines=["pipeline_a", "pipeline_b", "pipeline_c"],
        )

        # When: Displaying the error
        display_pipeline_not_found_error(error)

        # Then: Should show error message and suggestions
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator and pipeline name
        assert "❌" in output
        assert "missing_pipeline" in output
        assert "not found" in output

        # Check search paths are shown
        assert "/path/1" in output or "/path/2" in output

        # Check suggestions are shown
        assert "💡" in output
        assert "pipeline_a" in output

    @patch("sqlflow.cli.display.console")
    def test_display_pipeline_not_found_error_without_suggestions(self, mock_console):
        """Test that pipeline not found error works without suggestions."""
        # Given: Pipeline not found error without suggestions
        error = PipelineNotFoundError(
            pipeline_name="missing_pipeline",
            search_paths=["/path/1"],
            available_pipelines=[],
        )

        # When: Displaying the error
        display_pipeline_not_found_error(error)

        # Then: Should show error message without suggestions section
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error is shown
        assert "❌" in output
        assert "missing_pipeline" in output

        # Should not have suggestions section
        suggestion_calls = [arg for arg in call_args if "💡" in arg]
        assert len(suggestion_calls) == 0

    @patch("sqlflow.cli.display.console")
    def test_display_variable_parsing_error_shows_input_and_suggestions(
        self, mock_console
    ):
        """Test that variable parsing error shows input and helpful suggestions."""
        # Given: Variable parsing error
        error = VariableParsingError(
            variable_string="invalid-json", parse_error="Invalid JSON syntax"
        )

        # When: Displaying the error
        display_variable_parsing_error(error)

        # Then: Should show error, input, and suggestions
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator
        assert "❌" in output
        assert "Invalid variables parameter" in output

        # Check input is shown
        assert "invalid-json" in output

        # Check suggestions are shown
        assert "💡" in output

    @patch("sqlflow.cli.display.console")
    def test_display_json_error_shows_example(self, mock_console):
        """Test that JSON error displays helpful example."""
        # When: Displaying JSON error
        display_json_error()

        # Then: Should show error and example
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator and message
        assert "❌" in output
        assert "Invalid JSON" in output

        # Check example is provided
        assert "💡" in output
        assert "Example:" in output
        assert '"env"' in output
        assert '"prod"' in output

    @patch("sqlflow.cli.display.console")
    def test_display_pipeline_validation_error_limits_shown_errors(self, mock_console):
        """Test that validation error limits displayed errors to first 5."""
        # Given: Validation error with many issues
        many_errors = [f"Error {i}" for i in range(10)]
        error = PipelineValidationError(
            pipeline_name="test_pipeline", errors=many_errors
        )

        # When: Displaying the error
        display_pipeline_validation_error(error)

        # Then: Should show first 5 errors and indicate more exist
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator
        assert "❌" in output
        assert "test_pipeline" in output

        # Check first few errors are shown
        assert "Error 0" in output
        assert "Error 4" in output

        # Check indication of more errors
        assert "5 more issues" in output

    @patch("sqlflow.cli.display.console")
    def test_display_profile_not_found_error_shows_available_profiles(
        self, mock_console
    ):
        """Test that profile not found error shows available profiles."""
        # Given: Profile not found error with available profiles
        error = ProfileNotFoundError(
            profile_name="missing_profile",
            available_profiles=["dev", "staging", "prod"],
        )

        # When: Displaying the error
        display_profile_not_found_error(error)

        # Then: Should show error and available profiles
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator
        assert "❌" in output
        assert "missing_profile" in output

        # Check available profiles are shown
        assert "💡" in output
        assert "dev" in output
        assert "staging" in output

    @patch("sqlflow.cli.display.console")
    def test_display_compilation_error_shows_context(self, mock_console):
        """Test that compilation error shows context when available."""
        # Given: Compilation error with context
        error = PipelineCompilationError(
            pipeline_name="test_pipeline",
            error_message="SQL syntax error",
            context={"line": 42, "column": 15},
        )

        # When: Displaying the error
        display_compilation_error(error)

        # Then: Should show error message and context
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator
        assert "❌" in output
        assert "test_pipeline" in output
        assert "SQL syntax error" in output

        # Check context is shown
        assert "📊" in output
        assert "Context:" in output
        assert "42" in output
        assert "15" in output

    @patch("sqlflow.cli.display.console")
    def test_display_execution_error_shows_failed_step(self, mock_console):
        """Test that execution error shows failed step when available."""
        # Given: Execution error with failed step
        error = PipelineExecutionError(
            pipeline_name="test_pipeline",
            error_message="Connection timeout",
            failed_step="load_data",
        )

        # When: Displaying the error
        display_execution_error(error)

        # Then: Should show error message and failed step
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check error indicator
        assert "❌" in output
        assert "test_pipeline" in output
        assert "Connection timeout" in output

        # Check failed step is shown
        assert "📍" in output
        assert "load_data" in output

    @patch("sqlflow.cli.display.console")
    def test_display_pipeline_list_shows_empty_message_when_no_pipelines(
        self, mock_console
    ):
        """Test that pipeline list shows friendly message when empty."""
        # Given: Empty pipeline list
        pipelines = []
        profile = "test_profile"

        # When: Displaying pipeline list
        display_pipeline_list(pipelines, profile)

        # Then: Should show friendly empty message
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check empty message
        assert "📭" in output
        assert "No pipelines found" in output
        assert "test_profile" in output

    @patch("sqlflow.cli.display.console")
    def test_display_pipeline_list_shows_table_with_pipelines(self, mock_console):
        """Test that pipeline list shows formatted table with pipeline information."""
        # Given: List of pipelines
        pipelines = [
            {"name": "pipeline_1", "description": "First pipeline", "valid": True},
            {"name": "pipeline_2", "description": "Second pipeline", "valid": False},
        ]
        profile = "test_profile"

        # When: Displaying pipeline list
        display_pipeline_list(pipelines, profile)

        # Then: Should show header and table content
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check header
        assert "📋" in output
        assert "test_profile" in output

        # Check table is created (multiple print calls)
        assert mock_console.print.call_count >= 2

    @patch("sqlflow.cli.display.console")
    def test_display_execution_progress_shows_progress_indicator(self, mock_console):
        """Test that execution progress shows step progress."""
        # When: Displaying execution progress
        display_execution_progress("test_pipeline", 3, 10, "processing_data")

        # Then: Should show progress indicator
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check progress indicator
        assert "🔄" in output
        assert "[3/10]" in output
        assert "test_pipeline" in output
        assert "processing_data" in output

    @patch("sqlflow.cli.display.console")
    def test_display_execution_success_shows_summary_table(self, mock_console):
        """Test that execution success shows formatted summary."""
        # When: Displaying execution success
        display_execution_success("test_pipeline", "prod", 12.5, 8)

        # Then: Should show success indicator and summary
        call_args = [str(call[0][0]) for call in mock_console.print.call_args_list]
        output = " ".join(call_args)

        # Check success indicator
        assert "✅" in output
        assert "executed successfully" in output

        # Check table is created
        assert mock_console.print.call_count >= 2

    @patch("sqlflow.cli.display.console")
    def test_display_warning_shows_warning_icon(self, mock_console):
        """Test that warning message shows warning icon."""
        # When: Displaying warning
        display_warning("This is a warning")

        # Then: Should show warning icon and message
        mock_console.print.assert_called_once()
        call_arg = str(mock_console.print.call_args[0][0])
        assert "⚠️" in call_arg
        assert "This is a warning" in call_arg

    @patch("sqlflow.cli.display.console")
    def test_display_info_shows_info_icon(self, mock_console):
        """Test that info message shows info icon."""
        # When: Displaying info
        display_info("This is info")

        # Then: Should show info icon and message
        mock_console.print.assert_called_once()
        call_arg = str(mock_console.print.call_args[0][0])
        assert "ℹ️" in call_arg
        assert "This is info" in call_arg

    @patch("sqlflow.cli.display.console")
    def test_display_success_shows_success_icon(self, mock_console):
        """Test that success message shows success icon."""
        # When: Displaying success
        display_success("This is success")

        # Then: Should show success icon and message
        mock_console.print.assert_called_once()
        call_arg = str(mock_console.print.call_args[0][0])
        assert "✅" in call_arg
        assert "This is success" in call_arg

    @patch("sqlflow.cli.display.console")
    def test_display_error_shows_error_icon(self, mock_console):
        """Test that error message shows error icon."""
        # When: Displaying error
        display_error("This is an error")

        # Then: Should show error icon and message
        mock_console.print.assert_called_once()
        call_arg = str(mock_console.print.call_args[0][0])
        assert "❌" in call_arg
        assert "This is an error" in call_arg

    @patch("sqlflow.cli.display.console")
    def test_display_error_with_panel_shows_structured_error(self, mock_console):
        """Test that panel error shows structured format with suggestions."""
        # When: Displaying error with panel and suggestions
        suggestions = ["Try this", "Or try that"]
        display_error_with_panel("Test Error", "Something went wrong", suggestions)

        # Then: Should show panel format
        mock_console.print.assert_called_once()
        # Panel object is passed, so we can't easily check content,
        # but we can verify it was called
        assert mock_console.print.call_count == 1

    @patch("sqlflow.cli.display.console")
    def test_display_error_with_panel_without_suggestions(self, mock_console):
        """Test that panel error works without suggestions."""
        # When: Displaying error with panel but no suggestions
        display_error_with_panel("Test Error", "Something went wrong")

        # Then: Should show panel format without suggestions
        mock_console.print.assert_called_once()
        assert mock_console.print.call_count == 1


class TestDisplayFunctionTypeHints:
    """Test that all display functions have proper type hints."""

    def test_all_display_functions_have_type_hints(self):
        """Test that all display functions have complete type annotations."""
        import inspect

        from sqlflow.cli import display

        # Get all display functions
        display_functions = [
            getattr(display, name)
            for name in dir(display)
            if name.startswith("display_") and callable(getattr(display, name))
        ]

        for func in display_functions:
            sig = inspect.signature(func)

            # Check return type annotation
            assert (
                sig.return_annotation != inspect.Signature.empty
            ), f"{func.__name__} missing return type"

            # Check all parameters have type annotations
            for param_name, param in sig.parameters.items():
                assert (
                    param.annotation != inspect.Parameter.empty
                ), f"{func.__name__} parameter '{param_name}' missing type annotation"

    def test_display_functions_return_none(self):
        """Test that all display functions return None (side effect functions)."""
        import inspect

        from sqlflow.cli import display

        # Get all display functions
        display_functions = [
            getattr(display, name)
            for name in dir(display)
            if name.startswith("display_") and callable(getattr(display, name))
        ]

        for func in display_functions:
            sig = inspect.signature(func)
            # All display functions should return None
            assert (
                sig.return_annotation is type(None) or sig.return_annotation is None
            ), f"{func.__name__} should return None"
