"""Tests for CLI validation helper functions."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import typer

from sqlflow.cli.validation_helpers import (
    format_validation_errors_for_cli,
    print_validation_summary,
    validate_and_exit_on_error,
    validate_pipeline_with_caching,
)
from sqlflow.validation.errors import ValidationError


class TestValidatePipelineWithCaching:
    """Test pipeline validation with caching functionality."""

    def test_valid_pipeline_returns_empty_errors(self):
        """Test that a valid pipeline returns no errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a valid pipeline file - use proper JSON format
            pipeline_file = Path(temp_dir) / "valid.sf"
            pipeline_file.write_text(
                'SOURCE test TYPE CSV PARAMS {"path": "test.csv"};'
            )

            errors = validate_pipeline_with_caching(str(pipeline_file), temp_dir)

            assert errors == []

    def test_invalid_pipeline_returns_validation_errors(self):
        """Test that an invalid pipeline returns validation errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create an invalid pipeline file - missing required 'path'
            pipeline_file = Path(temp_dir) / "invalid.sf"
            pipeline_file.write_text("SOURCE test TYPE CSV PARAMS {};")

            errors = validate_pipeline_with_caching(str(pipeline_file), temp_dir)

            assert len(errors) > 0
            # The actual error is about PARAMS syntax, not specifically "path"
            assert any("params" in error.message.lower() for error in errors)

    def test_nonexistent_file_exits_with_error(self):
        """Test that nonexistent file raises typer.Exit."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(typer.Exit) as exc_info:
                validate_pipeline_with_caching("nonexistent.sf", temp_dir)

            assert exc_info.value.exit_code == 1

    def test_caching_works_correctly(self):
        """Test that validation results are cached correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a pipeline file - use proper JSON format
            pipeline_file = Path(temp_dir) / "test.sf"
            pipeline_file.write_text(
                'SOURCE test TYPE CSV PARAMS {"path": "test.csv"};'
            )

            # First validation - should not use cache
            with patch("sqlflow.cli.validation_helpers.logger") as mock_logger:
                errors1 = validate_pipeline_with_caching(str(pipeline_file), temp_dir)

                # Should not log cache hit on first run
                debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
                assert not any("Using cached" in call for call in debug_calls)

            # Second validation - should use cache
            with patch("sqlflow.cli.validation_helpers.logger") as mock_logger:
                errors2 = validate_pipeline_with_caching(str(pipeline_file), temp_dir)

                # Should log cache hit on second run
                debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
                assert any("Using cached" in call for call in debug_calls)

            # Results should be identical
            assert errors1 == errors2

    def test_parser_error_converted_to_validation_error(self):
        """Test that parser errors are converted to validation errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a file with syntax error - incomplete SOURCE statement
            pipeline_file = Path(temp_dir) / "syntax_error.sf"
            pipeline_file.write_text("SOURCE")

            errors = validate_pipeline_with_caching(str(pipeline_file), temp_dir)

            assert len(errors) > 0
            assert any(error.error_type == "Parser Error" for error in errors)

    def test_unreadable_file_exits_with_error(self):
        """Test that unreadable file raises typer.Exit."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create file but make it unreadable
            pipeline_file = Path(temp_dir) / "unreadable.sf"
            pipeline_file.write_text("test")
            pipeline_file.chmod(0o000)  # Remove all permissions

            try:
                with pytest.raises(typer.Exit) as exc_info:
                    validate_pipeline_with_caching(str(pipeline_file), temp_dir)

                assert exc_info.value.exit_code == 1
            finally:
                # Restore permissions for cleanup
                pipeline_file.chmod(0o644)


class TestFormatValidationErrorsForCLI:
    """Test validation error formatting for CLI output."""

    def test_empty_errors_returns_success_message(self):
        """Test that empty errors list returns success message."""
        result = format_validation_errors_for_cli([])
        assert "✅ Pipeline validation passed!" in result

    def test_single_error_formatted_correctly(self):
        """Test that single error is formatted correctly."""
        error = ValidationError(
            message="Test error",
            line=5,
            column=10,
            error_type="Test Error",
            suggestions=["Fix this", "Or try that"],
        )

        result = format_validation_errors_for_cli([error])

        assert "❌ Pipeline validation failed with 1 error(s)" in result
        assert "Line 5, Column 10: Test error" in result
        assert "💡 Fix this" in result
        assert "💡 Or try that" in result

    def test_multiple_errors_grouped_by_type(self):
        """Test that multiple errors are grouped by type."""
        errors = [
            ValidationError("Error 1", line=1, error_type="Type A"),
            ValidationError("Error 2", line=2, error_type="Type B"),
            ValidationError("Error 3", line=3, error_type="Type A"),
        ]

        result = format_validation_errors_for_cli(errors)

        assert "❌ Pipeline validation failed with 3 error(s)" in result
        assert "📋 Type As:" in result
        assert "📋 Type Bs:" in result

    def test_show_details_false_hides_suggestions(self):
        """Test that show_details=False hides suggestions."""
        error = ValidationError(
            message="Test error", line=1, suggestions=["This should not appear"]
        )

        result = format_validation_errors_for_cli([error], show_details=False)

        assert "This should not appear" not in result
        assert "Run with --verbose for detailed suggestions" in result

    def test_help_url_included_when_available(self):
        """Test that help URL is included when available."""
        error = ValidationError(
            message="Test error", line=1, help_url="https://example.com/help"
        )

        result = format_validation_errors_for_cli([error])

        assert "📖 Help: https://example.com/help" in result

    def test_error_without_column_formatted_correctly(self):
        """Test that error without column is formatted correctly."""
        error = ValidationError("Test error", line=5, column=0)

        result = format_validation_errors_for_cli([error])

        assert "Line 5: Test error" in result
        assert (
            "Column" not in result.split("\n")[2]
        )  # Check the error line specifically


class TestPrintValidationSummary:
    """Test validation summary printing."""

    @patch("typer.echo")
    def test_success_summary_printed_when_no_errors(self, mock_echo):
        """Test that success message is printed when no errors."""
        print_validation_summary([], "test_pipeline")

        mock_echo.assert_called_once_with(
            "✅ Pipeline 'test_pipeline' validation passed!"
        )

    @patch("typer.echo")
    def test_quiet_mode_suppresses_success_message(self, mock_echo):
        """Test that quiet mode suppresses success message."""
        print_validation_summary([], "test_pipeline", quiet=True)

        mock_echo.assert_not_called()

    @patch("typer.echo")
    def test_errors_printed_to_stderr(self, mock_echo):
        """Test that errors are printed to stderr."""
        errors = [ValidationError("Test error", line=1)]

        print_validation_summary(errors, "test_pipeline")

        # Check that typer.echo was called with err=True
        mock_echo.assert_called_once()
        call_args = mock_echo.call_args
        assert call_args[1]["err"] is True

    @patch("typer.echo")
    def test_quiet_mode_hides_detailed_errors(self, mock_echo):
        """Test that quiet mode hides detailed error information."""
        errors = [ValidationError("Test error", line=1, suggestions=["Fix this"])]

        print_validation_summary(errors, "test_pipeline", quiet=True)

        # Get the formatted message that was passed to echo
        formatted_message = mock_echo.call_args[0][0]
        assert "Fix this" not in formatted_message


class TestValidateAndExitOnError:
    """Test validation with exit on error functionality."""

    def test_valid_pipeline_does_not_exit(self):
        """Test that valid pipeline does not cause exit."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a valid pipeline file - use proper JSON format
            pipeline_file = Path(temp_dir) / "valid.sf"
            pipeline_file.write_text(
                'SOURCE test TYPE CSV PARAMS {"path": "test.csv"};'
            )

            # Should not raise typer.Exit
            validate_and_exit_on_error(str(pipeline_file), "test_pipeline")

    def test_invalid_pipeline_exits_with_error(self):
        """Test that invalid pipeline causes exit with error code."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create an invalid pipeline file - missing required 'path'
            pipeline_file = Path(temp_dir) / "invalid.sf"
            pipeline_file.write_text("SOURCE test TYPE CSV PARAMS {};")

            with pytest.raises(typer.Exit) as exc_info:
                validate_and_exit_on_error(str(pipeline_file), "test_pipeline")

            assert exc_info.value.exit_code == 1

    @patch("sqlflow.cli.validation_helpers.print_validation_summary")
    def test_error_summary_printed_before_exit(self, mock_print_summary):
        """Test that error summary is printed before exit."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create an invalid pipeline file - use proper JSON format
            pipeline_file = Path(temp_dir) / "invalid.sf"
            pipeline_file.write_text("SOURCE test TYPE CSV PARAMS {};")

            with pytest.raises(typer.Exit):
                validate_and_exit_on_error(str(pipeline_file), "test_pipeline")

            # Verify that print_validation_summary was called
            mock_print_summary.assert_called_once()
            call_args = mock_print_summary.call_args[0]
            errors = call_args[0]
            pipeline_name = call_args[1]

            assert len(errors) > 0
            assert pipeline_name == "test_pipeline"
