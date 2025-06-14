"""Tests for unified error handling strategy.

Comprehensive tests for the error handling components with minimal mocking,
focusing on real functionality and edge cases.
"""

from unittest.mock import patch

import pytest

from sqlflow.core.variables.error_handling import (
    ErrorHandler,
    ErrorReport,
    ErrorStrategy,
    VariableError,
    VariableSubstitutionError,
    create_error_handler,
    handle_variable_error,
)


class TestErrorStrategy:
    """Test ErrorStrategy enum."""

    def test_strategy_values(self):
        """Test that all expected strategies exist with correct values."""
        assert ErrorStrategy.FAIL_FAST.value == "fail_fast"
        assert ErrorStrategy.WARN_CONTINUE.value == "warn_continue"
        assert ErrorStrategy.IGNORE.value == "ignore"
        assert ErrorStrategy.COLLECT_REPORT.value == "collect_report"

    def test_strategy_membership(self):
        """Test strategy membership checks."""
        assert ErrorStrategy.FAIL_FAST in ErrorStrategy
        assert ErrorStrategy.WARN_CONTINUE in ErrorStrategy
        assert ErrorStrategy.IGNORE in ErrorStrategy
        assert ErrorStrategy.COLLECT_REPORT in ErrorStrategy


class TestVariableError:
    """Test VariableError dataclass."""

    def test_basic_error_creation(self):
        """Test creating basic error with minimal information."""
        error = VariableError(
            variable_name="test_var",
            error_type="not found",
            error_message="Variable not defined",
            context="sql",
            original_text="${test_var}",
        )

        assert error.variable_name == "test_var"
        assert error.error_type == "not found"
        assert error.error_message == "Variable not defined"
        assert error.context == "sql"
        assert error.original_text == "${test_var}"
        assert error.position is None
        assert error.line_number is None
        assert error.column_number is None
        assert error.suggested_fix is None
        assert error.severity == "error"

    def test_complete_error_creation(self):
        """Test creating error with all metadata."""
        error = VariableError(
            variable_name="missing_var",
            error_type="not found",
            error_message="Variable 'missing_var' not found in context",
            context="sql",
            original_text="${missing_var|default}",
            position=10,
            line_number=2,
            column_number=5,
            suggested_fix="Define variable 'missing_var' or check spelling",
            severity="warning",
        )

        assert error.variable_name == "missing_var"
        assert error.position == 10
        assert error.line_number == 2
        assert error.column_number == 5
        assert error.suggested_fix == "Define variable 'missing_var' or check spelling"
        assert error.severity == "warning"

    def test_error_string_representation(self):
        """Test error string formatting."""
        # Basic error
        error = VariableError(
            variable_name="test",
            error_type="not found",
            error_message="Missing variable",
            context="sql",
            original_text="${test}",
        )

        expected = "Variable 'test' not found in sql context : Missing variable"
        assert str(error) == expected

        # Error with position
        error.position = 10
        expected = (
            "Variable 'test' not found at position 10 in sql context : Missing variable"
        )
        assert str(error) == expected

        # Error with line/column
        error.line_number = 2
        error.column_number = 5
        expected = "Variable 'test' not found at position 10 (line 2, column 5) in sql context : Missing variable"
        assert str(error) == expected

    def test_error_without_message(self):
        """Test error formatting without message."""
        error = VariableError(
            variable_name="test",
            error_type="invalid format",
            error_message="",
            context="text",
            original_text="${test|}",
        )

        expected = "Variable 'test' invalid format in text context"
        assert str(error) == expected


class TestErrorReport:
    """Test ErrorReport dataclass."""

    def test_empty_report(self):
        """Test empty error report."""
        report = ErrorReport()

        assert not report.has_errors
        assert not report.has_warnings
        assert report.error_count == 0
        assert report.warning_count == 0
        assert report.success_count == 0
        assert report.total_variables == 0
        assert report.success_rate == 100.0
        assert len(report.get_missing_variables()) == 0

    def test_report_with_errors(self):
        """Test error report with errors and warnings."""
        report = ErrorReport(success_count=3, total_variables=5, context="sql")

        error1 = VariableError("var1", "not found", "Missing", "sql", "${var1}")
        error2 = VariableError(
            "var2", "invalid", "Bad format", "sql", "${var2|}", severity="warning"
        )

        report.add_error(error1)
        report.add_error(error2)

        assert report.has_errors
        assert report.has_warnings
        assert report.error_count == 1
        assert report.warning_count == 1
        assert report.success_rate == 60.0  # 3/5 * 100
        assert report.get_missing_variables() == {"var1"}

    def test_success_rate_calculation(self):
        """Test success rate calculation edge cases."""
        # No variables
        report = ErrorReport()
        assert report.success_rate == 100.0

        # All successful
        report = ErrorReport(success_count=5, total_variables=5)
        assert report.success_rate == 100.0

        # All failed
        report = ErrorReport(success_count=0, total_variables=5)
        assert report.success_rate == 0.0

        # Partial success
        report = ErrorReport(success_count=3, total_variables=4)
        assert report.success_rate == 75.0

    def test_get_missing_variables(self):
        """Test getting set of missing variables."""
        report = ErrorReport()

        # Add various error types
        report.add_error(VariableError("var1", "not found", "", "sql", "${var1}"))
        report.add_error(VariableError("var2", "type error", "", "sql", "${var2}"))
        report.add_error(
            VariableError("var3", "not found", "", "sql", "${var3}", severity="warning")
        )

        missing = report.get_missing_variables()
        assert missing == {"var1", "var3"}  # Only "not found" errors

    def test_error_summary(self):
        """Test error summary generation."""
        # No errors
        report = ErrorReport(success_count=5, total_variables=5)
        assert report.get_error_summary() == "All 5 variables processed successfully"

        # Errors only
        report = ErrorReport(success_count=2, total_variables=5)
        report.add_error(VariableError("var1", "not found", "", "sql", "${var1}"))
        report.add_error(VariableError("var2", "invalid", "", "sql", "${var2}"))

        summary = report.get_error_summary()
        assert "2 error(s)" in summary
        assert "40.0% success rate" in summary

        # Warnings only
        report = ErrorReport(success_count=4, total_variables=5)
        report.warnings = [
            VariableError("var1", "not found", "", "sql", "${var1}", severity="warning")
        ]

        summary = report.get_error_summary()
        assert "1 warning(s)" in summary
        assert "80.0% success rate" in summary

    def test_detailed_report_formatting(self):
        """Test detailed report formatting."""
        report = ErrorReport(
            success_count=2, total_variables=4, context="sql processing"
        )

        error = VariableError(
            "var1",
            "not found",
            "Variable not defined",
            "sql",
            "${var1}",
            suggested_fix="Check variable spelling",
        )
        warning = VariableError(
            "var2",
            "type mismatch",
            "Converting to string",
            "sql",
            "${var2}",
            severity="warning",
        )

        report.add_error(error)
        report.add_error(warning)

        detailed = report.format_detailed_report()

        # Check structure
        assert "Variable Substitution Error Report" in detailed
        assert "Context: sql processing" in detailed
        assert "Total Variables: 4" in detailed
        assert "Success Rate: 50.0%" in detailed
        assert "ERRORS:" in detailed
        assert "WARNINGS:" in detailed
        assert "Check variable spelling" in detailed


class TestVariableSubstitutionError:
    """Test VariableSubstitutionError exception."""

    def test_basic_exception(self):
        """Test basic exception creation."""
        exc = VariableSubstitutionError("Test error message")

        assert str(exc) == "Test error message"
        assert exc.error_report is not None
        assert isinstance(exc.error_report, ErrorReport)

    def test_exception_with_report(self):
        """Test exception with error report."""
        report = ErrorReport(success_count=1, total_variables=3)
        report.add_error(VariableError("var1", "not found", "", "sql", "${var1}"))

        exc = VariableSubstitutionError("Multiple errors occurred", report)

        assert str(exc) == "Multiple errors occurred"
        assert exc.error_report is report
        assert exc.error_report.error_count == 1


class TestErrorHandler:
    """Test ErrorHandler class."""

    def test_default_initialization(self):
        """Test default error handler initialization."""
        handler = ErrorHandler()

        assert handler.strategy == ErrorStrategy.WARN_CONTINUE
        report = handler.get_error_report()
        assert not report.has_errors
        assert not report.has_warnings

    def test_initialization_with_strategy(self):
        """Test initialization with specific strategy."""
        handler = ErrorHandler(ErrorStrategy.FAIL_FAST)
        assert handler.strategy == ErrorStrategy.FAIL_FAST

    def test_handle_missing_variable_fail_fast(self):
        """Test handling missing variable with FAIL_FAST strategy."""
        handler = ErrorHandler(ErrorStrategy.FAIL_FAST)

        with pytest.raises(VariableSubstitutionError) as exc_info:
            handler.handle_missing_variable("test_var", "sql")

        assert "Variable 'test_var' not found" in str(exc_info.value)

    @patch("sqlflow.core.variables.error_handling.logger")
    def test_handle_missing_variable_warn_continue(self, mock_logger):
        """Test handling missing variable with WARN_CONTINUE strategy."""
        handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)

        result = handler.handle_missing_variable("test_var", "sql")

        assert result == "NULL"  # SQL fallback
        mock_logger.warning.assert_called_once()

        report = handler.get_error_report()
        assert report.has_errors  # Missing variables are always errors
        assert report.error_count == 1

    def test_handle_missing_variable_ignore(self):
        """Test handling missing variable with IGNORE strategy."""
        handler = ErrorHandler(ErrorStrategy.IGNORE)

        result = handler.handle_missing_variable("test_var", "text")

        assert result == "${test_var}"  # Text fallback

        report = handler.get_error_report()
        assert report.has_errors  # Errors are still recorded for statistics
        assert report.error_count == 1

    def test_handle_missing_variable_collect_report(self):
        """Test handling missing variable with COLLECT_REPORT strategy."""
        handler = ErrorHandler(ErrorStrategy.COLLECT_REPORT)

        result1 = handler.handle_missing_variable("var1", "sql")
        result2 = handler.handle_missing_variable("var2", "text")

        assert result1 == "NULL"
        assert result2 == "${var2}"

        report = handler.get_error_report()
        assert report.error_count == 2
        assert not report.has_warnings

    def test_handle_invalid_format(self):
        """Test handling invalid format errors."""
        handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)

        result = handler.handle_invalid_format(
            "bad_var", "sql", "Invalid syntax", original_text="${bad_var|}"
        )

        assert result == "NULL"  # SQL fallback

        report = handler.get_error_report()
        assert report.has_errors  # Invalid format errors are also errors
        assert "Invalid syntax" in str(report.errors[0])

    def test_handle_type_error(self):
        """Test handling type conversion errors."""
        handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)

        result = handler.handle_type_error(
            "numeric_var", complex(1, 2), "sql", "Cannot convert complex number"
        )

        assert result == "(1+2j)"  # Returns string representation of value

        report = handler.get_error_report()
        assert report.has_warnings  # Type errors are warnings

    def test_record_success(self):
        """Test recording successful variable substitutions."""
        handler = ErrorHandler()

        handler.record_success("var1")
        handler.record_success("var2")

        report = handler.get_error_report()
        assert report.success_count == 2

    def test_set_total_variables(self):
        """Test setting total variable count."""
        handler = ErrorHandler()

        handler.set_total_variables(10)

        report = handler.get_error_report()
        assert report.total_variables == 10

    def test_set_context(self):
        """Test setting operation context."""
        handler = ErrorHandler()

        handler.set_context("SQL query processing")

        report = handler.get_error_report()
        assert report.context == "SQL query processing"

    @patch("sqlflow.core.variables.error_handling.logger")
    def test_finalize_with_errors(self, mock_logger):
        """Test finalizing handler with errors (COLLECT_REPORT)."""
        handler = ErrorHandler(ErrorStrategy.COLLECT_REPORT)
        handler.set_total_variables(3)
        handler.record_success("var1")
        handler.handle_missing_variable("var2", "sql")
        handler.handle_missing_variable("var3", "sql")

        # This should raise an exception for COLLECT_REPORT strategy
        with pytest.raises(VariableSubstitutionError) as exc_info:
            handler.finalize()

        assert "2 error(s)" in str(exc_info.value)

        # Should log detailed report
        mock_logger.error.assert_called_once()

    @patch("sqlflow.core.variables.error_handling.logger")
    def test_finalize_with_warnings_only(self, mock_logger):
        """Test finalizing handler with warnings only."""
        handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)
        handler.set_total_variables(2)
        handler.record_success("var1")
        handler.handle_missing_variable("var2", "sql")  # Creates warning

        handler.finalize()

        # Should log info summary for warnings only
        mock_logger.info.assert_called_once()

    def test_context_specific_fallbacks(self):
        """Test that fallbacks are context-appropriate."""
        handler = ErrorHandler(ErrorStrategy.IGNORE)

        # Test different contexts
        sql_result = handler.handle_missing_variable("var", "sql")
        text_result = handler.handle_missing_variable("var", "text")
        ast_result = handler.handle_missing_variable("var", "ast")
        json_result = handler.handle_missing_variable("var", "json")

        assert sql_result == "NULL"
        assert text_result == "${var}"
        assert ast_result == "None"
        assert json_result == "null"

    def test_position_information_handling(self):
        """Test handling of position information in errors."""
        handler = ErrorHandler(ErrorStrategy.COLLECT_REPORT)

        handler.handle_missing_variable(
            "test_var",
            "sql",
            position=15,
            line_number=3,
            column_number=8,
            original_text="${test_var}",
        )

        report = handler.get_error_report()
        error = report.errors[0]

        assert error.position == 15
        assert error.line_number == 3
        assert error.column_number == 8
        assert "line 3, column 8" in str(error)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_create_error_handler(self):
        """Test create_error_handler function."""
        # Default strategy
        handler = create_error_handler()
        assert handler.strategy == ErrorStrategy.WARN_CONTINUE

        # Specific strategy
        handler = create_error_handler(ErrorStrategy.FAIL_FAST)
        assert handler.strategy == ErrorStrategy.FAIL_FAST

    def test_handle_variable_error_function(self):
        """Test handle_variable_error convenience function."""
        # Test with default strategy (WARN_CONTINUE)
        result = handle_variable_error(
            "not found", "test_var", "sql", "Variable missing"
        )

        assert result == "NULL"  # SQL fallback

        # Test with FAIL_FAST strategy
        with pytest.raises(VariableSubstitutionError):
            handle_variable_error(
                "not found",
                "test_var",
                "sql",
                "Variable missing",
                strategy=ErrorStrategy.FAIL_FAST,
            )

    def test_handle_variable_error_with_metadata(self):
        """Test handle_variable_error with additional metadata."""
        result = handle_variable_error(
            "not found",
            "test_var",
            "sql",
            "Variable missing",
            strategy=ErrorStrategy.IGNORE,
            position=10,
            line_number=2,
            original_text="${test_var}",
        )

        assert result == "NULL"


class TestIntegrationScenarios:
    """Test complex integration scenarios."""

    def test_multiple_error_types(self):
        """Test handling multiple different error types."""
        handler = ErrorHandler(ErrorStrategy.COLLECT_REPORT)
        handler.set_total_variables(5)
        handler.set_context("Complex SQL template")

        # Various error types
        handler.record_success("good_var")
        handler.handle_missing_variable("missing_var", "sql")
        handler.handle_invalid_format("bad_format", "sql", "Empty default value")
        handler.handle_type_error(
            "complex_var", {"nested": {"data": True}}, "sql", "Complex object"
        )
        handler.record_success("another_good_var")

        report = handler.get_error_report()

        assert report.success_count == 2
        assert report.total_variables == 5
        assert (
            report.error_count == 2
        )  # missing + invalid format (type error is warning)
        assert report.warning_count == 1  # type error
        assert report.success_rate == 40.0

        # Check that different error types are captured
        error_types = {error.error_type for error in report.errors}
        assert "not found" in error_types
        assert "invalid format" in error_types

        warning_types = {warning.error_type for warning in report.warnings}
        assert "type error" in warning_types

    def test_error_strategy_comparison(self):
        """Test that different strategies produce different behaviors."""
        error_scenarios = [
            ("missing_var1", "sql", "Variable not found"),
            ("missing_var2", "text", "Variable not found"),
        ]

        # Test FAIL_FAST - should raise on first error
        fail_handler = ErrorHandler(ErrorStrategy.FAIL_FAST)
        with pytest.raises(VariableSubstitutionError):
            fail_handler.handle_missing_variable(*error_scenarios[0])

        # Test WARN_CONTINUE - should handle both and warn
        warn_handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)
        result1 = warn_handler.handle_missing_variable(*error_scenarios[0])
        result2 = warn_handler.handle_missing_variable(*error_scenarios[1])

        assert result1 == "NULL"
        assert result2 == "${missing_var2}"
        assert (
            warn_handler.get_error_report().error_count == 2
        )  # Missing variables are errors

        # Test IGNORE - should handle silently
        ignore_handler = ErrorHandler(ErrorStrategy.IGNORE)
        result1 = ignore_handler.handle_missing_variable(*error_scenarios[0])
        result2 = ignore_handler.handle_missing_variable(*error_scenarios[1])

        assert result1 == "NULL"
        assert result2 == "${missing_var2}"
        report = ignore_handler.get_error_report()
        assert report.has_errors  # Errors are still recorded for statistics

        # Test COLLECT_REPORT - should collect errors for later analysis
        collect_handler = ErrorHandler(ErrorStrategy.COLLECT_REPORT)
        result1 = collect_handler.handle_missing_variable(*error_scenarios[0])
        result2 = collect_handler.handle_missing_variable(*error_scenarios[1])

        assert result1 == "NULL"
        assert result2 == "${missing_var2}"
        assert collect_handler.get_error_report().error_count == 2

    @patch("sqlflow.core.variables.error_handling.logger")
    def test_comprehensive_logging_behavior(self, mock_logger):
        """Test that logging works correctly across all scenarios."""
        handler = ErrorHandler(ErrorStrategy.WARN_CONTINUE)

        # Generate various scenarios
        handler.handle_missing_variable("var1", "sql")
        handler.handle_invalid_format("var2", "text", "Bad syntax")
        handler.handle_type_error("var3", object(), "json", "Unsupported type")

        # Should have 2 warning calls (missing var + invalid format)
        # Type error uses debug logging for warnings in WARN_CONTINUE
        assert mock_logger.warning.call_count == 2

        # Check that messages contain expected content
        warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
        assert any("var1" in msg for msg in warning_calls)
        assert any("Bad syntax" in msg for msg in warning_calls)

        # Type error should be in debug calls since it's a warning-level error
        assert mock_logger.debug.call_count >= 1
