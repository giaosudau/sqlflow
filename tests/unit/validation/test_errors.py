"""Tests for validation error handling."""

from sqlflow.validation.errors import AggregatedValidationError, ValidationError


class TestValidationError:
    """Test ValidationError class functionality."""

    def test_basic_error_creation(self):
        """Test creating a basic validation error."""
        error = ValidationError(message="Test error message", line=5, column=10)

        assert error.message == "Test error message"
        assert error.line == 5
        assert error.column == 10
        assert error.error_type == "ValidationError"
        assert error.suggestions == []
        assert error.help_url is None

    def test_error_string_formatting(self):
        """Test error string formatting without suggestions."""
        error = ValidationError(
            message="Invalid connector type",
            line=3,
            column=15,
            error_type="Connector Error",
        )

        result = str(error)
        expected = "❌ Connector Error at line 3, column 15: Invalid connector type"
        assert result == expected

    def test_error_string_formatting_no_column(self):
        """Test error string formatting without column information."""
        error = ValidationError(
            message="Missing required field", line=7, error_type="Parameter Error"
        )

        result = str(error)
        expected = "❌ Parameter Error at line 7: Missing required field"
        assert result == expected

    def test_error_with_suggestions(self):
        """Test error formatting with suggestions."""
        error = ValidationError(
            message="Unknown connector type: MYSQL",
            line=2,
            column=20,
            error_type="Connector Error",
            suggestions=[
                "Available connector types: CSV, POSTGRES, S3",
                "Check the connector type spelling and case",
            ],
        )

        result = str(error)
        expected_lines = [
            "❌ Connector Error at line 2, column 20: Unknown connector type: MYSQL",
            "",
            "💡 Suggestions:",
            "  - Available connector types: CSV, POSTGRES, S3",
            "  - Check the connector type spelling and case",
        ]
        expected = "\n".join(expected_lines)
        assert result == expected

    def test_error_with_help_url(self):
        """Test error formatting with help URL."""
        error = ValidationError(
            message="Invalid parameter configuration",
            line=4,
            error_type="Parameter Error",
            help_url="https://docs.sqlflow.com/connectors/csv",
        )

        result = str(error)
        expected_lines = [
            "❌ Parameter Error at line 4: Invalid parameter configuration",
            "",
            "📖 Help: https://docs.sqlflow.com/connectors/csv",
        ]
        expected = "\n".join(expected_lines)
        assert result == expected

    def test_error_with_suggestions_and_help(self):
        """Test error formatting with both suggestions and help URL."""
        error = ValidationError(
            message="Configuration error",
            line=1,
            column=1,
            suggestions=["Check parameter types", "Verify required fields"],
            help_url="https://docs.sqlflow.com/validation",
        )

        result = str(error)
        expected_lines = [
            "❌ ValidationError at line 1, column 1: Configuration error",
            "",
            "💡 Suggestions:",
            "  - Check parameter types",
            "  - Verify required fields",
            "",
            "📖 Help: https://docs.sqlflow.com/validation",
        ]
        expected = "\n".join(expected_lines)
        assert result == expected

    def test_from_parse_error_with_position(self):
        """Test creating ValidationError from parser exception with position info."""

        class MockParseError(Exception):
            def __init__(self, message, line=None, column=None):
                super().__init__(message)
                self.line = line
                self.column = column

        parse_error = MockParseError("Unexpected token", line=5, column=12)
        validation_error = ValidationError.from_parse_error(parse_error)

        assert validation_error.message == "Unexpected token"
        assert validation_error.line == 5
        assert validation_error.column == 12
        assert validation_error.error_type == "Syntax Error"


class TestAggregatedValidationError:
    """Test AggregatedValidationError class functionality."""

    def test_empty_errors_list(self):
        """Test creating aggregated error with empty errors list."""
        aggregated_error = AggregatedValidationError([])

        assert str(aggregated_error) == "No validation errors found"
        assert aggregated_error.first_error is None

    def test_single_error_behavior(self):
        """Test that single error behaves like the original error."""
        error = ValidationError(
            message="Test error",
            line=5,
            column=10,
            error_type="Test Error",
            suggestions=["Fix this"],
        )

        aggregated_error = AggregatedValidationError([error])

        # Should format as a single error (no aggregation header)
        result = str(aggregated_error)
        expected = str(error)
        assert result == expected

        # First error should be the same
        assert aggregated_error.first_error == error

    def test_multiple_errors_formatting(self):
        """Test formatting of multiple errors with grouping."""
        error1 = ValidationError(
            message="Unknown connector type: MYSQL",
            line=2,
            error_type="Connector Error",
            suggestions=["Available connector types: CSV, POSTGRES, S3"],
        )

        error2 = ValidationError(
            message="LOAD references undefined source: 'products'",
            line=5,
            error_type="Reference Error",
            suggestions=["Define SOURCE 'products' first"],
        )

        error3 = ValidationError(
            message="Invalid path format",
            line=3,
            error_type="Parameter Error",
        )

        aggregated_error = AggregatedValidationError([error1, error2, error3])
        result = str(aggregated_error)

        # Should include summary header
        assert "❌ Pipeline validation failed with 3 error(s):" in result

        # Should have error type groupings
        assert "📋 Connector Errors:" in result
        assert "📋 Reference Errors:" in result
        assert "📋 Parameter Errors:" in result

        # Should include all error messages
        assert "Unknown connector type: MYSQL" in result
        assert "LOAD references undefined source: 'products'" in result
        assert "Invalid path format" in result

        # Should include suggestions
        assert "💡 Available connector types" in result
        assert "💡 Define SOURCE 'products' first" in result

    def test_errors_sorted_by_line_number(self):
        """Test that errors are sorted by line number within groups."""
        error1 = ValidationError("Error at line 10", line=10, error_type="Test Error")
        error2 = ValidationError("Error at line 5", line=5, error_type="Test Error")
        error3 = ValidationError("Error at line 8", line=8, error_type="Test Error")

        aggregated_error = AggregatedValidationError([error1, error2, error3])
        result = str(aggregated_error)

        # Find positions of the error messages
        line5_pos = result.find("Line 5")
        line8_pos = result.find("Line 8")
        line10_pos = result.find("Line 10")

        # Should be sorted by line number
        assert line5_pos < line8_pos < line10_pos

    def test_exception_message_summary(self):
        """Test the exception message provides a summary."""
        error1 = ValidationError("Error 1", line=1, error_type="Connector Error")
        error2 = ValidationError("Error 2", line=2, error_type="Connector Error")
        error3 = ValidationError("Error 3", line=3, error_type="Reference Error")

        aggregated_error = AggregatedValidationError([error1, error2, error3])

        # Exception message should summarize error types and counts
        exception_msg = str(aggregated_error.args[0]) if aggregated_error.args else ""
        assert "Pipeline validation failed with 3 error(s)" in exception_msg
        assert "2 Connector Error(s)" in exception_msg
        assert "1 Reference Error(s)" in exception_msg

    def test_first_error_property(self):
        """Test the first_error property returns the first error."""
        error1 = ValidationError("First error", line=1)
        error2 = ValidationError("Second error", line=2)

        aggregated_error = AggregatedValidationError([error1, error2])

        assert aggregated_error.first_error == error1
