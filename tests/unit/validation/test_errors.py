"""Tests for validation error handling."""

from sqlflow.validation.errors import ValidationError


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

    def test_from_parse_error_without_position(self):
        """Test creating ValidationError from parser exception without position info."""
        parse_error = Exception("Generic parse error")
        validation_error = ValidationError.from_parse_error(parse_error)

        assert validation_error.message == "Generic parse error"
        assert validation_error.line == 1  # Default line
        assert validation_error.column == 0  # Default column
        assert validation_error.error_type == "Syntax Error"
