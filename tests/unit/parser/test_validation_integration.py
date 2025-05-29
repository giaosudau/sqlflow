"""Tests for parser integration with validation module."""

import pytest

from sqlflow.parser.ast import LoadStep, SourceDefinitionStep
from sqlflow.parser.parser import Parser
from sqlflow.validation.errors import ValidationError


class TestParserValidationIntegration:
    """Test integration between parser and validation module."""

    def test_valid_pipeline_parsing_and_validation(self):
        """Test that a valid pipeline parses and validates successfully."""
        text = """
        SOURCE users TYPE CSV PARAMS {"path": "users.csv"};
        LOAD users_table FROM users;
        """

        parser = Parser()
        pipeline = parser.parse(text)

        assert len(pipeline.steps) == 2

        source_step = pipeline.steps[0]
        assert isinstance(source_step, SourceDefinitionStep)
        assert source_step.name == "users"

        load_step = pipeline.steps[1]
        assert isinstance(load_step, LoadStep)
        assert load_step.table_name == "users_table"

    def test_parser_raises_validation_error_for_unknown_connector(self):
        """Test that parser raises ValidationError for unknown connector types."""
        text = """
        SOURCE users TYPE MYSQL PARAMS {"host": "localhost"};
        """

        parser = Parser()

        with pytest.raises(ValidationError) as exc_info:
            parser.parse(text)

        error = exc_info.value
        assert error.error_type == "Connector Error"
        assert "Unknown connector type: MYSQL" in error.message
        assert error.line == 2  # Should point to the SOURCE line
        assert "Available connector types" in error.suggestions[0]

    def test_parser_raises_validation_error_for_invalid_parameters(self):
        """Test that parser raises ValidationError for invalid connector parameters."""
        text = """
        SOURCE users TYPE CSV PARAMS {"path": "users.txt"};
        """

        parser = Parser()

        with pytest.raises(ValidationError) as exc_info:
            parser.parse(text)

        error = exc_info.value
        assert error.error_type == "Parameter Error"
        assert "does not match required pattern" in error.message

    def test_parser_raises_validation_error_for_undefined_reference(self):
        """Test that parser raises ValidationError for undefined source references."""
        text = """
        SOURCE users TYPE CSV PARAMS {"path": "users.csv"};
        LOAD products_table FROM products;
        """

        parser = Parser()

        with pytest.raises(ValidationError) as exc_info:
            parser.parse(text)

        error = exc_info.value
        assert error.error_type == "Reference Error"
        assert "LOAD references undefined source: 'products'" in error.message
        assert error.line == 3  # Should point to the LOAD line

    def test_parser_raises_validation_error_for_duplicate_sources(self):
        """Test that parser raises ValidationError for duplicate source definitions."""
        text = """
        SOURCE users TYPE CSV PARAMS {"path": "users1.csv"};
        SOURCE users TYPE CSV PARAMS {"path": "users2.csv"};
        """

        parser = Parser()

        with pytest.raises(ValidationError) as exc_info:
            parser.parse(text)

        error = exc_info.value
        assert error.error_type == "Reference Error"
        assert "Duplicate source definition: 'users'" in error.message
        assert error.line == 3  # Should point to the second SOURCE line

    def test_parser_skips_validation_for_profile_based_connectors(self):
        """Test that parser skips connector validation for profile-based connectors."""
        text = """
        SOURCE users FROM "my_postgres" OPTIONS {"table": "users"};
        LOAD users_table FROM users;
        """

        parser = Parser()
        pipeline = parser.parse(text)

        # Should parse successfully without connector validation
        assert len(pipeline.steps) == 2

        source_step = pipeline.steps[0]
        assert isinstance(source_step, SourceDefinitionStep)
        assert source_step.is_from_profile is True
        assert source_step.profile_connector_name == "my_postgres"

    def test_parser_validation_error_formatting(self):
        """Test that ValidationError formatting includes suggestions and proper formatting."""
        text = """
        SOURCE users TYPE UNKNOWN PARAMS {"path": "users.csv"};
        """

        parser = Parser()

        with pytest.raises(ValidationError) as exc_info:
            parser.parse(text)

        error = exc_info.value
        error_str = str(error)

        # Check that the error string includes all expected components
        assert "❌ Connector Error" in error_str
        assert "line 2" in error_str
        assert "Unknown connector type: UNKNOWN" in error_str
        assert "💡 Suggestions:" in error_str
        assert "Available connector types" in error_str

    def test_parser_handles_multiple_validation_errors(self):
        """Test that parser reports the first validation error when multiple exist."""
        text = """
        SOURCE users TYPE MYSQL PARAMS {"host": "localhost"};
        LOAD products_table FROM undefined_source;
        """

        parser = Parser()

        with pytest.raises(ValidationError) as exc_info:
            parser.parse(text)

        # Should get the first error (connector error)
        error = exc_info.value
        assert error.error_type == "Connector Error"
        assert "Unknown connector type: MYSQL" in error.message

    def test_parser_syntax_errors_take_precedence_over_validation(self):
        """Test that syntax errors are reported before validation errors."""
        text = """
        SOURCE users TYPE CSV PARAMS {"path": "users.csv"}
        LOAD users_table FROM users;
        """  # Missing semicolon after first statement

        parser = Parser()

        # Should get a ParserError, not a ValidationError
        with pytest.raises(Exception) as exc_info:
            parser.parse(text)

        # Should be a ParserError (syntax error), not ValidationError
        assert "ParserError" in str(type(exc_info.value))

    def test_empty_pipeline_validation(self):
        """Test that empty pipelines are considered valid."""
        text = ""

        parser = Parser()
        pipeline = parser.parse(text)

        assert len(pipeline.steps) == 0
