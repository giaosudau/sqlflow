"""Tests for parser integration with validation module."""

import pytest

from sqlflow.parser.ast import LoadStep, SourceDefinitionStep
from sqlflow.parser.parser import Parser


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

        # Import the aggregated error class for type checking
        from sqlflow.validation import AggregatedValidationError

        with pytest.raises(AggregatedValidationError) as exc_info:
            parser.parse(text)

        # For single error, check the first (and only) error
        aggregated_error = exc_info.value
        assert len(aggregated_error.errors) == 1
        error = aggregated_error.errors[0]

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

        # Import the aggregated error class for type checking
        from sqlflow.validation import AggregatedValidationError

        with pytest.raises(AggregatedValidationError) as exc_info:
            parser.parse(text)

        # For single error, check the first (and only) error
        aggregated_error = exc_info.value
        assert len(aggregated_error.errors) == 1
        error = aggregated_error.errors[0]

        assert error.error_type == "Parameter Error"
        assert "does not match required pattern" in error.message

    def test_parser_raises_validation_error_for_undefined_reference(self):
        """Test that parser raises ValidationError for undefined source references."""
        text = """
        SOURCE users TYPE CSV PARAMS {"path": "users.csv"};
        LOAD products_table FROM products;
        """

        parser = Parser()

        # Import the aggregated error class for type checking
        from sqlflow.validation import AggregatedValidationError

        with pytest.raises(AggregatedValidationError) as exc_info:
            parser.parse(text)

        # For single error, check the first (and only) error
        aggregated_error = exc_info.value
        assert len(aggregated_error.errors) == 1
        error = aggregated_error.errors[0]

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

        # Import the aggregated error class for type checking
        from sqlflow.validation import AggregatedValidationError

        with pytest.raises(AggregatedValidationError) as exc_info:
            parser.parse(text)

        # For single error, check the first (and only) error
        aggregated_error = exc_info.value
        assert len(aggregated_error.errors) == 1
        error = aggregated_error.errors[0]

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

        # Import the aggregated error class for type checking
        from sqlflow.validation import AggregatedValidationError

        with pytest.raises(AggregatedValidationError) as exc_info:
            parser.parse(text)

        # Check aggregated error formatting
        aggregated_error = exc_info.value
        error_str = str(aggregated_error)

        # For single error, should show the error without aggregation header
        assert "❌ Connector Error" in error_str
        assert "line 2" in error_str
        assert "Unknown connector type: UNKNOWN" in error_str
        assert "💡 Suggestions:" in error_str
        assert "Available connector types" in error_str

    def test_parser_handles_multiple_validation_errors(self):
        """Test that parser reports all validation errors when multiple exist."""
        text = """
        SOURCE users TYPE MYSQL PARAMS {"host": "localhost"};
        LOAD products_table FROM undefined_source;
        """

        parser = Parser()

        # Import the aggregated error class for type checking
        from sqlflow.validation import AggregatedValidationError

        with pytest.raises(AggregatedValidationError) as exc_info:
            parser.parse(text)

        # Should get an aggregated error containing both errors
        aggregated_error = exc_info.value
        assert len(aggregated_error.errors) == 2

        # Verify we have both error types
        error_types = [error.error_type for error in aggregated_error.errors]
        assert "Connector Error" in error_types
        assert "Reference Error" in error_types

        # Verify specific error messages
        connector_errors = [
            e for e in aggregated_error.errors if e.error_type == "Connector Error"
        ]
        reference_errors = [
            e for e in aggregated_error.errors if e.error_type == "Reference Error"
        ]

        assert len(connector_errors) == 1
        assert "Unknown connector type: MYSQL" in connector_errors[0].message

        assert len(reference_errors) == 1
        assert (
            "LOAD references undefined source: 'undefined_source'"
            in reference_errors[0].message
        )

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
