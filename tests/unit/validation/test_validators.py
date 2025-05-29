"""Tests for validation functions."""

from sqlflow.parser.ast import LoadStep, Pipeline, SourceDefinitionStep
from sqlflow.validation.validators import (
    validate_connectors,
    validate_pipeline,
    validate_references,
)


class TestConnectorValidation:
    """Test connector parameter validation."""

    def test_valid_csv_connector(self):
        """Test validation of valid CSV connector."""
        pipeline = Pipeline()
        source = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users.csv"},
            line_number=1,
        )
        pipeline.add_step(source)

        errors = validate_connectors(pipeline)
        assert len(errors) == 0

    def test_unknown_connector_type(self):
        """Test validation of unknown connector type."""
        pipeline = Pipeline()
        source = SourceDefinitionStep(
            name="users",
            connector_type="MYSQL",
            params={"host": "localhost"},
            line_number=2,
        )
        pipeline.add_step(source)

        errors = validate_connectors(pipeline)
        assert len(errors) == 1
        assert errors[0].error_type == "Connector Error"
        assert "Unknown connector type: MYSQL" in errors[0].message
        assert errors[0].line == 2
        assert "Available connector types" in errors[0].suggestions[0]

    def test_invalid_connector_parameters(self):
        """Test validation of invalid connector parameters."""
        pipeline = Pipeline()
        source = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users.txt", "invalid_param": "value"},
            line_number=3,
        )
        pipeline.add_step(source)

        errors = validate_connectors(pipeline)
        assert len(errors) == 2  # Invalid file extension + unknown parameter

        # Check for unknown parameter error
        unknown_param_error = next(
            (e for e in errors if "Unknown parameters" in e.message), None
        )
        assert unknown_param_error is not None
        assert unknown_param_error.error_type == "Parameter Error"

        # Check for pattern validation error
        pattern_error = next(
            (e for e in errors if "does not match required pattern" in e.message), None
        )
        assert pattern_error is not None

    def test_missing_required_parameters(self):
        """Test validation of missing required parameters."""
        pipeline = Pipeline()
        source = SourceDefinitionStep(
            name="users",
            connector_type="POSTGRES",
            params={"table": "users"},  # Missing required 'connection'
            line_number=4,
        )
        pipeline.add_step(source)

        errors = validate_connectors(pipeline)
        assert len(errors) == 1
        assert "Required field 'connection' is missing" in errors[0].message
        assert errors[0].error_type == "Parameter Error"

    def test_skip_profile_based_connectors(self):
        """Test that profile-based connectors (FROM syntax) are skipped."""
        pipeline = Pipeline()
        source = SourceDefinitionStep(
            name="users",
            connector_type="",
            params={},
            is_from_profile=True,
            profile_connector_name="my_postgres",
            line_number=5,
        )
        pipeline.add_step(source)

        errors = validate_connectors(pipeline)
        assert len(errors) == 0  # Should be skipped


class TestReferenceValidation:
    """Test cross-reference validation."""

    def test_valid_references(self):
        """Test validation of valid source references."""
        pipeline = Pipeline()

        # Define source first
        source = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users.csv"},
            line_number=1,
        )
        pipeline.add_step(source)

        # Reference it in load
        load = LoadStep(table_name="users_table", source_name="users", line_number=2)
        pipeline.add_step(load)

        errors = validate_references(pipeline)
        assert len(errors) == 0

    def test_undefined_source_reference(self):
        """Test validation of undefined source reference."""
        pipeline = Pipeline()

        # Reference undefined source
        load = LoadStep(
            table_name="users_table", source_name="undefined_source", line_number=1
        )
        pipeline.add_step(load)

        errors = validate_references(pipeline)
        assert len(errors) == 1
        assert errors[0].error_type == "Reference Error"
        assert (
            "LOAD references undefined source: 'undefined_source'" in errors[0].message
        )
        assert "Available sources: none" in errors[0].suggestions[2]

    def test_duplicate_source_definitions(self):
        """Test validation of duplicate source definitions."""
        pipeline = Pipeline()

        # Define same source twice
        source1 = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users1.csv"},
            line_number=1,
        )
        pipeline.add_step(source1)

        source2 = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users2.csv"},
            line_number=3,
        )
        pipeline.add_step(source2)

        errors = validate_references(pipeline)
        assert len(errors) == 1
        assert errors[0].error_type == "Reference Error"
        assert "Duplicate source definition: 'users'" in errors[0].message
        assert errors[0].line == 3

    def test_multiple_sources_available_in_suggestions(self):
        """Test that available sources are listed in error suggestions."""
        pipeline = Pipeline()

        # Define multiple sources
        source1 = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users.csv"},
            line_number=1,
        )
        pipeline.add_step(source1)

        source2 = SourceDefinitionStep(
            name="orders",
            connector_type="CSV",
            params={"path": "orders.csv"},
            line_number=2,
        )
        pipeline.add_step(source2)

        # Reference undefined source
        load = LoadStep(
            table_name="products_table", source_name="products", line_number=3
        )
        pipeline.add_step(load)

        errors = validate_references(pipeline)
        assert len(errors) == 1
        suggestions_text = " ".join(errors[0].suggestions)
        assert "users" in suggestions_text
        assert "orders" in suggestions_text


class TestPipelineValidation:
    """Test complete pipeline validation."""

    def test_valid_pipeline(self):
        """Test validation of completely valid pipeline."""
        pipeline = Pipeline()

        source = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"path": "users.csv"},
            line_number=1,
        )
        pipeline.add_step(source)

        load = LoadStep(table_name="users_table", source_name="users", line_number=2)
        pipeline.add_step(load)

        errors = validate_pipeline(pipeline)
        assert len(errors) == 0

    def test_pipeline_with_multiple_error_types(self):
        """Test pipeline with multiple types of validation errors."""
        pipeline = Pipeline()

        # Invalid connector type
        source1 = SourceDefinitionStep(
            name="users",
            connector_type="MYSQL",
            params={"host": "localhost"},
            line_number=1,
        )
        pipeline.add_step(source1)

        # Valid source
        source2 = SourceDefinitionStep(
            name="orders",
            connector_type="CSV",
            params={"path": "orders.csv"},
            line_number=2,
        )
        pipeline.add_step(source2)

        # Reference undefined source
        load = LoadStep(
            table_name="users_table", source_name="undefined_source", line_number=3
        )
        pipeline.add_step(load)

        errors = validate_pipeline(pipeline)

        # Should have both connector and reference errors
        connector_errors = [e for e in errors if e.error_type == "Connector Error"]
        reference_errors = [e for e in errors if e.error_type == "Reference Error"]

        assert len(connector_errors) >= 1
        assert len(reference_errors) >= 1

    def test_pipeline_with_syntax_errors(self):
        """Test pipeline validation includes AST validation errors."""
        pipeline = Pipeline()

        # Create a step with validation errors
        source = SourceDefinitionStep(
            name="",  # Empty name should cause validation error
            connector_type="CSV",
            params={"path": "users.csv"},
            line_number=1,
        )
        pipeline.add_step(source)

        errors = validate_pipeline(pipeline)

        # Should include syntax error from AST validation
        syntax_errors = [e for e in errors if e.error_type == "Syntax Error"]
        assert len(syntax_errors) >= 1
        assert any("requires a name" in e.message for e in syntax_errors)

    def test_empty_pipeline(self):
        """Test validation of empty pipeline."""
        pipeline = Pipeline()

        errors = validate_pipeline(pipeline)
        assert len(errors) == 0  # Empty pipeline is valid
