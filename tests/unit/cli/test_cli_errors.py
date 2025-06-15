"""Tests for CLI errors module."""

from sqlflow.cli.errors import (
    PipelineCompilationError,
    PipelineExecutionError,
    PipelineNotFoundError,
    PipelineValidationError,
    ProfileNotFoundError,
    SQLFlowCLIError,
    VariableParsingError,
)


class TestSQLFlowCLIError:
    """Test base CLI error class."""

    def test_basic_error_creation(self):
        """Test basic error creation."""
        error = SQLFlowCLIError("test message")
        assert str(error) == "test message"
        assert error.message == "test message"
        assert error.suggestions == []

    def test_error_with_suggestions(self):
        """Test error creation with suggestions."""
        suggestions = ["try this", "or this"]
        error = SQLFlowCLIError("test message", suggestions)
        assert error.suggestions == suggestions


class TestPipelineNotFoundError:
    """Test pipeline not found error."""

    def test_basic_pipeline_not_found(self):
        """Test basic pipeline not found error."""
        error = PipelineNotFoundError("test_pipeline", ["/path1", "/path2"])
        assert "test_pipeline" in str(error)
        assert "/path1" in str(error)
        assert "/path2" in str(error)
        assert error.pipeline_name == "test_pipeline"
        assert error.search_paths == ["/path1", "/path2"]
        assert error.available_pipelines == []

    def test_pipeline_not_found_with_suggestions(self):
        """Test pipeline not found with available pipelines."""
        available = ["pipeline1", "pipeline2", "pipeline3"]
        error = PipelineNotFoundError("test", ["/path"], available)
        assert error.available_pipelines == available
        assert len(error.suggestions) == 3  # Should suggest first 3


class TestPipelineValidationError:
    """Test pipeline validation error."""

    def test_validation_error_creation(self):
        """Test validation error creation."""
        errors = ["error1", "error2"]
        error = PipelineValidationError("test_pipeline", errors)
        assert "test_pipeline" in str(error)
        assert error.pipeline_name == "test_pipeline"
        assert error.errors == errors


class TestProfileNotFoundError:
    """Test profile not found error."""

    def test_profile_not_found_basic(self):
        """Test basic profile not found error."""
        error = ProfileNotFoundError("prod")
        assert "prod" in str(error)
        assert error.profile_name == "prod"
        assert error.available_profiles == []

    def test_profile_not_found_with_available(self):
        """Test profile not found with available profiles."""
        available = ["dev", "staging", "prod", "test"]
        error = ProfileNotFoundError("missing", available)
        assert error.available_profiles == available
        assert len(error.suggestions) == 1  # Should show first 3 in one suggestion


class TestVariableParsingError:
    """Test variable parsing error."""

    def test_variable_parsing_error(self):
        """Test variable parsing error creation."""
        var_string = "invalid json"
        parse_error = "Expecting value: line 1 column 1 (char 0)"
        error = VariableParsingError(var_string, parse_error)

        assert error.variable_string == var_string
        assert error.parse_error == parse_error
        assert "Failed to parse variables" in str(error)
        assert len(error.suggestions) == 1  # Should have example


class TestPipelineCompilationError:
    """Test pipeline compilation error."""

    def test_compilation_error_basic(self):
        """Test basic compilation error."""
        error = PipelineCompilationError("test_pipeline", "syntax error")
        assert "test_pipeline" in str(error)
        assert "syntax error" in str(error)
        assert error.pipeline_name == "test_pipeline"
        assert error.error_message == "syntax error"
        assert error.context == {}

    def test_compilation_error_with_context(self):
        """Test compilation error with context."""
        context = {"line": 42, "column": 10}
        error = PipelineCompilationError("test", "error", context)
        assert error.context == context


class TestPipelineExecutionError:
    """Test pipeline execution error."""

    def test_execution_error_basic(self):
        """Test basic execution error."""
        error = PipelineExecutionError("test_pipeline", "connection failed")
        assert "test_pipeline" in str(error)
        assert "connection failed" in str(error)
        assert error.pipeline_name == "test_pipeline"
        assert error.error_message == "connection failed"
        assert error.failed_step is None

    def test_execution_error_with_step(self):
        """Test execution error with failed step."""
        error = PipelineExecutionError("test", "error", "step1")
        assert "step1" in str(error)
        assert error.failed_step == "step1"


class TestExceptionHierarchy:
    """Test exception hierarchy."""

    def test_all_errors_inherit_from_base(self):
        """Test that all CLI errors inherit from the base class."""
        errors = [
            PipelineNotFoundError("test", ["/path"]),
            PipelineValidationError("test", ["error"]),
            ProfileNotFoundError("test"),
            VariableParsingError("test", "error"),
            PipelineCompilationError("test", "error"),
            PipelineExecutionError("test", "error"),
        ]

        for error in errors:
            assert isinstance(error, SQLFlowCLIError)
            assert isinstance(error, Exception)

    def test_errors_can_be_caught_generically(self):
        """Test that all CLI errors can be caught with the base class."""
        errors = [
            PipelineNotFoundError("test", ["/path"]),
            PipelineValidationError("test", ["error"]),
            ProfileNotFoundError("test"),
            VariableParsingError("test", "error"),
            PipelineCompilationError("test", "error"),
            PipelineExecutionError("test", "error"),
        ]

        for error in errors:
            try:
                raise error
            except SQLFlowCLIError as e:
                assert isinstance(e, SQLFlowCLIError)
                assert hasattr(e, "message")
                assert hasattr(e, "suggestions")
