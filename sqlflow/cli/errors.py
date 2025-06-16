"""CLI-specific exceptions with Rich display support.

This module implements the Python-native exception hierarchy from Task 2.3
with Rich context and beautiful error display capabilities.
"""

from typing import List, Optional


class SQLFlowCLIError(Exception):
    """Base exception for CLI operations with Rich display support."""

    def __init__(self, message: str, suggestions: Optional[List[str]] = None):
        self.message = message
        self.suggestions = suggestions or []
        super().__init__(message)


class PipelineNotFoundError(SQLFlowCLIError):
    """Raised when a pipeline file cannot be found."""

    def __init__(
        self,
        pipeline_name: str,
        search_paths: List[str],
        available_pipelines: Optional[List[str]] = None,
    ):
        self.pipeline_name = pipeline_name
        self.search_paths = search_paths
        self.available_pipelines = available_pipelines or []

        message = (
            f"Pipeline '{pipeline_name}' not found in paths: {', '.join(search_paths)}"
        )
        suggestions = (
            [f"Try: {p}" for p in available_pipelines[:3]]
            if available_pipelines
            else []
        )

        super().__init__(message, suggestions)


class PipelineValidationError(SQLFlowCLIError):
    """Raised when pipeline validation fails."""

    def __init__(self, pipeline_name: str, errors: List[str]):
        self.pipeline_name = pipeline_name
        self.errors = errors
        message = f"Pipeline '{pipeline_name}' validation failed"
        super().__init__(message)


class ProfileNotFoundError(SQLFlowCLIError):
    """Raised when a profile cannot be found."""

    def __init__(
        self,
        profile_name: str,
        available_profiles: Optional[List[str]] = None,
        search_paths: Optional[List[str]] = None,
    ):
        self.profile_name = profile_name
        self.available_profiles = available_profiles or []
        self.search_paths = search_paths or []

        message = f"Profile '{profile_name}' not found"
        suggestions = []
        if self.search_paths:
            suggestions.append(f"Searched in: {', '.join(self.search_paths[:3])}")
        if self.available_profiles:
            # Create a single suggestion with multiple options
            profile_list = ", ".join(self.available_profiles[:3])
            suggestions.append(f"Try: {profile_list}")

        super().__init__(message, suggestions)


class ProfileValidationError(SQLFlowCLIError):
    """Raised when profile validation fails."""

    def __init__(self, profile_name: str, errors: List[str]):
        self.profile_name = profile_name
        self.errors = errors
        message = f"Profile '{profile_name}' validation failed"
        super().__init__(message)


class ProjectNotFoundError(SQLFlowCLIError):
    """Raised when SQLFlow project cannot be found."""

    def __init__(self, directory: str):
        self.directory = directory
        message = f"No SQLFlow project found in directory: {directory}"
        suggestions = [
            "Run 'sqlflow init <project_name>' to create a new project",
            "Make sure you're in the correct directory",
        ]
        super().__init__(message, suggestions)


class VariableParsingError(SQLFlowCLIError):
    """Raised when variable parsing fails."""

    def __init__(self, variable_string: str, parse_error: str):
        self.variable_string = variable_string
        self.parse_error = parse_error
        self.error_details = parse_error  # Alias for backward compatibility
        message = f"Failed to parse variables: {parse_error}"
        suggestions = [
            'Example JSON: --variables \'{"env": "prod", "debug": true}\'',
        ]
        super().__init__(message, suggestions)


class PipelineCompilationError(SQLFlowCLIError):
    """Raised when pipeline compilation fails."""

    def __init__(
        self,
        pipeline_name: str,
        error_message: Optional[str] = None,
        context: Optional[dict] = None,
    ):
        self.pipeline_name = pipeline_name
        self.error_message = error_message or "Compilation failed"
        self.context = context or {}

        message = f"{pipeline_name}: {self.error_message}"
        suggestions = ["Check pipeline syntax", "Verify variable references"]
        super().__init__(message, suggestions)


class PipelineExecutionError(SQLFlowCLIError):
    """Raised when pipeline execution fails."""

    def __init__(
        self,
        pipeline_name: str,
        error_message: Optional[str] = None,
        failed_step: Optional[str] = None,
    ):
        self.pipeline_name = pipeline_name
        self.error_message = error_message or "Execution failed"
        self.failed_step = failed_step

        message = f"{pipeline_name}: {self.error_message}"
        if failed_step:
            message += f" (step: {failed_step})"
        suggestions = ["Check connection settings", "Verify data sources"]
        super().__init__(message, suggestions)
