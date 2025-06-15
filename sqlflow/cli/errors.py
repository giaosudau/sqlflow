"""CLI-specific exceptions for SQLFlow.

This module provides a rich exception hierarchy for CLI operations,
following the technical design's Python-native approach with Rich display support.
"""

from typing import Any, Dict, List, Optional


class SQLFlowCLIError(Exception):
    """Base exception for CLI operations."""

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
        self, profile_name: str, available_profiles: Optional[List[str]] = None
    ):
        self.profile_name = profile_name
        self.available_profiles = available_profiles or []

        message = f"Profile '{profile_name}' not found"
        suggestions = (
            [f"Available: {', '.join(available_profiles[:3])}"]
            if available_profiles
            else []
        )

        super().__init__(message, suggestions)


class VariableParsingError(SQLFlowCLIError):
    """Raised when variable parsing fails."""

    def __init__(self, variable_string: str, parse_error: str):
        self.variable_string = variable_string
        self.parse_error = parse_error
        message = f"Failed to parse variables: {parse_error}"
        suggestions = ['Example: --variables \'{"env": "prod", "debug": true}\'']
        super().__init__(message, suggestions)


class PipelineCompilationError(SQLFlowCLIError):
    """Raised when pipeline compilation fails."""

    def __init__(
        self,
        pipeline_name: str,
        error_message: str,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.pipeline_name = pipeline_name
        self.error_message = error_message
        self.context = context or {}
        message = f"Pipeline '{pipeline_name}' compilation failed: {error_message}"
        super().__init__(message)


class PipelineExecutionError(SQLFlowCLIError):
    """Raised when pipeline execution fails."""

    def __init__(
        self, pipeline_name: str, error_message: str, failed_step: Optional[str] = None
    ):
        self.pipeline_name = pipeline_name
        self.error_message = error_message
        self.failed_step = failed_step
        message = f"Pipeline '{pipeline_name}' execution failed: {error_message}"
        if failed_step:
            message += f" (step: {failed_step})"
        super().__init__(message)
