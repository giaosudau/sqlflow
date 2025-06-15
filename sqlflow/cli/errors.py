"""Minimal CLI errors module stub for test compatibility."""


class SQLFlowCLIError(Exception):
    """Base exception for CLI operations."""
    pass


class PipelineNotFoundError(SQLFlowCLIError):
    """Raised when a pipeline file cannot be found."""
    pass


class PipelineValidationError(SQLFlowCLIError):
    """Raised when pipeline validation fails."""
    pass


class ProfileNotFoundError(SQLFlowCLIError):
    """Raised when a profile cannot be found."""
    pass


class VariableParsingError(SQLFlowCLIError):
    """Raised when variable parsing fails."""
    pass


class PipelineCompilationError(SQLFlowCLIError):
    """Raised when pipeline compilation fails."""
    pass


class PipelineExecutionError(SQLFlowCLIError):
    """Raised when pipeline execution fails."""
    pass
