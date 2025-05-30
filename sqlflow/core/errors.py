"""Error classes for SQLFlow."""

from typing import Any, Dict, List, Optional


class SQLFlowError(Exception):
    """Base class for all SQLFlow errors."""

    def __init__(self, message: str):
        """Initialize a SQLFlowError.

        Args:
        ----
            message: Error message

        """
        self.message = message
        super().__init__(message)


class CircularDependencyError(SQLFlowError):
    """Error raised when a circular dependency is detected in a pipeline."""

    def __init__(self, cycle: List[str]):
        """Initialize a CircularDependencyError.

        Args:
        ----
            cycle: List of pipeline names forming a cycle

        """
        self.cycle = cycle
        cycle_str = " -> ".join(cycle)
        message = f"Circular dependency detected: {cycle_str}"
        super().__init__(message)


class ValidationError(SQLFlowError):
    """Error raised when validation fails."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        """Initialize a ValidationError.

        Args:
        ----
            message: Error message
            errors: List of validation errors

        """
        self.errors = errors or []
        if errors:
            error_list = "\n - " + "\n - ".join(errors)
            message = f"{message}:{error_list}"
        super().__init__(message)


class ConnectorError(SQLFlowError):
    """Error raised when a connector operation fails."""

    def __init__(self, connector_name: str, message: str):
        """Initialize a ConnectorError.

        Args:
        ----
            connector_name: Name of the connector
            message: Error message

        """
        self.connector_name = connector_name
        super().__init__(f"Connector '{connector_name}' error: {message}")


class ExecutionError(SQLFlowError):
    """Error raised when pipeline execution fails."""

    def __init__(self, pipeline_name: str, step_name: str, message: str):
        """Initialize an ExecutionError.

        Args:
        ----
            pipeline_name: Name of the pipeline
            step_name: Name of the step that failed
            message: Error message

        """
        self.pipeline_name = pipeline_name
        self.step_name = step_name
        super().__init__(
            f"Execution error in pipeline '{pipeline_name}', step '{step_name}': {message}"
        )


class PlanningError(SQLFlowError):
    """Error raised when planning operations fails."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize a PlanningError.

        Args:
        ----
            message: Error message
            details: Additional details about the error

        """
        self.details = details or {}
        super().__init__(f"Planning error: {message}")
