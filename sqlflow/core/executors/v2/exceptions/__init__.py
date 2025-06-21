"""Comprehensive exception hierarchy for V2 Executor.

Following Week 7-8 requirements for proper exception handling:
- Clear error messages with actionable information
- No silent failures
- Proper error context propagation
- Hierarchical exception structure
"""

from .base import (
    SQLFlowError,
    SQLFlowWarning,
)
from .data import (
    DataConnectorError,
    DataTransformationError,
    DataValidationError,
    SchemaValidationError,
)
from .execution import (
    DependencyResolutionError,
    PipelineExecutionError,
    StepExecutionError,
    StepTimeoutError,
)
from .infrastructure import (
    ConfigurationError,
    ConnectionError,
    DatabaseError,
    ResourceExhaustionError,
)
from .runtime import (
    PermissionError,
    SecurityError,
    UDFExecutionError,
    VariableSubstitutionError,
)

__all__ = [
    # Base exceptions
    "SQLFlowError",
    "SQLFlowWarning",
    # Execution exceptions
    "StepExecutionError",
    "PipelineExecutionError",
    "DependencyResolutionError",
    "StepTimeoutError",
    # Data exceptions
    "DataValidationError",
    "DataTransformationError",
    "DataConnectorError",
    "SchemaValidationError",
    # Infrastructure exceptions
    "DatabaseError",
    "ConnectionError",
    "ResourceExhaustionError",
    "ConfigurationError",
    # Runtime exceptions
    "VariableSubstitutionError",
    "UDFExecutionError",
    "PermissionError",
    "SecurityError",
]
