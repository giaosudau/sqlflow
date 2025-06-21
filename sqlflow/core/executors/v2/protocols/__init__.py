"""Core protocols for clean architecture."""

from .core import (
    ConnectorProtocol,
    DatabaseEngine,
    ExecutionContext,
    ExecutionStrategy,
    MetricsCollector,
    ProfileManager,
    ResultBuilder,
    Step,
    StepExecutor,
    StepResult,
    UDFRegistry,
    Validator,
    VariableSubstitutor,
)

__all__ = [
    "Step",
    "StepResult",
    "ExecutionContext",
    "DatabaseEngine",
    "StepExecutor",
    "ConnectorProtocol",
    "VariableSubstitutor",
    "Validator",
    "ProfileManager",
    "UDFRegistry",
    "MetricsCollector",
    "ExecutionStrategy",
    "ResultBuilder",
]
