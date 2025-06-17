"""SQLFlow V2 Executor Architecture.

This module contains the next-generation executor system built on principles of:
- Single Responsibility Principle (SRP)
- Open/Closed Principle (OCP)
- Dependency Inversion Principle (DIP)
- Composable architecture
- Automatic observability

Version: 2.0.0-alpha
Author: Raymond Hettinger
"""

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.observability import ObservabilityManager
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import (
    BaseStep,
    ExportStep,
    LoadStep,
    SourceDefinitionStep,
    TransformStep,
)

__version__ = "2.0.0-alpha"
__all__ = [
    "ExecutionContext",
    "BaseStep",
    "LoadStep",
    "TransformStep",
    "ExportStep",
    "SourceDefinitionStep",
    "StepExecutionResult",
    "ObservabilityManager",
]
