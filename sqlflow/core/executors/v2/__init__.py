"""SQLFlow V2 Execution Engine.

Simple, Pythonic pipeline execution following the Zen of Python:
- "Simple is better than complex"
- "Explicit is better than implicit"
- "Flat is better than nested"
- "Readability counts"

Public API for V2 execution engine.
"""

# Core execution components
from .execution.context import (
    ExecutionContext,
    create_execution_context,
    create_test_context,
    with_engine,
    with_variables,
)
from .observability.metrics import SimpleObservabilityManager
from .orchestration.coordinator import ExecutionCoordinator

# Result models
from .results.models import (
    ExecutionResult,
    StepResult,
    create_error_result,
    create_execution_result,
    create_success_result,
)

# Step definitions and registry
from .steps.definitions import (
    ExportFormat,
    ExportStep,
    LoadMode,
    LoadStep,
    SourceStep,
    TransformStep,
    create_step_from_dict,
)
from .steps.registry import (
    StepExecutorRegistry,
    create_default_registry,
)

# Clean public API - only what users need
__all__ = [
    # Main execution
    "ExecutionCoordinator",
    # Context management
    "ExecutionContext",
    "create_execution_context",
    "create_test_context",
    "with_variables",
    "with_engine",
    # Step definitions
    "LoadStep",
    "TransformStep",
    "ExportStep",
    "SourceStep",
    "LoadMode",
    "ExportFormat",
    "create_step_from_dict",
    # Registry
    "StepExecutorRegistry",
    "create_default_registry",
    # Results
    "ExecutionResult",
    "StepResult",
    "create_success_result",
    "create_error_result",
    "create_execution_result",
    # Observability
    "SimpleObservabilityManager",
]
