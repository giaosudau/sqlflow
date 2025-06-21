"""V2 Executor: Clean, modular, and maintainable pipeline execution.

Refactored following expert recommendations from:
- Martin Fowler: Parameter Objects, Strategy Pattern, Factory Pattern
- Kent Beck: Simple Design, Small Methods, Test-Driven Development
- Robert Martin: SOLID Principles, Clean Code, Clean Architecture
- Andy Hunt & Dave Thomas: DRY, Orthogonality, Pragmatic Programming
- Jon Bentley: Performance Awareness, Algorithm Efficiency

Architecture:
- Separated concerns into focused modules
- Dependency injection for testability
- Strategy pattern for execution approaches
- Parameter objects for complex interfaces
- Builder pattern for result construction

Modules:
- orchestrator: Main orchestration logic (much smaller now)
- database_session: Database session management
- execution_context: Context factory and dependency injection
- execution_request: Parameter objects and request encapsulation
- orchestration_strategy: Strategy pattern for different execution approaches
- result_builder: Result construction with comprehensive metrics
- handlers_registration: Handler registration logic
"""

# Core components for extension and testing
from .database_session import DatabaseSessionManager
from .execution_context import ExecutionContextFactory
from .execution_request import (
    ExecutionEnvironment,
    ExecutionSummary,
    OrchestrationRequest,
)
from .handlers_registration import ensure_handlers_registered

# Existing components (maintain compatibility)
from .observability import ObservabilityManager
from .orchestration_strategy import (
    OrchestrationStrategy,
    PipelineExecutionError,
    SequentialOrchestrationStrategy,
    VariableSubstitutionMixin,
)

# Main orchestrator (backward compatibility)
from .orchestrator import LocalOrchestrator
from .result_builder import ExecutionResultBuilder
from .results import PipelineExecutionSummary, StepExecutionResult

__all__ = [
    # Main orchestrator
    "LocalOrchestrator",
    # Core components
    "DatabaseSessionManager",
    "ExecutionContextFactory",
    "ExecutionResultBuilder",
    # Request/response objects
    "OrchestrationRequest",
    "ExecutionSummary",
    "ExecutionEnvironment",
    # Strategy pattern
    "OrchestrationStrategy",
    "SequentialOrchestrationStrategy",
    "VariableSubstitutionMixin",
    # Observability and results
    "ObservabilityManager",
    "StepExecutionResult",
    "PipelineExecutionSummary",
    # Utilities
    "ensure_handlers_registered",
    "PipelineExecutionError",
]
