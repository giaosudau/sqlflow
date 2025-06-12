"""SQLFlow planner components.

This package contains the refactored planner components that follow the
Single Responsibility Principle (Zen of Python).

Following Zen of Python:
- Simple is better than complex: Each component has a clear responsibility
- Explicit is better than implicit: Clear component interfaces
- Practicality beats purity: Backward compatibility maintained
"""

# Avoid circular imports - let users import directly from planner_main if needed
# from sqlflow.core.planner_main import (
#     ExecutionPlanBuilder,
#     OperationPlanner,
#     Planner,
#     logger,
# )

from .dependency_analyzer import DependencyAnalyzer
from .errors import (
    DependencyError,
    PlannerError,
    StepBuildError,
    ValidationError,
    create_dependency_error,
    create_validation_error,
)
from .factory import PlannerConfig, PlannerFactory
from .interfaces import IDependencyAnalyzer, IExecutionOrderResolver, IStepBuilder
from .order_resolver import ExecutionOrderResolver
from .step_builder import StepBuilder

__all__ = [
    "DependencyAnalyzer",
    "ExecutionOrderResolver",
    "StepBuilder",
    "IDependencyAnalyzer",
    "IExecutionOrderResolver",
    "IStepBuilder",
    "PlannerConfig",
    "PlannerFactory",
    "PlannerError",
    "ValidationError",
    "DependencyError",
    "StepBuildError",
    "create_validation_error",
    "create_dependency_error",
    # "ExecutionPlanBuilder",
    # "OperationPlanner",
    # "Planner",
    # "logger",
]
