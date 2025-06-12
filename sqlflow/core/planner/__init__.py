"""SQLFlow planner components.

This package contains the refactored planner components that follow the
Single Responsibility Principle (Zen of Python).

Following Zen of Python:
- Simple is better than complex: Each component has a clear responsibility
- Explicit is better than implicit: Clear component interfaces
- Practicality beats purity: Backward compatibility maintained
"""

# Import main classes from planner_main.py for backward compatibility
from sqlflow.core.planner_main import (
    ExecutionPlanBuilder,
    OperationPlanner,
    Planner,
    logger,
)

from .dependency_analyzer import DependencyAnalyzer
from .order_resolver import ExecutionOrderResolver
from .step_builder import StepBuilder

__all__ = [
    "DependencyAnalyzer",
    "ExecutionOrderResolver",
    "StepBuilder",
    "ExecutionPlanBuilder",
    "OperationPlanner",
    "Planner",
    "logger",
]
