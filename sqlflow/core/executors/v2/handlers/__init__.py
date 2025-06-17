"""Step Handlers for SQLFlow V2 Executor.

This package implements the Strategy pattern for step execution, where each step type
has a dedicated handler that encapsulates its specific logic. This promotes loose
coupling, testability, and extensibility.

Key Components:
- StepHandler: Abstract base class defining the handler interface
- LoadStepHandler: Handles load operations with full observability
- StepHandlerFactory: Creates appropriate handlers based on step type
- observed_execution: Decorator that adds automatic observability to handlers

Example Usage:
    from sqlflow.core.executors.v2.handlers import StepHandlerFactory, LoadStepHandler

    # Get handler through factory
    handler = StepHandlerFactory.get_handler("load")
    result = handler.execute(load_step, context)

    # Or create handler directly
    load_handler = LoadStepHandler()
    result = load_handler.execute(load_step, context)
"""

from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.handlers.factory import StepHandlerFactory
from sqlflow.core.executors.v2.handlers.load_handler import LoadStepHandler

__all__ = [
    "StepHandler",
    "StepHandlerFactory",
    "LoadStepHandler",
    "observed_execution",
]
