"""Step Handler Factory for V2 Executor.

This module implements a simplified Factory pattern using a module-level registry
to create the appropriate StepHandler for a given step type.
"""

from typing import Dict, Optional, Type

from sqlflow.core.executors.v2.handlers.base import StepHandler
from sqlflow.logging import get_logger

logger = get_logger(__name__)


# Simple module-level registry - Pythonic and direct
_HANDLERS: Dict[str, Type[StepHandler]] = {}


def register_handler(step_type: str, handler_class: Type[StepHandler]) -> None:
    """
    Register a handler class for a specific step type.

    Args:
        step_type: The step type this handler manages (e.g., "load", "transform")
        handler_class: The handler class to instantiate for this step type

    Raises:
        TypeError: If step_type is not a string or handler_class is not a StepHandler subclass

    Example:
        register_handler("load", LoadStepHandler)
    """
    if not isinstance(step_type, str):
        raise TypeError("Step type must be a string")

    if not (isinstance(handler_class, type) and issubclass(handler_class, StepHandler)):
        raise TypeError("Handler class must be a subclass of StepHandler")

    _HANDLERS[step_type] = handler_class
    logger.debug(
        f"Registered handler {handler_class.__name__} for step type '{step_type}'"
    )


def get_handler(step_type: str) -> StepHandler:
    """
    Get handler for step type. Simple and direct.

    Args:
        step_type: The type of step to handle (e.g., "load", "transform")

    Returns:
        StepHandler instance capable of executing the step type

    Raises:
        ValueError: If no handler is registered for the step type

    Example:
        handler = get_handler("load")
        result = handler.execute(load_step, context)
    """
    # Ensure handlers are registered (lazy initialization)
    _ensure_handlers_registered()

    handler_class = _HANDLERS.get(step_type)
    if not handler_class:
        available_types = ", ".join(_HANDLERS.keys()) if _HANDLERS else "None"
        raise ValueError(
            f"Unsupported step type: '{step_type}'. Available types: {available_types}"
        )

    return handler_class()


def get_available_step_types() -> list[str]:
    """Get list of all registered step types."""
    return list(_HANDLERS.keys())


def is_step_type_supported(step_type: str) -> bool:
    """Check if a step type is supported."""
    return step_type in _HANDLERS


def clear_registry() -> None:
    """Clear all registered handlers (primarily for testing)."""
    _HANDLERS.clear()
    logger.debug("Cleared all handler registrations")


# Maintain backwards compatibility with existing StepHandlerFactory class
class StepHandlerFactoryMeta(type):
    """Metaclass to handle legacy _handlers attribute assignment."""

    def __setattr__(cls, name, value):
        if name == "_handlers":
            _HANDLERS.clear()
            _HANDLERS.update(value)
        else:
            super().__setattr__(name, value)

    @property
    def _handlers(cls):
        """Legacy property for backward compatibility."""
        return _HANDLERS


class StepHandlerFactory(metaclass=StepHandlerFactoryMeta):
    """Legacy wrapper for backwards compatibility."""

    @classmethod
    def register_handler(cls, step_type: str, handler_class: Type[StepHandler]) -> None:
        register_handler(step_type, handler_class)

    @classmethod
    def get_handler(cls, step_type: str) -> StepHandler:
        return get_handler(step_type)

    @classmethod
    def create_handler(cls, step_type: str) -> StepHandler:
        return get_handler(step_type)

    @classmethod
    def get_available_step_types(cls) -> list[str]:
        return get_available_step_types()

    @classmethod
    def is_step_type_supported(cls, step_type: str) -> bool:
        return is_step_type_supported(step_type)

    @classmethod
    def clear_registry(cls) -> None:
        clear_registry()

    @classmethod
    def get_handler_info(cls, step_type: Optional[str] = None) -> Dict[str, str]:
        """Legacy method for backward compatibility."""
        if step_type:
            handler_class = _HANDLERS.get(step_type)
            if handler_class:
                return {step_type: handler_class.__name__}
            return {}

        return {
            step_type: handler_class.__name__
            for step_type, handler_class in _HANDLERS.items()
        }


# Auto-register handlers when they're imported
# This happens at module import time to ensure handlers are available
def _auto_register_handlers():
    """
    Automatically register known handlers when the factory is imported.

    This function attempts to import and register handlers, but fails gracefully
    if handlers are not yet available (e.g., during development or testing).
    """
    try:
        # Import and register LoadStepHandler
        from sqlflow.core.executors.v2.handlers.load_handler import LoadStepHandler

        register_handler("load", LoadStepHandler)
        logger.debug("Auto-registered LoadStepHandler")
    except ImportError as e:
        logger.debug(f"Could not auto-register LoadStepHandler: {e}")

    # Register TransformStepHandler
    try:
        from sqlflow.core.executors.v2.handlers.transform_handler import (
            TransformStepHandler,
        )

        register_handler("transform", TransformStepHandler)
        logger.debug("Auto-registered TransformStepHandler")
    except ImportError as e:
        logger.debug(f"Could not auto-register TransformStepHandler: {e}")

    # Register ExportStepHandler
    try:
        from sqlflow.core.executors.v2.handlers.export_handler import ExportStepHandler

        register_handler("export", ExportStepHandler)
        logger.debug("Auto-registered ExportStepHandler")
    except ImportError as e:
        logger.debug(f"Could not auto-register ExportStepHandler: {e}")

    # Register SourceDefinitionHandler
    try:
        from sqlflow.core.executors.v2.handlers.source_handler import (
            SourceDefinitionHandler,
        )

        register_handler("source_definition", SourceDefinitionHandler)
        logger.debug("Auto-registered SourceDefinitionHandler")
    except ImportError as e:
        logger.debug(f"Could not auto-register SourceDefinitionHandler: {e}")


# Lazy registration: ensure handlers are registered when get_handler is called
def _ensure_handlers_registered():
    """Ensure handlers are registered before use (failsafe)."""
    if not _HANDLERS:
        _auto_register_handlers()


# Perform auto-registration when module is imported
_auto_register_handlers()
