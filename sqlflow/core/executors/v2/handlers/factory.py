"""Step Handler Factory for V2 Executor.

This module implements the Factory pattern to create the appropriate StepHandler
for a given step type. This centralized factory makes it easy to add new handlers
and ensures consistent handler creation across the system.
"""

from typing import Dict, Type, Optional

from sqlflow.core.executors.v2.handlers.base import StepHandler
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class StepHandlerFactory:
    """
    Factory class that provides the correct StepHandler for a given step type.
    
    This factory encapsulates the logic of mapping step types to their corresponding
    handlers, making it easy to add new step types without modifying the orchestrator.
    
    The factory follows the Registry pattern - handlers are registered with their
    corresponding step types, and the factory looks them up dynamically.
    """
    
    # Registry of step type -> handler class mappings
    _handlers: Dict[str, Type[StepHandler]] = {}
    # Singleton instances for stateless handlers (performance optimization)
    _handler_instances: Dict[str, StepHandler] = {}
    
    @classmethod
    def register_handler(cls, step_type: str, handler_class: Type[StepHandler]) -> None:
        """
        Register a handler class for a specific step type.
        
        Args:
            step_type: The step type this handler manages (e.g., "load", "transform")
            handler_class: The handler class to instantiate for this step type
            
        Raises:
            TypeError: If step_type is not a string or handler_class is not a StepHandler subclass
            
        Example:
            StepHandlerFactory.register_handler("load", LoadStepHandler)
        """
        # Validate input parameters
        if not isinstance(step_type, str):
            raise TypeError("Step type must be a string")
        
        if not (isinstance(handler_class, type) and issubclass(handler_class, StepHandler)):
            raise TypeError("Handler class must be a subclass of StepHandler")
        
        cls._handlers[step_type] = handler_class
        # Clear cached instance to force recreation with new handler
        cls._handler_instances.pop(step_type, None)
        logger.debug(f"Registered handler {handler_class.__name__} for step type '{step_type}'")
    
    @classmethod
    def get_handler(cls, step_type: str) -> StepHandler:
        """
        Get the appropriate handler for a given step type.
        
        Args:
            step_type: The type of step to handle (e.g., "load", "transform")
            
        Returns:
            StepHandler instance capable of executing the step type
            
        Raises:
            ValueError: If no handler is registered for the step type
            
        Example:
            handler = StepHandlerFactory.get_handler("load")
            result = handler.execute(load_step, context)
        """
        # Check if we have a cached instance (handlers are stateless)
        if step_type in cls._handler_instances:
            return cls._handler_instances[step_type]
        
        # Look up handler class
        handler_class = cls._handlers.get(step_type)
        if not handler_class:
            available_types = ", ".join(cls._handlers.keys()) if cls._handlers else "None"
            raise ValueError(
                f"No handler registered for step type: '{step_type}'. "
                f"Available types: {available_types}"
            )
        
        # Create and cache handler instance
        try:
            handler_instance = handler_class()
            cls._handler_instances[step_type] = handler_instance
            logger.debug(f"Created handler instance for step type '{step_type}'")
            return handler_instance
        except Exception as e:
            raise ValueError(
                f"Failed to create handler for step type '{step_type}': {e}"
            ) from e
    
    @classmethod
    def create_handler(cls, step_type: str) -> StepHandler:
        """
        Create a new handler instance for a given step type.
        
        This is an alias for get_handler() but always returns a new instance
        rather than using cached instances (for testing purposes).
        
        Args:
            step_type: The type of step to handle (e.g., "load", "transform")
            
        Returns:
            New StepHandler instance capable of executing the step type
            
        Raises:
            ValueError: If no handler is registered for the step type or creation fails
            
        Example:
            handler = StepHandlerFactory.create_handler("load")
            result = handler.execute(load_step, context)
        """
        # Look up handler class
        handler_class = cls._handlers.get(step_type)
        if not handler_class:
            available_types = ", ".join(cls._handlers.keys()) if cls._handlers else "None"
            raise ValueError(
                f"Unsupported step type: '{step_type}'. "
                f"Available types: {available_types}"
            )
        
        # Create new handler instance (don't use cache)
        try:
            handler_instance = handler_class()
            logger.debug(f"Created new handler instance for step type '{step_type}'")
            return handler_instance
        except Exception as e:
            raise ValueError(
                f"Failed to create handler for step type '{step_type}': {e}"
            ) from e
    
    @classmethod
    def get_available_step_types(cls) -> list[str]:
        """
        Get list of all registered step types.
        
        Returns:
            List of step type strings that have registered handlers
        """
        return list(cls._handlers.keys())
    
    @classmethod
    def is_step_type_supported(cls, step_type: str) -> bool:
        """
        Check if a step type is supported by the factory.
        
        Args:
            step_type: Step type to check
            
        Returns:
            True if the step type has a registered handler
        """
        return step_type in cls._handlers
    
    @classmethod
    def clear_registry(cls) -> None:
        """
        Clear all registered handlers (primarily for testing).
        
        This method removes all handler registrations and cached instances.
        Use with caution in production code.
        """
        cls._handlers.clear()
        cls._handler_instances.clear()
        logger.debug("Cleared all handler registrations")
    
    @classmethod
    def get_handler_info(cls, step_type: Optional[str] = None) -> Dict[str, str]:
        """
        Get information about registered handlers.
        
        Args:
            step_type: Optional specific step type to get info for
            
        Returns:
            Dictionary mapping step types to handler class names
        """
        if step_type:
            handler_class = cls._handlers.get(step_type)
            if handler_class:
                return {step_type: handler_class.__name__}
            return {}
        
        return {
            step_type: handler_class.__name__
            for step_type, handler_class in cls._handlers.items()
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
        StepHandlerFactory.register_handler("load", LoadStepHandler)
    except ImportError as e:
        logger.debug(f"Could not auto-register LoadStepHandler: {e}")
    
    # Future handlers will be added here as they're implemented
    # try:
    #     from sqlflow.core.executors.v2.handlers.transform_handler import TransformStepHandler
    #     StepHandlerFactory.register_handler("transform", TransformStepHandler)
    # except ImportError as e:
    #     logger.debug(f"Could not auto-register TransformStepHandler: {e}")


# Perform auto-registration when module is imported
_auto_register_handlers() 