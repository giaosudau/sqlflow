"""Step registry for finding the right executor for each step.

Following Python best practices and Raymond Hettinger's guidance:
- Use typing.Protocol for structural typing
- Clean, readable code with good error messages
- Simple registration mechanism
- Easy to test and extend

This registry uses the Registry pattern to decouple step types
from their executors, making the system more extensible.
"""

from typing import Dict, List

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step, StepExecutor, StepResult

logger = get_logger(__name__)


class StepExecutorRegistry:
    """Registry for step executors using the Registry pattern.

    This class maintains a mapping between step types and their executors,
    following the Open/Closed Principle - open for extension, closed for modification.
    """

    def __init__(self):
        self._executors: List[StepExecutor] = []
        self._type_cache: Dict[str, StepExecutor] = {}

    def register(self, executor: StepExecutor) -> None:
        """Register a step executor.

        The executor must implement the StepExecutor protocol.
        """
        if not hasattr(executor, "can_execute") or not hasattr(executor, "execute"):
            raise ValueError(
                f"Executor must implement StepExecutor protocol: {type(executor)}"
            )

        if executor not in self._executors:
            self._executors.append(executor)
            # Clear cache when new executor is registered
            self._type_cache.clear()
            logger.debug(f"Registered step executor: {type(executor).__name__}")

    def find_executor(self, step: Step) -> StepExecutor:
        """Find executor for the given step.

        Uses caching for performance and provides clear error messages.
        """
        step_type = getattr(step, "type", getattr(step, "step_type", "unknown"))
        step_id = getattr(step, "id", "unknown")

        # Check cache first
        if step_type in self._type_cache:
            logger.debug(
                f"Found cached executor for step {step_id} of type {step_type}"
            )
            return self._type_cache[step_type]

        # Find executor by capability
        for executor in self._executors:
            if executor.can_execute(step):
                # Cache the result
                self._type_cache[step_type] = executor
                logger.debug(
                    f"Found executor {type(executor).__name__} for step {step_id}"
                )
                return executor

        # No executor found - provide helpful error message
        available_types = self.get_supported_step_types()
        raise ValueError(
            f"No executor found for step '{step_id}' of type '{step_type}'. "
            f"Available types: {available_types}"
        )

    def execute_step(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute step using appropriate executor.

        Convenience method that finds and executes in one call.
        """
        executor = self.find_executor(step)
        return executor.execute(step, context)

    def get_supported_step_types(self) -> List[str]:
        """Get list of supported step types."""
        supported_types = []

        # Test common step types
        test_types = ["load", "transform", "export", "source", "source_definition"]

        for step_type in test_types:
            # Create a minimal test step
            test_step = type(
                "TestStep",
                (),
                {"type": step_type, "step_type": step_type, "id": "test"},
            )()

            for executor in self._executors:
                try:
                    if executor.can_execute(test_step):
                        supported_types.append(step_type)
                        break
                except Exception:
                    # Ignore errors during capability testing
                    continue

        return supported_types

    def get_registered_executors(self) -> List[StepExecutor]:
        """Get copy of all registered executors."""
        return self._executors.copy()

    def clear(self) -> None:
        """Clear all registered executors (useful for testing)."""
        self._executors.clear()
        self._type_cache.clear()
        logger.debug("Cleared all registered executors")

    def __len__(self) -> int:
        """Number of registered executors."""
        return len(self._executors)

    def __contains__(self, step_type: str) -> bool:
        """Check if a step type is supported."""
        return step_type in self.get_supported_step_types()


# Global registry instance for convenience
_global_registry = StepExecutorRegistry()


def get_registry() -> StepExecutorRegistry:
    """Get the global step executor registry."""
    return _global_registry


def register_executor(executor: StepExecutor) -> None:
    """Register an executor with the global registry."""
    _global_registry.register(executor)


def find_executor_for_step(step: Step) -> StepExecutor:
    """Find executor for a step using the global registry."""
    return _global_registry.find_executor(step)


def clear_registry() -> None:
    """Clear the global registry (useful for testing)."""
    _global_registry.clear()


def create_default_registry() -> StepExecutorRegistry:
    """Create registry with default step executors.

    This function creates a registry populated with all the standard
    step executors, following the Factory pattern for clean initialization.
    """
    registry = StepExecutorRegistry()

    # Import and register all default step executors
    from .export import ExportStepExecutor
    from .load import LoadStepExecutor
    from .source import SourceStepExecutor
    from .transform import TransformStepExecutor

    # Register step executors
    registry.register(SourceStepExecutor())
    registry.register(LoadStepExecutor())
    registry.register(TransformStepExecutor())
    registry.register(ExportStepExecutor())

    logger.info("Created default step registry with 4 executors")
    return registry


# Alias for backward compatibility
StepRegistry = StepExecutorRegistry
