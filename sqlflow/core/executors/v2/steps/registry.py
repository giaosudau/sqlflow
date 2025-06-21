"""Step Registry following Open/Closed Principle.

Registry pattern for step executors with easy extensibility:
- Open for extension (easy to add new step types)
- Closed for modification (no need to modify existing code)
- Type-safe executor discovery and execution
"""

from typing import List

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step, StepResult
from .base import StepExecutor

logger = get_logger(__name__)


class StepRegistry:
    """Registry of step executors following Open/Closed principle.

    Provides type-safe executor registration and discovery:
    - Easy to add new step types without modifying existing code
    - Clean separation of executor registration from execution logic
    - Comprehensive error handling for unknown step types
    """

    def __init__(self):
        """Initialize empty registry."""
        self._executors: List[StepExecutor] = []

    def register(self, executor: StepExecutor) -> None:
        """Register a step executor.

        Args:
            executor: Step executor that implements StepExecutor protocol

        Example:
            registry.register(LoadStepExecutor())
            registry.register(TransformStepExecutor())
        """
        if not hasattr(executor, "can_execute") or not hasattr(executor, "execute"):
            raise ValueError(
                f"Executor must implement StepExecutor protocol: {type(executor)}"
            )

        self._executors.append(executor)
        logger.debug(f"Registered step executor: {type(executor).__name__}")

    def find_executor(self, step: Step) -> StepExecutor:
        """Find appropriate executor for the given step.

        Args:
            step: The step to find an executor for

        Returns:
            StepExecutor that can handle the step

        Raises:
            ValueError: If no executor can handle the step
        """
        for executor in self._executors:
            if executor.can_execute(step):
                logger.debug(
                    f"Found executor {type(executor).__name__} for step {getattr(step, 'id', 'unknown')}"
                )
                return executor

        # Provide helpful error message
        step_type = getattr(step, "type", "unknown")
        step_id = getattr(step, "id", "unknown")
        available_types = self._get_supported_step_types()

        raise ValueError(
            f"No executor found for step '{step_id}' of type '{step_type}'. "
            f"Available step types: {available_types}"
        )

    def execute_step(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute step using appropriate executor.

        Args:
            step: The step to execute
            context: Execution context

        Returns:
            StepResult from the execution
        """
        executor = self.find_executor(step)
        return executor.execute(step, context)

    def get_registered_executors(self) -> List[StepExecutor]:
        """Get list of all registered executors."""
        return self._executors.copy()

    def clear(self) -> None:
        """Clear all registered executors (primarily for testing)."""
        self._executors.clear()
        logger.debug("Cleared all registered executors")

    def _get_supported_step_types(self) -> List[str]:
        """Get list of supported step types from registered executors."""
        supported_types = []

        # Test with dummy steps to discover supported types
        for step_type in ["load", "transform", "export", "source"]:
            dummy_step = type("DummyStep", (), {"type": step_type})()
            for executor in self._executors:
                if executor.can_execute(dummy_step):
                    supported_types.append(step_type)
                    break

        return supported_types


def create_default_registry() -> StepRegistry:
    """Create registry with default step executors.

    Provides standard executors for common step types.
    Easy to extend by adding new executors.
    """
    from .export import ExportStepExecutor
    from .load import LoadStepExecutor
    from .transform import TransformStepExecutor

    registry = StepRegistry()

    # Register default executors
    registry.register(LoadStepExecutor())
    registry.register(TransformStepExecutor())
    registry.register(ExportStepExecutor())

    logger.info(
        "Created default step registry with Load, Transform, and Export executors"
    )
    return registry
