"""Simple factory functions for planner components.

Raymond Hettinger: Simplify factory patterns. Functions are first-class citizens.
Replace complex DI classes with cached factory functions.
"""

from functools import lru_cache
from typing import Optional

from sqlflow.logging import get_logger

from .dependency_analyzer import DependencyAnalyzer
from .interfaces import IDependencyAnalyzer, IExecutionOrderResolver, IStepBuilder
from .order_resolver import ExecutionOrderResolver
from .step_builder import StepBuilder

logger = get_logger(__name__)


# Raymond Hettinger: Simple cached factory functions
@lru_cache(maxsize=8)
def create_dependency_analyzer() -> DependencyAnalyzer:
    """Create dependency analyzer - cached for performance.

    Raymond Hettinger: Built-in, tested, optimized.
    """
    logger.debug("Creating dependency analyzer")
    return DependencyAnalyzer()


@lru_cache(maxsize=8)
def create_order_resolver() -> ExecutionOrderResolver:
    """Create execution order resolver - cached for performance."""
    logger.debug("Creating execution order resolver")
    return ExecutionOrderResolver()


@lru_cache(maxsize=8)
def create_step_builder() -> StepBuilder:
    """Create step builder - cached for performance."""
    logger.debug("Creating step builder")
    return StepBuilder()


def create_planner_components() -> (
    tuple[DependencyAnalyzer, ExecutionOrderResolver, StepBuilder]
):
    """Create all planner components.

    Raymond Hettinger: Simple function composition over complex configuration.
    """
    logger.debug("Creating planner components")
    return (
        create_dependency_analyzer(),
        create_order_resolver(),
        create_step_builder(),
    )


# Backward compatibility for existing code
class PlannerConfig:
    """Legacy configuration object for backward compatibility."""

    def __init__(
        self,
        dependency_analyzer: Optional[IDependencyAnalyzer] = None,
        order_resolver: Optional[IExecutionOrderResolver] = None,
        step_builder: Optional[IStepBuilder] = None,
    ):
        self.dependency_analyzer = dependency_analyzer
        self.order_resolver = order_resolver
        self.step_builder = step_builder


class PlannerFactory:
    """Legacy factory wrapper for backward compatibility."""

    @staticmethod
    def create_dependency_analyzer(
        custom_analyzer: Optional[IDependencyAnalyzer] = None,
    ) -> IDependencyAnalyzer:
        return custom_analyzer or create_dependency_analyzer()

    @staticmethod
    def create_order_resolver(
        custom_resolver: Optional[IExecutionOrderResolver] = None,
    ) -> IExecutionOrderResolver:
        return custom_resolver or create_order_resolver()

    @staticmethod
    def create_step_builder(
        custom_builder: Optional[IStepBuilder] = None,
    ) -> IStepBuilder:
        return custom_builder or create_step_builder()

    @staticmethod
    def create_components_from_config(
        config: Optional[PlannerConfig] = None,
    ) -> tuple[IDependencyAnalyzer, IExecutionOrderResolver, IStepBuilder]:
        if config is None:
            return create_planner_components()

        return (
            config.dependency_analyzer or create_dependency_analyzer(),
            config.order_resolver or create_order_resolver(),
            config.step_builder or create_step_builder(),
        )
