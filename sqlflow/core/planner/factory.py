"""Factory for creating planner components with dependency injection.

Following Zen of Python:
- Simple is better than complex: Optional injection keeps things simple
- Explicit is better than implicit: Clear interfaces for testability
- Practicality beats purity: Backward compatibility maintained
"""

from typing import Optional

from sqlflow.logging import get_logger

from .dependency_analyzer import DependencyAnalyzer
from .interfaces import IDependencyAnalyzer, IExecutionOrderResolver, IStepBuilder
from .order_resolver import ExecutionOrderResolver
from .step_builder import StepBuilder

logger = get_logger(__name__)


class PlannerConfig:
    """Configuration for planner component dependencies.
    
    Simple configuration object that allows for optional component injection.
    Following Zen of Python: Simple is better than complex.
    """

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
    """Factory for creating planner components with optional dependency injection.
    
    Following Zen of Python:
    - Simple is better than complex: Default components for simple usage
    - Explicit is better than implicit: Clear injection points for testing
    - Practicality beats purity: Both injection and default construction work
    """

    @staticmethod
    def create_dependency_analyzer(
        custom_analyzer: Optional[IDependencyAnalyzer] = None,
    ) -> IDependencyAnalyzer:
        """Create dependency analyzer with optional injection.
        
        Args:
            custom_analyzer: Optional custom implementation
            
        Returns:
            IDependencyAnalyzer instance
        """
        if custom_analyzer is not None:
            logger.debug("Using injected dependency analyzer")
            return custom_analyzer
        
        logger.debug("Creating default dependency analyzer")
        return DependencyAnalyzer()

    @staticmethod
    def create_order_resolver(
        custom_resolver: Optional[IExecutionOrderResolver] = None,
    ) -> IExecutionOrderResolver:
        """Create execution order resolver with optional injection.
        
        Args:
            custom_resolver: Optional custom implementation
            
        Returns:
            IExecutionOrderResolver instance
        """
        if custom_resolver is not None:
            logger.debug("Using injected execution order resolver")
            return custom_resolver
        
        logger.debug("Creating default execution order resolver")
        return ExecutionOrderResolver()

    @staticmethod
    def create_step_builder(
        custom_builder: Optional[IStepBuilder] = None,
    ) -> IStepBuilder:
        """Create step builder with optional injection.
        
        Args:
            custom_builder: Optional custom implementation
            
        Returns:
            IStepBuilder instance
        """
        if custom_builder is not None:
            logger.debug("Using injected step builder")
            return custom_builder
        
        logger.debug("Creating default step builder")
        return StepBuilder()

    @staticmethod
    def create_components_from_config(
        config: Optional[PlannerConfig] = None,
    ) -> tuple[IDependencyAnalyzer, IExecutionOrderResolver, IStepBuilder]:
        """Create all planner components from optional configuration.
        
        Args:
            config: Optional configuration with injected components
            
        Returns:
            Tuple of (dependency_analyzer, order_resolver, step_builder)
        """
        if config is None:
            logger.debug("Creating default planner components")
            return (
                DependencyAnalyzer(),
                ExecutionOrderResolver(),
                StepBuilder(),
            )
        
        logger.debug("Creating planner components from configuration")
        return (
            PlannerFactory.create_dependency_analyzer(config.dependency_analyzer),
            PlannerFactory.create_order_resolver(config.order_resolver),
            PlannerFactory.create_step_builder(config.step_builder),
        ) 