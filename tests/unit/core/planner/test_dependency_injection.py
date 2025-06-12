"""Tests for planner dependency injection.

Following Zen of Python:
- Explicit is better than implicit: Test all injection scenarios
- Readability counts: Clear test names and descriptions  
- Simple is better than complex: Test one thing at a time
"""

import pytest
from unittest.mock import Mock

from sqlflow.core.planner.factory import PlannerConfig, PlannerFactory
from sqlflow.core.planner.interfaces import IDependencyAnalyzer, IExecutionOrderResolver, IStepBuilder
from sqlflow.core.planner_main import ExecutionPlanBuilder, Planner
from sqlflow.parser.ast import Pipeline


class MockDependencyAnalyzer(IDependencyAnalyzer):
    """Mock dependency analyzer for testing."""
    
    def analyze(self, pipeline, step_id_map):
        return {"step1": [], "step2": ["step1"]}
    
    def get_undefined_table_references(self):
        return []


class MockOrderResolver(IExecutionOrderResolver):
    """Mock order resolver for testing."""
    
    def resolve(self, step_dependencies, all_step_ids):
        return ["step1", "step2"]


class MockStepBuilder(IStepBuilder):
    """Mock step builder for testing."""
    
    def build_steps(self, pipeline, execution_order, step_id_map, step_dependencies):
        return [
            {"id": "step1", "type": "mock"},
            {"id": "step2", "type": "mock", "depends_on": ["step1"]}
        ]


class TestPlannerConfig:
    """Test PlannerConfig functionality."""
    
    def test_config_creation_with_defaults(self):
        """Config can be created with all defaults."""
        config = PlannerConfig()
        assert config.dependency_analyzer is None
        assert config.order_resolver is None
        assert config.step_builder is None
    
    def test_config_creation_with_partial_injection(self):
        """Config can be created with partial component injection."""
        mock_analyzer = MockDependencyAnalyzer()
        config = PlannerConfig(dependency_analyzer=mock_analyzer)
        
        assert config.dependency_analyzer is mock_analyzer
        assert config.order_resolver is None
        assert config.step_builder is None
    
    def test_config_creation_with_full_injection(self):
        """Config can be created with all components injected."""
        mock_analyzer = MockDependencyAnalyzer()
        mock_resolver = MockOrderResolver()
        mock_builder = MockStepBuilder()
        
        config = PlannerConfig(
            dependency_analyzer=mock_analyzer,
            order_resolver=mock_resolver,
            step_builder=mock_builder
        )
        
        assert config.dependency_analyzer is mock_analyzer
        assert config.order_resolver is mock_resolver
        assert config.step_builder is mock_builder


class TestPlannerFactory:
    """Test PlannerFactory functionality."""
    
    def test_create_dependency_analyzer_default(self):
        """Factory creates default dependency analyzer when none provided."""
        analyzer = PlannerFactory.create_dependency_analyzer()
        assert analyzer is not None
        assert hasattr(analyzer, 'analyze')
    
    def test_create_dependency_analyzer_with_injection(self):
        """Factory uses injected dependency analyzer when provided."""
        mock_analyzer = MockDependencyAnalyzer()
        analyzer = PlannerFactory.create_dependency_analyzer(mock_analyzer)
        assert analyzer is mock_analyzer
    
    def test_create_order_resolver_default(self):
        """Factory creates default order resolver when none provided."""
        resolver = PlannerFactory.create_order_resolver()
        assert resolver is not None
        assert hasattr(resolver, 'resolve')
    
    def test_create_order_resolver_with_injection(self):
        """Factory uses injected order resolver when provided."""
        mock_resolver = MockOrderResolver()
        resolver = PlannerFactory.create_order_resolver(mock_resolver)
        assert resolver is mock_resolver
    
    def test_create_step_builder_default(self):
        """Factory creates default step builder when none provided."""
        builder = PlannerFactory.create_step_builder()
        assert builder is not None
        assert hasattr(builder, 'build_steps')
    
    def test_create_step_builder_with_injection(self):
        """Factory uses injected step builder when provided."""
        mock_builder = MockStepBuilder()
        builder = PlannerFactory.create_step_builder(mock_builder)
        assert builder is mock_builder
    
    def test_create_components_from_config_default(self):
        """Factory creates all default components when no config provided."""
        analyzer, resolver, builder = PlannerFactory.create_components_from_config()
        
        assert analyzer is not None
        assert resolver is not None  
        assert builder is not None
        assert hasattr(analyzer, 'analyze')
        assert hasattr(resolver, 'resolve')
        assert hasattr(builder, 'build_steps')
    
    def test_create_components_from_config_with_injection(self):
        """Factory uses injected components when config provided."""
        mock_analyzer = MockDependencyAnalyzer()
        mock_resolver = MockOrderResolver()
        mock_builder = MockStepBuilder()
        
        config = PlannerConfig(
            dependency_analyzer=mock_analyzer,
            order_resolver=mock_resolver,
            step_builder=mock_builder
        )
        
        analyzer, resolver, builder = PlannerFactory.create_components_from_config(config)
        
        assert analyzer is mock_analyzer
        assert resolver is mock_resolver
        assert builder is mock_builder


class TestExecutionPlanBuilderInjection:
    """Test ExecutionPlanBuilder dependency injection."""
    
    def test_default_construction(self):
        """ExecutionPlanBuilder works with default construction."""
        builder = ExecutionPlanBuilder()
        
        assert builder._dependency_analyzer is not None
        assert builder._order_resolver is not None
        assert builder._step_builder is not None
        assert hasattr(builder._dependency_analyzer, 'analyze')
        assert hasattr(builder._order_resolver, 'resolve')
        assert hasattr(builder._step_builder, 'build_steps')
    
    def test_individual_component_injection(self):
        """ExecutionPlanBuilder works with individual component injection."""
        mock_analyzer = MockDependencyAnalyzer()
        mock_resolver = MockOrderResolver()
        mock_builder = MockStepBuilder()
        
        builder = ExecutionPlanBuilder(
            dependency_analyzer=mock_analyzer,
            order_resolver=mock_resolver,
            step_builder=mock_builder
        )
        
        assert builder._dependency_analyzer is mock_analyzer
        assert builder._order_resolver is mock_resolver
        assert builder._step_builder is mock_builder
    
    def test_partial_component_injection(self):
        """ExecutionPlanBuilder works with partial component injection."""
        mock_analyzer = MockDependencyAnalyzer()
        
        builder = ExecutionPlanBuilder(dependency_analyzer=mock_analyzer)
        
        assert builder._dependency_analyzer is mock_analyzer
        assert builder._order_resolver is not None  # Should be default
        assert builder._step_builder is not None    # Should be default
    
    def test_config_based_injection(self):
        """ExecutionPlanBuilder works with config-based injection."""
        mock_analyzer = MockDependencyAnalyzer()
        mock_resolver = MockOrderResolver()
        mock_builder = MockStepBuilder()
        
        config = PlannerConfig(
            dependency_analyzer=mock_analyzer,
            order_resolver=mock_resolver,
            step_builder=mock_builder
        )
        
        builder = ExecutionPlanBuilder(config=config)
        
        assert builder._dependency_analyzer is mock_analyzer
        assert builder._order_resolver is mock_resolver
        assert builder._step_builder is mock_builder
    
    def test_config_overrides_individual_components(self):
        """Config-based injection overrides individual component parameters."""
        mock_analyzer1 = MockDependencyAnalyzer()
        mock_analyzer2 = MockDependencyAnalyzer()
        
        config = PlannerConfig(dependency_analyzer=mock_analyzer2)
        
        builder = ExecutionPlanBuilder(
            dependency_analyzer=mock_analyzer1,  # Should be overridden
            config=config
        )
        
        assert builder._dependency_analyzer is mock_analyzer2


class TestPlannerInjection:
    """Test Planner dependency injection."""
    
    def test_default_construction(self):
        """Planner works with default construction."""
        planner = Planner()
        
        assert planner.builder is not None
        assert isinstance(planner.builder, ExecutionPlanBuilder)
    
    def test_builder_injection(self):
        """Planner works with builder injection."""
        mock_builder = Mock(spec=ExecutionPlanBuilder)
        planner = Planner(builder=mock_builder)
        
        assert planner.builder is mock_builder
    
    def test_config_based_injection(self):
        """Planner works with config-based injection."""
        mock_analyzer = MockDependencyAnalyzer()
        config = PlannerConfig(dependency_analyzer=mock_analyzer)
        
        planner = Planner(config=config)
        
        assert planner.builder is not None
        assert planner.builder._dependency_analyzer is mock_analyzer


class TestBackwardCompatibility:
    """Test backward compatibility of dependency injection."""
    
    def test_existing_code_still_works(self):
        """Existing code that doesn't use injection still works."""
        # This is how code currently constructs planners
        builder = ExecutionPlanBuilder()
        planner = Planner()
        
        # Should work without any issues
        assert builder is not None
        assert planner is not None
        assert planner.builder is not None
    
    def test_injection_is_optional(self):
        """All injection parameters are optional."""
        # All these should work without errors
        ExecutionPlanBuilder()
        ExecutionPlanBuilder(dependency_analyzer=None)
        ExecutionPlanBuilder(order_resolver=None)
        ExecutionPlanBuilder(step_builder=None)
        ExecutionPlanBuilder(config=None)
        
        Planner()
        Planner(builder=None)
        Planner(config=None)
    
    def test_mixed_construction_patterns(self):
        """Different construction patterns can be mixed."""
        # Factory-created components
        analyzer = PlannerFactory.create_dependency_analyzer()
        
        # Individual injection
        builder1 = ExecutionPlanBuilder(dependency_analyzer=analyzer)
        
        # Config-based injection
        config = PlannerConfig(dependency_analyzer=analyzer)
        builder2 = ExecutionPlanBuilder(config=config)
        
        # Planner with different injection patterns
        planner1 = Planner(builder=builder1)
        planner2 = Planner(config=config)
        
        assert all(p is not None for p in [builder1, builder2, planner1, planner2])


class TestTestability:
    """Test that dependency injection improves testability."""
    
    def test_can_inject_mock_components(self):
        """Mock components can be injected for testing."""
        mock_analyzer = Mock(spec=IDependencyAnalyzer)
        mock_resolver = Mock(spec=IExecutionOrderResolver)
        mock_builder = Mock(spec=IStepBuilder)
        
        # Should work without errors
        ExecutionPlanBuilder(
            dependency_analyzer=mock_analyzer,
            order_resolver=mock_resolver,
            step_builder=mock_builder
        )
    
    def test_can_inject_test_doubles(self):
        """Test double implementations can be injected."""
        test_analyzer = MockDependencyAnalyzer()
        test_resolver = MockOrderResolver()
        test_builder = MockStepBuilder()
        
        builder = ExecutionPlanBuilder(
            dependency_analyzer=test_analyzer,
            order_resolver=test_resolver,
            step_builder=test_builder
        )
        
        # Components should be the injected test doubles
        assert builder._dependency_analyzer is test_analyzer
        assert builder._order_resolver is test_resolver
        assert builder._step_builder is test_builder
    
    def test_config_enables_easy_test_setup(self):
        """PlannerConfig makes test setup easier."""
        config = PlannerConfig(
            dependency_analyzer=MockDependencyAnalyzer(),
            order_resolver=MockOrderResolver(),
            step_builder=MockStepBuilder()
        )
        
        # Easy to create multiple instances with same test config
        builder1 = ExecutionPlanBuilder(config=config)
        builder2 = ExecutionPlanBuilder(config=config)
        planner = Planner(config=config)
        
        assert all(b is not None for b in [builder1, builder2, planner])


# Integration tests would go here but require more complex setup
# These focus on the dependency injection mechanism itself 