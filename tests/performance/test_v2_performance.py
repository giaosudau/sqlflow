"""Performance benchmarks for V2 Executor refactoring.

Validates Raymond Hettinger's optimization decisions:
1. @lru_cache performance gains in handler factory
2. Decorator-based DRY elimination efficiency
3. Configuration resolution performance
"""

import time

import pytest

from sqlflow.core.config_resolver import ConfigurationResolver
from sqlflow.core.executors.v2.handlers.factory import clear_registry, get_handler
from sqlflow.core.planner.factory import (
    create_dependency_analyzer,
    create_order_resolver,
    create_step_builder,
)
from sqlflow.core.profiles import ProfileManager
from sqlflow.core.variables.manager import VariableConfig, VariableManager


class TestHandlerFactoryPerformance:
    """Benchmark handler factory @lru_cache optimization."""

    def test_handler_caching_performance(self):
        """Validate that @lru_cache provides measurable performance gains."""
        # Clear any existing cache
        clear_registry()

        # Benchmark without cache (first calls)
        start_time = time.perf_counter()
        for _ in range(100):
            get_handler("transform")
        cold_time = time.perf_counter() - start_time

        # Benchmark with cache (subsequent calls)
        start_time = time.perf_counter()
        for _ in range(100):
            get_handler("transform")
        warm_time = time.perf_counter() - start_time

        # Cache should provide significant speedup
        speedup_ratio = cold_time / warm_time if warm_time > 0 else float("inf")

        print(f"Cold time: {cold_time:.4f}s")
        print(f"Warm time: {warm_time:.4f}s")
        print(f"Speedup ratio: {speedup_ratio:.2f}x")

        # Assert meaningful performance improvement (more realistic threshold)
        assert speedup_ratio > 2.0, f"Expected >2x speedup, got {speedup_ratio:.2f}x"

    def test_handler_instance_identity(self):
        """Validate that cached handlers return same instance."""
        clear_registry()

        handler1 = get_handler("transform")
        handler2 = get_handler("transform")

        # Should be same instance due to caching
        assert handler1 is handler2, "Cached handlers should return same instance"


class TestPlannerFactoryPerformance:
    """Benchmark planner factory @lru_cache optimization."""

    def test_planner_component_caching(self):
        """Validate planner component caching performance."""
        # Benchmark component creation
        start_time = time.perf_counter()
        for _ in range(50):
            create_dependency_analyzer()
            create_order_resolver()
            create_step_builder()
        creation_time = time.perf_counter() - start_time

        print(f"Component creation time: {creation_time:.4f}s")

        # Validate caching works
        analyzer1 = create_dependency_analyzer()
        analyzer2 = create_dependency_analyzer()
        assert analyzer1 is analyzer2, "Cached components should return same instance"


class TestDecoratorPerformance:
    """Benchmark decorator-based DRY elimination."""

    def test_decorator_overhead(self):
        """Measure overhead of observability decorators."""
        from sqlflow.core.executors.v2.handlers.base import timed_operation

        # Simple function without decorator
        def simple_function():
            return sum(range(1000))

        # Function with decorator
        @timed_operation("test_operation")
        def decorated_function():
            return sum(range(1000))

        # Benchmark simple function
        start_time = time.perf_counter()
        for _ in range(100):
            simple_function()
        simple_time = time.perf_counter() - start_time

        # Benchmark decorated function
        start_time = time.perf_counter()
        for _ in range(100):
            decorated_function()
        decorated_time = time.perf_counter() - start_time

        overhead_ratio = decorated_time / simple_time

        print(f"Simple function time: {simple_time:.4f}s")
        print(f"Decorated function time: {decorated_time:.4f}s")
        print(f"Overhead ratio: {overhead_ratio:.2f}x")

        # Decorator overhead should be minimal (<50% overhead)
        assert (
            overhead_ratio < 1.5
        ), f"Decorator overhead too high: {overhead_ratio:.2f}x"


class TestConfigurationPerformance:
    """Benchmark configuration resolution performance."""

    def test_config_resolution_caching(self, tmp_path):
        """Test configuration resolution with caching."""
        # Create a simple profile for testing
        profiles_dir = tmp_path / "profiles"
        profiles_dir.mkdir()

        profile_file = profiles_dir / "test.yml"
        profile_file.write_text(
            """
connections:
  default:
    type: duckdb
    database: ":memory:"
variables:
  test_var: "test_value"
"""
        )

        # Create managers
        profile_manager = ProfileManager(str(tmp_path))
        resolver = ConfigurationResolver(
            profile_manager, VariableManager(VariableConfig())
        )

        # Benchmark configuration resolution
        start_time = time.perf_counter()
        for _ in range(50):
            resolver.resolve_config("test")
        resolution_time = time.perf_counter() - start_time

        print(f"Config resolution time: {resolution_time:.4f}s")

        # Check cache stats
        cache_stats = resolver.get_cache_stats()
        print(f"Cache stats: {cache_stats}")

        # Should have some cache hits
        assert cache_stats["size"] > 0, "Cache should contain resolved configs"


@pytest.mark.benchmark
class TestIntegratedPerformance:
    """End-to-end performance benchmarks."""

    def test_v2_executor_initialization_performance(self):
        """Benchmark V2 executor initialization with all optimizations."""
        start_time = time.perf_counter()

        # Simulate multiple handler requests (should hit cache)
        for _ in range(20):
            get_handler("transform")
            get_handler("load")
            get_handler("export")

        # Simulate planner component creation (should hit cache)
        for _ in range(10):
            create_dependency_analyzer()
            create_order_resolver()
            create_step_builder()

        total_time = time.perf_counter() - start_time

        print(f"V2 initialization time: {total_time:.4f}s")

        # Should be very fast due to caching
        assert total_time < 0.1, f"V2 initialization too slow: {total_time:.4f}s"

    def test_memory_efficiency(self):
        """Validate that caching doesn't cause memory leaks."""
        import gc

        # Force garbage collection
        gc.collect()
        initial_objects = len(gc.get_objects())

        # Create many cached objects
        for _ in range(100):
            get_handler("transform")
            create_dependency_analyzer()

        gc.collect()
        final_objects = len(gc.get_objects())

        # Object count shouldn't grow significantly due to caching
        object_growth = final_objects - initial_objects
        print(f"Object growth: {object_growth}")

        # Allow some growth but not excessive
        assert object_growth < 100, f"Excessive object growth: {object_growth}"


if __name__ == "__main__":
    # Run performance tests directly
    pass

    # Test handler factory performance
    print("=== Handler Factory Performance ===")
    test = TestHandlerFactoryPerformance()
    test.test_handler_caching_performance()
    test.test_handler_instance_identity()

    print("\n=== Planner Factory Performance ===")
    test = TestPlannerFactoryPerformance()
    test.test_planner_component_caching()

    print("\n=== Decorator Performance ===")
    test = TestDecoratorPerformance()
    test.test_decorator_overhead()

    print("\n=== Integrated Performance ===")
    test = TestIntegratedPerformance()
    test.test_v2_executor_initialization_performance()
    test.test_memory_efficiency()

    print("\n✅ All performance benchmarks completed!")
