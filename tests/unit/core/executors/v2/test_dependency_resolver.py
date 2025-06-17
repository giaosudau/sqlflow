"""Tests for V2 Dependency Resolution Engine.

Following Kent Beck's TDD principles:
- Test behavior, not implementation
- Clear test names that explain scenarios
- Minimal mocking, real implementations
- Tests as documentation
"""

import pytest

from sqlflow.core.executors.v2.dependency_resolver import (
    ConcurrencyAnalyzer,
    DependencyGraph,
    analyze_dependencies,
    plan_concurrent_execution,
)
from sqlflow.core.executors.v2.steps import BaseStep


class TestDependencyGraph:
    """Test dependency graph creation and validation."""

    def test_creates_graph_from_simple_steps(self):
        """Should create dependency graph from list of steps."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
            BaseStep(id="step3", type="export", depends_on=["step2"]),
        ]

        graph = DependencyGraph.from_steps(steps)

        assert len(graph.steps) == 3
        assert graph.dependencies["step2"] == {"step1"}
        assert graph.dependencies["step3"] == {"step2"}
        assert graph.dependents["step1"] == {"step2"}
        assert graph.dependents["step2"] == {"step3"}

    def test_handles_steps_with_no_dependencies(self):
        """Should handle steps that don't depend on anything."""
        steps = [
            BaseStep(id="independent1", type="load"),
            BaseStep(id="independent2", type="load"),
        ]

        graph = DependencyGraph.from_steps(steps)

        assert len(graph.steps) == 2
        assert len(graph.dependencies) == 0
        assert len(graph.dependents) == 0

    def test_handles_multiple_dependencies(self):
        """Should handle steps with multiple dependencies."""
        steps = [
            BaseStep(id="source1", type="load"),
            BaseStep(id="source2", type="load"),
            BaseStep(id="join", type="transform", depends_on=["source1", "source2"]),
        ]

        graph = DependencyGraph.from_steps(steps)

        assert graph.dependencies["join"] == {"source1", "source2"}
        assert graph.dependents["source1"] == {"join"}
        assert graph.dependents["source2"] == {"join"}

    def test_get_ready_steps_with_no_completed(self):
        """Should return steps with no dependencies when nothing completed."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
        ]
        graph = DependencyGraph.from_steps(steps)

        ready = graph.get_ready_steps(set())

        assert ready == ["step1"]

    def test_get_ready_steps_after_completion(self):
        """Should return dependent steps after dependencies complete."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
            BaseStep(id="step3", type="export", depends_on=["step2"]),
        ]
        graph = DependencyGraph.from_steps(steps)

        ready = graph.get_ready_steps({"step1"})

        assert ready == ["step2"]

    def test_validate_acyclic_with_valid_graph(self):
        """Should pass validation for acyclic dependency graph."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
            BaseStep(id="step3", type="export", depends_on=["step2"]),
        ]
        graph = DependencyGraph.from_steps(steps)

        # Should not raise exception
        graph.validate_acyclic()

    def test_validate_acyclic_detects_simple_cycle(self):
        """Should detect and reject simple circular dependencies."""
        steps = [
            BaseStep(id="step1", type="transform", depends_on=["step2"]),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
        ]
        graph = DependencyGraph.from_steps(steps)

        with pytest.raises(ValueError, match="Circular dependency detected"):
            graph.validate_acyclic()

    def test_validate_acyclic_detects_complex_cycle(self):
        """Should detect circular dependencies in complex graphs."""
        steps = [
            BaseStep(id="step1", type="transform", depends_on=["step3"]),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
            BaseStep(id="step3", type="transform", depends_on=["step2"]),
        ]
        graph = DependencyGraph.from_steps(steps)

        with pytest.raises(ValueError, match="Circular dependency detected"):
            graph.validate_acyclic()


class TestConcurrencyAnalyzer:
    """Test concurrency analysis for optimal parallel execution."""

    def test_single_execution_level_for_sequential_steps(self):
        """Should create single execution level for sequential dependencies."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
            BaseStep(id="step3", type="export", depends_on=["step2"]),
        ]
        graph = DependencyGraph.from_steps(steps)
        analyzer = ConcurrencyAnalyzer(graph)

        levels = analyzer.get_execution_levels()

        assert levels == [["step1"], ["step2"], ["step3"]]

    def test_parallel_execution_levels_for_independent_steps(self):
        """Should group independent steps into same execution level."""
        steps = [
            BaseStep(id="load1", type="load"),
            BaseStep(id="load2", type="load"),
            BaseStep(id="load3", type="load"),
            BaseStep(
                id="join", type="transform", depends_on=["load1", "load2", "load3"]
            ),
        ]
        graph = DependencyGraph.from_steps(steps)
        analyzer = ConcurrencyAnalyzer(graph)

        levels = analyzer.get_execution_levels()

        assert len(levels) == 2
        assert set(levels[0]) == {"load1", "load2", "load3"}
        assert levels[1] == ["join"]

    def test_mixed_parallel_and_sequential_execution(self):
        """Should handle complex graphs with mixed parallelism."""
        steps = [
            BaseStep(id="source1", type="load"),
            BaseStep(id="source2", type="load"),
            BaseStep(id="transform1", type="transform", depends_on=["source1"]),
            BaseStep(id="transform2", type="transform", depends_on=["source2"]),
            BaseStep(
                id="final", type="export", depends_on=["transform1", "transform2"]
            ),
        ]
        graph = DependencyGraph.from_steps(steps)
        analyzer = ConcurrencyAnalyzer(graph)

        levels = analyzer.get_execution_levels()

        assert len(levels) == 3
        assert set(levels[0]) == {"source1", "source2"}
        assert set(levels[1]) == {"transform1", "transform2"}
        assert levels[2] == ["final"]

    def test_estimate_parallelism_metrics(self):
        """Should provide accurate parallelism metrics."""
        steps = [
            BaseStep(id="load1", type="load"),
            BaseStep(id="load2", type="load"),
            BaseStep(id="transform", type="transform", depends_on=["load1", "load2"]),
        ]
        graph = DependencyGraph.from_steps(steps)
        analyzer = ConcurrencyAnalyzer(graph)

        metrics = analyzer.estimate_parallelism()

        assert metrics["total_steps"] == 3.0
        assert metrics["execution_levels"] == 2.0
        assert metrics["max_parallel_steps"] == 2.0
        assert metrics["sequential_bottlenecks"] == 1.0  # transform step is sequential
        assert metrics["parallelism_factor"] == 1.5  # (2+1)/2 levels

    def test_single_step_parallelism_metrics(self):
        """Should handle single step case correctly."""
        steps = [BaseStep(id="only_step", type="load")]
        graph = DependencyGraph.from_steps(steps)
        analyzer = ConcurrencyAnalyzer(graph)

        metrics = analyzer.estimate_parallelism()

        assert metrics["total_steps"] == 1.0
        assert metrics["execution_levels"] == 1.0
        assert metrics["max_parallel_steps"] == 1.0
        assert metrics["parallelism_factor"] == 1.0


class TestConvenienceFunctions:
    """Test high-level convenience functions."""

    def test_analyze_dependencies_validates_graph(self):
        """Should create and validate dependency graph."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
        ]

        graph = analyze_dependencies(steps)

        assert len(graph.steps) == 2
        assert graph.dependencies["step2"] == {"step1"}

    def test_analyze_dependencies_rejects_cycles(self):
        """Should reject graphs with circular dependencies."""
        steps = [
            BaseStep(id="step1", type="transform", depends_on=["step2"]),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
        ]

        with pytest.raises(ValueError, match="Circular dependency detected"):
            analyze_dependencies(steps)

    def test_plan_concurrent_execution_returns_levels(self):
        """Should return execution levels for concurrent planning."""
        steps = [
            BaseStep(id="load1", type="load"),
            BaseStep(id="load2", type="load"),
            BaseStep(id="join", type="transform", depends_on=["load1", "load2"]),
        ]

        levels = plan_concurrent_execution(steps)

        assert len(levels) == 2
        assert set(levels[0]) == {"load1", "load2"}
        assert levels[1] == ["join"]

    def test_empty_steps_list(self):
        """Should handle empty steps list gracefully."""
        graph = analyze_dependencies([])

        assert len(graph.steps) == 0
        assert len(graph.dependencies) == 0
        assert len(graph.dependents) == 0

    def test_nonexistent_dependency_reference(self):
        """Should handle steps referencing nonexistent dependencies."""
        steps = [
            BaseStep(id="step1", type="transform", depends_on=["nonexistent"]),
        ]

        graph = DependencyGraph.from_steps(steps)
        # Graph creation succeeds, but dependency validation should catch this
        ready = graph.get_ready_steps(set())

        # Step with missing dependency should not be ready
        assert ready == []


# Integration test examples (following testing standards)
class TestDependencyResolverIntegration:
    """Integration tests with real step scenarios."""

    def test_realistic_etl_pipeline_dependencies(self):
        """Should handle realistic ETL pipeline with proper dependencies."""
        steps = [
            # Extract phase - can run in parallel
            BaseStep(id="extract_customers", type="load"),
            BaseStep(id="extract_orders", type="load"),
            BaseStep(id="extract_products", type="load"),
            # Transform phase - depends on extracts
            BaseStep(
                id="clean_customers", type="transform", depends_on=["extract_customers"]
            ),
            BaseStep(
                id="enrich_orders",
                type="transform",
                depends_on=["extract_orders", "extract_products"],
            ),
            # Load phase - depends on transforms
            BaseStep(
                id="load_warehouse",
                type="export",
                depends_on=["clean_customers", "enrich_orders"],
            ),
        ]

        levels = plan_concurrent_execution(steps)

        # Verify optimal execution plan
        assert len(levels) == 3
        assert set(levels[0]) == {
            "extract_customers",
            "extract_orders",
            "extract_products",
        }
        assert set(levels[1]) == {
            "clean_customers",
            "enrich_orders",
        }  # Both transforms can run after extracts
        assert levels[2] == ["load_warehouse"]  # Final step after all transforms

    def test_diamond_dependency_pattern(self):
        """Should handle diamond dependency pattern correctly."""
        steps = [
            BaseStep(id="source", type="load"),
            BaseStep(id="branch1", type="transform", depends_on=["source"]),
            BaseStep(id="branch2", type="transform", depends_on=["source"]),
            BaseStep(id="merge", type="export", depends_on=["branch1", "branch2"]),
        ]

        levels = plan_concurrent_execution(steps)

        assert len(levels) == 3
        assert levels[0] == ["source"]
        assert set(levels[1]) == {"branch1", "branch2"}
        assert levels[2] == ["merge"]
