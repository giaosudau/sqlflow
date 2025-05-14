"""Tests for the dependency resolver."""

import pytest

from sqlflow.sqlflow.core.dependencies import DependencyResolver
from sqlflow.sqlflow.core.errors import CircularDependencyError


class TestDependencyResolver:
    """Test cases for the DependencyResolver class."""

    def test_simple_dependency(self):
        """Test that simple dependencies are resolved correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_b")
        assert order == ["pipeline_a", "pipeline_b"]

    def test_multiple_dependencies(self):
        """Test that multiple dependencies are resolved correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_c", "pipeline_a")
        resolver.add_dependency("pipeline_c", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_c")
        assert order == ["pipeline_a", "pipeline_b", "pipeline_c"]

    def test_simple_cycle_detection(self):
        """Test that a simple cycle is detected correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        with pytest.raises(CircularDependencyError) as excinfo:
            resolver.resolve_dependencies("pipeline_a")

        cycle = excinfo.value.cycle
        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert len(cycle) == 3  # [pipeline_a, pipeline_b, pipeline_a]

    def test_complex_cycle_detection(self):
        """Test that a complex cycle is detected correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_c")
        resolver.add_dependency("pipeline_c", "pipeline_a")

        with pytest.raises(CircularDependencyError) as excinfo:
            resolver.resolve_dependencies("pipeline_a")

        cycle = excinfo.value.cycle
        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert "pipeline_c" in cycle
        assert len(cycle) == 4  # [pipeline_a, pipeline_b, pipeline_c, pipeline_a]

    def test_cycle_with_multiple_dependencies(self):
        """Test that a cycle is detected correctly when there are multiple dependencies."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_a", "pipeline_c")
        resolver.add_dependency("pipeline_b", "pipeline_d")
        resolver.add_dependency("pipeline_d", "pipeline_a")

        with pytest.raises(CircularDependencyError) as excinfo:
            resolver.resolve_dependencies("pipeline_a")

        cycle = excinfo.value.cycle
        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert "pipeline_d" in cycle
        assert len(cycle) == 4

    def test_no_cycle(self):
        """Test that no cycle is detected when there isn't one."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_a", "pipeline_c")
        resolver.add_dependency("pipeline_b", "pipeline_d")

        order = resolver.resolve_dependencies("pipeline_a")
        assert order == ["pipeline_d", "pipeline_b", "pipeline_c", "pipeline_a"]

    def test_find_cycle_method(self):
        """Test the _find_cycle method directly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_c")
        resolver.add_dependency("pipeline_c", "pipeline_a")

        resolver.temp_visited = {"pipeline_a", "pipeline_b", "pipeline_c"}

        cycle = resolver._find_cycle("pipeline_a")

        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert "pipeline_c" in cycle
        assert cycle[-1] == "pipeline_a"
