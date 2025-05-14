"""Integration tests for cycle detection in SQLFlow pipelines."""

import os
import tempfile

import pytest

from sqlflow.sqlflow.core.dependencies import DependencyResolver
from sqlflow.sqlflow.core.errors import CircularDependencyError


def test_cycle_detection_integration():
    """Test that cycle detection works correctly in an integration scenario."""
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

    assert "Circular dependency detected:" in str(excinfo.value)
    assert "pipeline_a -> pipeline_b -> pipeline_c -> pipeline_a" in str(excinfo.value)
