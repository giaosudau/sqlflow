"""Tests for Step Executor Registry - Clean Architecture.

Following SQLFlow testing standards:
- Focus on observable behaviors, not implementation details
- Use real implementations where possible
- Test both positive and error scenarios
- Clear, descriptive test names
"""

from unittest.mock import Mock

import pytest

from sqlflow.core.executors.v2.protocols.core import ExecutionContext, Step, StepResult
from sqlflow.core.executors.v2.steps.definitions import (
    ExportStep,
    LoadStep,
    TransformStep,
)
from sqlflow.core.executors.v2.steps.registry import (
    StepExecutorRegistry,
    create_default_registry,
)


class MockStepExecutor:
    """Mock step executor for testing."""

    def __init__(self, step_type: str):
        self.step_type = step_type

    def can_execute(self, step: Step) -> bool:
        return hasattr(step, "step_type") and step.step_type == self.step_type

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        return StepResult(step_id=step.id, success=True, duration_ms=100.0)


class TestStepExecutorRegistry:
    """Test the clean architecture step registry."""

    def test_registry_initialization(self):
        """Test that step registry initializes correctly."""
        registry = StepExecutorRegistry()

        # Registry should start empty
        assert len(registry._executors) == 0
        assert len(registry._type_cache) == 0

    def test_register_step_executor(self):
        """Test registering a step executor."""
        registry = StepExecutorRegistry()
        executor = MockStepExecutor("load")

        # Register executor
        registry.register(executor)

        # Verify registration
        assert len(registry._executors) == 1
        assert executor in registry._executors

    def test_register_invalid_executor_raises_error(self):
        """Test registering invalid executor raises error."""
        registry = StepExecutorRegistry()

        # Try to register object that doesn't implement protocol
        invalid_executor = "not an executor"

        with pytest.raises(ValueError, match="must implement StepExecutor protocol"):
            registry.register(invalid_executor)

    def test_find_executor_for_registered_step(self):
        """Test finding executor for registered step type."""
        registry = StepExecutorRegistry()
        load_executor = MockStepExecutor("load")
        registry.register(load_executor)

        # Create test step
        step = LoadStep(id="test_load", source="test.csv", target_table="test_table")

        # Find executor
        found_executor = registry.find_executor(step)

        # Should find the registered executor
        assert found_executor == load_executor

    def test_find_executor_for_unregistered_step_raises_error(self):
        """Test finding executor for unregistered step type raises error."""
        registry = StepExecutorRegistry()

        step = LoadStep(id="test_load", source="test.csv", target_table="test_table")

        # Should raise error for unregistered step type
        with pytest.raises(ValueError, match="No executor found for step"):
            registry.find_executor(step)

    def test_multiple_executors_registration(self):
        """Test registering multiple executors."""
        registry = StepExecutorRegistry()

        load_executor = MockStepExecutor("load")
        transform_executor = MockStepExecutor("transform")

        registry.register(load_executor)
        registry.register(transform_executor)

        # Should have both executors
        assert len(registry._executors) == 2
        assert load_executor in registry._executors
        assert transform_executor in registry._executors

    def test_duplicate_registration_ignored(self):
        """Test that duplicate executor registration is ignored."""
        registry = StepExecutorRegistry()
        executor = MockStepExecutor("load")

        registry.register(executor)
        registry.register(executor)  # Register same executor again

        # Should only have one instance
        assert len(registry._executors) == 1

    def test_cache_behavior(self):
        """Test that executor cache works correctly."""
        registry = StepExecutorRegistry()
        executor = MockStepExecutor("load")
        registry.register(executor)

        step = LoadStep(id="test", source="test.csv", target_table="table")

        # First call should populate cache
        found_executor = registry.find_executor(step)
        assert found_executor == executor

        # Cache should be populated
        assert len(registry._type_cache) > 0

        # Second call should use cache
        found_executor_cached = registry.find_executor(step)
        assert found_executor_cached == executor

    def test_cache_cleared_on_new_registration(self):
        """Test that cache is cleared when new executor is registered."""
        registry = StepExecutorRegistry()
        executor1 = MockStepExecutor("load")
        registry.register(executor1)

        step = LoadStep(id="test", source="test.csv", target_table="table")
        registry.find_executor(step)  # Populate cache

        assert len(registry._type_cache) > 0

        # Register new executor
        executor2 = MockStepExecutor("transform")
        registry.register(executor2)

        # Cache should be cleared
        assert len(registry._type_cache) == 0


class TestDefaultRegistry:
    """Test the default registry factory."""

    def test_create_default_registry(self):
        """Test creating default registry with standard executors."""
        registry = create_default_registry()

        # Should have registered executors
        assert len(registry._executors) > 0

        # Should be able to find executors for standard step types
        load_step = LoadStep(id="test_load", source="test.csv", target_table="table")
        transform_step = TransformStep(
            id="test_transform", sql="SELECT 1", target_table="result"
        )
        export_step = ExportStep(
            id="test_export", source_table="table", destination="output.csv"
        )

        # Should not raise errors
        load_executor = registry.find_executor(load_step)
        transform_executor = registry.find_executor(transform_step)
        export_executor = registry.find_executor(export_step)

        # Executors should be different instances
        assert load_executor != transform_executor
        assert transform_executor != export_executor

    def test_default_registry_executors_work(self):
        """Integration test that default registry executors actually work."""
        registry = create_default_registry()

        # Create test step and context
        step = LoadStep(
            id="integration_test", source="test.csv", target_table="test_table"
        )
        context = Mock()  # Mock context for testing

        # Find and execute
        executor = registry.find_executor(step)

        # This tests that the executor can be called
        # (actual execution behavior is tested in executor-specific tests)
        assert callable(executor.execute)
        assert callable(executor.can_execute)
        assert executor.can_execute(step) is True
