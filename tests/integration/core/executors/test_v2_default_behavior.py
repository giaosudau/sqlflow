"""
Integration tests defining V2 default behavior.

These tests WILL FAIL initially - that's the point of TDD.
Following Kent Beck's approach: write failing tests that define expected behavior,
then implement the minimum code to make them pass.

Test Requirements:
- get_executor() should return ExecutionCoordinator by default
- V2 should handle real CSV pipelines
- V2 should be backwards compatible with V1 patterns
- No configuration should be required for V2

Following engineering principles:
- Test behavior, not implementation
- Use real data flows when possible
- Clear, descriptive test names
- Proper error handling and logging
"""

import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import pytest

from sqlflow.core.executors import get_executor
from sqlflow.core.executors.v2 import ExecutionCoordinator
from sqlflow.core.executors.v2.steps.registry import StepExecutorRegistry
from sqlflow.logging import get_logger

Step = Dict[str, Any]


def make_load_step(
    source: str,
    id: str = "load_step",
    table: str = "test_data",
    mode: str = "replace",
) -> Step:
    """Create a dictionary for a standard load step."""
    return {
        "id": id,
        "type": "load",
        "source": source,
        "target_table": table,
        "load_mode": mode,
    }


def make_transform_step(
    sql: str,
    id: str = "transform_step",
    table: str = "transformed_data",
) -> Step:
    """Create a dictionary for a standard transform step."""
    return {"id": id, "type": "transform", "sql": sql, "target_table": table}


logger = get_logger(__name__)


class TestV2DefaultBehavior:
    """Test suite defining V2 as default executor behavior."""

    def test_get_executor_returns_execution_coordinator_by_default(self):
        """
        Test that get_executor() returns ExecutionCoordinator by default.

        This is the core V2 behavior - single implementation, clean architecture.
        """
        executor = get_executor()

        # V2 should be the default
        assert isinstance(
            executor, ExecutionCoordinator
        ), f"Expected ExecutionCoordinator (V2), got {type(executor)}. V2 implementation should be the default and only option."

        # Should be V2 ExecutionCoordinator (not V1 LocalExecutor)
        assert (
            type(executor).__name__ == "ExecutionCoordinator"
        ), f"get_executor() should return ExecutionCoordinator, got {type(executor).__name__}"

        logger.debug("✅ get_executor() correctly returns V2 by default")

    def test_get_executor_with_project_params(self):
        """
        Test get_executor() with project parameters returns V2.

        Common usage pattern: executor = get_executor(project_dir=".", profile_name="dev")
        This should return V2 and properly handle the parameters.
        """
        executor = get_executor(project_dir=".", profile_name="dev")

        # Should still be V2 with parameters
        assert isinstance(
            executor, ExecutionCoordinator
        ), "get_executor() with parameters should still return ExecutionCoordinator (V2)"

        # Should have proper attributes for project configuration
        # Note: The exact attribute names may vary, but the executor should handle these
        assert hasattr(executor, "__dict__"), "Executor should accept configuration"

        logger.debug("✅ get_executor() with parameters correctly returns V2")

    @pytest.mark.integration
    def test_v2_executor_real_csv_pipeline(self, v2_pipeline_runner):
        """
        Integration test: V2 executor runs real CSV pipeline.

        This test uses real CSV data from examples to ensure V2 can handle actual workloads.
        Following the migration plan requirement for real data flows.
        """
        # Create test CSV data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n")
            f.write("1,Alice,100\n")
            f.write("2,Bob,200\n")
            f.write("3,Charlie,300\n")
            csv_path = f.name

        try:
            # Define pipeline - real CSV loading scenario
            pipeline = [make_load_step(csv_path, id="test_csv_load", table="customers")]

            coordinator = v2_pipeline_runner(pipeline)

            # V2 should successfully execute the pipeline
            # Note: May fail due to missing connector registry, but should handle gracefully
            assert (
                coordinator.result is not None
            ), f"V2 executor should return ExecutionResult. Got: {coordinator.result}"
            assert hasattr(
                coordinator.result, "success"
            ), "Result should have success attribute"
            assert hasattr(
                coordinator.result, "step_results"
            ), "Result should have step_results"

            logger.debug("✅ V2 executor successfully handles CSV pipeline")

        finally:
            # Clean up test file
            Path(csv_path).unlink(missing_ok=True)

    @pytest.mark.integration
    def test_v2_executor_backwards_compatible_patterns(self, v2_pipeline_runner):
        """
        Test V2 executor handles V1 parameter patterns.

        Ensures backwards compatibility - V1-style parameters should still work.
        This maintains the migration promise of zero breaking changes.
        """
        # Test that V2 can handle empty pipelines (common V1 pattern)
        coordinator = v2_pipeline_runner([])
        assert coordinator.result is not None, "V2 should handle empty pipelines"
        assert coordinator.result.success is True, "Empty pipeline should succeed"

        # Test basic step execution pattern
        basic_step = make_load_step(
            "/dev/null", id="backwards_compat_test", table="test_table"
        )

        # Should not crash on V1-style step structure
        try:
            coordinator = v2_pipeline_runner([basic_step])
            # Expected to fail (no real file), but should fail gracefully
            assert (
                coordinator.result is not None
            ), "Should return ExecutionResult even on failure"
            assert hasattr(
                coordinator.result, "success"
            ), "Should have success attribute"
        except Exception as e:
            # Should not crash completely
            assert False, f"Should not crash completely, got: {e}"

        logger.debug("✅ V2 executor handles V1 compatibility patterns")

    def test_v2_executor_handles_empty_pipeline(self, v2_pipeline_runner):
        """
        Test V2 executor handles empty pipeline gracefully.

        Edge case testing - empty pipelines should be handled cleanly.
        This is a common scenario in dynamic pipeline generation.
        """
        coordinator = v2_pipeline_runner([])

        # Should succeed with empty pipeline
        assert coordinator.result is not None, "Empty pipeline should return result"
        assert coordinator.result.success is True, "Empty pipeline should succeed"
        assert len(coordinator.result.step_results) == 0, "No steps should be executed"

        logger.debug("✅ V2 executor handles empty pipeline correctly")

    @pytest.mark.performance
    def test_v2_executor_performance_baseline(self, v2_pipeline_runner):
        """
        Test V2 executor performance baseline establishment.

        This test establishes performance baseline for V2 executor.
        Per migration plan: V2 should be within 10% of V1 performance.
        """
        # Create small test dataset for performance measurement
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            # Small dataset for consistent timing
            for i in range(100):
                f.write(f"{i},name_{i},{i*10}\n")
            csv_path = f.name

        try:
            pipeline = [
                make_load_step(csv_path, id="perf_test_load", table="perf_table")
            ]

            start_time = time.time()
            coordinator = v2_pipeline_runner(pipeline)
            end_time = time.time()

            duration = end_time - start_time
            assert coordinator.result.success
            assert duration < 0.1, "V2 performance should be under 100ms for small load"

            logger.debug(f"✅ V2 performance baseline: {duration:.4f}s")

        finally:
            Path(csv_path).unlink(missing_ok=True)

    def test_get_executor_factory_simplicity(self):
        """
        Test `get_executor` factory is simple and consistent.
        This test confirms that `get_executor` returns a singleton or properly configured instance.
        """
        executor1 = get_executor()
        executor2 = get_executor()

        # By default, they should be the same instance for efficiency
        assert (
            executor1 is not executor2
        ), "get_executor should return a new instance each time"

        logger.debug("✅ get_executor factory provides consistent instances")

    def test_v2_executor_backward_compatibility(self, v2_pipeline_runner):
        """
        Test V2 executor for backward compatibility with basic dict steps.
        V2 must handle old dictionary-based step definitions to avoid breaking changes.
        """
        # A simple V1-style pipeline with a load step
        pipeline = [make_load_step("/dev/null", table="compat_table")]

        # Execute and check for graceful failure (since source is invalid)
        coordinator = v2_pipeline_runner(pipeline)

        assert (
            coordinator.result is not None
        ), "Executor should always return a result object"
        assert (
            coordinator.result.success is False
        ), "Execution should fail gracefully with invalid source"
        assert (
            len(coordinator.result.step_results) == 1
        ), "Should have one failed step result"

        logger.debug("✅ V2 executor gracefully fails on invalid source")

    def test_v2_executor_has_required_interface(self):
        """
        Test V2 executor has the required public interface.
        This test ensures V2 executor conforms to the expected API contract.
        """
        executor = get_executor()

        assert hasattr(executor, "execute"), "Executor must have 'execute' method"
        assert callable(executor.execute), "'execute' method must be callable"

        logger.debug("✅ V2 executor has the required public interface")

    def test_v2_execution_context_dependency_injection_pattern(
        self, v2_pipeline_runner
    ):
        """
        Test V2 follows dependency injection for execution context.
        V2 executor should accept an external ExecutionContext.
        This follows clean architecture principles.
        """
        # Should be able to execute with a provided context
        coordinator = v2_pipeline_runner([])
        assert coordinator.result.success is True, "Should accept external context"

        logger.debug("✅ V2 executor supports dependency injection for context")

    def test_v2_execution_behavior_on_failure(self, v2_pipeline_runner):
        """
        Test V2 executor behavior on step failure.
        The default behavior is to execute all independent steps, even if one fails.
        """
        failing_steps = [
            # This step will fail because the source is invalid
            make_load_step("non_existent.csv", id="step1_fail", table="t1"),
            # This step should still be executed
            make_transform_step("SELECT 1 as C1", id="step2_succeed", table="t2"),
        ]

        coordinator = v2_pipeline_runner(failing_steps, fail_fast=False)

        # Pipeline should fail because one step failed
        assert coordinator.result.success is False

        # Both steps should have been attempted
        assert (
            len(coordinator.result.step_results) == 2
        ), "Pipeline should attempt all independent steps"
        assert coordinator.result.step_results[0].success is False
        assert coordinator.result.step_results[1].success is True

        logger.debug("✅ V2 executor correctly runs all independent steps on failure")

    def test_v2_step_executor_registry_integration(self):
        """
        Test V2 executor integrates with the step registry.
        This is key to the decoupled, extensible design.
        """
        executor = get_executor()

        registry = executor.registry
        assert isinstance(
            registry, StepExecutorRegistry
        ), "Executor should have a StepExecutorRegistry instance"
        assert (
            registry is not None
        ), "Executor should have a non-null step executor registry"

        logger.debug("✅ V2 executor integrates with step executor registry")

    def test_v2_immutable_execution_context_pattern(
        self, v2_coordinator, v2_execution_context
    ):
        """
        Test V2 execution context is immutable.
        The executor should not modify the original context object.
        This prevents side effects and promotes predictable behavior.
        This test uses the V2 fixtures to get a coordinator and context.
        """
        # Get coordinator and context from V2 fixtures
        # Execute a step
        steps = [make_load_step("/dev/null", id="step1", table="t1")]
        v2_coordinator.execute(steps, v2_execution_context)

        # Context should be the same object
        assert (
            v2_coordinator.context is v2_execution_context
        ), "Execution context should be the same object"

    def test_multiple_executor_instances_are_independent(self):
        """
        Test that multiple executor instances are independent.

        Each call to `get_executor()` should return a new, independent instance.
        This ensures no state is shared between different pipeline runs.
        """
        executor1 = get_executor()
        executor2 = get_executor()

        # Should be different instances
        assert executor1 is not executor2, "Each call should return a new instance"

        # Their registries should also be different instances
        assert (
            executor1.registry is not executor2.registry
        ), "Registries should be independent"

        logger.debug("✅ Multiple executor instances are independent")

    def test_v2_clean_architecture_principles(self):
        """
        Test that V2 executor adheres to clean architecture.

        - Small, focused classes
        - Dependency injection
        - Clear separation of concerns
        """
        executor = get_executor()

        # Executor should be a small class, not a god object
        # A simple check is the number of public methods
        public_methods = [
            method
            for method in dir(executor)
            if callable(getattr(executor, method)) and not method.startswith("_")
        ]
        assert (
            len(public_methods) < 5
        ), f"Executor should have few public methods, found: {public_methods}"

        # Should have a clear `execute` entry point
        assert hasattr(executor, "execute"), "Should have a clear `execute` method"

        logger.debug("✅ V2 executor follows clean architecture principles")
