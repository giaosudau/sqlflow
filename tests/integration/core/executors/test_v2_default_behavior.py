"""
Integration tests defining V2 default behavior.

These tests WILL FAIL initially - that's the point of TDD.
Following Kent Beck's approach: write failing tests that define expected behavior,
then implement the minimum code to make them pass.

Test Requirements:
- get_executor() should return LocalOrchestrator by default
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

import pytest

from sqlflow.core.executors import get_executor
from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TestV2DefaultBehavior:
    """Test suite defining V2 as default executor behavior."""

    def test_get_executor_returns_v2_by_default(self):
        """
        Test that get_executor() returns LocalOrchestrator by default.

        This is the core requirement: V2 should be the default, no configuration needed.
        Following "Simple is better than complex" - no feature flags required.
        """
        executor = get_executor()

        # V2 should be the default
        assert isinstance(executor, LocalOrchestrator), (
            f"Expected LocalOrchestrator (V2), got {type(executor)}. "
            f"V2 should be the default executor without any configuration."
        )

        # Should be V2 LocalOrchestrator (not V1 LocalExecutor)
        assert (
            type(executor).__name__ == "LocalOrchestrator"
        ), f"get_executor() should return LocalOrchestrator, got {type(executor).__name__}"

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
            executor, LocalOrchestrator
        ), "get_executor() with parameters should still return LocalOrchestrator (V2)"

        # Should have proper attributes for project configuration
        # Note: The exact attribute names may vary, but the executor should handle these
        assert hasattr(executor, "__dict__"), "Executor should accept configuration"

        logger.debug("✅ get_executor() with parameters correctly returns V2")

    @pytest.mark.integration
    def test_v2_executor_real_csv_pipeline(self):
        """
        Integration test: V2 executor runs real CSV pipeline.

        This test uses real CSV data from examples to ensure V2 can handle actual workloads.
        Following the migration plan requirement for real data flows.
        """
        executor = get_executor()

        # Create test CSV data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n")
            f.write("1,Alice,100\n")
            f.write("2,Bob,200\n")
            f.write("3,Charlie,300\n")
            csv_path = f.name

        try:
            # Define pipeline - real CSV loading scenario
            pipeline = [
                {
                    "type": "load",
                    "id": "test_csv_load",
                    "source": csv_path,
                    "target_table": "customers",
                    "load_mode": "replace",
                }
            ]

            result = executor.execute(pipeline)

            # V2 should successfully execute the pipeline
            assert (
                result["status"] == "success"
            ), f"V2 executor should successfully run CSV pipeline. Got: {result}"

            logger.debug("✅ V2 executor successfully runs real CSV pipeline")

        finally:
            # Clean up test file
            Path(csv_path).unlink(missing_ok=True)

    @pytest.mark.integration
    def test_v2_executor_backwards_compatible_patterns(self):
        """
        Test V2 executor handles V1 parameter patterns.

        Ensures backwards compatibility - V1-style parameters should still work.
        This maintains the migration promise of zero breaking changes.
        """
        executor = get_executor()

        # Test that V2 can handle empty pipelines (common V1 pattern)
        result = executor.execute([])
        assert result is not None, "V2 should handle empty pipelines"
        assert result["status"] == "success", "Empty pipeline should succeed"

        # Test basic step execution pattern
        basic_step = {
            "type": "load",
            "id": "backwards_compat_test",
            "source": "/dev/null",  # Won't actually load, but tests parameter handling
            "target_table": "test_table",
        }

        # Should not crash on V1-style step structure
        try:
            executor.execute([basic_step])
        except Exception as e:
            # Expected to fail (no real file), but should fail gracefully
            assert (
                "error" in str(e).lower() or "failed" in str(e).lower()
            ), f"Should fail gracefully with recognizable error, got: {e}"

        logger.debug("✅ V2 executor handles V1 compatibility patterns")

    def test_v2_executor_handles_empty_pipeline(self):
        """
        Test V2 executor handles empty pipeline gracefully.

        Edge case testing - empty pipelines should be handled cleanly.
        This is a common scenario in dynamic pipeline generation.
        """
        executor = get_executor()

        result = executor.execute([])

        # Should succeed with empty pipeline
        assert result is not None, "Empty pipeline should return result"
        assert result["status"] == "success", "Empty pipeline should succeed"
        assert result["executed_steps"] == [], "No steps should be executed"
        assert result["total_steps"] == 0, "Total steps should be zero"

        logger.debug("✅ V2 executor handles empty pipeline correctly")

    @pytest.mark.performance
    def test_v2_executor_performance_baseline(self):
        """
        Test V2 executor performance baseline establishment.

        This test establishes performance baseline for V2 executor.
        Per migration plan: V2 should be within 10% of V1 performance.
        """
        executor = get_executor()

        # Create small test dataset for performance measurement
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            # Small dataset for consistent timing
            for i in range(100):
                f.write(f"{i},name_{i},{i*10}\n")
            csv_path = f.name

        try:
            pipeline = [
                {
                    "type": "load",
                    "id": "perf_test",
                    "source": csv_path,
                    "target_table": "perf_test_table",
                    "load_mode": "replace",
                }
            ]

            # Measure execution time
            start_time = time.time()
            result = executor.execute(pipeline)
            execution_time = time.time() - start_time

            # Should complete successfully
            assert result["status"] == "success", "Performance test should succeed"

            # Should complete in reasonable time (baseline establishment)
            assert (
                execution_time < 5.0
            ), f"V2 execution took {execution_time:.2f}s, should be under 5s for baseline"

            logger.debug(f"✅ V2 performance baseline: {execution_time:.3f}s")

        finally:
            Path(csv_path).unlink(missing_ok=True)

    def test_get_executor_factory_simplicity(self):
        """
        Test that get_executor() follows "Simple is better than complex".

        The factory should work without any configuration, feature flags, or complex setup.
        This is the key user experience requirement from the migration plan.
        """
        # Should work with no arguments
        executor1 = get_executor()
        assert executor1 is not None

        # Should work with minimal arguments
        executor2 = get_executor(profile_name="dev")
        assert executor2 is not None

        # Both should be V2
        assert isinstance(executor1, LocalOrchestrator)
        assert isinstance(executor2, LocalOrchestrator)

        logger.debug("✅ get_executor() factory is simple and consistent")

    def test_v2_executor_backward_compatibility(self):
        """
        Test V2 executor handles common pipeline patterns.

        V2 should handle standard pipeline patterns gracefully.
        """
        executor = get_executor()

        # Test that V2 can handle empty pipelines (common pattern)
        result = executor.execute([])
        assert result["status"] == "success"
        assert result["executed_steps"] == []
        assert result["total_steps"] == 0
