"""V2 API Migration Showcase Tests.

This file demonstrates the Raymond Hettinger approach to V2 test patterns:
- "Simple is better than complex"
- "Explicit is better than implicit"
- "There should be one obvious way to do it"
- "Readability counts"

These tests serve as canonical examples for migrating V1 patterns to V2.
Following 04_testing_standards.md: No mocks, use real implementations.
"""

import tempfile
from pathlib import Path

import pytest

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TestV2CoordinatorShowcase:
    """Showcase V2 ExecutionCoordinator patterns following Raymond Hettinger's principles."""

    @pytest.fixture
    def real_csv_data(self):
        """Create real CSV test data following V2 memory pattern."""
        # V2 requires real files, not in-memory data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("customer_id,name,email,status\n")
            f.write("1001,Alice Johnson,alice@example.com,active\n")
            f.write("1002,Bob Smith,bob@example.com,pending\n")
            f.write("1003,Charlie Brown,charlie@example.com,active\n")
            csv_path = f.name

        yield csv_path

        # Cleanup (Pythonic resource management)
        Path(csv_path).unlink(missing_ok=True)

    def test_coordinator_executes_load_step_successfully_with_real_csv(
        self, v2_pipeline_runner, real_csv_data
    ):
        """Test V2 coordinator executes CSV load step successfully with real file.

        V2 Pattern Showcase:
        - ExecutionCoordinator (not get_executor)
        - ExecutionContext (explicit, not executor.context)
        - result.step_results (not result["step_results"])
        - Real file path (not in-memory table registration)
        """
        # Given: V2 coordinator and context from fixtures
        steps = [
            {
                "id": "load_customer_data",
                "type": "load",
                "source": real_csv_data,
                "target_table": "customers",
            }
        ]

        # When: Execute V2 pipeline
        coordinator = v2_pipeline_runner(steps)
        result = coordinator.result

        # Then: V2 result structure validation
        assert (
            result.success is True
        ), f"Load failed: {result.step_results[0].error_message if result.step_results else 'Unknown error'}"
        assert len(result.step_results) == 1
        assert result.step_results[0].success is True
        assert result.step_results[0].step_id == "load_customer_data"

        # V2 engine access pattern (context.engine, not executor.context.engine_adapter.engine)
        engine = coordinator.context.engine
        assert engine is not None, "ExecutionContext should auto-initialize engine"
        assert engine.table_exists("customers")

        # Verify data integrity
        customer_count = engine.execute_query(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0]
        assert customer_count == 3, f"Expected 3 customers, got {customer_count}"

    def test_coordinator_handles_missing_file_with_clear_error_message(
        self, v2_pipeline_runner
    ):
        """Test V2 coordinator provides clear error when source file doesn't exist.

        V2 Error Handling Pattern:
        - Graceful failure (not exceptions)
        - Clear error messages
        - Structured error information in step results
        """
        # Given: V2 coordinator with non-existent file
        steps = [
            {
                "id": "load_missing_file",
                "type": "load",
                "source": "/nonexistent/path/missing.csv",
                "target_table": "should_not_exist",
            }
        ]

        # When: Execute with missing file
        coordinator = v2_pipeline_runner(steps)
        result = coordinator.result

        # Then: V2 error handling validation
        assert result.success is False
        assert len(result.step_results) == 1

        error_step = result.step_results[0]
        assert error_step.success is False
        assert error_step.step_id == "load_missing_file"
        assert error_step.error_message is not None

        # Error message should be informative (Raymond's principle: explicit is better than implicit)
        error_msg = error_step.error_message.lower()
        assert any(
            keyword in error_msg for keyword in ["file", "not found", "missing"]
        ), f"Error message should mention file issue: {error_step.error_message}"

    def test_coordinator_executes_complete_load_transform_export_pipeline(
        self, v2_pipeline_runner, real_csv_data
    ):
        """Test V2 coordinator executes complete data pipeline successfully.

        V2 Multi-Step Pipeline Pattern:
        - Sequential step execution
        - Context preservation across steps
        - Complete data flow validation
        """
        # Given: V2 coordinator for complete pipeline
        # Create output file path
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            pipeline_steps = [
                {
                    "id": "load_raw_customers",
                    "type": "load",
                    "source": real_csv_data,
                    "target_table": "raw_customers",
                },
                {
                    "id": "transform_active_customers",
                    "type": "transform",
                    "target_table": "active_customers",
                    "sql": "SELECT customer_id, name, email FROM raw_customers WHERE status = 'active'",
                },
                {
                    "id": "export_active_customers",
                    "type": "export",
                    "source_table": "active_customers",
                    "destination": output_path,
                    "format": "csv",
                },
            ]

            # When: Execute complete V2 pipeline
            coordinator = v2_pipeline_runner(pipeline_steps)
            result = coordinator.result

            # Then: V2 multi-step validation
            assert (
                result.success is True
            ), f"Pipeline failed: {[step.error_message for step in result.step_results if not step.success]}"
            assert len(result.step_results) == 3

            # Verify each step succeeded
            for i, step_result in enumerate(result.step_results):
                assert (
                    step_result.success is True
                ), f"Step {i} failed: {step_result.error_message}"

            # Verify pipeline data flow (V2 engine access pattern)
            engine = coordinator.context.engine
            assert engine is not None, "ExecutionContext should auto-initialize engine"
            assert engine.table_exists("raw_customers")
            assert engine.table_exists("active_customers")

            # Verify output file was created
            assert Path(output_path).exists(), "Export should create output file"

            # Verify exported data content
            with open(output_path, "r") as f:
                exported_content = f.read()
                assert "Alice Johnson" in exported_content
                assert "Charlie Brown" in exported_content
                assert (
                    "Bob Smith" not in exported_content
                )  # Should be filtered out (pending status)

        finally:
            # Cleanup (Pythonic resource management)
            Path(output_path).unlink(missing_ok=True)

    def test_coordinator_preserves_execution_context_across_steps(
        self, v2_pipeline_runner, real_csv_data
    ):
        """Test V2 coordinator preserves execution context across multiple steps.

        V2 Context Management Pattern:
        - Immutable context preservation
        - Variable substitution across steps
        - State consistency validation
        """
        # Given: V2 coordinator with variables in context
        steps = [
            {
                "id": "load_with_variables",
                "type": "load",
                "source": real_csv_data,
                "target_table": "customers_${table_suffix}",
            },
            {
                "id": "transform_with_variables",
                "type": "transform",
                "target_table": "filtered_customers",
                "sql": "SELECT * FROM customers_${table_suffix} WHERE status = '${filter_status}'",
            },
        ]

        # When: Execute pipeline with variable substitution
        coordinator = v2_pipeline_runner(
            steps,
            variables={"table_suffix": "processed", "filter_status": "active"},
        )
        result = coordinator.result

        # Then: V2 context preservation validation
        assert (
            result.success is True
        ), f"Pipeline failed: {[step.error_message for step in result.step_results if not step.success]}"
        assert len(result.step_results) == 2

        # Verify variables were substituted correctly (V2 engine access pattern)
        engine = coordinator.context.engine
        assert engine is not None, "ExecutionContext should auto-initialize engine"
        assert engine.table_exists("customers_processed")
        assert engine.table_exists("filtered_customers")

        # Verify filtered data
        filtered_count = engine.execute_query(
            "SELECT COUNT(*) FROM filtered_customers"
        ).fetchone()[0]
        assert filtered_count == 2, "Should have 2 active customers"

    def test_coordinator_handles_empty_pipeline_gracefully(self, v2_pipeline_runner):
        """Test V2 coordinator handles empty pipeline without errors.

        V2 Edge Case Pattern:
        - Graceful handling of edge cases
        - Clear success indication for empty operations
        - No side effects
        """
        # Given: V2 coordinator with empty pipeline
        # When: Execute empty pipeline
        coordinator = v2_pipeline_runner([])
        result = coordinator.result

        # Then: V2 empty pipeline validation
        assert result.success is True
        assert len(result.step_results) == 0
