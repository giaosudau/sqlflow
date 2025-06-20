"""Tests for pipeline failure handling and error propagation."""

import tempfile

import pytest

from sqlflow.core.executors import get_executor


class TestPipelineFailureHandling:
    """Test suite for pipeline failure handling and error propagation using real implementations."""

    @pytest.fixture
    def local_executor(self):
        """Create a LocalExecutor with real DuckDB engine."""
        # Use a temporary directory for the test database
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = get_executor(project_dir=temp_dir)
            yield executor

    def test_sql_execution_failure_propagates_correctly(self, local_executor):
        """Test that SQL execution failures are properly propagated as pipeline failures.

        This test specifically addresses the bug where SQL errors were masked by
        fallback to mock data creation.
        """
        # Create a plan with a transform step that references a non-existent table
        plan = [
            {
                "type": "transform",
                "id": "transform_failed_table",
                "name": "failed_table",
                "query": "SELECT * FROM users_table_failed",
            }
        ]

        # Execute the pipeline - this should fail due to missing table
        result = local_executor.execute(plan)

        # Verify the pipeline fails (not succeeds)
        assert result["status"] == "failed"
        assert "failed_step" in result
        assert result["failed_step"] == "transform_failed_table"
        assert "does not exist" in result["error"] or "not found" in result["error"]

    def test_missing_table_reference_fails_pipeline(self, local_executor):
        """Test that referencing non-existent tables causes pipeline failure."""
        plan = [
            {
                "type": "transform",
                "id": "step_1",
                "name": "result_table",
                "query": "SELECT * FROM nonexistent_table",
            }
        ]

        result = local_executor.execute(plan)

        assert result["status"] == "failed"
        assert (
            "nonexistent_table" in result["error"]
            or "does not exist" in result["error"]
        )

    def test_multiple_steps_fail_fast(self, local_executor):
        """Test that pipeline stops at first failure (fail-fast behavior)."""
        plan = [
            {
                "type": "transform",
                "id": "step_1",
                "name": "table_1",
                "query": "SELECT 1 as id",
            },
            {
                "type": "transform",
                "id": "step_2_fails",
                "name": "table_2",
                "query": "SELECT * FROM missing_table",
            },
            {
                "type": "transform",
                "id": "step_3_should_not_run",
                "name": "table_3",
                "query": "SELECT 1 as id",
            },
        ]

        result = local_executor.execute(plan)

        # Verify pipeline failed at step 2
        assert result["status"] == "failed"
        assert result["failed_step"] == "step_2_fails"
        assert result["failed_at_step"] == 2
        assert "step_1" in result["executed_steps"]
        assert "step_2_fails" not in result["executed_steps"]
        assert "step_3_should_not_run" not in result["executed_steps"]

        # Verify first table was created but second was not
        assert local_executor.duckdb_engine.table_exists("table_1")
        assert not local_executor.duckdb_engine.table_exists("table_2")
        assert not local_executor.duckdb_engine.table_exists("table_3")

    def test_sql_syntax_error_fails_pipeline(self, local_executor):
        """Test that SQL syntax errors cause pipeline failure."""
        plan = [
            {
                "type": "transform",
                "id": "syntax_error_step",
                "name": "result",
                "query": "SELET * FORM table",  # Intentional typos
            }
        ]

        result = local_executor.execute(plan)

        assert result["status"] == "failed"
        assert "syntax" in result["error"].lower() or "error" in result["error"].lower()

    def test_valid_pipeline_succeeds(self, local_executor):
        """Test that a valid pipeline executes successfully."""
        plan = [
            {
                "type": "transform",
                "id": "step_1",
                "name": "base_table",
                "query": "SELECT 1 as id, 'test' as name",
            },
            {
                "type": "transform",
                "id": "step_2",
                "name": "derived_table",
                "query": "SELECT id, UPPER(name) as upper_name FROM base_table",
            },
        ]

        result = local_executor.execute(plan)

        # This should succeed
        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 2
        assert "step_1" in result["executed_steps"]
        assert "step_2" in result["executed_steps"]

        # Verify tables were created
        assert local_executor.duckdb_engine.table_exists("base_table")
        assert local_executor.duckdb_engine.table_exists("derived_table")

    def test_pipeline_with_mock_fallback_for_test_mode(self, local_executor):
        """Test that mock data fallback works when no SQL query is provided (test mode)."""
        plan = [
            {
                "type": "transform",
                "id": "mock_step",
                "name": "test_table",
                # No query - should trigger mock data creation
            }
        ]

        result = local_executor.execute(plan)

        # This should succeed because it's legitimate test mode
        assert result["status"] == "success"
        assert "mock_step" in result["executed_steps"]

    def test_invalid_step_configuration_fails(self, local_executor):
        """Test that invalid step configurations cause failures."""
        plan = [
            {
                "type": "transform",
                "id": "invalid_step",
                # Missing both name and query
            }
        ]

        result = local_executor.execute(plan)

        assert result["status"] == "failed"
        assert (
            "invalid" in result["error"].lower() or "no table name" in result["error"]
        )

    def test_pipeline_error_context_included(self, local_executor):
        """Test that error responses include helpful context."""
        plan = [
            {
                "type": "transform",
                "id": "context_test",
                "name": "test_table",
                "query": "SELECT * FROM missing",
            }
        ]

        result = local_executor.execute(plan)

        # Verify comprehensive error context
        assert result["status"] == "failed"
        assert "failed_step" in result
        assert "failed_step_type" in result
        assert "failed_at_step" in result
        assert "total_steps" in result
        assert "executed_steps" in result

        assert result["failed_step"] == "context_test"
        assert result["failed_step_type"] == "transform"
        assert result["failed_at_step"] == 1
        assert result["total_steps"] == 1


class TestRealWorldFailureScenarios:
    """Test real-world failure scenarios that users commonly encounter."""

    @pytest.fixture(scope="function")
    def executor(self):
        """Create a real LocalExecutor for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = get_executor(project_dir=temp_dir)

            # Ensure complete test isolation by clearing any existing tables
            # This prevents test interference when table names are reused
            if hasattr(executor, "duckdb_engine") and executor.duckdb_engine:  # type: ignore
                try:
                    # Get list of all tables and drop them
                    tables_result = executor.duckdb_engine.execute_query(  # type: ignore
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                    )
                    for row in tables_result.fetchall():
                        table_name = row[0]
                        executor.duckdb_engine.execute_query(f"DROP TABLE IF EXISTS {table_name}")  # type: ignore
                except Exception:
                    pass  # Ignore errors if no tables exist yet

            yield executor

    def test_typo_in_table_name_scenario(self, executor):
        """Test the exact scenario from the user's bug report."""
        # Create the exact plan that would cause this error
        plan = [
            {
                "type": "transform",
                "id": "transform_export_data",
                "name": "export_data",
                "query": "SELECT * FROM users_table_failed",
            }
        ]

        result = executor.execute(plan)

        # This MUST fail, not succeed
        assert (
            result["status"] == "failed"
        ), "Pipeline should fail when table doesn't exist"
        assert (
            "users_table_failed" in result["error"]
            and "does not exist" in result["error"]
        )
        assert result["failed_step"] == "transform_export_data"

    def test_dependency_chain_with_valid_tables(self, executor):
        """Test that dependency chains work when tables exist."""
        plan = [
            {
                "type": "transform",
                "id": "step_1",
                "name": "base_table",
                "query": "SELECT 1 as id, 'Alice' as name",
            },
            {
                "type": "transform",
                "id": "step_2",
                "name": "derived_table",
                "query": "SELECT id, UPPER(name) as upper_name FROM base_table",
            },
        ]

        result = executor.execute(plan)

        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 2

        # Verify both tables exist and have correct data
        assert executor.duckdb_engine.table_exists("base_table")
        assert executor.duckdb_engine.table_exists("derived_table")

    def test_dependency_chain_with_missing_table(self, executor):
        """Test that dependency chains fail when intermediate table is missing."""
        plan = [
            {
                "type": "transform",
                "id": "step_1",
                "name": "base_table",
                "query": "SELECT 1 as id",
            },
            {
                "type": "transform",
                "id": "step_2_depends_on_missing",
                "name": "derived_table",
                "query": "SELECT * FROM base_table JOIN missing_table ON base_table.id = missing_table.id",
            },
        ]

        result = executor.execute(plan)

        assert result["status"] == "failed"
        assert result["failed_step"] == "step_2_depends_on_missing"
        assert len(result["executed_steps"]) == 1  # Only first step executed

        # First table should exist, second should not
        assert executor.duckdb_engine.table_exists("base_table")
        assert not executor.duckdb_engine.table_exists("derived_table")

    def test_complex_sql_with_functions(self, executor):
        """Test complex SQL operations to ensure they work or fail properly."""
        plan = [
            {
                "type": "transform",
                "id": "complex_step",
                "name": "complex_table",
                "query": """
                    SELECT 
                        1 as id,
                        CURRENT_DATE as created_date,
                        RANDOM() as random_value,
                        'test_' || CAST(1 as VARCHAR) as label
                """,
            }
        ]

        result = executor.execute(plan)

        # This should succeed with valid DuckDB functions
        assert result["status"] == "success"
        assert executor.duckdb_engine.table_exists("complex_table")

    def test_invalid_function_call(self, executor):
        """Test that invalid function calls cause proper failures."""
        plan = [
            {
                "type": "transform",
                "id": "invalid_function_step",
                "name": "invalid_table",
                "query": "SELECT NONEXISTENT_FUNCTION(1) as result",
            }
        ]

        result = executor.execute(plan)

        assert result["status"] == "failed"
        assert (
            "NONEXISTENT_FUNCTION" in result["error"]
            or "function" in result["error"].lower()
        )

    def test_empty_sql_query(self, executor):
        """Test handling of empty SQL queries."""
        plan = [
            {
                "type": "transform",
                "id": "empty_query_step",
                "name": "empty_table",
                "query": "",
            }
        ]

        result = executor.execute(plan)

        # Should fall back to mock data creation for empty queries
        assert result["status"] == "success"

    def test_whitespace_only_query(self, executor):
        """Test handling of whitespace-only queries."""
        plan = [
            {
                "type": "transform",
                "id": "whitespace_step",
                "name": "whitespace_table",
                "query": "   \n\t  ",
            }
        ]

        result = executor.execute(plan)

        # Whitespace-only queries should fail with syntax error
        assert result["status"] == "failed"
        assert (
            "syntax error" in result["error"].lower()
            or "parser error" in result["error"].lower()
        )
