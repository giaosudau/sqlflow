"""Integration tests for Load Step executor.

These tests verify that the load step executor works correctly with real files,
CSV connectors, and DuckDB engine integration.
"""

import os
import tempfile

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2.execution.context import (
    create_test_context,
)
from sqlflow.core.executors.v2.steps.load import LoadStepExecutor


@pytest.mark.integration
class TestLoadStepIntegration:
    """Integration tests for Load Step executor using real files and connectors."""

    @pytest.fixture(scope="function")
    def real_duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        return DuckDBEngine(":memory:")

    @pytest.fixture(scope="function")
    def execution_context(self, real_duckdb_engine):
        """Create execution context with real DuckDB engine."""
        return create_test_context(engine=real_duckdb_engine)

    @pytest.fixture(scope="function")
    def load_executor(self):
        """Create load step executor."""
        return LoadStepExecutor()

    @pytest.fixture(scope="function")
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,age,city\n")
            f.write("1,Alice,25,NYC\n")
            f.write("2,Bob,30,LA\n")
            f.write("3,Charlie,22,Chicago\n")
            csv_path = f.name

        try:
            yield csv_path
        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_load_csv_file_replace_mode(
        self, execution_context, load_executor, real_duckdb_engine, sample_csv_file
    ):
        """Test loading CSV file in REPLACE mode using V2 pattern."""
        from sqlflow.core.executors import get_executor
        from sqlflow.core.planner_main import Planner
        from sqlflow.parser import SQLFlowParser

        # Use the working V2 pattern like load_modes_regression tests
        pipeline_sql = f"""
        SOURCE test_csv TYPE CSV PARAMS {{
            "path": "{sample_csv_file}", 
            "has_header": true
        }};
        
        LOAD users FROM test_csv MODE REPLACE;
        """

        # Parse and execute using the V2 pattern that works
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Execute via V2 coordinator
        executor = get_executor()
        result = executor.execute(execution_plan, execution_context)

        # Verify success
        assert result.success is True, f"Load failed: {result}"
        assert len(result.step_results) >= 1

        # Verify table was created and data loaded
        assert real_duckdb_engine.table_exists("users")

        query_result = real_duckdb_engine.execute_query("SELECT COUNT(*) FROM users")
        count = query_result.fetchone()[0]
        assert count == 3

        # Verify data content
        query_result = real_duckdb_engine.execute_query(
            "SELECT * FROM users ORDER BY id"
        )
        rows = query_result.fetchall()
        assert rows[0][1] == "Alice"  # name
        assert rows[1][1] == "Bob"
        assert rows[2][1] == "Charlie"

    def test_load_csv_file_append_mode(
        self, execution_context, load_executor, real_duckdb_engine, sample_csv_file
    ):
        """Test loading CSV file in APPEND mode using V2 pattern."""
        from sqlflow.core.executors import get_executor
        from sqlflow.core.planner_main import Planner
        from sqlflow.parser import SQLFlowParser

        # Setup: Create initial table
        real_duckdb_engine.execute_query(
            """
            CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER, city VARCHAR)
        """
        )
        real_duckdb_engine.execute_query(
            """
            INSERT INTO users VALUES (999, 'Existing', 40, 'Boston')
        """
        )

        # Use the working V2 pattern like load_modes_regression tests
        pipeline_sql = f"""
        SOURCE test_csv TYPE CSV PARAMS {{
            "path": "{sample_csv_file}", 
            "has_header": true
        }};
        
        LOAD users FROM test_csv MODE APPEND;
        """

        # Parse and execute using the V2 pattern that works
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Execute via V2 coordinator
        executor = get_executor()
        result = executor.execute(execution_plan, execution_context)

        # Verify success
        assert result.success is True, f"Append failed: {result}"
        assert len(result.step_results) >= 1

        # Verify data was appended (should have 4 total rows)
        query_result = real_duckdb_engine.execute_query("SELECT COUNT(*) FROM users")
        count = query_result.fetchone()[0]
        assert count == 4

        # Verify both existing and new data
        query_result = real_duckdb_engine.execute_query(
            "SELECT name FROM users ORDER BY id"
        )
        names = [row[0] for row in query_result.fetchall()]
        assert "Alice" in names
        assert "Existing" in names

    def test_load_error_handling_invalid_file(self, execution_context, load_executor):
        """Test error handling when CSV file doesn't exist using V2 pattern."""
        from sqlflow.core.executors import get_executor
        from sqlflow.core.planner_main import Planner
        from sqlflow.parser import SQLFlowParser

        # Use the working V2 pattern with non-existent file
        pipeline_sql = """
        SOURCE invalid_csv TYPE CSV PARAMS {
            "path": "/path/to/nonexistent/file.csv", 
            "has_header": true
        };
        
        LOAD error_table FROM invalid_csv MODE REPLACE;
        """

        # Parse and execute using the V2 pattern
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Execute via V2 coordinator (should fail)
        executor = get_executor()
        result = executor.execute(execution_plan, execution_context)

        # Verify failure
        assert result.success is False, "Should fail with non-existent file"
        assert len(result.step_results) >= 1

        # Check that at least one step failed with appropriate error
        failed_step = None
        for step_result in result.step_results:
            if not step_result.success:
                failed_step = step_result
                break

        assert failed_step is not None, "Should have at least one failed step"
        assert failed_step.error_message is not None, "Should have error message"

        # Error should mention file or path issue
        error_msg = failed_step.error_message.lower()
        assert any(
            keyword in error_msg
            for keyword in ["file", "not found", "no such", "does not exist"]
        ), f"Error should mention file issue: {failed_step.error_message}"
