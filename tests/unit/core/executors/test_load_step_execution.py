"""Tests for execution of LoadStep with different modes."""

from unittest.mock import MagicMock

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.parser.ast import LoadStep, Pipeline


@pytest.fixture
def local_executor():
    """Create a LocalExecutor with mocked engine for testing."""
    executor = LocalExecutor()

    # Mock the duckdb_engine instead of engine
    executor.duckdb_engine = MagicMock()

    # Mock generate_load_sql method
    executor.duckdb_engine.generate_load_sql = MagicMock(return_value="MOCK SQL")

    # Mock execute_query method
    executor.duckdb_engine.execute_query = MagicMock()

    # Mock table_exists and get_table_schema to avoid database calls
    executor.duckdb_engine.table_exists = MagicMock(return_value=True)
    executor.duckdb_engine.get_table_schema = MagicMock(
        return_value={"id": "INTEGER", "name": "VARCHAR"}
    )

    return executor


def test_execute_load_step_replace_mode(local_executor):
    """Test execution of LoadStep with REPLACE mode."""
    # Create a LoadStep with REPLACE mode
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="REPLACE",
        line_number=1,
    )

    # Execute the step
    local_executor.execute_load_step(load_step)

    # Verify generate_load_sql was called with the step
    local_executor.duckdb_engine.generate_load_sql.assert_called_once_with(load_step)

    # Verify execute_query was called with the SQL
    local_executor.duckdb_engine.execute_query.assert_called_once_with("MOCK SQL")


def test_execute_load_step_append_mode(local_executor):
    """Test execution of LoadStep with APPEND mode."""
    # Create a LoadStep with APPEND mode
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="APPEND",
        line_number=1,
    )

    # Execute the step
    local_executor.execute_load_step(load_step)

    # Verify generate_load_sql was called with the step
    local_executor.duckdb_engine.generate_load_sql.assert_called_once_with(load_step)

    # Verify execute_query was called with the SQL
    local_executor.duckdb_engine.execute_query.assert_called_once_with("MOCK SQL")


def test_execute_load_step_merge_mode(local_executor):
    """Test execution of LoadStep with MERGE mode."""
    # Create a LoadStep with MERGE mode
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="MERGE",
        merge_keys=["user_id"],
        line_number=1,
    )

    # Execute the step
    local_executor.execute_load_step(load_step)

    # Verify generate_load_sql was called with the step
    local_executor.duckdb_engine.generate_load_sql.assert_called_once_with(load_step)

    # Verify execute_query was called with the SQL
    local_executor.duckdb_engine.execute_query.assert_called_once_with("MOCK SQL")


def test_execute_pipeline_with_load_steps(local_executor):
    """Test execution of a pipeline with multiple LoadSteps of different modes."""
    # Create a pipeline with multiple LoadSteps
    pipeline = Pipeline()

    # Add LoadSteps with different modes
    pipeline.add_step(
        LoadStep(
            table_name="users_table",
            source_name="users_source",
            mode="REPLACE",
            line_number=1,
        )
    )

    pipeline.add_step(
        LoadStep(
            table_name="orders_table",
            source_name="orders_source",
            mode="APPEND",
            line_number=2,
        )
    )

    pipeline.add_step(
        LoadStep(
            table_name="customers_table",
            source_name="customers_source",
            mode="MERGE",
            merge_keys=["customer_id"],
            line_number=3,
        )
    )

    # Execute the pipeline (don't mock execute_step this time to test full integration)
    local_executor.execute_pipeline(pipeline)

    # Verify generate_load_sql and execute_query were called three times
    assert local_executor.duckdb_engine.generate_load_sql.call_count == 3
    assert local_executor.duckdb_engine.execute_query.call_count == 3


def test_execute_load_step_with_error(local_executor):
    """Test error handling during LoadStep execution."""
    # Create a LoadStep
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="REPLACE",
        line_number=1,
    )

    # Configure the generate_load_sql mock to return SQL
    local_executor.duckdb_engine.generate_load_sql.return_value = "MOCK SQL"

    # Make execute_query raise an exception
    local_executor.duckdb_engine.execute_query.side_effect = Exception(
        "SQL execution failed"
    )

    # Execute the step - it should NOT raise an exception because we handle it in the method
    result = local_executor.execute_load_step(load_step)

    # Verify the error was captured in the result
    assert result["status"] == "error"
    assert "SQL execution failed" in result["message"]


def test_execute_load_step_schema_compatibility_error(local_executor):
    """Test error handling when LoadStep execution encounters schema compatibility issues."""
    # Create a LoadStep with APPEND mode
    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
        mode="APPEND",
        line_number=1,
    )

    # Make generate_load_sql raise a ValueError for schema incompatibility
    local_executor.duckdb_engine.generate_load_sql.side_effect = ValueError(
        "Column 'email' in source does not exist in target table 'users_table'"
    )

    # Execute the step - it should handle the schema validation error
    result = local_executor.execute_load_step(load_step)

    # Verify the error was captured in the result
    assert result["status"] == "error"
    assert "Column 'email' in source does not exist" in result["message"]

    # Reset the side effect
    local_executor.duckdb_engine.generate_load_sql.side_effect = None


def test_execute_pipeline_schema_incompatibility(local_executor):
    """Test that a pipeline execution fails appropriately when schema is incompatible."""
    # Create a pipeline with a LoadStep that will trigger a schema validation error
    pipeline = Pipeline()

    # Add a LoadStep with APPEND mode
    pipeline.add_step(
        LoadStep(
            table_name="users_table",
            source_name="users_source",
            mode="APPEND",
            line_number=1,
        )
    )

    # Make generate_load_sql raise a ValueError for schema incompatibility
    local_executor.duckdb_engine.generate_load_sql.side_effect = ValueError(
        "Column 'email' in source has incompatible types: source=VARCHAR, target=INTEGER"
    )

    # Execute the pipeline
    result = local_executor.execute_pipeline(pipeline)

    # Verify the pipeline execution failed with the schema error
    assert result["status"] == "failed"
    assert "Column 'email' in source has incompatible types" in result["error"]

    # Reset the side effect
    local_executor.duckdb_engine.generate_load_sql.side_effect = None
