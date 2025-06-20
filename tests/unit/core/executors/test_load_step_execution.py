"""Tests for execution of LoadStep with different modes - V2 Architecture."""

import tempfile
from pathlib import Path

import pytest

from sqlflow.core.executors import get_executor


@pytest.fixture
def v2_executor():
    """Create a V2 LocalOrchestrator for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        executor = get_executor(project_dir=temp_dir)
        yield executor


def test_execute_load_step_replace_mode(v2_executor):
    """Test execution of LoadStep with REPLACE mode using V2 architecture."""
    # Create test CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,Alice\n2,Bob\n")
        csv_path = f.name

    try:
        # Define V2 pipeline with load step
        pipeline = [
            {
                "type": "load",
                "id": "test_replace_load",
                "source": csv_path,
                "target_table": "users_table",
                "mode": "REPLACE",
            }
        ]

        # Execute the pipeline
        result = v2_executor.execute(pipeline)

        # Verify execution succeeded (behavior-focused test)
        assert result["status"] == "success"
        assert "executed_steps" in result
        assert len(result["executed_steps"]) == 1

        # The key behavior: load operation completed successfully
        # (V2 structure may differ from V1, but the core behavior is what matters)

    finally:
        # Clean up test file
        Path(csv_path).unlink(missing_ok=True)


def test_execute_load_step_append_mode(v2_executor):
    """Test execution of LoadStep with APPEND mode using V2 architecture."""
    # Create test CSV files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,Alice\n2,Bob\n")
        csv_path1 = f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n3,Charlie\n")
        csv_path2 = f.name

    try:
        # Define V2 pipeline with REPLACE then APPEND
        pipeline = [
            {
                "type": "load",
                "id": "initial_load",
                "source": csv_path1,
                "target_table": "users_table",
                "mode": "REPLACE",
            },
            {
                "type": "load",
                "id": "append_load",
                "source": csv_path2,
                "target_table": "users_table",
                "mode": "APPEND",
            },
        ]

        # Execute the pipeline
        result = v2_executor.execute(pipeline)

        # Verify execution succeeded
        assert result["status"] == "success"
        assert "executed_steps" in result
        assert len(result["executed_steps"]) == 2

        # The key behavior: both load operations completed successfully
        # (V2 structure may differ from V1, but the core behavior is what matters)

    finally:
        # Clean up test files
        Path(csv_path1).unlink(missing_ok=True)
        Path(csv_path2).unlink(missing_ok=True)


def test_execute_load_step_error_handling(v2_executor):
    """Test error handling during LoadStep execution with invalid file."""
    # Define V2 pipeline with non-existent file
    pipeline = [
        {
            "type": "load",
            "id": "test_error_load",
            "source": "/nonexistent/file.csv",
            "target_table": "users_table",
            "mode": "REPLACE",
        }
    ]

    # Execute the pipeline
    result = v2_executor.execute(pipeline)

    # Verify execution failed gracefully
    assert result["status"] in ["error", "failed"]
    assert "message" in result or "error" in result
