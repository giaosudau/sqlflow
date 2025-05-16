"""Tests for the ArtifactManager class."""

import json
import os
from pathlib import Path

from sqlflow.core.storage.artifact_manager import ArtifactManager


def test_artifact_manager_setup(tmp_path: Path) -> None:
    """Test that ArtifactManager creates directories correctly."""
    # Arrange
    project_dir = tmp_path

    # Act
    ArtifactManager(str(project_dir))

    # Assert
    assert os.path.exists(project_dir / "target" / "compiled")
    assert os.path.exists(project_dir / "target" / "run")
    assert os.path.exists(project_dir / "target" / "logs")


def test_save_load_compiled_plan(tmp_path: Path) -> None:
    """Test saving and loading a compiled plan."""
    # Arrange
    project_dir = tmp_path
    manager = ArtifactManager(str(project_dir))
    plan = {
        "pipeline_metadata": {"name": "test_pipeline"},
        "operations": [{"id": "test_op", "type": "transform"}],
    }

    # Act
    path = manager.save_compiled_plan("test_pipeline", plan)
    loaded_plan = manager.load_compiled_plan("test_pipeline")

    # Assert
    assert os.path.exists(path)
    assert loaded_plan["pipeline_metadata"]["name"] == "test_pipeline"
    assert loaded_plan["operations"][0]["id"] == "test_op"


def test_execution_tracking_workflow(tmp_path: Path) -> None:
    """Test full execution tracking workflow."""
    # Arrange
    project_dir = tmp_path
    manager = ArtifactManager(str(project_dir))

    # Act - Initialize execution
    execution_id, metadata = manager.initialize_execution(
        "test_pipeline", {"var1": "value1"}, "dev"
    )

    # Assert - Initialize
    assert execution_id is not None
    assert metadata["status"] == "running"
    assert metadata["variables"]["var1"] == "value1"

    # Act - Record operation start
    op_metadata = manager.record_operation_start(
        "test_pipeline", "test_op", "transform", "SELECT 1;"
    )

    # Assert - Operation recorded
    assert op_metadata["status"] == "running"
    assert os.path.exists(
        project_dir / "target" / "run" / "test_pipeline" / "test_op.sql"
    )
    assert os.path.exists(
        project_dir / "target" / "run" / "test_pipeline" / "test_op.json"
    )

    # Verify SQL file contents
    with open(
        project_dir / "target" / "run" / "test_pipeline" / "test_op.sql", "r"
    ) as f:
        sql_content = f.read()
        assert "SELECT 1;" in sql_content

    # Act - Complete operation (success)
    success_result = {"rows_affected": 10, "database_info": {"engine": "duckdb"}}
    op_result = manager.record_operation_completion(
        "test_pipeline", "test_op", True, success_result
    )

    # Assert - Operation completed successfully
    assert op_result["status"] == "success"
    assert op_result["rows_affected"] == 10
    assert op_result["database_info"]["engine"] == "duckdb"

    # Read pipeline metadata
    with open(
        project_dir / "target" / "run" / "test_pipeline" / "metadata.json", "r"
    ) as f:
        pipeline_metadata = json.load(f)

    # Assert pipeline metadata updated
    assert pipeline_metadata["operations_summary"]["total"] == 1
    assert pipeline_metadata["operations_summary"]["success"] == 1
    assert pipeline_metadata["operations_summary"]["failed"] == 0

    # Act - Finalize execution
    final_metadata = manager.finalize_execution("test_pipeline", True)

    # Assert - Finalized metadata
    assert final_metadata["status"] == "success"
    assert final_metadata["completed_at"] is not None
    assert final_metadata["duration_ms"] > 0


def test_failed_operation_recording(tmp_path: Path) -> None:
    """Test recording a failed operation."""
    # Arrange
    project_dir = tmp_path
    manager = ArtifactManager(str(project_dir))

    # Initialize execution
    execution_id, _ = manager.initialize_execution("test_pipeline", {}, "dev")

    # Record operation start
    manager.record_operation_start(
        "test_pipeline",
        "failed_op",
        "transform",
        "SELECT invalid_column FROM nonexistent_table;",
    )

    # Act - Record failure
    error_result = {
        "error": "Table 'nonexistent_table' not found",
        "traceback": "Traceback...",
    }
    op_result = manager.record_operation_completion(
        "test_pipeline", "failed_op", False, error_result
    )

    # Assert - Operation recorded as failed
    assert op_result["status"] == "failed"
    assert op_result["error_details"] == "Table 'nonexistent_table' not found"

    # Read pipeline metadata
    with open(
        project_dir / "target" / "run" / "test_pipeline" / "metadata.json", "r"
    ) as f:
        pipeline_metadata = json.load(f)

    # Assert pipeline metadata updated
    assert pipeline_metadata["operations_summary"]["total"] == 1
    assert pipeline_metadata["operations_summary"]["success"] == 0
    assert pipeline_metadata["operations_summary"]["failed"] == 1

    # Act - Finalize execution
    final_metadata = manager.finalize_execution("test_pipeline", False)

    # Assert - Finalized metadata
    assert final_metadata["status"] == "failed"
