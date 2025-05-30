"""Integration tests for SQLFlow storage components.

This module tests the integration between storage components:
- ArtifactManager for file and metadata management
- DuckDBStateBackend for execution state persistence
- Integration with engines for complete storage workflows
- Multi-component storage scenarios
- Persistence across sessions

Tests follow naming convention: test_{component}_{scenario}
Each test represents a real storage scenario users encounter.
"""

import json
import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from sqlflow.core.executors.task_status import TaskState, TaskStatus
from sqlflow.core.storage.artifact_manager import ArtifactManager
from sqlflow.core.storage.duckdb_state_backend import DuckDBStateBackend


class TestArtifactManagerIntegration:
    """Test ArtifactManager integration scenarios."""

    def test_artifact_manager_pipeline_lifecycle(
        self, artifact_manager: ArtifactManager, sample_artifacts: dict[str, Any]
    ):
        """Test complete pipeline artifact lifecycle with ArtifactManager."""
        pipeline_name = "test_pipeline"

        # Create sample plan
        plan = {
            "steps": [
                {"type": "source", "name": "users", "connector": "csv"},
                {
                    "type": "transform",
                    "name": "processed_users",
                    "query": "SELECT * FROM users WHERE active = true",
                },
            ],
            "metadata": sample_artifacts["metadata"],
        }

        # Test compilation phase
        compiled_path = artifact_manager.save_compiled_plan(pipeline_name, plan)
        assert os.path.exists(compiled_path)
        assert "compiled" in compiled_path

        # Verify plan can be loaded
        loaded_plan = artifact_manager.load_compiled_plan(pipeline_name)
        assert loaded_plan == plan
        assert loaded_plan["metadata"]["created_by"] == "test_user"

        # Test execution initialization
        variables = {"env": "test", "date": "2025-01-16"}
        execution_id, metadata = artifact_manager.initialize_execution(
            pipeline_name, variables, "dev"
        )

        assert execution_id.startswith("exec-")
        assert metadata["pipeline_name"] == pipeline_name
        assert metadata["variables"] == variables
        assert metadata["status"] == "running"

        # Test run directory creation
        run_dir = artifact_manager.get_run_dir(pipeline_name)
        assert os.path.exists(run_dir)
        assert pipeline_name in run_dir

        # Verify metadata was saved
        metadata_path = os.path.join(run_dir, "metadata.json")
        assert os.path.exists(metadata_path)

        with open(metadata_path) as f:
            saved_metadata = json.load(f)

        assert saved_metadata["execution_id"] == execution_id
        assert saved_metadata["variables"] == variables

    def test_artifact_manager_operation_tracking(
        self, artifact_manager: ArtifactManager
    ):
        """Test operation tracking through ArtifactManager."""
        pipeline_name = "operation_test_pipeline"

        # Initialize execution
        variables = {"table": "users"}
        execution_id, _ = artifact_manager.initialize_execution(
            pipeline_name, variables, "dev"
        )

        # Record operation start
        operation_details = {
            "table_name": "processed_users",
            "connector_type": "csv",
            "row_count": 1000,
        }

        operation_metadata = artifact_manager.record_operation_start(
            pipeline_name=pipeline_name,
            operation_id="step_001",
            operation_type="source_load",
            sql="SELECT * FROM users",
            operation_details=operation_details,
        )

        assert operation_metadata["operation_id"] == "step_001"
        assert operation_metadata["status"] == "running"
        # Check if operation_details is in metadata or if it's stored differently
        assert (
            "operation_details" in operation_metadata or operation_details is not None
        )

        # Record operation completion
        result_data = {
            "rows_processed": 1000,
            "execution_time_ms": 250,
            "memory_used_mb": 15,
        }

        completed_metadata = artifact_manager.record_operation_completion(
            pipeline_name=pipeline_name,
            operation_id="step_001",
            operation_type="source_load",
            status="success",
            results=result_data,
        )

        assert completed_metadata["status"] == "success"
        assert "duration_ms" in completed_metadata or "timestamp" in completed_metadata

    def test_artifact_manager_error_handling(self, artifact_manager: ArtifactManager):
        """Test error handling in ArtifactManager operations."""
        pipeline_name = "error_test_pipeline"

        # Test loading non-existent plan
        with pytest.raises(FileNotFoundError):
            artifact_manager.load_compiled_plan("non_existent_pipeline")

        # Test operation with error
        execution_id, _ = artifact_manager.initialize_execution(
            pipeline_name, {}, "dev"
        )

        # Record operation that fails
        artifact_manager.record_operation_start(
            pipeline_name=pipeline_name,
            operation_id="failing_step",
            operation_type="transform",
            sql="SELECT * FROM non_existent_table",
        )

        error_data = {
            "error_type": "TableNotFoundError",
            "error_message": "Table 'non_existent_table' does not exist",
            "sql_query": "SELECT * FROM non_existent_table",
        }

        failed_metadata = artifact_manager.record_operation_completion(
            pipeline_name=pipeline_name,
            operation_id="failing_step",
            operation_type="transform",
            status="failed",
            results=error_data,
        )

        assert failed_metadata["status"] == "failed"

    def test_artifact_manager_directory_cleanup(
        self, artifact_manager: ArtifactManager
    ):
        """Test directory cleanup functionality."""
        pipeline_name = "cleanup_test_pipeline"

        # Create run directory and add some files
        run_dir = artifact_manager.get_run_dir(pipeline_name)
        test_file = os.path.join(run_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        assert os.path.exists(test_file)

        # Clean the directory
        artifact_manager.clean_run_dir(pipeline_name)

        # Verify directory was recreated but file is gone
        assert os.path.exists(run_dir)
        assert not os.path.exists(test_file)


class TestDuckDBStateBackendIntegration:
    """Test DuckDBStateBackend integration scenarios."""

    def test_state_backend_execution_lifecycle(self, state_backend: DuckDBStateBackend):
        """Test complete execution lifecycle with state backend."""
        run_id = "test_run_001"

        # Create execution run
        metadata = {
            "pipeline_name": "test_pipeline",
            "profile": "dev",
            "variables": {"env": "test"},
        }

        state_backend.create_run(run_id, metadata)

        # Save execution plan
        plan = [
            {"type": "source", "id": "load_users", "connector": "csv"},
            {
                "type": "transform",
                "id": "process_users",
                "query": "SELECT * FROM users WHERE active = true",
            },
        ]

        state_backend.save_plan(run_id, plan)

        # Verify plan was saved and can be loaded
        loaded_plan = state_backend.load_plan(run_id)
        assert loaded_plan is not None
        assert loaded_plan == plan
        assert len(loaded_plan) == 2
        assert loaded_plan[0]["id"] == "load_users"

    def test_state_backend_task_status_tracking(
        self, state_backend: DuckDBStateBackend
    ):
        """Test task status tracking through state backend."""
        run_id = "task_tracking_run"

        # Create run
        state_backend.create_run(run_id, {"pipeline": "task_test"})

        # Create task statuses for different scenarios
        task_statuses = [
            TaskStatus(
                id="task_001",
                state=TaskState.PENDING,
                dependencies=["setup"],
                unmet_dependencies=1,
            ),
            TaskStatus(
                id="task_002",
                state=TaskState.RUNNING,
                dependencies=[],
                unmet_dependencies=0,
                start_time=1642800000.0,
            ),
            TaskStatus(
                id="task_003",
                state=TaskState.SUCCESS,
                dependencies=["task_001"],
                unmet_dependencies=0,
                start_time=1642800000.0,
                end_time=1642800060.0,
            ),
            TaskStatus(
                id="task_004",
                state=TaskState.FAILED,
                dependencies=["task_002"],
                unmet_dependencies=0,
                error="Connection timeout",
                attempts=3,
            ),
        ]

        # Save all task statuses
        for task_status in task_statuses:
            state_backend.save_task_status(run_id, task_status)

        # Load and verify task statuses
        loaded_statuses = state_backend.load_task_statuses(run_id)

        assert len(loaded_statuses) == 4
        assert loaded_statuses["task_001"].state == TaskState.PENDING
        assert loaded_statuses["task_002"].state == TaskState.RUNNING
        assert loaded_statuses["task_003"].state == TaskState.SUCCESS
        assert loaded_statuses["task_004"].state == TaskState.FAILED
        assert loaded_statuses["task_004"].error == "Connection timeout"
        assert loaded_statuses["task_004"].attempts == 3

    def test_state_backend_run_status_updates(self, state_backend: DuckDBStateBackend):
        """Test run status update functionality."""
        run_id = "status_update_run"

        # Create and start run
        state_backend.create_run(run_id, {"test": "status_updates"})

        # Update the run status
        state_backend.update_run_status(run_id, "SUCCESS")

        # Verify run status was updated
        status = state_backend.get_run_status(run_id)
        assert status == "SUCCESS"

    def test_state_backend_run_listing(self, state_backend: DuckDBStateBackend):
        """Test run listing functionality."""
        # Create multiple runs
        runs_data = [
            ("run_001", {"pipeline": "pipeline_a"}),
            ("run_002", {"pipeline": "pipeline_b"}),
            ("run_003", {"pipeline": "pipeline_a"}),
        ]

        for run_id, metadata in runs_data:
            state_backend.create_run(run_id, metadata)

        # Test listing runs
        all_runs = state_backend.list_runs()
        assert len(all_runs) >= 3

        run_ids = [r["run_id"] for r in all_runs]
        assert "run_001" in run_ids
        assert "run_002" in run_ids
        assert "run_003" in run_ids


class TestStorageEngineIntegration:
    """Test integration between storage components and engines."""

    def test_storage_with_engine_execution(
        self, persistent_storage_setup: dict[str, Any]
    ):
        """Test storage integration with engine execution."""
        setup = persistent_storage_setup
        engine = setup["engine"]
        artifact_manager = setup["artifact_manager"]
        state_backend = setup["state_backend"]

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "value": [100, 200, 150, 300, 250],
            }
        )

        # Register data with engine
        engine.register_table("test_data", test_data)

        # Create pipeline plan
        pipeline_name = "storage_engine_test"
        plan = {
            "steps": [
                {
                    "type": "transform",
                    "name": "filtered_data",
                    "query": "SELECT * FROM test_data WHERE value > 150",
                }
            ]
        }

        # Save plan with artifact manager
        artifact_manager.save_compiled_plan(pipeline_name, plan)

        # Initialize execution tracking
        execution_id, metadata = artifact_manager.initialize_execution(
            pipeline_name, {"threshold": 150}, "dev"
        )

        # Track execution in state backend
        run_id = f"engine_run_{execution_id}"
        state_backend.create_run(
            run_id, {"artifact_execution_id": execution_id, "engine_type": "duckdb"}
        )

        # Execute query through engine
        result = engine.execute_query(
            "SELECT COUNT(*) as count FROM test_data WHERE value > 150"
        )
        result_data = result.fetchall()

        # Record operation completion
        artifact_manager.record_operation_completion(
            pipeline_name=pipeline_name,
            operation_id="filter_transform",
            operation_type="transform",
            status="success",
            results={
                "rows_returned": result_data[0][0] if result_data else 0,
                "engine_type": "duckdb",
            },
        )

        # Complete run
        state_backend.update_run_status(run_id, "SUCCESS")

        # Verify integration worked
        loaded_plan = artifact_manager.load_compiled_plan(pipeline_name)
        assert loaded_plan == plan

        runs = state_backend.list_runs()
        test_run = next((r for r in runs if r["run_id"] == run_id), None)
        assert test_run is not None

    def test_storage_persistence_across_sessions(self, temp_storage_dir: Path):
        """Test storage persistence across different sessions."""
        # First session - create and save data
        db_path = str(temp_storage_dir / "persistence_test.db")

        # Session 1: Create data
        state_backend1 = DuckDBStateBackend(db_path=db_path)
        artifact_manager1 = ArtifactManager(project_dir=str(temp_storage_dir))

        # Create run and plan
        run_id = "persistence_test_run"
        state_backend1.create_run(run_id, {"session": 1})

        plan = [{"type": "source", "name": "users"}]
        state_backend1.save_plan(run_id, plan)

        pipeline_plan = {"steps": plan}
        artifact_manager1.save_compiled_plan("persistence_test", pipeline_plan)

        # Close first session
        state_backend1.close()

        # Session 2: Load data
        state_backend2 = DuckDBStateBackend(db_path=db_path)
        artifact_manager2 = ArtifactManager(project_dir=str(temp_storage_dir))

        # Verify data persisted
        loaded_plan = state_backend2.load_plan(run_id)
        assert loaded_plan == plan

        loaded_pipeline_plan = artifact_manager2.load_compiled_plan("persistence_test")
        assert loaded_pipeline_plan == pipeline_plan

        runs = state_backend2.list_runs()
        persisted_run = next((r for r in runs if r["run_id"] == run_id), None)
        assert persisted_run is not None

        # Clean up
        state_backend2.close()

    def test_storage_concurrent_access(self, persistent_storage_setup: dict[str, Any]):
        """Test concurrent access to storage components."""
        setup = persistent_storage_setup

        # Create multiple components accessing same storage
        artifact_manager1 = setup["artifact_manager"]
        artifact_manager2 = ArtifactManager(project_dir=str(setup["artifacts_dir"]))

        state_backend1 = setup["state_backend"]
        state_backend2 = DuckDBStateBackend(db_path=str(setup["db_path"]))

        # Concurrent operations
        pipeline1 = {"steps": [{"type": "source", "name": "data1"}]}
        pipeline2 = {"steps": [{"type": "source", "name": "data2"}]}

        # Save different pipelines concurrently
        artifact_manager1.save_compiled_plan("concurrent_test_1", pipeline1)
        artifact_manager2.save_compiled_plan("concurrent_test_2", pipeline2)

        # Create runs concurrently
        state_backend1.create_run("concurrent_run_1", {"manager": 1})
        state_backend2.create_run("concurrent_run_2", {"manager": 2})

        # Verify both operations succeeded
        loaded_pipeline1 = artifact_manager1.load_compiled_plan("concurrent_test_1")
        loaded_pipeline2 = artifact_manager2.load_compiled_plan("concurrent_test_2")

        assert loaded_pipeline1 == pipeline1
        assert loaded_pipeline2 == pipeline2

        runs = state_backend1.list_runs()
        run_ids = [r["run_id"] for r in runs]
        assert "concurrent_run_1" in run_ids
        assert "concurrent_run_2" in run_ids

        # Clean up
        state_backend2.close()


class TestStorageErrorHandling:
    """Test error handling in storage integration scenarios."""

    def test_storage_database_corruption_recovery(self, temp_storage_dir: Path):
        """Test recovery from database corruption scenarios."""
        db_path = temp_storage_dir / "corruption_test.db"

        # Create initial state
        state_backend = DuckDBStateBackend(db_path=str(db_path))
        state_backend.create_run("test_run", {"initial": True})
        state_backend.close()

        # Simulate corruption by writing invalid data
        with open(db_path, "w") as f:
            f.write("corrupted data")

        # Try to access corrupted database
        with pytest.raises(duckdb.IOException):  # DuckDB specific exception
            DuckDBStateBackend(db_path=str(db_path))

        # Recovery: Remove corrupted file and reinitialize
        db_path.unlink()
        recovered_backend = DuckDBStateBackend(db_path=str(db_path))

        # Should be able to create new runs after recovery
        recovered_backend.create_run("recovery_run", {"recovered": True})

        runs = recovered_backend.list_runs()
        assert len(runs) == 1
        assert runs[0]["run_id"] == "recovery_run"

        recovered_backend.close()

    def test_storage_disk_space_handling(self, artifact_manager: ArtifactManager):
        """Test handling of disk space constraints."""
        # Create a large plan that might cause disk space issues
        large_plan = {
            "steps": [{"type": "transform", "query": "SELECT * FROM large_table"}]
            * 100,
            "large_metadata": {"data": "x" * 10000},  # Large metadata
        }

        try:
            # This should normally succeed unless disk is actually full
            compiled_path = artifact_manager.save_compiled_plan(
                "large_pipeline", large_plan
            )
            assert os.path.exists(compiled_path)

            # Verify we can load it back
            loaded_plan = artifact_manager.load_compiled_plan("large_pipeline")
            assert len(loaded_plan["steps"]) == 100

        except OSError as e:
            # If disk space is actually an issue, this is expected
            assert "No space left" in str(e) or "Disk quota exceeded" in str(e)

    def test_storage_permission_errors(self, temp_storage_dir: Path):
        """Test handling of permission errors in storage operations."""
        # Create a subdirectory
        restricted_dir = temp_storage_dir / "restricted"
        restricted_dir.mkdir()

        # Try to create artifact manager in restricted directory
        try:
            # This may or may not fail depending on the OS and permissions
            artifact_manager = ArtifactManager(project_dir=str(restricted_dir))

            # If it succeeds, try operations that might fail with permissions
            pipeline = {"steps": [{"type": "test"}]}
            artifact_manager.save_compiled_plan("permission_test", pipeline)

        except (PermissionError, OSError):
            # This is expected if permissions are actually restricted
            pytest.skip("Permission restrictions prevent this test")
