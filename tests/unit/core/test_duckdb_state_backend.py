"""Unit tests for the DuckDB state backend."""

import os
import tempfile
import uuid

import pytest

from sqlflow.sqlflow.core.executors.task_status import TaskState, TaskStatus
from sqlflow.sqlflow.core.storage.duckdb_state_backend import DuckDBStateBackend


class TestDuckDBStateBackend:
    """Tests for the DuckDBStateBackend class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.gettempdir()
        db_path = os.path.join(temp_dir, f"sqlflow_test_{uuid.uuid4()}.db")

        yield db_path

        if os.path.exists(db_path):
            os.unlink(db_path)

    def test_init(self, temp_db_path):
        """Test initializing a DuckDBStateBackend."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        assert backend.db_path == temp_db_path
        assert backend.conn is not None

        backend.close()

    def test_create_run(self, temp_db_path):
        """Test creating a new execution run."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        run_id = "test_run"
        metadata = {"test_key": "test_value"}

        backend.create_run(run_id, metadata)

        status = backend.get_run_status(run_id)
        assert status == "RUNNING"

        runs = backend.list_runs()
        assert len(runs) == 1
        assert runs[0]["run_id"] == run_id
        assert runs[0]["status"] == "RUNNING"
        assert runs[0]["metadata"] == metadata

        backend.close()

    def test_save_load_plan(self, temp_db_path):
        """Test saving and loading an execution plan."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        run_id = "test_run"
        backend.create_run(run_id)

        plan = [
            {"id": "step1", "type": "test"},
            {"id": "step2", "type": "test", "depends_on": ["step1"]},
        ]

        backend.save_plan(run_id, plan)

        loaded_plan = backend.load_plan(run_id)
        assert loaded_plan is not None
        assert len(loaded_plan) == 2
        assert loaded_plan[0]["id"] == "step1"
        assert loaded_plan[1]["id"] == "step2"
        assert loaded_plan[1]["depends_on"] == ["step1"]

        backend.close()

    def test_save_load_task_status(self, temp_db_path):
        """Test saving and loading task statuses."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        run_id = "test_run"
        backend.create_run(run_id)

        task_status = TaskStatus(
            id="task1",
            state=TaskState.RUNNING,
            unmet_dependencies=0,
            dependencies=[],
            attempts=1,
            start_time=123.45,
            end_time=None,
            error=None,
        )

        backend.save_task_status(run_id, task_status)

        task_statuses = backend.load_task_statuses(run_id)
        assert len(task_statuses) == 1
        assert "task1" in task_statuses
        assert task_statuses["task1"].id == "task1"
        assert task_statuses["task1"].state == TaskState.RUNNING
        assert task_statuses["task1"].unmet_dependencies == 0
        assert task_statuses["task1"].dependencies == []
        assert task_statuses["task1"].attempts == 1
        assert task_statuses["task1"].start_time == 123.45
        assert task_statuses["task1"].end_time is None
        assert task_statuses["task1"].error is None

        backend.close()

    def test_update_run_status(self, temp_db_path):
        """Test updating the status of a run."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        run_id = "test_run"
        backend.create_run(run_id)

        status = backend.get_run_status(run_id)
        assert status == "RUNNING"

        backend.update_run_status(run_id, "SUCCESS")

        status = backend.get_run_status(run_id)
        assert status == "SUCCESS"

        backend.close()

    def test_list_runs(self, temp_db_path):
        """Test listing execution runs."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        run_id1 = "test_run1"
        run_id2 = "test_run2"

        backend.create_run(run_id1, {"test_key": "test_value1"})
        backend.create_run(run_id2, {"test_key": "test_value2"})

        runs = backend.list_runs()
        assert len(runs) == 2

        assert runs[0]["run_id"] == run_id2
        assert runs[1]["run_id"] == run_id1

        backend.close()

    def test_nonexistent_run(self, temp_db_path):
        """Test handling nonexistent runs."""
        backend = DuckDBStateBackend(db_path=temp_db_path)

        status = backend.get_run_status("nonexistent_run")
        assert status is None

        plan = backend.load_plan("nonexistent_run")
        assert plan is None

        task_statuses = backend.load_task_statuses("nonexistent_run")
        assert task_statuses == {}

        backend.close()
