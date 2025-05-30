"""Unit tests for the thread-pool executor."""

import json
import logging
import threading
import time
from unittest.mock import MagicMock, patch

from sqlflow.core.executors.task_status import TaskState, TaskStatus
from sqlflow.core.executors.thread_pool_executor import ThreadPoolTaskExecutor


class TestTaskStatus:
    """Tests for the TaskStatus class."""

    def test_to_json(self):
        """Test converting a TaskStatus to JSON."""
        status = TaskStatus(
            id="test_task",
            state=TaskState.PENDING,
            unmet_dependencies=2,
            dependencies=["task1", "task2"],
            attempts=0,
            error=None,
            start_time=None,
            end_time=None,
        )

        json_str = status.to_json()
        data = json.loads(json_str)

        assert data["id"] == "test_task"
        assert data["state"] == "PENDING"
        assert data["unmet_dependencies"] == 2
        assert data["dependencies"] == ["task1", "task2"]
        assert data["attempts"] == 0
        assert data["error"] is None
        assert data["start_time"] is None
        assert data["end_time"] is None


class TestThreadPoolTaskExecutor:
    """Tests for the ThreadPoolTaskExecutor class."""

    def test_init(self):
        """Test initializing a ThreadPoolTaskExecutor."""
        executor = ThreadPoolTaskExecutor(max_workers=4)

        assert executor.max_workers == 4
        assert executor.task_statuses == {}
        assert executor.results == {}
        assert executor.failed_step is None
        assert executor.executed_steps == set()

    def test_init_default_workers(self):
        """Test initializing a ThreadPoolTaskExecutor with default workers."""
        with patch("os.cpu_count", return_value=8):
            executor = ThreadPoolTaskExecutor()

            assert executor.max_workers == 8

    def test_execute_empty_plan(self):
        """Test executing an empty plan."""
        executor = ThreadPoolTaskExecutor()

        results = executor.execute([])

        assert results == {}
        assert executor.task_statuses == {}

    def test_execute_single_step(self):
        """Test executing a plan with a single step."""
        executor = ThreadPoolTaskExecutor()
        executor.execute_step = MagicMock(return_value={"status": "success"})

        plan = [{"id": "step1", "type": "test"}]

        results = executor.execute(plan)

        assert "step1" in results
        assert results["step1"] == {"status": "success"}
        assert executor.executed_steps == {"step1"}
        assert executor.task_statuses["step1"].state == TaskState.SUCCESS

    def test_execute_multiple_steps_no_dependencies(self):
        """Test executing a plan with multiple steps but no dependencies."""
        executor = ThreadPoolTaskExecutor()
        executor.execute_step = MagicMock(return_value={"status": "success"})

        plan = [
            {"id": "step1", "type": "test"},
            {"id": "step2", "type": "test"},
            {"id": "step3", "type": "test"},
        ]

        results = executor.execute(plan)

        assert "step1" in results
        assert "step2" in results
        assert "step3" in results
        assert executor.executed_steps == {"step1", "step2", "step3"}
        assert executor.task_statuses["step1"].state == TaskState.SUCCESS
        assert executor.task_statuses["step2"].state == TaskState.SUCCESS
        assert executor.task_statuses["step3"].state == TaskState.SUCCESS

    def test_execute_with_dependencies(self):
        """Test executing a plan with dependencies."""
        executor = ThreadPoolTaskExecutor(
            max_workers=1
        )  # Use single worker for predictable execution

        execution_order = []
        execution_lock = threading.Lock()

        def mock_execute_step(step):
            time.sleep(0.01)
            with execution_lock:
                execution_order.append(step["id"])
            return {"status": "success"}

        executor.execute_step = MagicMock(side_effect=mock_execute_step)

        plan = [
            {"id": "step1", "type": "test"},
            {"id": "step2", "type": "test", "depends_on": ["step1"]},
            {"id": "step3", "type": "test", "depends_on": ["step2"]},
        ]

        results = executor.execute(plan)

        assert "step1" in results
        assert "step2" in results
        assert "step3" in results
        assert executor.executed_steps == {"step1", "step2", "step3"}

        assert execution_order.index("step1") < execution_order.index("step2")
        assert execution_order.index("step2") < execution_order.index("step3")

    def test_execute_with_complex_dependencies(self):
        """Test executing a plan with complex dependencies."""
        executor = ThreadPoolTaskExecutor()

        execution_order = []

        def mock_execute_step(step):
            execution_order.append(step["id"])
            return {"status": "success"}

        executor.execute_step = MagicMock(side_effect=mock_execute_step)

        plan = [
            {"id": "step1", "type": "test"},
            {"id": "step2", "type": "test"},
            {"id": "step3", "type": "test", "depends_on": ["step1", "step2"]},
            {"id": "step4", "type": "test", "depends_on": ["step1"]},
            {"id": "step5", "type": "test", "depends_on": ["step3", "step4"]},
        ]

        results = executor.execute(plan)

        assert "step1" in results
        assert "step2" in results
        assert "step3" in results
        assert "step4" in results
        assert "step5" in results
        assert executor.executed_steps == {"step1", "step2", "step3", "step4", "step5"}

        assert execution_order.index("step1") < execution_order.index("step3")
        assert execution_order.index("step2") < execution_order.index("step3")
        assert execution_order.index("step1") < execution_order.index("step4")
        assert execution_order.index("step3") < execution_order.index("step5")
        assert execution_order.index("step4") < execution_order.index("step5")

    def test_execute_step_failure(self):
        """Test handling a step failure."""
        executor = ThreadPoolTaskExecutor()

        def mock_execute_step(step):
            if step["id"] == "step2":
                raise Exception("Test error")
            return {"status": "success"}

        executor.execute_step = MagicMock(side_effect=mock_execute_step)

        plan = [
            {"id": "step1", "type": "test"},
            {"id": "step2", "type": "test", "depends_on": ["step1"]},
            {"id": "step3", "type": "test", "depends_on": ["step2"]},
        ]

        results = executor.execute(plan)

        assert "step1" in results
        assert "error" in results
        assert "failed_step" in results
        assert results["failed_step"] == "step2"
        assert "step3" not in results
        assert executor.executed_steps == {"step1"}
        assert executor.task_statuses["step1"].state == TaskState.SUCCESS
        assert executor.task_statuses["step2"].state == TaskState.FAILED
        assert executor.task_statuses["step3"].state == TaskState.PENDING
        assert executor.failed_step is not None
        assert executor.failed_step["id"] == "step2"

    def test_can_resume(self):
        """Test checking if the executor can resume."""
        executor = ThreadPoolTaskExecutor()

        assert not executor.can_resume()

        executor.failed_step = {"id": "step1", "type": "test"}

        assert executor.can_resume()

    def test_resume_nothing_to_resume(self):
        """Test resuming when there's nothing to resume."""
        executor = ThreadPoolTaskExecutor()

        results = executor.resume()

        assert results == {"status": "nothing_to_resume"}

    def test_resume_success(self):
        """Test resuming from a failure successfully."""
        executor = ThreadPoolTaskExecutor()
        executor.failed_step = {"id": "step2", "type": "test"}
        executor.results = {"step1": {"status": "success"}}
        executor.executed_steps = {"step1"}
        executor.task_statuses = {
            "step1": TaskStatus(
                id="step1",
                state=TaskState.SUCCESS,
                unmet_dependencies=0,
                dependencies=[],
            ),
            "step2": TaskStatus(
                id="step2",
                state=TaskState.FAILED,
                unmet_dependencies=0,
                dependencies=["step1"],
                error="Test error",
            ),
        }

        executor.execute_step = MagicMock(return_value={"status": "success"})

        results = executor.resume()

        assert "step1" in results
        assert "step2" in results
        assert executor.executed_steps == {"step1", "step2"}
        assert executor.task_statuses["step2"].state == TaskState.SUCCESS
        assert executor.failed_step is None

    def test_resume_failure(self):
        """Test resuming from a failure but failing again."""
        executor = ThreadPoolTaskExecutor()
        executor.failed_step = {"id": "step2", "type": "test"}
        executor.results = {"step1": {"status": "success"}}
        executor.executed_steps = {"step1"}
        executor.task_statuses = {
            "step1": TaskStatus(
                id="step1",
                state=TaskState.SUCCESS,
                unmet_dependencies=0,
                dependencies=[],
            ),
            "step2": TaskStatus(
                id="step2",
                state=TaskState.FAILED,
                unmet_dependencies=0,
                dependencies=["step1"],
                error="Test error",
            ),
        }

        def mock_execute_step(step):
            raise Exception("Another error")

        executor.execute_step = MagicMock(side_effect=mock_execute_step)

        results = executor.resume()

        assert "step1" in results
        assert "error" in results
        assert "failed_step" in results
        assert results["failed_step"] == "step2"
        assert executor.executed_steps == {"step1"}
        assert executor.task_statuses["step2"].state == TaskState.FAILED
        assert executor.failed_step is not None
        assert executor.failed_step["id"] == "step2"

    def test_log_state_transition(self, caplog):
        """Test logging state transitions."""
        caplog.set_level(logging.INFO)

        executor = ThreadPoolTaskExecutor()
        executor.task_statuses = {
            "step1": TaskStatus(
                id="step1",
                state=TaskState.PENDING,
                unmet_dependencies=0,
                dependencies=[],
            ),
        }

        executor._log_state_transition("step1", TaskState.PENDING, TaskState.RUNNING)

        # Find the log record from thread_pool_executor
        found = False
        for record in caplog.records:
            if (
                record.name == "sqlflow.core.executors.thread_pool_executor"
                and record.levelname == "INFO"
            ):
                log_data = json.loads(record.message)
                if (
                    log_data.get("event") == "task_state_transition"
                    and log_data.get("task_id") == "step1"
                ):
                    found = True
                    assert log_data["old_state"] == "PENDING"
                    assert log_data["new_state"] == "RUNNING"
                    assert "timestamp" in log_data
        assert found, (
            "No task_state_transition log record found from thread_pool_executor"
        )
