"""Integration tests for the thread-pool executor."""

import json
import logging
import time
from typing import Any, Dict

from sqlflow.core.executors.thread_pool_executor import (
    TaskState,
    ThreadPoolTaskExecutor,
)


class TestThreadPoolExecutorIntegration:
    """Integration tests for the ThreadPoolTaskExecutor."""

    def test_concurrent_execution(self, caplog):
        """Test that tasks are executed concurrently when possible."""
        # Configure logging to use caplog's handler
        logger = logging.getLogger("sqlflow.core.executors.thread_pool_executor")
        logger.handlers = []  # Remove any existing handlers
        logger.addHandler(caplog.handler)
        logger.propagate = True
        caplog.set_level(logging.INFO)

        plan = [
            {"id": "task1", "type": "test"},
            {"id": "task2", "type": "test"},
            {"id": "task3", "type": "test"},
            {"id": "task4", "type": "test", "depends_on": ["task1", "task2"]},
            {"id": "task5", "type": "test", "depends_on": ["task3"]},
            {"id": "task6", "type": "test", "depends_on": ["task4", "task5"]},
        ]

        class TimingExecutor(ThreadPoolTaskExecutor):
            def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
                time.sleep(0.1)
                return {"status": "success", "task_id": step["id"]}

        executor = TimingExecutor(max_workers=3)
        results = executor.execute(plan)

        assert len(results) == 6
        assert all(f"task{i}" in results for i in range(1, 7))

        # Collect all transitions
        transitions = {}
        for record in caplog.records:
            if (
                record.name == "sqlflow.core.executors.thread_pool_executor"
                and record.levelname == "INFO"
            ):
                try:
                    data = json.loads(record.message)
                    if (
                        data.get("event") == "task_state_transition"
                        and data.get("new_state") == "RUNNING"
                    ):
                        task_id = data["task_id"]
                        timestamp = data["timestamp"]
                        transitions[task_id] = timestamp
                except json.JSONDecodeError:
                    continue  # Skip non-JSON log messages

        # Verify we have all the transitions we need
        assert (
            len(transitions) >= 6
        ), f"Expected transitions for all 6 tasks, but got {len(transitions)}: {transitions}"
        assert all(
            f"task{i}" in transitions for i in range(1, 7)
        ), f"Missing transitions for some tasks. Got: {transitions}"

        # Verify concurrent execution of independent tasks
        assert (
            abs(transitions["task1"] - transitions["task2"]) < 0.05
        ), "task1 and task2 should start at similar times"
        assert (
            abs(transitions["task1"] - transitions["task3"]) < 0.05
        ), "task1 and task3 should start at similar times"
        assert (
            abs(transitions["task2"] - transitions["task3"]) < 0.05
        ), "task2 and task3 should start at similar times"

        # Verify dependent tasks start after their dependencies
        assert (
            transitions["task4"] > transitions["task1"]
        ), "task4 should start after task1"
        assert (
            transitions["task4"] > transitions["task2"]
        ), "task4 should start after task2"

        assert (
            transitions["task5"] > transitions["task3"]
        ), "task5 should start after task3"

        assert (
            transitions["task6"] > transitions["task4"]
        ), "task6 should start after task4"
        assert (
            transitions["task6"] > transitions["task5"]
        ), "task6 should start after task5"

    def test_execution_order_with_dependencies(self):
        """Test that execution order respects dependencies."""
        plan = [
            {"id": "task1", "type": "test"},
            {"id": "task2", "type": "test", "depends_on": ["task1"]},
            {"id": "task3", "type": "test", "depends_on": ["task2"]},
            {"id": "task4", "type": "test", "depends_on": ["task1"]},
            {"id": "task5", "type": "test", "depends_on": ["task4"]},
        ]

        execution_order = []

        class OrderTrackingExecutor(ThreadPoolTaskExecutor):
            def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
                execution_order.append(step["id"])
                return {"status": "success", "task_id": step["id"]}

        executor = OrderTrackingExecutor(max_workers=2)
        results = executor.execute(plan)

        assert len(results) == 5
        assert all(f"task{i}" in results for i in range(1, 6))

        assert execution_order.index("task1") < execution_order.index("task2")
        assert execution_order.index("task2") < execution_order.index("task3")
        assert execution_order.index("task1") < execution_order.index("task4")
        assert execution_order.index("task4") < execution_order.index("task5")

    def test_execution_with_failure(self):
        """Test that execution stops on failure and tracks the failed step."""
        plan = [
            {"id": "task1", "type": "test"},
            {"id": "task2", "type": "test", "depends_on": ["task1"]},
            {"id": "task3", "type": "test", "depends_on": ["task2"]},
        ]

        class FailingExecutor(ThreadPoolTaskExecutor):
            def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
                if step["id"] == "task2":
                    raise Exception("Simulated failure")
                return {"status": "success", "task_id": step["id"]}

        executor = FailingExecutor(max_workers=2)
        results = executor.execute(plan)

        assert "task1" in results
        assert "error" in results
        assert "failed_step" in results
        assert results["failed_step"] == "task2"

        assert "task3" not in results

        assert executor.task_statuses["task1"].state == TaskState.SUCCESS
        assert executor.task_statuses["task2"].state == TaskState.FAILED
        assert executor.task_statuses["task3"].state == TaskState.PENDING

        assert executor.failed_step is not None
        assert executor.failed_step["id"] == "task2"

    def test_resume_from_failure(self):
        """Test resuming execution from a failure."""
        plan = [
            {"id": "task1", "type": "test"},
            {"id": "task2", "type": "test", "depends_on": ["task1"]},
            {"id": "task3", "type": "test", "depends_on": ["task2"]},
        ]

        failure_count = 0

        class ResumableExecutor(ThreadPoolTaskExecutor):
            def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
                nonlocal failure_count
                if step["id"] == "task2" and failure_count == 0:
                    failure_count += 1
                    raise Exception("Simulated failure")
                return {"status": "success", "task_id": step["id"]}

        executor = ResumableExecutor(max_workers=2)
        results = executor.execute(plan)

        assert "task1" in results
        assert "error" in results
        assert "failed_step" in results
        assert results["failed_step"] == "task2"

        print(f"Failed step before resume: {executor.failed_step}")

        task2_step = next(step for step in plan if step["id"] == "task2")
        executor.failed_step = task2_step

        resume_results = executor.resume()

        assert "task1" in resume_results
        assert "task2" in resume_results
        assert "task3" in resume_results

        assert executor.task_statuses["task1"].state == TaskState.SUCCESS
        assert executor.task_statuses["task2"].state == TaskState.SUCCESS
        assert executor.task_statuses["task3"].state == TaskState.SUCCESS

        assert executor.failed_step is None
