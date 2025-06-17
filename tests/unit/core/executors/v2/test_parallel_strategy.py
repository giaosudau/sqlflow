"""Tests for V2 Parallel Orchestration Strategy.

Following Kent Beck's TDD principles:
- Test behavior, not implementation
- Clear test names that explain scenarios
- Minimal mocking, focus on integration testing
"""

import re
from datetime import datetime
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from sqlflow.core.executors.v2.parallel_strategy import (
    ParallelOrchestrationStrategy,
    TaskState,
    TaskStatus,
)
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep


class TestTaskStatus:
    """Test task status tracking and immutable state transitions."""

    def test_creates_task_status_with_defaults(self):
        """Should create task status with sensible defaults."""
        status = TaskStatus(step_id="test_step")

        assert status.step_id == "test_step"
        assert status.state == TaskState.PENDING
        assert status.start_time is None
        assert status.end_time is None
        assert status.attempts == 0
        assert status.dependencies == set()
        assert status.error_message is None

    def test_with_state_creates_new_immutable_instance(self):
        """Should create new TaskStatus instance when updating state."""
        original = TaskStatus(step_id="test_step", state=TaskState.PENDING)

        updated = original.with_state(TaskState.RUNNING, attempts=1)

        # Original unchanged (immutable)
        assert original.state == TaskState.PENDING
        assert original.attempts == 0

        # New instance has updates
        assert updated.state == TaskState.RUNNING
        assert updated.attempts == 1
        assert updated.step_id == "test_step"  # Preserved


class TestParallelOrchestrationStrategy:
    """Test parallel orchestration strategy behavior."""

    def setup_method(self):
        """Set up test fixtures."""
        self.strategy = ParallelOrchestrationStrategy(max_workers=2, max_retries=1)
        self.mock_context = Mock()
        self.mock_db_session = Mock()

    def test_initializes_with_default_workers(self):
        """Should initialize with reasonable default worker count."""
        strategy = ParallelOrchestrationStrategy()

        # Should use CPU count or fallback to 4
        assert strategy.max_workers >= 4

    def test_initializes_task_statuses_correctly(self):
        """Should initialize task statuses based on dependencies."""
        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
            BaseStep(id="step3", type="load"),  # Independent
        ]

        from sqlflow.core.executors.v2.dependency_resolver import DependencyGraph

        graph = DependencyGraph.from_steps(steps)
        statuses = self.strategy._initialize_task_statuses(graph)

        # Independent steps should be eligible immediately
        assert statuses["step1"].state == TaskState.ELIGIBLE
        assert statuses["step3"].state == TaskState.ELIGIBLE

        # Dependent steps should be pending
        assert statuses["step2"].state == TaskState.PENDING
        assert statuses["step2"].dependencies == {"step1"}

    def test_gets_eligible_steps_correctly(self):
        """Should identify steps eligible for execution."""
        statuses = {
            "step1": TaskStatus("step1", TaskState.ELIGIBLE),
            "step2": TaskStatus("step2", TaskState.PENDING),
            "step3": TaskStatus("step3", TaskState.RUNNING),
            "step4": TaskStatus("step4", TaskState.SUCCESS),
        }

        eligible = self.strategy._get_eligible_steps(statuses, {"step4"})

        # Fix the test expectation - step2 has no dependencies so it's also eligible
        assert set(eligible) == {"step1", "step2"}

    def test_empty_pipeline_returns_empty_results(self):
        """Should handle empty pipeline gracefully."""
        results = self.strategy.execute_pipeline(
            [], self.mock_context, self.mock_db_session
        )

        assert results == []

    def test_handles_step_success_correctly(self):
        """Should handle successful step completion properly."""
        from sqlflow.core.executors.v2.dependency_resolver import DependencyGraph

        steps = [
            BaseStep(id="step1", type="load"),
            BaseStep(id="step2", type="transform", depends_on=["step1"]),
        ]
        graph = DependencyGraph.from_steps(steps)

        statuses = {
            "step1": TaskStatus("step1", TaskState.RUNNING),
            "step2": TaskStatus("step2", TaskState.PENDING, dependencies={"step1"}),
        }
        results = {}
        completed = set()

        success_result = StepExecutionResult.success(
            step_id="step1",
            step_type="load",
            start_time=datetime.now(),
            rows_affected=100,
        )

        self.strategy._handle_step_success(
            "step1", success_result, statuses, results, completed, graph
        )

        # Step should be marked as successful
        assert statuses["step1"].state == TaskState.SUCCESS
        assert "step1" in completed
        assert results["step1"] == success_result

        # Dependent step should be eligible
        assert statuses["step2"].state == TaskState.ELIGIBLE


class TestParallelExecutionIntegration:
    """Integration tests for complete parallel execution scenarios."""

    def test_executes_simple_plan_successfully(self):
        """Should execute simple plan with proper step creation."""
        strategy = ParallelOrchestrationStrategy(max_workers=2)
        mock_context = Mock()
        mock_db_session = Mock()
        mock_db_session.commit_changes = Mock()

        # Create proper plan with required fields
        plan = [
            {
                "id": "load1",
                "type": "load",
                "source": "test_source",
                "target_table": "table1",
            },
        ]

        # Mock source connector that returns data chunks
        mock_connector = MagicMock()
        mock_connector.__iter__.return_value = iter(
            [pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})]
        )

        # Mock context with connector registry
        mock_context.connector_registry = Mock()
        mock_context.connector_registry.create_source_connector.return_value = (
            mock_connector
        )

        # Mock handler execution that returns success
        def mock_execute(step, context):
            return StepExecutionResult.success(
                step_id=step.id,
                step_type=step.type,
                start_time=datetime.now(),
                rows_affected=50,
            )

        mock_handler = Mock()
        mock_handler.execute = mock_execute

        # Mock the handler factory
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                "sqlflow.core.executors.v2.handlers.factory.StepHandlerFactory.get_handler",
                lambda step_type: mock_handler,
            )

            results = strategy.execute_pipeline(plan, mock_context, mock_db_session)

        assert len(results) == 1
        assert results[0].step_id == "load1"
        assert results[0].status == "SUCCESS"

    def test_deadlock_detection(self):
        """Should raise ValueError on cyclic dependency (deadlock)."""
        strategy = ParallelOrchestrationStrategy(max_workers=2)
        mock_context = Mock()
        mock_db_session = Mock()
        mock_db_session.commit_changes = Mock()

        # Create a plan with a cycle: step1 -> step2 -> step1
        plan = [
            {
                "id": "step1",
                "type": "load",
                "depends_on": ["step2"],
                "source": "dummy",
                "target_table": "dummy",
            },
            {
                "id": "step2",
                "type": "load",
                "depends_on": ["step1"],
                "source": "dummy",
                "target_table": "dummy",
            },
        ]

        # Mock handler always returns success (should not be reached)
        def mock_execute(step, context):
            return StepExecutionResult.success(
                step_id=step.id,
                step_type=step.type,
                start_time=datetime.now(),
                rows_affected=1,
            )

        mock_handler = Mock()
        mock_handler.execute = mock_execute
        regex = re.compile("circular dependency", re.IGNORECASE)
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                "sqlflow.core.executors.v2.handlers.factory.StepHandlerFactory.get_handler",
                lambda step_type: mock_handler,
            )
            with pytest.raises(ValueError, match=regex):
                strategy.execute_pipeline(plan, mock_context, mock_db_session)

    def test_parallel_performance(self):
        """Should execute many independent steps in parallel faster than sequentially."""
        import time

        strategy = ParallelOrchestrationStrategy(max_workers=8)
        mock_context = Mock()
        mock_db_session = Mock()
        mock_db_session.commit_changes = Mock()

        # 16 independent steps (no dependencies)
        plan = [
            {
                "id": f"step{i}",
                "type": "load",
                "source": "dummy",
                "target_table": "dummy",
            }
            for i in range(16)
        ]

        # Each handler sleeps for 0.2s to simulate work
        def mock_execute(step, context):
            time.sleep(0.2)
            return StepExecutionResult.success(
                step_id=step.id,
                step_type=step.type,
                start_time=datetime.now(),
                rows_affected=1,
            )

        mock_handler = Mock()
        mock_handler.execute = mock_execute
        # Setup mock connector for all steps
        mock_connector = MagicMock()
        mock_connector.__iter__.return_value = iter(
            [pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})]
        )
        mock_context.connector_registry = Mock()
        mock_context.connector_registry.create_source_connector.return_value = (
            mock_connector
        )
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                "sqlflow.core.executors.v2.handlers.factory.StepHandlerFactory.get_handler",
                lambda step_type: mock_handler,
            )
            start = time.time()
            results = strategy.execute_pipeline(plan, mock_context, mock_db_session)
            elapsed = time.time() - start
        # If run sequentially: 16*0.2 = 3.2s; parallel (8 workers): should be ~0.4-0.5s
        print(f"Parallel execution of 16 steps took {elapsed:.2f} seconds")
        assert elapsed < 1.5, f"Parallel execution too slow: {elapsed:.2f}s"
        assert len(results) == 16
