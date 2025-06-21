"""Tests for clean orchestration architecture."""

from sqlflow.core.executors.v2.execution import (
    ExecutionContext,
    ExecutionContextFactory,
    StepExecutionResult,
)
from sqlflow.core.executors.v2.orchestration import (
    PipelineCoordinator,
    SequentialExecutionStrategy,
)


class MockStepExecutor:
    """Mock step executor for testing."""

    def __init__(self, supported_types: list, results: dict = None):
        self.supported_types = supported_types
        self.results = results or {}
        self.executed_steps = []

    def can_execute(self, step: dict) -> bool:
        return step.get("type") in self.supported_types

    def execute(self, step: dict, context: ExecutionContext) -> StepExecutionResult:
        step_id = step.get("id")
        self.executed_steps.append(step)

        # Return predefined result or default success
        if step_id in self.results:
            result = self.results[step_id]
            if isinstance(result, Exception):
                raise result
            return result

        return StepExecutionResult.success(step_id, "Mock execution successful")


class TestPipelineCoordinator:
    """Test pipeline coordinator with clean architecture."""

    def test_successful_pipeline_execution(self):
        """Test successful pipeline execution with multiple steps."""
        # Setup mock executors
        load_executor = MockStepExecutor(["load"])
        transform_executor = MockStepExecutor(["transform"])

        # Setup strategy and coordinator
        strategy = SequentialExecutionStrategy([load_executor, transform_executor])
        coordinator = PipelineCoordinator(strategy, [load_executor, transform_executor])

        # Setup test data with valid required fields
        steps = [
            {
                "type": "load",
                "id": "load_step",
                "source": "test.csv",
                "target_table": "test_table",
            },
            {
                "type": "transform",
                "id": "transform_step",
                "query": "SELECT * FROM test_table",
                "target_table": "transformed_table",
            },
        ]
        context = ExecutionContextFactory.create()

        # Execute
        result = coordinator.execute_pipeline(steps, context)

        # Verify
        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 2
        assert "load_step" in result["executed_steps"]
        assert "transform_step" in result["executed_steps"]
        assert result["total_steps"] == 2
        assert "execution_time" in result

    def test_pipeline_failure_stops_execution(self):
        """Test that pipeline stops on first failure."""
        # Setup executor that will fail on second step
        executor = MockStepExecutor(
            ["load", "transform"],
            results={
                "load_step": StepExecutionResult.success("load_step"),
                "transform_step": StepExecutionResult.with_error(
                    "transform_step", "Mock error"
                ),
            },
        )

        strategy = SequentialExecutionStrategy([executor])
        coordinator = PipelineCoordinator(strategy, [executor])

        steps = [
            {
                "type": "load",
                "id": "load_step",
                "source": "test.csv",
                "target_table": "test_table",
            },
            {
                "type": "transform",
                "id": "transform_step",
                "query": "SELECT * FROM test_table",
                "target_table": "transformed_table",
            },
            {
                "type": "transform",
                "id": "should_not_execute",
                "query": "SELECT * FROM transformed_table",
                "target_table": "final_table",
            },
        ]
        context = ExecutionContextFactory.create()

        # Execute
        result = coordinator.execute_pipeline(steps, context)

        # Verify failure handling
        assert result["status"] == "failed"
        assert len(result["executed_steps"]) == 2  # Only first two steps
        assert result["step_results"][1]["status"] == "error"
        assert len(executor.executed_steps) == 2  # Third step not executed

    def test_no_executor_found_error(self):
        """Test error when no executor found for step type."""
        executor = MockStepExecutor(["load"])  # Only supports load
        strategy = SequentialExecutionStrategy([executor])
        coordinator = PipelineCoordinator(
            strategy, [executor], enable_validation=False
        )  # Disable validation

        steps = [{"type": "unknown_type", "id": "unknown_step"}]
        context = ExecutionContextFactory.create()

        result = coordinator.execute_pipeline(steps, context)

        assert result["status"] == "failed"
        assert "No executor found" in result["step_results"][0]["error"]

    def test_validation_error_prevents_execution(self):
        """Test that validation errors prevent execution."""
        executor = MockStepExecutor(["load"])
        strategy = SequentialExecutionStrategy([executor])
        coordinator = PipelineCoordinator(strategy, [executor])

        # Invalid step (missing required fields)
        steps = [{"type": "load"}]  # Missing id field
        context = ExecutionContextFactory.create()

        result = coordinator.execute_pipeline(steps, context)

        assert result["status"] == "validation_error"
        assert len(result["errors"]) > 0
        assert len(executor.executed_steps) == 0  # No execution happened

    def test_variable_substitution(self):
        """Test variable substitution in steps."""
        executor = MockStepExecutor(["load"])
        strategy = SequentialExecutionStrategy([executor])
        coordinator = PipelineCoordinator(strategy, [executor])

        steps = [
            {
                "type": "load",
                "id": "load_step",
                "source": "$source_file",
                "target_table": "users",
            }
        ]
        context = ExecutionContextFactory.create()
        variables = {"source_file": "users.csv"}

        result = coordinator.execute_pipeline(steps, context, variables)

        assert result["status"] == "success"
        # Check that the executor received the substituted step
        executed_step = executor.executed_steps[0]
        assert executed_step["source"] == "users.csv"

    def test_context_with_variables(self):
        """Test execution context with variables."""
        executor = MockStepExecutor(["load"])
        strategy = SequentialExecutionStrategy([executor])
        coordinator = PipelineCoordinator(strategy, [executor])

        steps = [
            {
                "type": "load",
                "id": "load_step",
                "source": "test.csv",
                "target_table": "test_table",
            }
        ]
        context = ExecutionContextFactory.create(variables={"existing": "value"})
        additional_vars = {"new": "variable"}

        result = coordinator.execute_pipeline(steps, context, additional_vars)

        assert result["status"] == "success"
        # Verify both original and additional variables are available
        # (This would be verified through executor behavior in real implementation)

    def test_disable_validation(self):
        """Test disabling validation allows invalid steps."""
        executor = MockStepExecutor(["load"])
        strategy = SequentialExecutionStrategy([executor])
        coordinator = PipelineCoordinator(strategy, [executor], enable_validation=False)

        # Invalid step but validation disabled
        steps = [{"type": "load"}]  # Missing id field
        context = ExecutionContextFactory.create()

        result = coordinator.execute_pipeline(steps, context)

        # Should proceed to execution (though executor might handle missing id)
        assert result["status"] != "validation_error"


class TestSequentialExecutionStrategy:
    """Test sequential execution strategy."""

    def test_executes_steps_in_order(self):
        """Test that steps are executed in the correct order."""
        executor = MockStepExecutor(["test"])
        strategy = SequentialExecutionStrategy([executor])

        steps = [
            {"type": "test", "id": "step1"},
            {"type": "test", "id": "step2"},
            {"type": "test", "id": "step3"},
        ]
        context = ExecutionContextFactory.create()

        results = strategy.execute_steps(steps, context)

        assert len(results) == 3
        assert results[0].step_id == "step1"
        assert results[1].step_id == "step2"
        assert results[2].step_id == "step3"

        # Verify execution order
        executed_ids = [step["id"] for step in executor.executed_steps]
        assert executed_ids == ["step1", "step2", "step3"]

    def test_stops_on_first_error(self):
        """Test fail-fast behavior."""
        executor = MockStepExecutor(
            ["test"],
            results={"step2": StepExecutionResult.with_error("step2", "Test error")},
        )
        strategy = SequentialExecutionStrategy([executor])

        steps = [
            {"type": "test", "id": "step1"},
            {"type": "test", "id": "step2"},
            {"type": "test", "id": "step3"},
        ]
        context = ExecutionContextFactory.create()

        results = strategy.execute_steps(steps, context)

        assert len(results) == 2  # Only first two steps executed
        assert results[0].status == "success"
        assert results[1].status == "error"
        assert len(executor.executed_steps) == 2


class TestExecutionContext:
    """Test immutable execution context."""

    def test_context_immutability(self):
        """Test that context is truly immutable."""
        context = ExecutionContextFactory.create(variables={"original": "value"})

        # Adding variables should create new context
        new_context = context.with_variables({"new": "variable"})

        # Original context unchanged
        assert context.variables == {"original": "value"}
        # New context has both variables
        assert new_context.variables == {"original": "value", "new": "variable"}
        # They are different objects
        assert context is not new_context

    def test_context_factory_creates_unique_ids(self):
        """Test that factory creates unique execution IDs."""
        context1 = ExecutionContextFactory.create()
        context2 = ExecutionContextFactory.create()

        assert context1.execution_id != context2.execution_id
        assert context1.started_at <= context2.started_at

    def test_source_definition_addition(self):
        """Test adding source definitions to context."""
        context = ExecutionContextFactory.create()

        new_context = context.with_source_definition(
            "test_source", {"type": "csv", "path": "test.csv"}
        )

        assert "test_source" not in context.source_definitions
        assert "test_source" in new_context.source_definitions
        assert new_context.source_definitions["test_source"]["type"] == "csv"
