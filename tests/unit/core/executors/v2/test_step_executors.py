"""Tests for Step Executors and Registry implementation.

Following SQLFlow testing standards:
- Focus on observable behaviors, not internal implementation details
- Use real implementations and data where possible; minimize mocking
- Descriptive test names that clearly state scenario and expected result
- Arrange-Act-Assert pattern throughout
"""

from unittest.mock import Mock

import pytest

from sqlflow.core.executors.v2.step_executors import (
    ExportStepExecutor,
    LoadStepExecutor,
    StepRegistry,
    TransformStepExecutor,
    create_default_registry,
)
from sqlflow.core.executors.v2.step_executors.base import StepExecutionResult


class TestLoadStepExecutor:
    """Test LoadStepExecutor step identification and execution behaviors."""

    def setup_method(self):
        """Setup for each test."""
        self.executor = LoadStepExecutor()

    def test_can_execute_load_step_with_type_attribute(self):
        """Test executor can identify load steps with type attribute."""
        # Given: Step with type attribute
        step = Mock()
        step.type = "load"

        # When: Check if executor can handle it
        result = self.executor.can_execute(step)

        # Then: Executor can handle it
        assert result is True

    def test_can_execute_load_step_with_dict_format(self):
        """Test executor can identify load steps in dict format."""
        # Given: Step as dictionary
        step = {"type": "load", "source": "test.csv"}

        # When: Check if executor can handle it
        result = self.executor.can_execute(step)

        # Then: Executor can handle it
        assert result is True

    def test_cannot_execute_non_load_step(self):
        """Test executor rejects non-load steps."""
        # Given: Non-load step
        step = Mock()
        step.type = "transform"

        # When: Check if executor can handle it
        result = self.executor.can_execute(step)

        # Then: Executor cannot handle it
        assert result is False

    def test_execute_load_step_success(self):
        """Test successful load step execution."""
        # Given: Valid load step and context
        step = Mock()
        step.type = "load"
        step.id = "test_load"
        step.source = "test.csv"
        step.target_table = "test_table"
        step.load_mode = "replace"
        step.options = {}

        # Mock context with required components
        context = Mock()
        context.connector_registry = Mock()
        context.session = Mock()
        context.observability = Mock()
        context.observability.measure_scope.return_value.__enter__ = Mock()
        context.observability.measure_scope.return_value.__exit__ = Mock()

        # Mock connector and data loading
        mock_connector = Mock()
        mock_connector.read.return_value = [{"id": 1, "name": "test"}]
        context.connector_registry.create_source_connector.return_value = mock_connector
        context.session.load_data.return_value = 1

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: Execution succeeds
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "test_load"
        assert result.status == "success"
        assert result.rows_affected == 1
        assert result.table_name == "test_table"

    def test_execute_load_step_missing_source(self):
        """Test load step execution with missing source."""
        # Given: Load step with missing source
        step = Mock()
        step.type = "load"
        step.id = "test_load"
        step.source = ""
        step.target_table = "test_table"

        context = Mock()

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: Execution fails with proper error
        assert result.status == "error"
        assert "source cannot be empty" in result.error

    def test_execute_load_step_missing_target_table(self):
        """Test load step execution with missing target table."""
        # Given: Load step with missing target table
        step = Mock()
        step.type = "load"
        step.id = "test_load"
        step.source = "test.csv"
        step.target_table = ""

        context = Mock()

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: Execution fails with proper error
        assert result.status == "error"
        assert "target_table cannot be empty" in result.error


class TestTransformStepExecutor:
    """Test TransformStepExecutor SQL execution and UDF registration behaviors."""

    def setup_method(self):
        """Setup for each test."""
        self.executor = TransformStepExecutor()

    def test_can_execute_transform_step(self):
        """Test executor can identify transform steps."""
        # Given: Transform step
        step = Mock()
        step.type = "transform"

        # When: Check if executor can handle it
        result = self.executor.can_execute(step)

        # Then: Executor can handle it
        assert result is True

    def test_cannot_execute_non_transform_step(self):
        """Test executor rejects non-transform steps."""
        # Given: Non-transform step
        step = Mock()
        step.type = "load"

        # When: Check if executor can handle it
        result = self.executor.can_execute(step)

        # Then: Executor cannot handle it
        assert result is False

    def test_execute_transform_step_success(self):
        """Test successful transform step execution."""
        # Given: Valid transform step and context
        step = Mock()
        step.type = "transform"
        step.id = "test_transform"
        step.sql = "SELECT * FROM users"
        step.target_table = "transformed_users"
        step.udf_dependencies = []

        # Mock context
        context = Mock()
        context.variables = {"table_name": "users"}
        context.session = Mock()
        context.observability = Mock()
        context.observability.measure_scope.return_value.__enter__ = Mock()
        context.observability.measure_scope.return_value.__exit__ = Mock()

        # Mock SQL execution
        mock_result = Mock()
        mock_result.rowcount = 5
        context.session.execute.return_value = mock_result

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: Execution succeeds
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "test_transform"
        assert result.status == "success"
        assert result.rows_affected == 5
        assert result.table_name == "transformed_users"

    def test_execute_transform_step_with_udf_registration(self):
        """Test transform step execution with UDF registration."""
        # Given: Transform step with UDF dependencies
        step = Mock()
        step.type = "transform"
        step.id = "test_transform"
        step.sql = "SELECT custom_func(name) FROM users"
        step.target_table = None
        step.udf_dependencies = ["custom_func"]

        # Mock context with UDF registry
        context = Mock()
        context.variables = {}
        context.session = Mock()
        context.udf_registry = Mock()
        context.udf_registry.has_udf.return_value = True
        context.udf_registry.get_udf.return_value = Mock()
        context.observability = Mock()
        context.observability.measure_scope.return_value.__enter__ = Mock()
        context.observability.measure_scope.return_value.__exit__ = Mock()

        # Mock SQL execution
        mock_result = Mock()
        mock_result.rowcount = 3
        context.session.execute.return_value = mock_result

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: UDF is registered and execution succeeds
        context.udf_registry.has_udf.assert_called_with("custom_func")
        context.session.register_udf.assert_called_once()
        assert result.status == "success"

    def test_execute_transform_step_empty_sql(self):
        """Test transform step execution with empty SQL."""
        # Given: Transform step with empty SQL
        step = Mock()
        step.type = "transform"
        step.id = "test_transform"
        step.sql = ""

        context = Mock()

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: Execution fails with proper error
        assert result.status == "error"
        assert "SQL cannot be empty" in result.error


class TestExportStepExecutor:
    """Test ExportStepExecutor data export behaviors."""

    def setup_method(self):
        """Setup for each test."""
        self.executor = ExportStepExecutor()

    def test_can_execute_export_step(self):
        """Test executor can identify export steps."""
        # Given: Export step
        step = Mock()
        step.type = "export"

        # When: Check if executor can handle it
        result = self.executor.can_execute(step)

        # Then: Executor can handle it
        assert result is True

    def test_execute_export_step_success(self):
        """Test successful export step execution."""
        # Given: Valid export step and context
        step = Mock()
        step.type = "export"
        step.id = "test_export"
        step.source_table = "users"
        step.destination = "output.csv"
        step.format = "csv"
        step.options = {}

        # Mock context
        context = Mock()
        context.session = Mock()
        context.connector_registry = Mock()
        context.observability = Mock()
        context.observability.measure_scope.return_value.__enter__ = Mock()
        context.observability.measure_scope.return_value.__exit__ = Mock()

        # Mock data reading and writing
        mock_data = [{"id": 1, "name": "test"}]
        context.session.execute.return_value = Mock()
        context.session.execute.return_value.fetchall.return_value = mock_data

        mock_connector = Mock()
        mock_connector.write.return_value = 1
        context.connector_registry.create_destination_connector.return_value = (
            mock_connector
        )

        # When: Execute the step
        result = self.executor.execute(step, context)

        # Then: Execution succeeds
        assert isinstance(result, StepExecutionResult)
        assert result.step_id == "test_export"
        assert result.status == "success"
        assert result.rows_affected == 1


class TestStepRegistry:
    """Test StepRegistry executor registration and discovery behaviors."""

    def setup_method(self):
        """Setup for each test."""
        self.registry = StepRegistry()

    def test_register_and_find_executor(self):
        """Test registering and finding executors."""
        # Given: Registry and executor
        executor = LoadStepExecutor()

        # When: Register executor
        self.registry.register(executor)

        # Then: Can find executor for load steps
        step = Mock()
        step.type = "load"
        found_executor = self.registry.find_executor(step)
        assert found_executor is executor

    def test_find_executor_for_multiple_types(self):
        """Test finding different executors for different step types."""
        # Given: Registry with multiple executors
        load_executor = LoadStepExecutor()
        transform_executor = TransformStepExecutor()

        self.registry.register(load_executor)
        self.registry.register(transform_executor)

        # When: Find executors for different step types
        load_step = Mock()
        load_step.type = "load"

        transform_step = Mock()
        transform_step.type = "transform"

        # Then: Correct executors are found
        assert self.registry.find_executor(load_step) is load_executor
        assert self.registry.find_executor(transform_step) is transform_executor

    def test_find_executor_not_found(self):
        """Test error when no executor can handle step."""
        # Given: Empty registry
        step = Mock()
        step.type = "unknown"
        step.id = "test_step"

        # When/Then: Finding executor raises error
        with pytest.raises(ValueError) as exc_info:
            self.registry.find_executor(step)

        assert "No executor found" in str(exc_info.value)
        assert "unknown" in str(exc_info.value)

    def test_execute_step_delegates_to_executor(self):
        """Test that execute_step delegates to appropriate executor."""
        # Given: Registry with mock executor
        mock_executor = Mock()
        mock_executor.can_execute.return_value = True
        mock_result = StepExecutionResult(step_id="test", status="success")
        mock_executor.execute.return_value = mock_result

        self.registry.register(mock_executor)

        step = Mock()
        context = Mock()

        # When: Execute step through registry
        result = self.registry.execute_step(step, context)

        # Then: Executor is called and result returned
        mock_executor.execute.assert_called_once_with(step, context)
        assert result is mock_result

    def test_get_registered_executors(self):
        """Test getting list of registered executors."""
        # Given: Registry with executors
        executor1 = LoadStepExecutor()
        executor2 = TransformStepExecutor()

        self.registry.register(executor1)
        self.registry.register(executor2)

        # When: Get registered executors
        executors = self.registry.get_registered_executors()

        # Then: All executors are returned
        assert len(executors) == 2
        assert executor1 in executors
        assert executor2 in executors

    def test_clear_registry(self):
        """Test clearing registry."""
        # Given: Registry with executors
        self.registry.register(LoadStepExecutor())

        # When: Clear registry
        self.registry.clear()

        # Then: No executors remain
        assert len(self.registry.get_registered_executors()) == 0


class TestDefaultRegistry:
    """Test default registry factory creates executors for standard step types."""

    def test_create_default_registry(self):
        """Test creating default registry with standard executors."""
        # When: Create default registry
        registry = create_default_registry()

        # Then: Registry contains expected executors
        executors = registry.get_registered_executors()
        assert len(executors) == 3

        # Test that each step type can be handled
        load_step = Mock()
        load_step.type = "load"

        transform_step = Mock()
        transform_step.type = "transform"

        export_step = Mock()
        export_step.type = "export"

        # All step types should have executors
        assert registry.find_executor(load_step) is not None
        assert registry.find_executor(transform_step) is not None
        assert registry.find_executor(export_step) is not None


class TestPipelineExecution:
    """Test complete pipeline execution flows using step executors and registry."""

    def test_complete_pipeline_execution(self):
        """Test complete pipeline execution using the new architecture."""
        # Given: Registry with all executors
        registry = create_default_registry()

        # Mock context
        context = Mock()
        context.variables = {"env": "test"}
        context.session = Mock()
        context.connector_registry = Mock()
        context.observability = Mock()
        context.observability.measure_scope.return_value.__enter__ = Mock()
        context.observability.measure_scope.return_value.__exit__ = Mock()

        # Mock successful operations
        mock_connector = Mock()
        mock_connector.read.return_value = [{"id": 1}]
        mock_connector.write.return_value = 1
        context.connector_registry.create_source_connector.return_value = mock_connector
        context.connector_registry.create_destination_connector.return_value = (
            mock_connector
        )

        mock_result = Mock()
        mock_result.rowcount = 1
        mock_result.fetchall.return_value = [{"id": 1}]
        context.session.execute.return_value = mock_result
        context.session.load_data.return_value = 1

        # Pipeline steps
        load_step = Mock()
        load_step.type = "load"
        load_step.id = "load_data"
        load_step.source = "input.csv"
        load_step.target_table = "raw_data"
        load_step.load_mode = "replace"
        load_step.options = {}

        transform_step = Mock()
        transform_step.type = "transform"
        transform_step.id = "process_data"
        transform_step.sql = "SELECT * FROM raw_data WHERE env = '${env}'"
        transform_step.target_table = "processed_data"
        transform_step.udf_dependencies = []

        export_step = Mock()
        export_step.type = "export"
        export_step.id = "export_results"
        export_step.source_table = "processed_data"
        export_step.destination = "output.csv"
        export_step.format = "csv"
        export_step.options = {}

        # When: Execute pipeline steps
        load_result = registry.execute_step(load_step, context)
        transform_result = registry.execute_step(transform_step, context)
        export_result = registry.execute_step(export_step, context)

        # Then: All steps execute successfully
        assert load_result.status == "success"
        assert transform_result.status == "success"
        assert export_result.status == "success"

        # Verify proper sequence of operations
        context.session.load_data.assert_called_once()
        context.session.execute.assert_called()
        context.connector_registry.create_destination_connector.assert_called_once()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
