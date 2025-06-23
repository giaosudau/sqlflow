"""Unit tests for clean V2 result models.

Testing immutable result dataclasses for comprehensive observability.
Following Raymond Hettinger's dataclass best practices.
"""

from datetime import datetime

import pytest

from sqlflow.core.executors.v2.results.models import (
    ExecutionResult,
    StepResult,
    create_error_result,
    create_execution_result,
    create_success_result,
)


class TestStepResult:
    """Test immutable StepResult dataclass."""

    def test_step_result_immutability(self):
        """Test that StepResult is immutable."""
        result = StepResult(
            step_id="test_step", success=True, duration_ms=100.0, rows_affected=5
        )

        # Should not be able to modify after creation
        with pytest.raises(AttributeError):
            result.step_id = "modified"  # Should raise FrozenInstanceError

    def test_step_result_success_creation(self):
        """Test creating successful StepResult."""
        result = StepResult(
            step_id="test_step", success=True, duration_ms=150.5, rows_affected=42
        )

        assert result.step_id == "test_step"
        assert result.success is True
        assert result.duration_ms == 150.5
        assert result.rows_affected == 42
        assert result.error_message is None
        assert isinstance(result.start_time, datetime)

    def test_step_result_error_creation(self):
        """Test creating error StepResult."""
        result = StepResult(
            step_id="failed_step",
            success=False,
            duration_ms=50.0,
            error_message="Something went wrong",
        )

        assert result.step_id == "failed_step"
        assert result.success is False
        assert result.duration_ms == 50.0
        assert result.rows_affected == 0  # Default
        assert result.error_message == "Something went wrong"

    def test_step_result_validation(self):
        """Test StepResult validation logic."""
        # Failed step must have error message when validated
        invalid_result = StepResult(
            step_id="test",
            success=False,
            duration_ms=100.0,
            # Missing error_message
        )
        with pytest.raises(ValueError, match="Failed step must have error_message"):
            invalid_result.validate()

        # Successful step cannot have error message when validated
        invalid_result2 = StepResult(
            step_id="test",
            success=True,
            duration_ms=100.0,
            error_message="This shouldn't be here",
        )
        with pytest.raises(
            ValueError, match="Successful step cannot have error_message"
        ):
            invalid_result2.validate()

    def test_step_result_with_metadata(self):
        """Test StepResult with metadata."""
        metadata = {"table_size": 1000, "query_plan": "index_scan"}

        result = StepResult(
            step_id="test_step", success=True, duration_ms=100.0, metadata=metadata
        )

        assert result.metadata == metadata
        # With the factory function, metadata should be the same reference
        # since we're passing the dict directly. This is acceptable for our use case.
        assert result.metadata is metadata

    def test_step_result_memory_optimization(self):
        """Test that StepResult uses __slots__ for memory optimization."""
        result = StepResult(step_id="test", success=True, duration_ms=100.0)

        # Should have __slots__
        assert hasattr(result, "__slots__")

        # Should not have __dict__ (slots optimization)
        assert not hasattr(result, "__dict__")

        # Should be frozen (immutable)
        with pytest.raises((AttributeError, TypeError)):
            result.step_id = "new_id"


class TestExecutionResult:
    """Test immutable ExecutionResult dataclass."""

    def test_execution_result_creation(self):
        """Test creating ExecutionResult."""
        step_results = [
            StepResult("step1", True, 100.0, 10),
            StepResult("step2", True, 200.0, 20),
        ]

        result = ExecutionResult(
            success=True,
            step_results=tuple(step_results),
            total_duration_ms=300.0,
            variables={"var1": "value1"},
        )

        assert result.success is True
        assert len(result.step_results) == 2
        assert result.total_duration_ms == 300.0
        assert result.variables == {"var1": "value1"}
        assert isinstance(result.start_time, datetime)

    def test_execution_result_properties(self):
        """Test computed properties of ExecutionResult."""
        step_results = [
            StepResult("step1", True, 100.0, 10),
            StepResult("step2", False, 200.0, 0, error_message="Failed"),
            StepResult("step3", True, 150.0, 5),
        ]

        result = ExecutionResult(
            success=False,  # Overall failure due to step2
            step_results=tuple(step_results),
            total_duration_ms=450.0,
        )

        # Test total_rows_affected property
        assert result.total_rows_affected == 15  # 10 + 0 + 5

        # Test failed_steps property
        failed_steps = result.failed_steps
        assert len(failed_steps) == 1
        assert failed_steps[0].step_id == "step2"

        # Test successful_steps property
        successful_steps = result.successful_steps
        assert len(successful_steps) == 2
        assert successful_steps[0].step_id == "step1"
        assert successful_steps[1].step_id == "step3"

    def test_execution_result_validation(self):
        """Test ExecutionResult validation."""
        # Empty step results are now allowed (for empty pipelines)
        result = ExecutionResult(
            success=True, step_results=tuple(), total_duration_ms=0.0  # Empty
        )
        assert result.success
        assert len(result.step_results) == 0

        # Success must match step results
        step_results = [
            StepResult("step1", True, 100.0),
            StepResult("step2", False, 200.0, error_message="Failed"),
        ]

        with pytest.raises(
            ValueError, match="ExecutionResult.success must match all step results"
        ):
            ExecutionResult(
                success=True,  # Claiming success but step2 failed
                step_results=tuple(step_results),
                total_duration_ms=300.0,
            )

    def test_execution_result_immutability(self):
        """Test that ExecutionResult is immutable."""
        step_results = [StepResult("step1", True, 100.0)]

        result = ExecutionResult(
            success=True, step_results=tuple(step_results), total_duration_ms=100.0
        )

        # Should not be able to modify
        with pytest.raises(AttributeError):
            result.success = False

        # Step results should be a tuple (immutable)
        assert isinstance(result.step_results, tuple)


class TestFactoryFunctions:
    """Test factory functions for creating results."""

    def test_create_success_result(self):
        """Test create_success_result factory function."""
        result = create_success_result(
            step_id="test_step",
            duration_ms=123.5,
            rows_affected=42,
            metadata={"info": "test"},
        )

        assert isinstance(result, StepResult)
        assert result.step_id == "test_step"
        assert result.success is True
        assert result.duration_ms == 123.5
        assert result.rows_affected == 42
        assert result.error_message is None
        assert result.metadata == {"info": "test"}

    def test_create_error_result(self):
        """Test create_error_result factory function."""
        result = create_error_result(
            step_id="failed_step",
            duration_ms=50.0,
            error_message="Something went wrong",
            metadata={"error_code": 500},
        )

        assert isinstance(result, StepResult)
        assert result.step_id == "failed_step"
        assert result.success is False
        assert result.duration_ms == 50.0
        assert result.rows_affected == 0
        assert result.error_message == "Something went wrong"
        assert result.metadata == {"error_code": 500}

    def test_create_execution_result(self):
        """Test create_execution_result factory function."""
        step_results = [
            create_success_result("step1", 100.0, 10),
            create_success_result("step2", 200.0, 20),
        ]

        result = create_execution_result(
            step_results=step_results,
            variables={"var1": "value1"},
            metadata={"pipeline": "test"},
        )

        assert isinstance(result, ExecutionResult)
        assert result.success is True  # All steps successful
        assert result.total_duration_ms == 300.0  # Auto-calculated
        assert len(result.step_results) == 2
        assert result.variables == {"var1": "value1"}
        assert result.metadata == {"pipeline": "test"}

    def test_create_execution_result_with_failure(self):
        """Test create_execution_result with failed steps."""
        step_results = [
            create_success_result("step1", 100.0, 10),
            create_error_result("step2", 50.0, "Failed"),
        ]

        result = create_execution_result(step_results)

        assert result.success is False  # One step failed
        assert result.total_duration_ms == 150.0
        assert len(result.failed_steps) == 1
        assert len(result.successful_steps) == 1

    def test_factory_function_validation(self):
        """Test factory function validation."""
        # create_execution_result now handles empty step lists (for empty pipelines)
        result = create_execution_result([])
        assert result.success
        assert len(result.step_results) == 0
        assert result.total_duration_ms == 0.0

    def test_factory_function_defaults(self):
        """Test factory function defaults."""
        # create_success_result with minimal parameters
        result = create_success_result("test", 100.0)
        assert result.rows_affected == 0  # Default
        assert result.metadata == {}  # Default

        # create_error_result with minimal parameters
        result = create_error_result("test", 100.0, "error")
        assert result.metadata == {}  # Default

        # create_execution_result with minimal parameters
        step_results = [create_success_result("test", 100.0)]
        result = create_execution_result(step_results)
        assert result.variables == {}  # Default
        assert result.metadata == {}  # Default
