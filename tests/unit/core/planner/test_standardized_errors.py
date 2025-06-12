"""Tests for standardized planner error handling.

Following Zen of Python:
- Errors should never pass silently: All validation errors are caught and reported
- Explicit is better than implicit: Clear error messages with context
- Simple is better than complex: Consistent error handling patterns
"""

import pytest

from sqlflow.core.planner.errors import (
    DependencyError,
    PlannerError,
    StepBuildError,
    ValidationError,
    create_dependency_error,
    create_validation_error,
)


class TestPlannerError:
    """Test base PlannerError functionality."""

    def test_basic_error_creation(self):
        """Basic error can be created with just a message."""
        error = PlannerError("Something went wrong")
        assert str(error) == "Something went wrong"
        assert error.message == "Something went wrong"
        assert error.context == {}

    def test_error_with_context(self):
        """Error with context information formats correctly."""
        context = {
            "step": "transform_users",
            "line_number": 42,
            "variables": ["name", "age"],
        }
        error = PlannerError("Processing failed", context)

        error_str = str(error)
        assert "Processing failed" in error_str
        assert "Context:" in error_str
        assert "step: transform_users" in error_str
        assert "line_number: 42" in error_str
        assert "variables: name, age" in error_str

    def test_error_with_empty_context_values(self):
        """Error with empty context values doesn't include them."""
        context = {
            "step": "transform_users",
            "empty_list": [],
            "empty_string": "",
            "none_value": None,
        }
        error = PlannerError("Test error", context)

        error_str = str(error)
        assert "step: transform_users" in error_str
        assert "empty_list" not in error_str
        assert "empty_string" not in error_str
        assert "none_value" not in error_str


class TestValidationError:
    """Test ValidationError functionality."""

    def test_validation_error_missing_variables(self):
        """ValidationError handles missing variables correctly."""
        error = ValidationError(
            "Variables not found",
            missing_variables=["name", "age"],
            context_locations={"name": ["line 10", "line 20"]},
        )

        assert error.missing_variables == ["name", "age"]
        assert error.context_locations == {"name": ["line 10", "line 20"]}

        error_str = str(error)
        assert "Variables not found" in error_str
        assert "missing_variables: name, age" in error_str
        assert "name referenced at: line 10, line 20" in error_str

    def test_validation_error_missing_tables(self):
        """ValidationError handles missing tables correctly."""
        error = ValidationError(
            "Tables not found",
            missing_tables=["users", "orders"],
            context_locations={"users": ["line 5 in export_step"]},
        )

        assert error.missing_tables == ["users", "orders"]

        error_str = str(error)
        assert "Tables not found" in error_str
        assert "missing_tables: users, orders" in error_str
        assert "users referenced at: line 5 in export_step" in error_str

    def test_validation_error_all_types(self):
        """ValidationError handles all error types together."""
        error = ValidationError(
            "Multiple validation issues",
            missing_variables=["var1"],
            missing_tables=["table1"],
            invalid_references=["${invalid}"],
            context_locations={"var1": ["line 1"], "table1": ["line 2"]},
        )

        error_str = str(error)
        assert "Multiple validation issues" in error_str
        assert "missing_variables: var1" in error_str
        assert "missing_tables: table1" in error_str
        assert "invalid_references: ${invalid}" in error_str


class TestDependencyError:
    """Test DependencyError functionality."""

    def test_dependency_error_cycles(self):
        """DependencyError handles circular dependencies correctly."""
        cycles = [
            ["step_a", "step_b", "step_a"],
            ["step_x", "step_y", "step_z", "step_x"],
        ]
        error = DependencyError("Circular dependencies found", cycles=cycles)

        assert error.cycles == cycles

        error_str = str(error)
        assert "Circular dependencies found" in error_str
        assert "Cycle 1: step_a → step_b → step_a" in error_str
        assert "Cycle 2: step_x → step_y → step_z → step_x" in error_str

    def test_dependency_error_missing_dependencies(self):
        """DependencyError handles missing dependencies correctly."""
        error = DependencyError(
            "Dependencies not satisfied", missing_dependencies=["step_1", "step_2"]
        )

        assert error.missing_dependencies == ["step_1", "step_2"]

        error_str = str(error)
        assert "Dependencies not satisfied" in error_str
        assert "missing_dependencies: step_1, step_2" in error_str

    def test_dependency_error_conflicts(self):
        """DependencyError handles conflicting dependencies correctly."""
        conflicts = {"step_a": ["step_b", "step_c"], "step_x": ["step_y"]}
        error = DependencyError(
            "Dependency conflicts", conflicting_dependencies=conflicts
        )

        assert error.conflicting_dependencies == conflicts

        error_str = str(error)
        assert "Dependency conflicts" in error_str
        assert "step_a conflicts with: step_b, step_c" in error_str
        assert "step_x conflicts with: step_y" in error_str


class TestStepBuildError:
    """Test StepBuildError functionality."""

    def test_step_build_error_failed_steps(self):
        """StepBuildError handles failed steps correctly."""
        error = StepBuildError(
            "Step building failed",
            failed_steps=["transform_users", "export_csv"],
            step_errors={
                "transform_users": "SQL syntax error",
                "export_csv": "Invalid file path",
            },
        )

        assert error.failed_steps == ["transform_users", "export_csv"]
        assert error.step_errors == {
            "transform_users": "SQL syntax error",
            "export_csv": "Invalid file path",
        }

        error_str = str(error)
        assert "Step building failed" in error_str
        assert "failed_steps: transform_users, export_csv" in error_str
        assert "transform_users: SQL syntax error" in error_str
        assert "export_csv: Invalid file path" in error_str


class TestErrorCreationUtilities:
    """Test utility functions for creating standardized errors."""

    def test_create_validation_error_with_missing_variables(self):
        """create_validation_error handles missing variables correctly."""
        error = create_validation_error(missing_variables=["name", "age"])

        assert isinstance(error, ValidationError)
        assert error.missing_variables == ["name", "age"]
        assert "Missing variables: name, age" in str(error)

    def test_create_validation_error_with_missing_tables(self):
        """create_validation_error handles missing tables correctly."""
        error = create_validation_error(missing_tables=["users", "orders"])

        assert isinstance(error, ValidationError)
        assert error.missing_tables == ["users", "orders"]
        assert "Missing tables: users, orders" in str(error)

    def test_create_validation_error_multiple_issues(self):
        """create_validation_error handles multiple issues correctly."""
        error = create_validation_error(
            missing_variables=["var1"],
            missing_tables=["table1"],
            invalid_references=["${bad}"],
        )

        error_str = str(error)
        assert "Missing variables: var1" in error_str
        assert "Missing tables: table1" in error_str
        assert "Invalid references: ${bad}" in error_str

    def test_create_validation_error_empty(self):
        """create_validation_error handles empty case correctly."""
        error = create_validation_error()

        assert isinstance(error, ValidationError)
        assert "Pipeline validation failed" in str(error)

    def test_create_dependency_error_with_cycles(self):
        """create_dependency_error handles cycles correctly."""
        cycles = [["a", "b", "a"]]
        error = create_dependency_error(cycles=cycles)

        assert isinstance(error, DependencyError)
        assert error.cycles == cycles
        assert "Circular dependencies detected (1 cycles)" in str(error)

    def test_create_dependency_error_multiple_issues(self):
        """create_dependency_error handles multiple issues correctly."""
        error = create_dependency_error(
            cycles=[["a", "b", "a"]],
            missing_dependencies=["step1"],
            conflicting_dependencies={"step2": ["step3"]},
        )

        error_str = str(error)
        assert "Circular dependencies detected (1 cycles)" in error_str
        assert "Missing dependencies: step1" in error_str
        assert "Conflicting dependencies for 1 steps" in error_str

    def test_create_dependency_error_empty(self):
        """create_dependency_error handles empty case correctly."""
        error = create_dependency_error()

        assert isinstance(error, DependencyError)
        assert "Dependency resolution failed" in str(error)


class TestErrorInheritance:
    """Test error inheritance and type checking."""

    def test_all_errors_inherit_from_planner_error(self):
        """All planner errors inherit from PlannerError."""
        validation_error = ValidationError("test")
        dependency_error = DependencyError("test")
        step_build_error = StepBuildError("test")

        assert isinstance(validation_error, PlannerError)
        assert isinstance(dependency_error, PlannerError)
        assert isinstance(step_build_error, PlannerError)

    def test_all_errors_inherit_from_exception(self):
        """All planner errors inherit from Exception."""
        planner_error = PlannerError("test")
        validation_error = ValidationError("test")
        dependency_error = DependencyError("test")
        step_build_error = StepBuildError("test")

        assert isinstance(planner_error, Exception)
        assert isinstance(validation_error, Exception)
        assert isinstance(dependency_error, Exception)
        assert isinstance(step_build_error, Exception)

    def test_errors_can_be_raised_and_caught(self):
        """All error types can be raised and caught properly."""
        with pytest.raises(ValidationError) as exc_info:
            raise ValidationError("test validation error")
        assert "test validation error" in str(exc_info.value)

        with pytest.raises(DependencyError) as exc_info:
            raise DependencyError("test dependency error")
        assert "test dependency error" in str(exc_info.value)

        with pytest.raises(StepBuildError) as exc_info:
            raise StepBuildError("test step build error")
        assert "test step build error" in str(exc_info.value)

        # Can also catch as base PlannerError
        with pytest.raises(PlannerError):
            raise ValidationError("test")

        with pytest.raises(PlannerError):
            raise DependencyError("test")

        with pytest.raises(PlannerError):
            raise StepBuildError("test")


class TestErrorContextFormatting:
    """Test error context formatting in various scenarios."""

    def test_context_with_various_types(self):
        """Context formatting handles various data types correctly."""
        context = {
            "string_value": "test",
            "number_value": 42,
            "list_value": ["a", "b", "c"],
            "empty_list": [],
            "boolean_value": True,
            "none_value": None,
        }
        error = PlannerError("Test", context)

        error_str = str(error)
        assert "string_value: test" in error_str
        assert "number_value: 42" in error_str
        assert "list_value: a, b, c" in error_str
        assert "boolean_value: True" in error_str
        # Empty/None values should not appear
        assert "empty_list" not in error_str
        assert "none_value" not in error_str

    def test_location_formatting(self):
        """Location formatting in ValidationError works correctly."""
        locations = {
            "variable1": ["line 5 in step_a", "line 10 in step_b"],
            "variable2": ["line 15 in step_c"],
        }
        error = ValidationError(
            "Test",
            missing_variables=["variable1", "variable2"],
            context_locations=locations,
        )

        error_str = str(error)
        assert (
            "variable1 referenced at: line 5 in step_a, line 10 in step_b" in error_str
        )
        assert "variable2 referenced at: line 15 in step_c" in error_str
