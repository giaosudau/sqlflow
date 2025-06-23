"""Unit tests for the V2 simplified exception hierarchy."""

import pytest

from sqlflow.core.executors.v2.exceptions import (
    ExecutionError,
    SQLFlowError,
    SQLFlowWarning,
)


def test_sqlflow_error_basic():
    """Test basic properties of SQLFlowError."""
    error = SQLFlowError("A standard error occurred")
    assert error.message == "A standard error occurred"
    assert error.error_code == "SQLFLOWERROR"
    assert str(error) == "A standard error occurred"


def test_sqlflow_error_with_context():
    """Test SQLFlowError with additional context."""
    context = {"step_id": "step_1", "details": "some detail"}
    error = SQLFlowError("Error with context", context=context)
    assert error.context == context
    assert "Context: step_id=step_1, details=some detail" in str(error)


def test_sqlflow_warning():
    """Test the SQLFlowWarning class."""
    warning = SQLFlowWarning("This is a warning", context={"info": "extra"})
    assert "This is a warning" in str(warning)
    assert warning.context == {"info": "extra"}


def test_execution_error():
    """Test the ExecutionError class."""
    original = ValueError("Original problem")
    error = ExecutionError("Step failed", step_id="load_data", original_error=original)
    assert error.step_id == "load_data"
    assert error.original_error == original
    assert "original_error_type=ValueError" in str(error)
    assert "step_id=load_data" in str(error)


def test_execution_error_wraps_udf_errors():
    """Test that ExecutionError can wrap UDF execution errors."""
    original = TypeError("Bad type in UDF")
    udf_context = {"udf_name": "my_func", "input_data": "sample"}
    error = ExecutionError(
        "UDF 'my_func' failed: Bad type in UDF",
        step_id="transform_data",
        original_error=original,
        context=udf_context,
    )
    assert error.step_id == "transform_data"
    assert error.original_error == original
    assert "udf_name=my_func" in str(error)
    assert "original_error_type=TypeError" in str(error)


def test_configuration_errors_use_valueerror():
    """Test that configuration errors should use standard ValueError."""
    # This test demonstrates the new approach - use standard Python exceptions
    # for configuration errors instead of a custom ConfigurationError
    with pytest.raises(ValueError, match="Invalid database URL"):
        database_url = ""
        if not database_url:
            raise ValueError("Invalid database URL: cannot be empty")


def test_error_to_dict_serialization():
    """Test serialization of an error to a dictionary."""
    error = SQLFlowError(
        "Serialization test",
        error_code="TEST_CODE",
        context={"a": 1},
        suggested_actions=["Do this"],
    )
    error_dict = error.to_dict()

    assert error_dict["error_type"] == "SQLFlowError"
    assert error_dict["message"] == "Serialization test"
    assert error_dict["error_code"] == "TEST_CODE"
    assert error_dict["context"] == {"a": 1}
    assert error_dict["suggested_actions"] == ["Do this"]
    assert "timestamp" in error_dict
