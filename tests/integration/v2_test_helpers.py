"""
V2 Test Helpers for Integration Tests.

This module provides helper functions for writing clean V2 integration tests
that follow the v2_clean_implementation_plan.md guidelines.
"""

from typing import Any, Dict, List, Optional

from sqlflow.core.executors.v2 import ExecutionCoordinator
from sqlflow.core.executors.v2.execution.context import ExecutionContext
from sqlflow.core.executors.v2.steps.definitions import Step, StepType


def create_v2_test_context(
    profile: Optional[Dict[str, Any]] = None, variables: Optional[Dict[str, Any]] = None
) -> ExecutionContext:
    """Create a test ExecutionContext for V2 tests."""
    return ExecutionContext(profile=profile or {}, variables=variables or {}, config={})


def create_v2_test_step(
    step_type: StepType,
    name: str,
    config: Dict[str, Any],
    dependencies: Optional[List[str]] = None,
) -> Step:
    """Create a test Step for V2 tests."""
    return Step(
        id=name,
        type=step_type,
        name=name,
        config=config,
        dependencies=dependencies or [],
    )


def execute_v2_pipeline(steps: List[Step], context: ExecutionContext) -> Any:
    """Execute a V2 pipeline and return results."""
    coordinator = ExecutionCoordinator()
    return coordinator.execute(steps, context)


def assert_v2_success(result: Any) -> None:
    """Assert that a V2 execution result indicates success."""
    assert result.success is True, f"Expected success, got: {result}"
    assert (
        result.success == "SUCCESS"
    ), f"Expected SUCCESS status, got: {result.success}"


def assert_v2_failure(result: Any, expected_error: Optional[str] = None) -> None:
    """Assert that a V2 execution result indicates failure."""
    assert result.success is False, f"Expected failure, got: {result}"
    if expected_error:
        assert expected_error in str(
            result.error
        ), f"Expected error containing '{expected_error}', got: {result.error}"


"""
V2 Integration Test Helpers

This module provides helper functions for creating V2 pipeline steps
to reduce boilerplate and improve readability in integration tests.

Following the Zen of Python:
- "Simple is better than complex"
- "Don't repeat yourself" (DRY)
"""

from typing import Any, Dict

# Type alias for better readability
Step = Dict[str, Any]


def make_load_step(
    source: str,
    id: str = "load_step",
    table: str = "test_data",
    mode: str = "replace",
) -> Step:
    """Create a dictionary for a standard load step."""
    return {
        "id": id,
        "type": "load",
        "source": source,
        "target_table": table,
        "load_mode": mode,
    }


def make_transform_step(
    sql: str,
    id: str = "transform_step",
    table: str = "transformed_data",
) -> Step:
    """Create a dictionary for a standard transform step."""
    return {"id": id, "type": "transform", "sql": sql, "target_table": table}


def make_export_step(
    source_table: str,
    destination: str,
    id: str = "export_step",
    format: str = "parquet",
) -> Step:
    """Create a dictionary for a standard export step."""
    return {
        "id": id,
        "type": "export",
        "source_table": source_table,
        "destination": destination,
        "format": format,
    }
