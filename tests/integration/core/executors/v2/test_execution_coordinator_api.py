"""Test the V2 ExecutionCoordinator API contract with real code and data.

Follows SQLFlow 04_testing_standards:
- No mocks or patching
- Use real step executors, registry, and data
- Test real behavior, not implementation details
- Each test is a usage example
"""

from pathlib import Path
from typing import Any, Dict, List

import pytest

from sqlflow.core.executors.v2 import ExecutionCoordinator

# Type aliases for better readability
Step = Dict[str, Any]
Pipeline = List[Step]


# Step creation helpers to reduce boilerplate and improve readability
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


def test_coordinator_has_execute_method(v2_coordinator: ExecutionCoordinator) -> None:
    """Test that ExecutionCoordinator has the correct API methods."""
    assert hasattr(
        v2_coordinator, "execute"
    ), "V2 coordinator should have execute() method"
    assert callable(v2_coordinator.execute), "execute should be callable"


def test_execute_with_empty_steps_returns_success(v2_pipeline_runner) -> None:
    """Test that execute() with empty steps returns success result."""
    empty_steps: List[Step] = []
    coordinator = v2_pipeline_runner(empty_steps)
    assert coordinator.result.success, "Empty pipeline should succeed"
    assert (
        len(coordinator.result.step_results) == 0
    ), "No step results for empty pipeline"


def test_coordinator_has_registry_attribute(
    v2_coordinator: ExecutionCoordinator,
) -> None:
    """Test that ExecutionCoordinator has step registry."""
    assert hasattr(v2_coordinator, "registry"), "Should have step registry"
    assert v2_coordinator.registry is not None, "Registry should not be None"


def test_coordinator_accepts_custom_registry() -> None:
    """Test that ExecutionCoordinator accepts custom registry."""
    from sqlflow.core.executors.v2.steps.registry import create_default_registry

    custom_registry = create_default_registry()
    coordinator = ExecutionCoordinator(registry=custom_registry)
    assert coordinator.registry is custom_registry, "Should use provided registry"


def test_execute_with_load_step_real_csv(v2_pipeline_runner, temp_csv_file):
    """Test execute() with a real load step and CSV file."""
    load_step = make_load_step(temp_csv_file, id="load_test_data", table="test_data")
    coordinator = v2_pipeline_runner([load_step])
    assert coordinator.result.success, "Load step should succeed"
    assert len(coordinator.result.step_results) == 1, "Should have one step result"
    step_result = coordinator.result.step_results[0]
    assert step_result.success, "Step should succeed"
    assert step_result.rows_affected == 3, "Should load 3 rows"


def test_execute_with_transform_step_real_sql(
    v2_pipeline_runner, temp_csv_file: str
) -> None:
    """Test execute() with a real transform step and SQL query."""
    load_step = make_load_step(temp_csv_file, id="load_test_data", table="test_data")
    transform_step = make_transform_step(
        "SELECT name, value * 2 as doubled_value FROM test_data",
        id="transform_data",
        table="transformed_data",
    )

    # Execute the pipeline
    coordinator = v2_pipeline_runner([load_step, transform_step])

    # Verify the results
    assert coordinator.result.success, "Pipeline should succeed"
    assert len(coordinator.result.step_results) == 2, "Should have two step results"
    assert coordinator.result.step_results[0].success, "Load step should succeed"
    assert coordinator.result.step_results[1].success, "Transform step should succeed"


def test_execute_with_export_step_real_file(
    v2_pipeline_runner, temp_csv_file: str, temp_output_file: str
) -> None:
    """Test execute() with a real export step to a file."""
    load_step = make_load_step(temp_csv_file, id="load_test_data", table="test_data")
    export_step = make_export_step(
        "test_data", temp_output_file, id="export_data", format="parquet"
    )

    # Execute the pipeline
    coordinator = v2_pipeline_runner([load_step, export_step])

    # Verify the results
    assert coordinator.result.success, "Pipeline should succeed"
    assert len(coordinator.result.step_results) == 2, "Should have two step results"
    assert coordinator.result.step_results[0].success, "Load step should succeed"
    assert coordinator.result.step_results[1].success, "Export step should succeed"
    assert Path(temp_output_file).exists(), "Exported file should exist"


def test_execute_complete_pipeline_load_transform_export(
    v2_pipeline_runner, temp_csv_file: str, temp_output_file: str
) -> None:
    """Test a complete pipeline with load, transform, and export steps."""
    # Define the pipeline steps
    pipeline: Pipeline = [
        make_load_step(temp_csv_file, id="load_test_data", table="test_data"),
        make_transform_step(
            id="transform_data",
            table="transformed_data",
            sql="""
                SELECT 
                    name, 
                    value * 2 as doubled_value 
                FROM test_data 
                WHERE value > 1
            """,
        ),
        make_export_step(
            id="export_data",
            source_table="transformed_data",
            destination=temp_output_file,
            format="parquet",
        ),
    ]

    # Execute the pipeline
    coordinator = v2_pipeline_runner(pipeline)

    # Verify the results
    assert coordinator.result.success, "Pipeline should succeed"
    assert len(coordinator.result.step_results) == 3, "Should have three step results"
    assert coordinator.result.step_results[0].success, "Load step should succeed"
    assert coordinator.result.step_results[1].success, "Transform step should succeed"
    assert coordinator.result.step_results[2].success, "Export step should succeed"
    assert Path(temp_output_file).exists(), "Output file should exist"


def test_pipeline_fail_fast_on_step_failure(v2_pipeline_runner) -> None:
    """Test that pipeline raises ValueError for an invalid step definition."""
    # Define the pipeline with a failing step
    pipeline: Pipeline = [
        {"id": "invalid_step", "type": "invalid_type", "some_parameter": "value"}
    ]

    # Execute and expect an error (depending on fail_fast)
    with pytest.raises(ValueError, match="Invalid step definition for 'invalid_step'"):
        v2_pipeline_runner(pipeline)
