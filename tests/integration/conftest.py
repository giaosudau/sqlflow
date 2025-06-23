import os
import tempfile
from typing import Any, Dict, List, Optional

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2 import ExecutionCoordinator
from sqlflow.core.executors.v2.execution.context import (
    ExecutionContext,
    create_test_context,
)
from sqlflow.project import Project

# Type alias for better readability
StepDict = Dict[str, Any]


@pytest.fixture
def v2_engine() -> DuckDBEngine:
    """A fixture to provide a V2 DuckDB engine."""
    return DuckDBEngine()


@pytest.fixture
def v2_execution_context(v2_engine: DuckDBEngine) -> ExecutionContext:
    """A fixture to provide a V2 execution context."""
    return create_test_context(engine=v2_engine)


@pytest.fixture
def v2_coordinator() -> ExecutionCoordinator:
    """A fixture to provide a V2 execution coordinator."""
    return ExecutionCoordinator()


@pytest.fixture
def v2_pipeline_runner(
    v2_coordinator: ExecutionCoordinator, v2_execution_context: ExecutionContext
):
    """A factory fixture to create and run a V2 pipeline."""

    def runner(
        pipeline: List[StepDict],
        project_dir: Optional[str] = None,
        engine: Optional[Any] = None,
        variables: Optional[Dict[str, Any]] = None,
        artifact_manager: Optional[Any] = None,
        state_backend: Optional[Any] = None,
        fail_fast: bool = True,
    ) -> ExecutionCoordinator:
        """Runs a V2 pipeline and returns the coordinator."""
        # Determine the engine to use
        active_engine = engine or v2_execution_context.engine

        # If a project_dir is provided, create a project-aware context.
        if project_dir:
            project = Project(project_dir)
            context = create_test_context(
                engine=active_engine,
                variables=variables,
                project=project,
                artifact_manager=artifact_manager,
                state_backend=state_backend,
            )

            # Register UDFs if project directory contains them
            try:
                from sqlflow.udfs.manager import PythonUDFManager

                udf_manager = PythonUDFManager(project_dir=project_dir)
                udf_manager.discover_udfs()
                udf_manager.register_udfs_with_engine(active_engine)
            except Exception as e:
                # Don't fail if UDF registration fails - some tests don't need UDFs
                import logging

                logging.debug(f"UDF registration failed (non-critical): {e}")
        else:
            context = create_test_context(
                engine=active_engine,
                variables=variables,
                artifact_manager=artifact_manager,
                state_backend=state_backend,
            )

        v2_coordinator.execute(pipeline, context, fail_fast=fail_fast)

        # Return the coordinator for inspection
        return v2_coordinator

    return runner


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file with sample data for testing.

    Returns path to a CSV file with id,name,age,city columns and 3 rows of data.
    Automatically cleans up the file after test completion.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name,age,city\n")
        f.write("1,Alice,25,NYC\n")
        f.write("2,Bob,30,LA\n")
        f.write("3,Charlie,22,Chicago\n")
        csv_path = f.name

    try:
        yield csv_path
    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)


@pytest.fixture
def step_factory():
    """Factory for creating common step dictionaries.

    Returns a factory object with methods for creating different types of steps.
    """

    class StepFactory:
        @staticmethod
        def load_step(
            source: str,
            target_table: str = "test_table",
            mode: str = "replace",
            step_id: str = "load_step",
            upsert_keys: Optional[List[str]] = None,
        ) -> StepDict:
            """Create a load step dictionary."""
            step = {
                "id": step_id,
                "type": "load",
                "source": source,
                "target_table": target_table,
                "mode": mode,
            }
            if upsert_keys:
                step["upsert_keys"] = upsert_keys
            return step

        @staticmethod
        def transform_step(
            sql: str,
            target_table: str = "transformed_table",
            step_id: str = "transform_step",
        ) -> StepDict:
            """Create a transform step dictionary."""
            return {
                "id": step_id,
                "type": "transform",
                "sql": sql,
                "target_table": target_table,
            }

        @staticmethod
        def source_step(
            name: str,
            connector_type: str = "csv",
            configuration: Optional[Dict[str, Any]] = None,
            step_id: str = "source_step",
        ) -> StepDict:
            """Create a source step dictionary."""
            return {
                "id": step_id,
                "type": "source",
                "name": name,
                "connector_type": connector_type,
                "configuration": configuration or {},
            }

    return StepFactory()


@pytest.fixture
def v2_pipeline_executor():
    """High-level fixture for executing complete pipelines with minimal setup.

    This combines parsing, execution, and result checking into a single fixture
    for tests that need to verify end-to-end pipeline behavior.
    """

    def execute_pipeline(
        pipeline_sql: str,
        context: Optional[ExecutionContext] = None,
        fail_fast: bool = True,
    ):
        """Execute a complete pipeline from SQL string to results.

        Args:
            pipeline_sql: SQLFlow pipeline as string
            context: Optional execution context (creates default if None)
            fail_fast: Whether to stop on first failure

        Returns:
            Tuple of (ExecutionResult, ExecutionCoordinator)
        """
        from sqlflow.core.executors import get_executor
        from sqlflow.core.planner_main import Planner
        from sqlflow.parser import SQLFlowParser

        # Parse pipeline
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Create context if not provided
        if context is None:
            engine = DuckDBEngine()
            context = create_test_context(engine=engine)

        # Execute
        executor = get_executor()
        result = executor.execute(execution_plan, context, fail_fast=fail_fast)

        return result, executor

    return execute_pipeline
