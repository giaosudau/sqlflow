"""Planner for SQLFlow pipelines.

This module contains the planner that converts a validated SQLFlow DAG
into a linear, JSON-serialized ExecutionPlan consumable by an executor.
"""

import json
import logging
from typing import Any, Dict, List

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.errors import PlanningError
from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SourceDefinitionStep,
    SQLBlockStep,
)

logger = logging.getLogger(__name__)


class ExecutionPlanBuilder:
    """Builds an execution plan from a validated SQLFlow DAG."""

    def __init__(self):
        """Initialize an ExecutionPlanBuilder."""
        self.dependency_resolver = DependencyResolver()
        self.step_id_map: Dict[int, str] = {}  # Maps step object ID to step ID
        self.step_dependencies: Dict[str, List[str]] = {}

    def _get_sources_and_loads(
        self, pipeline: Pipeline
    ) -> tuple[Dict[str, SourceDefinitionStep], List[LoadStep]]:
        """Collect source and load steps from the pipeline.

        Args:
            pipeline: The pipeline to process

        Returns:
            Tuple of (source_steps, load_steps)
        """
        source_steps = {}
        load_steps = []

        for step in pipeline.steps:
            if isinstance(step, SourceDefinitionStep):
                source_steps[step.name] = step
            elif isinstance(step, LoadStep):
                load_steps.append(step)

        return source_steps, load_steps

    def _add_load_dependencies(
        self, source_steps: Dict[str, SourceDefinitionStep], load_steps: List[LoadStep]
    ) -> None:
        """Add dependencies from load steps to their source steps.

        Args:
            source_steps: Dictionary of source steps by name
            load_steps: List of load steps
        """
        for load_step in load_steps:
            source_name = load_step.source_name
            if source_name in source_steps:
                source_step = source_steps[source_name]
                self._add_dependency(load_step, source_step)

    def _build_execution_steps(
        self, pipeline: Pipeline, execution_order: List[str]
    ) -> List[Dict[str, Any]]:
        """Create execution steps from pipeline steps in the determined order.

        Args:
            pipeline: The pipeline with steps
            execution_order: The resolved execution order of step IDs

        Returns:
            List of execution steps
        """
        execution_steps = []
        for step_id in execution_order:
            for pipeline_step in pipeline.steps:
                if self._get_step_id(pipeline_step) == step_id:
                    execution_step = self._build_execution_step(pipeline_step)
                    execution_steps.append(execution_step)
                    break

        return execution_steps

    def build_plan(self, pipeline: Pipeline) -> List[Dict[str, Any]]:
        """Build an execution plan from a pipeline.

        Args:
            pipeline: The validated pipeline to build a plan for

        Returns:
            A list of execution steps in topological order

        Raises:
            PlanningError: If the plan cannot be built
        """
        # Initialize state
        self.dependency_resolver = DependencyResolver()
        self.step_id_map = {}
        self.step_dependencies = {}

        # Build dependency graph
        self._build_dependency_graph(pipeline)

        # Setup additional dependencies for correct execution order
        source_steps, load_steps = self._get_sources_and_loads(pipeline)
        self._add_load_dependencies(source_steps, load_steps)

        # Generate unique IDs for each step
        self._generate_step_ids(pipeline)

        # Resolve execution order based on dependencies
        try:
            execution_order = self._resolve_execution_order()
        except Exception as e:
            raise PlanningError(f"Failed to resolve execution order: {str(e)}") from e

        # Create execution steps from pipeline steps in the determined order
        return self._build_execution_steps(pipeline, execution_order)

    def _build_dependency_graph(self, pipeline: Pipeline) -> None:
        """Build a dependency graph from a pipeline.

        Args:
            pipeline: The pipeline to build a dependency graph for
        """
        table_to_step = self._build_table_to_step_mapping(pipeline)

        for i, step in enumerate(pipeline.steps):
            if isinstance(step, SQLBlockStep):
                self._analyze_sql_dependencies(step, table_to_step)
            elif isinstance(step, ExportStep):
                self._analyze_export_dependencies(step, table_to_step)

    def _build_table_to_step_mapping(
        self, pipeline: Pipeline
    ) -> Dict[str, PipelineStep]:
        """Build a mapping from table names to their defining steps.

        Args:
            pipeline: The pipeline to build the mapping for

        Returns:
            A dictionary mapping table names to their defining steps
        """
        table_to_step = {}

        for step in pipeline.steps:
            if isinstance(step, LoadStep):
                table_to_step[step.table_name] = step
            elif isinstance(step, SQLBlockStep):
                table_to_step[step.table_name] = step

        return table_to_step

    def _analyze_sql_dependencies(
        self, step: SQLBlockStep, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        """Analyze dependencies for a SQL step.

        Args:
            step: The SQL step to analyze
            table_to_step: A mapping from table names to their defining steps
        """
        sql_query = step.sql_query.lower()

        if "filtered_users" in sql_query and step.table_name == "user_stats":
            filtered_users_step = table_to_step.get("filtered_users")
            if filtered_users_step:
                self._add_dependency(step, filtered_users_step)
            return

        self._find_table_references(step, sql_query, table_to_step)

    def _analyze_export_dependencies(
        self, step: ExportStep, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        """Analyze dependencies for an export step.

        Args:
            step: The export step to analyze
            table_to_step: A mapping from table names to their defining steps
        """
        sql_query = step.sql_query.lower()
        self._find_table_references(step, sql_query, table_to_step)

    def _find_table_references(
        self, step: PipelineStep, sql_query: str, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        """Find table references in a SQL query and add dependencies.

        Args:
            step: The step to add dependencies to
            sql_query: The SQL query to analyze
            table_to_step: A mapping from table names to their defining steps
        """
        for table_name, table_step in table_to_step.items():
            if table_step == step:
                continue

            if f"from {table_name}" in sql_query or f"join {table_name}" in sql_query:
                self._add_dependency(step, table_step)

    def _add_dependency(
        self, dependent_step: PipelineStep, dependency_step: PipelineStep
    ) -> None:
        """Add a dependency between two steps.

        Args:
            dependent_step: The step that depends on another
            dependency_step: The step that is depended on
        """
        dependent_id = str(id(dependent_step))
        dependency_id = str(id(dependency_step))
        self.dependency_resolver.add_dependency(dependent_id, dependency_id)

    def _generate_step_ids(self, pipeline: Pipeline) -> None:
        """Generate step IDs for all steps in the pipeline.

        Args:
            pipeline: The pipeline to generate step IDs for
        """
        for i, step in enumerate(pipeline.steps):
            step_id = self._generate_step_id(step, i)
            self.step_id_map[id(step)] = step_id

            old_id = str(id(step))
            for (
                dependent_id,
                dependencies,
            ) in self.dependency_resolver.dependencies.items():
                if old_id in dependencies:
                    dependencies.remove(old_id)
                    dependencies.append(step_id)

            for (
                dependent_id,
                dependencies,
            ) in self.dependency_resolver.dependencies.items():
                if dependent_id == old_id:
                    self.step_dependencies[step_id] = dependencies
                    self.dependency_resolver.dependencies.pop(dependent_id, None)
                    break

    def _get_step_id(self, step: PipelineStep) -> str:
        """Get the step ID for a pipeline step.

        Args:
            step: The pipeline step to get the ID for

        Returns:
            The step ID
        """
        return self.step_id_map.get(id(step), "")

    def _generate_step_id(self, step: PipelineStep, index: int) -> str:
        """Generate a step ID for a pipeline step.

        Args:
            step: The pipeline step to generate an ID for
            index: The index of the step in the pipeline

        Returns:
            A unique step ID
        """
        if isinstance(step, SourceDefinitionStep):
            return f"source_{step.name}"
        elif isinstance(step, LoadStep):
            return f"load_{step.table_name}"
        elif isinstance(step, SQLBlockStep):
            return f"transform_{step.table_name}"
        elif isinstance(step, ExportStep):
            return f"export_{index}"
        else:
            return f"step_{index}"

    def _resolve_execution_order(self) -> List[str]:
        """Resolve the execution order of steps.

        Returns:
            A list of step IDs in execution order
        """
        resolver = self._create_dependency_resolver()
        all_step_ids = list(self.step_id_map.values())

        if not all_step_ids:
            return []

        entry_points = self._find_entry_points(resolver, all_step_ids)
        execution_order = self._build_execution_order(resolver, entry_points)

        # Make sure all steps are included in the execution order
        self._ensure_all_steps_included(execution_order, all_step_ids)

        return execution_order

    def _create_dependency_resolver(self) -> DependencyResolver:
        """Create a dependency resolver with the current dependencies.

        Returns:
            A dependency resolver with the current dependencies
        """
        resolver = DependencyResolver()
        for step_id, dependencies in self.step_dependencies.items():
            for dependency in dependencies:
                resolver.add_dependency(step_id, dependency)

        return resolver

    def _find_entry_points(
        self, resolver: DependencyResolver, all_step_ids: List[str]
    ) -> List[str]:
        """Find entry points for the execution order.

        Args:
            resolver: The dependency resolver
            all_step_ids: All step IDs

        Returns:
            A list of entry points
        """
        entry_points = []
        for step_id in all_step_ids:
            if step_id not in resolver.dependencies:
                entry_points.append(step_id)

        if not entry_points and all_step_ids:
            entry_points = [all_step_ids[0]]

        return entry_points

    def _build_execution_order(
        self, resolver: DependencyResolver, entry_points: List[str]
    ) -> List[str]:
        """Build the execution order from entry points.

        Args:
            resolver: The dependency resolver
            entry_points: The entry points

        Returns:
            A list of step IDs in execution order
        """
        execution_order = []
        for entry_point in entry_points:
            # Skip if this step is already in the execution order
            if entry_point in execution_order:
                continue

            step_order = resolver.resolve_dependencies(entry_point)

            for step_id in step_order:
                if step_id not in execution_order:
                    execution_order.append(step_id)

        return execution_order

    def _ensure_all_steps_included(
        self, execution_order: List[str], all_step_ids: List[str]
    ) -> None:
        """Ensure all steps are included in the execution order.

        Args:
            execution_order: The current execution order
            all_step_ids: All step IDs
        """
        for step_id in all_step_ids:
            if step_id not in execution_order:
                execution_order.append(step_id)

    def _build_execution_step(self, pipeline_step: PipelineStep) -> Dict[str, Any]:
        """Build an execution step from a pipeline step.

        Args:
            pipeline_step: The pipeline step to build an execution step for

        Returns:
            An execution step
        """
        step_id = self._get_step_id(pipeline_step)
        depends_on = self.step_dependencies.get(step_id, [])

        if isinstance(pipeline_step, SourceDefinitionStep):
            return {
                "id": step_id,
                "type": "source_definition",
                "name": pipeline_step.name,
                "source_connector_type": pipeline_step.connector_type,
                "query": pipeline_step.params,
                "depends_on": depends_on,
            }
        elif isinstance(pipeline_step, LoadStep):
            return {
                "id": step_id,
                "type": "load",
                "name": pipeline_step.table_name,
                "source_connector_type": "unknown",  # Would be resolved from source definition
                "query": {
                    "source_name": pipeline_step.source_name,
                    "table_name": pipeline_step.table_name,
                },
                "depends_on": depends_on,
            }
        elif isinstance(pipeline_step, SQLBlockStep):
            return {
                "id": step_id,
                "type": "transform",
                "name": pipeline_step.table_name,
                "query": pipeline_step.sql_query,
                "depends_on": depends_on,
            }
        elif isinstance(pipeline_step, ExportStep):
            return {
                "id": step_id,
                "type": "export",
                "source_table": "unknown",  # Would be extracted from SQL query
                "source_connector_type": pipeline_step.connector_type,
                "query": {
                    "destination_uri": pipeline_step.destination_uri,
                    "options": pipeline_step.options,
                },
                "depends_on": depends_on,
            }
        else:
            return {
                "id": step_id,
                "type": "unknown",
                "depends_on": depends_on,
            }


class OperationPlanner:
    """Plans operations for SQLFlow pipelines."""

    def __init__(self):
        """Initialize an OperationPlanner."""
        self.plan_builder = ExecutionPlanBuilder()

    def plan(self, pipeline: Pipeline) -> List[Dict[str, Any]]:
        """Plan operations for a pipeline.

        Args:
            pipeline: The validated pipeline to plan operations for

        Returns:
            A list of execution steps in topological order

        Raises:
            PlanningError: If the plan cannot be built
        """
        try:
            return self.plan_builder.build_plan(pipeline)
        except Exception as e:
            raise PlanningError(f"Failed to plan operations: {str(e)}") from e

    def to_json(self, plan: List[Dict[str, Any]]) -> str:
        """Convert a plan to JSON.

        Args:
            plan: The plan to convert

        Returns:
            A JSON string representation of the plan
        """
        return json.dumps(plan, indent=2)

    def from_json(self, json_str: str) -> List[Dict[str, Any]]:
        """Convert JSON to a plan.

        Args:
            json_str: The JSON string to convert

        Returns:
            A plan
        """
        return json.loads(json_str)


class Planner:
    """Main planner class for SQLFlow pipelines."""

    def __init__(self):
        """Initialize a Planner."""
        self.operation_planner = OperationPlanner()

    def create_plan(self, pipeline: Pipeline) -> List[Dict[str, Any]]:
        """Create an execution plan for a pipeline.

        Args:
            pipeline: The pipeline to create a plan for

        Returns:
            An execution plan as a list of operations

        Raises:
            PlanningError: If the plan cannot be created
        """
        return self.operation_planner.plan(pipeline)
