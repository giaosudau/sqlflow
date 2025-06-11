"""Bridge between AST and DAG for SQLFlow pipelines."""

from typing import Dict, List, Optional

from sqlflow.parser.ast import (
    ExportStep,
    IncludeStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)
from sqlflow.visualizer.dag_builder import DAGBuilder, PipelineDAG


class ASTToDAGConverter:
    """Converts an AST to a DAG."""

    def __init__(self):
        """Initialize an ASTToDAGConverter."""
        self.dag_builder = DAGBuilder()

    def convert(self, pipeline: Pipeline) -> PipelineDAG:
        """Convert a pipeline AST to a DAG.

        Args:
        ----
            pipeline: Pipeline AST

        Returns:
        -------
            PipelineDAG

        """
        pipeline_steps = []

        for i, step in enumerate(pipeline.steps):
            step_dict = self._convert_step_to_dict(step, i)
            if step_dict:
                pipeline_steps.append(step_dict)

        self._add_dependencies(pipeline_steps)

        return self.dag_builder.build_dag(pipeline_steps)

    def _convert_step_to_dict(self, step: PipelineStep, index: int) -> Optional[Dict]:
        """Convert a pipeline step to a dictionary.

        Args:
        ----
            step: Pipeline step
            index: Step index

        Returns:
        -------
            Dictionary representation of the step, or None if the step cannot be
            converted

        """
        step_id = f"step_{index}"

        if isinstance(step, SourceDefinitionStep):
            return {
                "id": step_id,
                "name": step.name,
                "type": "SOURCE",
                "connector_type": step.connector_type,
                "params": step.params,
                "is_from_profile": step.is_from_profile,
                "profile_connector_name": step.profile_connector_name,
                "line_number": step.line_number,
            }
        elif isinstance(step, LoadStep):
            return {
                "id": step_id,
                "table_name": step.table_name,
                "source_name": step.source_name,
                "type": "LOAD",
                "line_number": step.line_number,
            }
        elif isinstance(step, ExportStep):
            return {
                "id": step_id,
                "sql_query": step.sql_query,
                "destination_uri": step.destination_uri,
                "connector_type": step.connector_type,
                "options": step.options,
                "type": "EXPORT",
                "line_number": step.line_number,
            }
        elif isinstance(step, IncludeStep):
            return {
                "id": step_id,
                "file_path": step.file_path,
                "alias": step.alias,
                "type": "INCLUDE",
                "line_number": step.line_number,
            }
        elif isinstance(step, SetStep):
            return {
                "id": step_id,
                "variable_name": step.variable_name,
                "variable_value": step.variable_value,
                "type": "SET",
                "line_number": step.line_number,
            }
        elif isinstance(step, SQLBlockStep):
            return {
                "id": step_id,
                "table_name": step.table_name,
                "sql_query": step.sql_query,
                "type": "SQL_BLOCK",
                "line_number": step.line_number,
            }

        return None

    def _build_source_map(self, pipeline_steps: List[Dict]) -> Dict[str, str]:
        """Build a map of source names to step IDs.

        Args:
        ----
            pipeline_steps: List of pipeline steps

        Returns:
        -------
            Map of source names to step IDs

        """
        source_map = {}
        for step in pipeline_steps:
            if step["type"] == "SOURCE":
                source_map[step["name"]] = step["id"]
        return source_map

    def _build_table_map(self, pipeline_steps: List[Dict]) -> Dict[str, str]:
        """Build a map of table names to step IDs.

        Args:
        ----
            pipeline_steps: List of pipeline steps

        Returns:
        -------
            Map of table names to step IDs

        """
        table_map = {}
        for step in pipeline_steps:
            if step["type"] in ["LOAD", "SQL_BLOCK"]:
                table_map[step["table_name"]] = step["id"]
        return table_map

    def _add_load_dependencies(self, step: Dict, source_map: Dict[str, str]) -> None:
        """Add dependencies for LOAD steps.

        Args:
        ----
            step: Pipeline step
            source_map: Map of source names to step IDs

        """
        source_name = step["source_name"]
        if source_name in source_map:
            step["depends_on"].append(source_map[source_name])

    def _add_sql_dependencies(self, step: Dict, table_map: Dict[str, str]) -> None:
        """Add dependencies for SQL-based steps.

        Args:
        ----
            step: Pipeline step
            table_map: Map of table names to step IDs

        """
        for table_name in table_map:
            if table_name in step["sql_query"]:
                step["depends_on"].append(table_map[table_name])

    def _add_dependencies(self, pipeline_steps: List[Dict]) -> None:
        """Add dependencies between pipeline steps.

        Args:
        ----
            pipeline_steps: List of pipeline steps

        """
        source_map = self._build_source_map(pipeline_steps)
        table_map = self._build_table_map(pipeline_steps)

        for step in pipeline_steps:
            if "depends_on" not in step:
                step["depends_on"] = []

            if step["type"] == "LOAD":
                self._add_load_dependencies(step, source_map)
            elif step["type"] in ["EXPORT", "SQL_BLOCK"]:
                self._add_sql_dependencies(step, table_map)
