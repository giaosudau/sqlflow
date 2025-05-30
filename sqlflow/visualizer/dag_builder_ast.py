"""DAG builder for SQLFlow pipelines from AST."""

from typing import Dict

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


class ASTDAGBuilder(DAGBuilder):
    """Builds a DAG from a pipeline AST."""

    def build_dag_from_ast(self, pipeline: Pipeline) -> PipelineDAG:
        """Build a DAG from a pipeline AST.

        Args:
        ----
            pipeline: Pipeline AST

        Returns:
        -------
            PipelineDAG

        """
        dag = PipelineDAG()

        for i, step in enumerate(pipeline.steps):
            node_id = f"step_{i}"
            node_attrs = self._get_node_attributes(step, i)
            dag.add_node(node_id, **node_attrs)

        self._add_dependencies(dag, pipeline)

        return dag

    def _get_node_attributes(self, step: PipelineStep, index: int) -> Dict[str, str]:
        """Get node attributes for a pipeline step.

        Args:
        ----
            step: Pipeline step
            index: Step index

        Returns:
        -------
            Node attributes

        """
        attrs = {
            "label": f"Step {index}",
            "type": "unknown",
        }

        if isinstance(step, SourceDefinitionStep):
            attrs["label"] = f"SOURCE: {step.name}"
            attrs["type"] = "SOURCE"
        elif isinstance(step, LoadStep):
            attrs["label"] = f"LOAD: {step.table_name}"
            attrs["type"] = "LOAD"
        elif isinstance(step, ExportStep):
            attrs["label"] = f"EXPORT: {step.destination_uri}"
            attrs["type"] = "EXPORT"
        elif isinstance(step, IncludeStep):
            attrs["label"] = f"INCLUDE: {step.file_path}"
            attrs["type"] = "INCLUDE"
        elif isinstance(step, SetStep):
            attrs["label"] = f"SET: {step.variable_name}"
            attrs["type"] = "SET"
        elif isinstance(step, SQLBlockStep):
            attrs["label"] = f"SQL_BLOCK: {step.table_name}"
            attrs["type"] = "SQL_BLOCK"

        return attrs

    def _build_source_map(self, pipeline: Pipeline) -> Dict[str, str]:
        """Build a map of source names to node IDs.

        Args:
        ----
            pipeline: Pipeline AST

        Returns:
        -------
            Map of source names to node IDs

        """
        source_map = {}
        for i, step in enumerate(pipeline.steps):
            if isinstance(step, SourceDefinitionStep):
                source_map[step.name] = f"step_{i}"
        return source_map

    def _build_table_map(self, pipeline: Pipeline) -> Dict[str, str]:
        """Build a map of table names to node IDs.

        Args:
        ----
            pipeline: Pipeline AST

        Returns:
        -------
            Map of table names to node IDs

        """
        table_map = {}
        for i, step in enumerate(pipeline.steps):
            if isinstance(step, LoadStep) or isinstance(step, SQLBlockStep):
                table_map[step.table_name] = f"step_{i}"
        return table_map

    def _add_load_dependencies(
        self, dag: PipelineDAG, node_id: str, step: LoadStep, source_map: Dict[str, str]
    ) -> None:
        """Add dependencies for LOAD steps.

        Args:
        ----
            dag: PipelineDAG
            node_id: Node ID
            step: Pipeline step
            source_map: Map of source names to node IDs

        """
        source_name = step.source_name
        if source_name in source_map:
            dag.add_edge(source_map[source_name], node_id)

    def _add_sql_dependencies(
        self, dag: PipelineDAG, node_id: str, sql_query: str, table_map: Dict[str, str]
    ) -> None:
        """Add dependencies for SQL-based steps.

        Args:
        ----
            dag: PipelineDAG
            node_id: Node ID
            sql_query: SQL query
            table_map: Map of table names to node IDs

        """
        for table_name in table_map:
            if table_name in sql_query:
                dag.add_edge(table_map[table_name], node_id)

    def _add_dependencies(self, dag: PipelineDAG, pipeline: Pipeline) -> None:
        """Add dependencies between pipeline steps.

        Args:
        ----
            dag: PipelineDAG
            pipeline: Pipeline AST

        """
        source_map = self._build_source_map(pipeline)
        table_map = self._build_table_map(pipeline)

        for i, step in enumerate(pipeline.steps):
            node_id = f"step_{i}"

            if isinstance(step, LoadStep):
                self._add_load_dependencies(dag, node_id, step, source_map)
            elif isinstance(step, ExportStep):
                self._add_sql_dependencies(dag, node_id, step.sql_query, table_map)
            elif isinstance(step, SQLBlockStep):
                self._add_sql_dependencies(dag, node_id, step.sql_query, table_map)
