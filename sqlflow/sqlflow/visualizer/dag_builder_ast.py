"""DAG builder for SQLFlow pipelines from AST."""

from typing import Dict, List, Optional, Set, Tuple

import networkx as nx

from sqlflow.sqlflow.parser.ast import (
    Pipeline, PipelineStep, SourceDefinitionStep, LoadStep, ExportStep,
    IncludeStep, SetStep, SQLBlockStep
)
from sqlflow.sqlflow.visualizer.dag_builder import PipelineDAG, DAGBuilder


class ASTDAGBuilder(DAGBuilder):
    """Builds a DAG from a pipeline AST."""
    
    def build_dag_from_ast(self, pipeline: Pipeline) -> PipelineDAG:
        """Build a DAG from a pipeline AST.
        
        Args:
            pipeline: Pipeline AST
            
        Returns:
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
            step: Pipeline step
            index: Step index
            
        Returns:
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
    
    def _add_dependencies(self, dag: PipelineDAG, pipeline: Pipeline) -> None:
        """Add dependencies between pipeline steps.
        
        Args:
            dag: PipelineDAG
            pipeline: Pipeline AST
        """
        source_map = {}
        
        table_map = {}
        
        for i, step in enumerate(pipeline.steps):
            node_id = f"step_{i}"
            
            if isinstance(step, SourceDefinitionStep):
                source_map[step.name] = node_id
            elif isinstance(step, LoadStep):
                table_map[step.table_name] = node_id
            elif isinstance(step, SQLBlockStep):
                table_map[step.table_name] = node_id
        
        for i, step in enumerate(pipeline.steps):
            node_id = f"step_{i}"
            
            if isinstance(step, LoadStep):
                source_name = step.source_name
                if source_name in source_map:
                    dag.add_edge(source_map[source_name], node_id)
            elif isinstance(step, ExportStep):
                for table_name in table_map:
                    if table_name in step.sql_query:
                        dag.add_edge(table_map[table_name], node_id)
            elif isinstance(step, SQLBlockStep):
                for table_name in table_map:
                    if table_name in step.sql_query:
                        dag.add_edge(table_map[table_name], node_id)
