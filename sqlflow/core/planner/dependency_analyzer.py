"""Dependency analysis for SQLFlow pipeline steps.

This module extracts the dependency analysis logic from ExecutionPlanBuilder
to follow the Single Responsibility Principle (Zen of Python).

Following Zen of Python:
- Simple is better than complex: Clear, focused responsibility
- Explicit is better than implicit: Clear dependency relationships
- Practicality beats purity: Compatible with existing ExecutionPlanBuilder
"""

import re
from typing import Dict, List

from sqlflow.logging import get_logger
from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SourceDefinitionStep,
    SQLBlockStep,
)

logger = get_logger(__name__)


class DependencyAnalyzer:
    """Analyzes dependencies between pipeline steps.

    Following Zen of Python: Simple is better than complex.
    Focuses solely on dependency analysis logic.
    """

    def __init__(self):
        """Initialize the dependency analyzer."""
        self.step_dependencies: Dict[str, List[str]] = {}
        self.step_id_map: Dict[int, str] = {}
        logger.debug("DependencyAnalyzer initialized")

    def analyze(
        self, pipeline: Pipeline, step_id_map: Dict[int, str]
    ) -> Dict[str, List[str]]:
        """Analyze dependencies in a pipeline.

        Args:
            pipeline: The pipeline to analyze
            step_id_map: Mapping from step object IDs to step IDs

        Returns:
            Dictionary mapping step IDs to their dependencies

        Following Zen of Python: Explicit is better than implicit.
        """
        self.step_id_map = step_id_map
        self.step_dependencies = {}

        # Initialize dependencies for all steps
        for step_id in step_id_map.values():
            self.step_dependencies[step_id] = []

        # Build table name to step mapping for dependency analysis
        table_to_step = self._build_table_to_step_mapping(pipeline)

        # Analyze dependencies for each step type
        for step in pipeline.steps:
            if isinstance(step, SQLBlockStep):
                self._analyze_sql_dependencies(step, table_to_step)
            elif isinstance(step, ExportStep):
                self._analyze_export_dependencies(step, table_to_step)

        # Add load dependencies
        source_steps, load_steps = self._get_sources_and_loads(pipeline)
        self._add_load_dependencies(source_steps, load_steps)

        logger.debug(
            f"Dependency analysis complete with {len(self.step_dependencies)} steps"
        )
        return self.step_dependencies

    def _build_table_to_step_mapping(
        self, pipeline: Pipeline
    ) -> Dict[str, PipelineStep]:
        """Build mapping from table names to the steps that create them.

        Following Zen of Python: Explicit is better than implicit.
        Clear mapping between tables and their creating steps.
        """
        table_to_step = {}
        duplicate_tables = []

        for step in pipeline.steps:
            if isinstance(step, (LoadStep, SQLBlockStep)):
                table_name = step.table_name

                if table_name in table_to_step:
                    existing_step = table_to_step[table_name]

                    # Allow multiple LoadSteps on the same table (for different load modes)
                    if isinstance(step, LoadStep) and isinstance(
                        existing_step, LoadStep
                    ):
                        continue
                    elif isinstance(step, SQLBlockStep) and isinstance(
                        existing_step, SQLBlockStep
                    ):
                        # Allow SQLBlockStep to redefine a table if it uses CREATE OR REPLACE
                        if getattr(step, "is_replace", False):
                            table_to_step[table_name] = step
                            continue
                        else:
                            duplicate_tables.append((table_name, step.line_number))
                    else:
                        duplicate_tables.append((table_name, step.line_number))
                else:
                    table_to_step[table_name] = step

        if duplicate_tables:
            from sqlflow.core.errors import PlanningError

            error_msg = "Duplicate table definitions found:\n" + "".join(
                f"  - Table '{table}' defined at line {line}, but already defined at line {getattr(table_to_step[table], 'line_number', 'unknown')}\n"
                for table, line in duplicate_tables
            )
            raise PlanningError(error_msg)

        return table_to_step

    def _analyze_sql_dependencies(
        self, step: SQLBlockStep, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        """Analyze dependencies for SQL block steps.

        Following Zen of Python: Simple is better than complex.
        Clear logic for SQL dependency analysis.
        """
        referenced_tables = self._extract_referenced_tables(step.sql_query)
        for table_name in referenced_tables:
            if table_name in table_to_step:
                dependency_step = table_to_step[table_name]
                self._add_dependency(step, dependency_step)

    def _analyze_export_dependencies(
        self, step: ExportStep, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        """Analyze dependencies for export steps.

        Following Zen of Python: Practicality beats purity.
        Handle different export step formats pragmatically.
        """
        # Handle export step dependencies
        if hasattr(step, "sql_query") and step.sql_query:
            # Export with SQL query
            referenced_tables = self._extract_referenced_tables(step.sql_query)
            for table_name in referenced_tables:
                if table_name in table_to_step:
                    dependency_step = table_to_step[table_name]
                    self._add_dependency(step, dependency_step)
        elif hasattr(step, "table_name") and step.table_name:
            # Export of existing table
            if step.table_name in table_to_step:
                dependency_step = table_to_step[step.table_name]
                self._add_dependency(step, dependency_step)

    def _extract_referenced_tables(self, sql_query: str) -> List[str]:
        """Extract table names referenced in SQL query.

        Following Zen of Python: Simple is better than complex.
        Clear pattern matching for table references.
        """
        sql_lower = sql_query.lower()
        tables = []

        # DuckDB built-in functions that are not table references
        builtin_functions = {
            "read_csv_auto",
            "read_csv",
            "read_parquet",
            "read_json",
            "information_schema",
            "pg_catalog",
            "main",
        }

        # Handle standard SQL FROM clauses
        from_matches = re.finditer(
            r"from\s+([a-zA-Z0-9_]+(?:\s*,\s*[a-zA-Z0-9_]+)*)", sql_lower
        )
        for match in from_matches:
            table_list = match.group(1).split(",")
            for table in table_list:
                table_name = table.strip()
                if (
                    table_name
                    and table_name not in tables
                    and table_name not in builtin_functions
                ):
                    tables.append(table_name)

        # Handle standard SQL JOINs
        join_matches = re.finditer(r"join\s+([a-zA-Z0-9_]+)", sql_lower)
        for match in join_matches:
            table_name = match.group(1).strip()
            if (
                table_name
                and table_name not in tables
                and table_name not in builtin_functions
            ):
                tables.append(table_name)

        # Handle table UDF pattern: PYTHON_FUNC("module.function", table_name)
        udf_table_matches = re.finditer(
            r"python_func\s*\(\s*['\"][\w\.]+['\"]\s*,\s*([a-zA-Z0-9_]+)", sql_lower
        )
        for match in udf_table_matches:
            table_name = match.group(1).strip()
            if (
                table_name
                and table_name not in tables
                and table_name not in builtin_functions
            ):
                tables.append(table_name)

        return tables

    def _add_dependency(
        self, dependent_step: PipelineStep, dependency_step: PipelineStep
    ) -> None:
        """Add a dependency relationship between steps.

        Following Zen of Python: Explicit is better than implicit.
        Clear dependency relationship establishment.
        """
        dependent_id = self.step_id_map.get(id(dependent_step))
        dependency_id = self.step_id_map.get(id(dependency_step))

        if dependent_id and dependency_id:
            if dependency_id not in self.step_dependencies[dependent_id]:
                self.step_dependencies[dependent_id].append(dependency_id)
                logger.debug(
                    f"Added dependency: {dependent_id} depends on {dependency_id}"
                )

    def _get_sources_and_loads(
        self, pipeline: Pipeline
    ) -> tuple[Dict[str, SourceDefinitionStep], List[LoadStep]]:
        """Get source definitions and load steps from pipeline.

        Following Zen of Python: Simple is better than complex.
        Clear separation of step types.
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
        """Add dependencies between load steps and their source definitions.

        Following Zen of Python: Explicit is better than implicit.
        Clear load-to-source dependency relationships.
        """
        for load_step in load_steps:
            if load_step.source_name in source_steps:
                source_step = source_steps[load_step.source_name]
                self._add_dependency(load_step, source_step)
