"""Step building for SQLFlow execution steps.

This module extracts the step building logic from ExecutionPlanBuilder
to follow the Single Responsibility Principle (Zen of Python).

Following Zen of Python:
- Simple is better than complex: Clear step building logic
- Explicit is better than implicit: Clear step type handling
- Practicality beats purity: Compatible with existing pipeline steps
"""

import re
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger
from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)

logger = get_logger(__name__)


class StepBuilder:
    """Builds execution steps from pipeline steps.

    Following Zen of Python: Simple is better than complex.
    Focuses solely on step building logic.
    """

    def __init__(self):
        """Initialize the step builder."""
        self._source_definitions: Dict[str, Dict[str, Any]] = {}
        logger.debug("StepBuilder initialized")

    def build_steps(
        self,
        pipeline: Pipeline,
        execution_order: List[str],
        step_id_map: Dict[int, str],
        step_dependencies: Dict[str, List[str]],
    ) -> List[Dict[str, Any]]:
        """Build execution steps from pipeline and execution order.

        Args:
            pipeline: The pipeline to build steps for
            execution_order: The order of steps to execute
            step_id_map: Mapping from step object IDs to step IDs
            step_dependencies: Dictionary mapping step IDs to their dependencies

        Returns:
            List of executable steps

        Following Zen of Python: Explicit is better than implicit.
        Clear step building process.
        """
        self.step_id_map = step_id_map
        self.step_dependencies = step_dependencies

        # Build source definitions mapping
        self._build_source_definitions(pipeline)

        # Create step lookup mapping
        step_id_to_pipeline_step = self._create_step_lookup_mapping(pipeline)

        # Process steps in execution order
        execution_steps = self._process_steps_in_execution_order(
            execution_order, step_id_to_pipeline_step
        )

        # Add any missing steps
        execution_steps = self._add_missing_steps(
            pipeline, execution_steps, step_id_to_pipeline_step
        )

        logger.info(f"Built execution plan with {len(execution_steps)} steps")
        return execution_steps

    def _build_source_definitions(self, pipeline: Pipeline) -> None:
        """Build source definitions mapping.

        Following Zen of Python: Simple is better than complex.
        Clear source definition building.
        """
        self._source_definitions = {}

        for step in pipeline.steps:
            if isinstance(step, SourceDefinitionStep):
                self._source_definitions[step.name] = {
                    "name": step.name,
                    "connector_type": step.connector_type,
                    "params": step.params,
                    "is_from_profile": getattr(step, "is_from_profile", False),
                }

    def _create_step_lookup_mapping(
        self, pipeline: Pipeline
    ) -> Dict[str, PipelineStep]:
        """Create mapping from step_id to pipeline_step for faster lookup.

        Following Zen of Python: Explicit is better than implicit.
        Clear step lookup mapping creation.
        """
        step_id_to_pipeline_step = {}

        for pipeline_step in pipeline.steps:
            # Skip SET statements from being added to the execution plan
            if isinstance(pipeline_step, SetStep):
                continue

            step_id = self._get_step_id(pipeline_step)
            if step_id:
                step_id_to_pipeline_step[step_id] = pipeline_step

        return step_id_to_pipeline_step

    def _process_steps_in_execution_order(
        self,
        execution_order: List[str],
        step_id_to_pipeline_step: Dict[str, PipelineStep],
    ) -> List[Dict[str, Any]]:
        """Process steps in the execution order.

        Following Zen of Python: Simple is better than complex.
        Clear step processing in order.
        """
        execution_steps = []

        for step_id in execution_order:
            if step_id in step_id_to_pipeline_step:
                pipeline_step = step_id_to_pipeline_step[step_id]
                execution_step = self._build_execution_step(pipeline_step)
                if execution_step:  # Skip None returns (like SET statements)
                    execution_steps.append(execution_step)

        return execution_steps

    def _add_missing_steps(
        self,
        pipeline: Pipeline,
        execution_steps: List[Dict[str, Any]],
        step_id_to_pipeline_step: Dict[str, PipelineStep],
    ) -> List[Dict[str, Any]]:
        """Add steps that weren't included in the execution order.

        Following Zen of Python: Practicality beats purity.
        Ensure all steps are included even if not in order.
        """
        existing_step_ids = [s["id"] for s in execution_steps]

        for pipeline_step in pipeline.steps:
            # Skip SET statements
            if isinstance(pipeline_step, SetStep):
                continue

            step_id = self._get_step_id(pipeline_step)
            if step_id and step_id not in existing_step_ids:
                logger.debug(f"Adding missing step to execution plan: {step_id}")
                execution_step = self._build_execution_step(pipeline_step)
                if execution_step:  # Skip None returns (like SET statements)
                    execution_steps.append(execution_step)

        return execution_steps

    def _get_step_id(self, step: PipelineStep) -> str:
        """Get step ID for a pipeline step.

        Following Zen of Python: Simple is better than complex.
        Clear step ID retrieval.
        """
        return self.step_id_map.get(id(step), "")

    def _build_execution_step(
        self, pipeline_step: PipelineStep
    ) -> Optional[Dict[str, Any]]:
        """Build a single execution step from a pipeline step.

        Args:
            pipeline_step: The pipeline step to convert

        Returns:
            An execution step dictionary or None for steps like SET that don't
            correspond to executable steps

        Following Zen of Python: Explicit is better than implicit.
        Clear step type handling.
        """
        step_id = self._get_step_id(pipeline_step)
        depends_on = self.step_dependencies.get(step_id, [])

        # Skip SET statements as they aren't execution steps, just variable definitions
        if isinstance(pipeline_step, SetStep):
            logger.debug(
                f"Skipping SET statement for {pipeline_step.variable_name} as it's not an execution step"
            )
            return None

        # Delegate to specific builders based on step type
        if isinstance(pipeline_step, SourceDefinitionStep):
            return self._build_source_definition_step(pipeline_step)
        elif isinstance(pipeline_step, LoadStep):
            return self._build_load_step(pipeline_step, step_id, depends_on)
        elif isinstance(pipeline_step, SQLBlockStep):
            return self._build_sql_block_step(pipeline_step, step_id, depends_on)
        elif isinstance(pipeline_step, ExportStep):
            return self._build_export_step(pipeline_step, step_id, depends_on)
        else:
            return {
                "id": step_id,
                "type": "unknown",
                "depends_on": depends_on,
            }

    def _build_source_definition_step(
        self, step: SourceDefinitionStep
    ) -> Dict[str, Any]:
        """Build an execution step for a source definition.

        Following Zen of Python: Explicit is better than implicit.
        Clear source definition step building.
        """
        step_id = self.step_id_map.get(id(step), f"source_{step.name}")

        # Extract industry-standard parameters from SOURCE params
        params = step.params.copy()
        sync_mode = params.get("sync_mode", "full_refresh")
        cursor_field = params.get("cursor_field")
        primary_key = params.get("primary_key", [])

        # Ensure primary_key is always a list for consistency
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Check if this is a profile-based source definition (FROM syntax)
        if step.is_from_profile:
            return {
                "id": step_id,
                "type": "source_definition",
                "name": step.name,
                "is_from_profile": True,
                "profile_connector_name": step.profile_connector_name,
                "query": params,  # These are the OPTIONS for the source
                "sync_mode": sync_mode,
                "cursor_field": cursor_field,
                "primary_key": primary_key,
                "depends_on": [],
            }
        else:
            return {
                "id": step_id,
                "type": "source_definition",
                "name": step.name,
                "source_connector_type": step.connector_type,
                "sync_mode": sync_mode,
                "cursor_field": cursor_field,
                "primary_key": primary_key,
                "query": params,
                "depends_on": [],
            }

    def _build_load_step(
        self, step: LoadStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a load step.

        Following Zen of Python: Simple is better than complex.
        Clear load step building.
        """
        # Get source connector type from source definitions
        source_connector_type = "unknown"
        if step.source_name in self._source_definitions:
            source_connector_type = self._source_definitions[step.source_name][
                "connector_type"
            ]

        return {
            "id": step_id,
            "type": "load",
            "name": step.table_name,
            "source_name": step.source_name,
            "target_table": step.table_name,
            "source_connector_type": source_connector_type,
            "mode": getattr(step, "mode", "REPLACE"),
            "upsert_keys": getattr(step, "upsert_keys", []),
            "query": {
                "source_name": step.source_name,
                "table_name": step.table_name,
            },
            "depends_on": depends_on,
        }

    def _build_sql_block_step(
        self, step: SQLBlockStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a SQL block.

        Following Zen of Python: Simple is better than complex.
        Clear SQL block step building.
        """
        sql_query = step.sql_query
        if not sql_query.strip():
            logger.warning(f"Empty SQL query in step {step_id}")

        # Validate SQL syntax
        self._validate_sql_syntax(sql_query, step_id, getattr(step, "line_number", -1))

        return {
            "id": step_id,
            "type": "transform",
            "name": step.table_name,
            "query": sql_query,
            "is_replace": getattr(step, "is_replace", False),
            "depends_on": depends_on,
        }

    def _build_export_step(
        self, step: ExportStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for an export step.

        Following Zen of Python: Explicit is better than implicit.
        Clear export step building.
        """
        # Determine table name from step or SQL query
        table_name = getattr(
            step, "table_name", None
        ) or self._extract_table_name_from_sql(getattr(step, "sql_query", ""))
        connector_type = getattr(step, "connector_type", "unknown")

        # Use the actual step_id or generate a fallback
        export_id = step_id
        if not export_id:
            export_id = f"export_{connector_type.lower()}_{table_name or 'unknown'}"

        # Get destination URI and options
        destination_uri = getattr(step, "destination_uri", "")
        options = getattr(step, "options", {})

        logger.debug(f"Export step {export_id} with destination: {destination_uri}")

        return {
            "id": export_id,
            "type": "export",
            "source_table": table_name,
            "source_connector_type": connector_type,
            "query": {
                "sql_query": getattr(step, "sql_query", ""),
                "destination_uri": destination_uri,
                "options": options,
                "type": connector_type,
            },
            "depends_on": depends_on,
        }

    def _validate_sql_syntax(
        self, sql_query: str, step_id: str, line_number: int
    ) -> None:
        """Validate basic SQL syntax.

        Following Zen of Python: Simple is better than complex.
        Basic SQL validation to catch obvious errors.
        """
        if not sql_query or not sql_query.strip():
            logger.warning(f"Empty SQL query in step {step_id} at line {line_number}")
            return

        # Basic syntax checks
        sql_lower = sql_query.lower().strip()

        # Check for unmatched parentheses
        open_parens = sql_query.count("(")
        close_parens = sql_query.count(")")
        if open_parens != close_parens:
            logger.warning(
                f"Unmatched parentheses in SQL at step {step_id}, line {line_number}"
            )

        # Check for basic SQL keywords
        if not any(
            keyword in sql_lower
            for keyword in ["select", "create", "insert", "update", "delete", "with"]
        ):
            logger.warning(
                f"No recognized SQL keywords in step {step_id} at line {line_number}"
            )

    def _extract_table_name_from_sql(self, sql_query: str) -> Optional[str]:
        """Extract table name from SQL query.

        Following Zen of Python: Simple is better than complex.
        Basic pattern matching for table name extraction.
        """
        if not sql_query:
            return None

        # Try different SQL patterns
        patterns = [
            r"FROM\s+([a-zA-Z0-9_]+)",
            r"INSERT\s+INTO\s+([a-zA-Z0-9_]+)",
            r"UPDATE\s+([a-zA-Z0-9_]+)",
            r"CREATE\s+TABLE\s+([a-zA-Z0-9_]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, sql_query, re.IGNORECASE)
            if match:
                return match.group(1)

        return None
