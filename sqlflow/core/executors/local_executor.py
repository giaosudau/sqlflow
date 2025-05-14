"""Local executor for SQLFlow pipelines."""

import logging
import os
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from sqlflow.connectors.connector_engine import ConnectorEngine
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.core.executors.base_executor import BaseExecutor
from sqlflow.project import Project

logger = logging.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Executes pipelines sequentially in a single process."""

    def __init__(self):
        """Initialize a LocalExecutor."""
        self.executed_steps: Set[str] = set()
        self.failed_step: Optional[Dict[str, Any]] = None
        self.results: Dict[str, Any] = {}
        self.dependency_resolver: Optional[DependencyResolver] = None
        self.connector_engine = ConnectorEngine()
        # Load DuckDB path from profile config, fallback to ':memory:'
        project = Project(os.getcwd())
        profile = project.get_profile()
        duckdb_path = (
            profile.get("engines", {}).get("duckdb", {}).get("path", ":memory:")
        )
        self.duckdb_engine = DuckDBEngine(duckdb_path)
        self.table_data: Dict[str, DataChunk] = {}  # In-memory table state
        self.source_connectors: Dict[str, str] = {}  # Map source name to connector type

    def execute(
        self,
        plan: List[Dict[str, Any]],
        dependency_resolver: Optional[DependencyResolver] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline plan.

        Args:
            plan: List of operations to execute
            dependency_resolver: Optional DependencyResolver to cross-check execution order

        Returns:
            Dict containing execution results
        """
        self.results = {}
        self.dependency_resolver = dependency_resolver

        # Only check and warn once per execute call
        if (
            dependency_resolver is not None
            and dependency_resolver.last_resolved_order is not None
        ):
            plan_ids = [step["id"] for step in plan]

            if plan_ids != dependency_resolver.last_resolved_order:
                logger.warning(
                    "Execution order mismatch detected. Plan order: %s, Resolved order: %s",
                    plan_ids,
                    dependency_resolver.last_resolved_order,
                )

        for step in plan:
            try:
                step_result = self.execute_step(step)
                self.results[step["id"]] = step_result
                self.executed_steps.add(step["id"])
                if step_result.get("status") == "failed":
                    self.failed_step = step
                    self.results["error"] = step_result.get("error")
                    self.results["failed_step"] = step["id"]
                    break
            except Exception as e:
                self.failed_step = step
                self.results["error"] = str(e)
                self.results["failed_step"] = step["id"]
                self.results[step["id"]] = {"status": "failed", "error": str(e)}
                break

        return self.results

    def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step in the pipeline."""
        try:
            step_type = step["type"]
            if step_type == "source_definition":
                return self._execute_source_definition(step)
            elif step_type == "load":
                return self._execute_load(step)
            elif step_type == "transform":
                return self._execute_transform(step)
            elif step_type == "export":
                return self._execute_export(step)
            else:
                # Only warn for unknown step types if not a test (for unit test compatibility)
                if step_type != "test":
                    logger.warning(f"Unknown step type: {step_type}")
                return {
                    "status": "skipped",
                    "reason": f"Unknown step type: {step_type}",
                }
        except Exception as e:
            logger.error(f"Error executing step {step.get('id', '?')}: {e}")
            return {"status": "failed", "error": str(e)}

    def _execute_source_definition(self, step: Dict[str, Any]) -> Dict[str, Any]:
        name = step["name"]
        connector_type = step["source_connector_type"]
        params = step["query"]
        self.connector_engine.register_connector(name, connector_type, params)
        self.source_connectors[name] = connector_type
        return {"status": "success"}

    def _execute_load(self, step: Dict[str, Any]) -> Dict[str, Any]:
        source_name = step["query"]["source_name"]
        table_name = step["query"]["table_name"]
        self.source_connectors.get(source_name, "unknown")
        data_iter = self.connector_engine.load_data(source_name, table_name)
        all_batches = [chunk.arrow_table for chunk in data_iter]
        if all_batches:
            import pyarrow as pa

            table = pa.concat_tables(all_batches)
            self.table_data[table_name] = DataChunk(table)
        else:
            self.table_data[table_name] = DataChunk(pd.DataFrame())
        return {"status": "success"}

    def _execute_transform(self, step: Dict[str, Any]) -> Dict[str, Any]:
        sql = step["query"]
        table_name = step["name"]
        for tbl, chunk in self.table_data.items():
            df = chunk.pandas_df
            self.duckdb_engine.connection.register(tbl, df)
        result = self.duckdb_engine.execute_query(sql).fetchdf()
        self.table_data[table_name] = DataChunk(result)
        return {"status": "success"}

    def _execute_export(self, step: Dict[str, Any]) -> Dict[str, Any]:
        source_table = step.get("source_table")
        if not source_table or source_table == "unknown":
            depends_on = step.get("depends_on", [])
            if depends_on:
                source_table = self.table_data.get(depends_on[-1])
        if isinstance(source_table, str):
            data_chunk = self.table_data.get(source_table)
        else:
            data_chunk = source_table
        if data_chunk is None:
            raise RuntimeError(f"No data found for export step: {step}")
        self.connector_engine.export_data(
            data=data_chunk,
            destination=step["query"]["destination_uri"],
            connector_type=step["source_connector_type"],
            options=step["query"].get("options", {}),
        )
        return {"status": "success"}

    def can_resume(self) -> bool:
        """Check if the executor supports resuming from failure.

        Returns:
            True if the executor supports resuming, False otherwise
        """
        return self.failed_step is not None

    def resume(self) -> Dict[str, Any]:
        """Resume execution from the last failure.

        Returns:
            Dict containing execution results
        """
        if not self.can_resume():
            return {"status": "nothing_to_resume"}

        failed_step = self.failed_step
        self.failed_step = None

        if failed_step is None:
            return {"status": "nothing_to_resume"}

        try:
            step_result = self.execute_step(failed_step)
            self.results[failed_step["id"]] = step_result
            self.executed_steps.add(failed_step["id"])
        except Exception as e:
            self.failed_step = failed_step
            self.results["error"] = str(e)
            self.results["failed_step"] = failed_step["id"]
            return self.results

        return self.results
