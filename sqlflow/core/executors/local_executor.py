"""Local executor for SQLFlow pipelines."""

import logging
import os
import re
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
        print(f"DEBUG: LocalExecutor initializing DuckDB with path: {duckdb_path}")
        self.duckdb_engine = DuckDBEngine(duckdb_path)
        self.table_data: Dict[str, DataChunk] = {}  # In-memory table state
        self.source_connectors: Dict[str, str] = {}  # Map source name to connector type
        self.step_table_map: Dict[str, str] = {}  # Map step ID to table name
        self.step_output_mapping: Dict[str, List[str]] = (
            {}
        )  # Map step ID to list of table names it produces

    def execute(
        self,
        steps: List[Dict[str, Any]],
        dependency_resolver: Optional[DependencyResolver] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline.

        Args:
            steps: List of operations to execute
            dependency_resolver: Optional dependency resolver

        Returns:
            Dict containing execution results
        """
        # Reset state for this execution
        self.executed_steps = set()
        self.failed_step = None
        self.results = {}
        self.dependency_resolver = dependency_resolver
        self.step_table_map = {}
        self.step_output_mapping = {}

        # Only check and warn once per execute call
        if (
            dependency_resolver is not None
            and dependency_resolver.last_resolved_order is not None
            and not dependency_resolver.last_resolved_order
        ):
            logger.warning("No steps to execute in pipeline")
            return {"status": "no_steps"}

        if dependency_resolver is not None:
            # Print debugging information about dependency resolution
            print(f"DEBUG: Steps to execute: {len(steps)}")
            print(
                f"DEBUG: Dependency resolver execution order: {dependency_resolver.last_resolved_order}"
            )

            # Check for execution order mismatch
            if dependency_resolver.last_resolved_order:
                plan_order = [step["id"] for step in steps]
                resolved_order = dependency_resolver.last_resolved_order

                # Filter plan_order to only include steps that are in resolved_order
                filtered_plan_order = [s for s in plan_order if s in resolved_order]

                # Check if the filtered plan order matches the resolved order
                if filtered_plan_order != resolved_order:
                    logger.warning(
                        "Execution order mismatch detected. Plan order: %s, Resolved dependency order: %s",
                        filtered_plan_order,
                        resolved_order,
                    )

            # Execute steps in dependency order
            for step_id in dependency_resolver.last_resolved_order or []:
                # Find the step in the steps list
                step = next((s for s in steps if s["id"] == step_id), None)
                if step is None:
                    logger.warning(f"Step {step_id} not found")
                    print(f"DEBUG: Step {step_id} not found in plan")
                    continue

                print(f"DEBUG: Executing step {step_id} ({step['type']})")
                result = self.execute_step(step)
                self.results[step_id] = result
                print(f"DEBUG: Step {step_id} execution result: {result}")

                if result.get("status") == "failed":
                    self.failed_step = step
                    self.results["error"] = result.get("error", "Unknown error")
                    self.results["failed_step"] = step_id
                    print(
                        f"DEBUG: Step {step_id} failed: {result.get('error', 'Unknown error')}"
                    )
                    break

                self.executed_steps.add(step_id)
        else:
            # No dependency resolver, just execute steps in order
            for step in steps:
                result = self.execute_step(step)
                self.results[step["id"]] = result
                if result.get("status") == "failed":
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

        # Before attempting to load data, ensure the source connector is registered
        if source_name not in self.connector_engine.registered_connectors:
            # If source definition step executed but connector not registered, try to find it
            # This is needed because the dependency resolution didn't properly establish relationships
            for src_step in [
                s for s in self.step_table_map.keys() if s.startswith("source_")
            ]:
                if src_step == f"source_{source_name}":
                    print(
                        f"DEBUG: Found matching source step '{src_step}' for source '{source_name}'"
                    )
                    # Connector found but not registered - likely execution order issue
                    # Re-run source definition step
                    return {
                        "status": "failed",
                        "error": f"Connector '{source_name}' not registered",
                    }

            # No matching source step found
            return {
                "status": "failed",
                "error": f"Connector '{source_name}' not registered",
            }

        self.source_connectors.get(source_name, "unknown")
        data_iter = self.connector_engine.load_data(source_name, table_name)
        all_batches = [chunk.arrow_table for chunk in data_iter]
        if all_batches:
            import pyarrow as pa

            table = pa.concat_tables(all_batches)
            self.table_data[table_name] = DataChunk(table)
            self.step_table_map[step["id"]] = table_name
            if step["id"] not in self.step_output_mapping:
                self.step_output_mapping[step["id"]] = []
            self.step_output_mapping[step["id"]].append(table_name)
            print(f"DEBUG: Load step {step['id']} loaded table '{table_name}'")
        else:
            self.table_data[table_name] = DataChunk(pd.DataFrame())
            self.step_table_map[step["id"]] = table_name
            if step["id"] not in self.step_output_mapping:
                self.step_output_mapping[step["id"]] = []
            self.step_output_mapping[step["id"]].append(table_name)
            print(f"DEBUG: Load step {step['id']} created empty table '{table_name}'")
        return {"status": "success"}

    def _execute_transform(self, step: Dict[str, Any]) -> Dict[str, Any]:
        sql = step["query"]
        table_name = step["name"]
        # Register all tables with DuckDB
        for tbl, chunk in self.table_data.items():
            df = chunk.pandas_df
            self.duckdb_engine.connection.register(tbl, df)

        # Execute the query and store result
        result = self.duckdb_engine.execute_query(sql).fetchdf()
        self.table_data[table_name] = DataChunk(result)
        self.step_table_map[step["id"]] = table_name
        if step["id"] not in self.step_output_mapping:
            self.step_output_mapping[step["id"]] = []
        self.step_output_mapping[step["id"]].append(table_name)
        print(
            f"DEBUG: Transform step {step['id']} created table '{table_name}' with {len(result)} rows"
        )
        return {"status": "success"}

    def _extract_source_table(self, step: Dict[str, Any]) -> str:
        """Extract table name from SQL query in the export step.

        Args:
            step: Export step configuration

        Returns:
            Table name if found, empty string otherwise
        """
        query = step.get("query", {})
        if isinstance(query, dict) and "sql_query" in query:
            sql_query = query["sql_query"]
            # Extract table name using regex
            from_match = re.search(r"FROM\s+([a-zA-Z0-9_]+)", sql_query, re.IGNORECASE)
            if from_match:
                table_name = from_match.group(1)
                print(f"DEBUG: Extracted source_table from SQL query: {table_name}")
                return table_name
        return ""

    def _get_table_from_step_map(self, dep_id: str) -> tuple[str, Optional[DataChunk]]:
        """Find data for a dependency ID using the step table mapping.

        Args:
            dep_id: Dependency ID to check

        Returns:
            Tuple of (table_name, data_chunk)
        """
        source_table = ""
        data_chunk = None

        table_name = self.step_table_map.get(dep_id)
        if table_name and table_name in self.table_data:
            print(f"DEBUG: Found table '{table_name}' for dependency {dep_id}")
            data_chunk = self.table_data.get(table_name)
            source_table = table_name
            if data_chunk:
                print(
                    f"DEBUG: Using data from table '{table_name}' for export ({len(data_chunk.pandas_df)} rows)"
                )

        return source_table, data_chunk

    def _get_table_from_output_map(
        self, dep_id: str
    ) -> tuple[str, Optional[DataChunk]]:
        """Find data for a dependency ID using step output mapping.

        Args:
            dep_id: Dependency ID to check

        Returns:
            Tuple of (table_name, data_chunk)
        """
        source_table = ""
        data_chunk = None

        output_tables = self.step_output_mapping.get(dep_id, [])
        for table in output_tables:
            if table in self.table_data:
                data_chunk = self.table_data.get(table)
                source_table = table
                if data_chunk:
                    print(
                        f"DEBUG: Using data from output table '{table}' for export ({len(data_chunk.pandas_df)} rows)"
                    )
                    break

        return source_table, data_chunk

    def _find_table_by_dep_name(
        self, depends_on: List[str]
    ) -> tuple[str, Optional[DataChunk]]:
        """Find data by matching dependency name as substring of table names.

        Args:
            depends_on: List of dependency IDs

        Returns:
            Tuple of (table_name, data_chunk)
        """
        source_table = ""
        data_chunk = None

        for table_name in self.table_data.keys():
            for dep_id in depends_on:
                # Get the last part of the dependency ID
                dep_name = dep_id.split(".")[-1]
                if dep_name.lower() in table_name.lower():
                    data_chunk = self.table_data.get(table_name)
                    source_table = table_name
                    print(
                        f"DEBUG: Matched table '{table_name}' with dependency name {dep_name}"
                    )
                    return source_table, data_chunk

        return source_table, data_chunk

    def _resolve_table_from_dependencies(
        self, step: Dict[str, Any]
    ) -> tuple[str, Optional[DataChunk]]:
        """Locate data by checking various dependency relationships.

        Args:
            step: Export step configuration

        Returns:
            Tuple of (table_name, data_chunk)
        """
        source_table = ""
        data_chunk = None
        depends_on = step.get("depends_on", [])
        print(f"DEBUG: Export step depends_on: {depends_on}")

        if not depends_on:
            return source_table, data_chunk

        # Try each dependency, starting with the last one
        for dep_id in reversed(depends_on):
            # Try to get table from step_table_map
            source_table, data_chunk = self._get_table_from_step_map(dep_id)
            if data_chunk:
                break

            # Try to get tables from step_output_mapping
            source_table, data_chunk = self._get_table_from_output_map(dep_id)
            if data_chunk:
                break

        # If we still don't have data, try finding any table with a dependency substring in its name
        if not data_chunk and depends_on:
            source_table, data_chunk = self._find_table_by_dep_name(depends_on)

        return source_table, data_chunk

    def _get_table_by_name(self, source_table: str) -> tuple[str, Optional[DataChunk]]:
        """Get data using exact or partial table name matching.

        Args:
            source_table: Source table name to look for

        Returns:
            Tuple of (table_name, data_chunk)
        """
        data_chunk = None
        result_table = source_table

        # Try exact match
        if source_table in self.table_data:
            data_chunk = self.table_data[source_table]
            print(
                f"DEBUG: Using explicit table '{source_table}' with {len(data_chunk.pandas_df)} rows for export"
            )
        else:
            print(f"DEBUG: Table '{source_table}' not found in available tables")
            # Try partial matches on table name
            for table_name in self.table_data.keys():
                if source_table.lower() in table_name.lower():
                    data_chunk = self.table_data[table_name]
                    print(
                        f"DEBUG: Found partial match '{table_name}' for '{source_table}'"
                    )
                    result_table = table_name
                    break

        return result_table, data_chunk

    def _get_latest_table(self) -> tuple[str, Optional[DataChunk]]:
        """Get data from the most recently created table as fallback.

        Returns:
            Tuple of (table_name, data_chunk)
        """
        source_table = ""
        data_chunk = None

        if self.table_data:
            # Try using the most recent table created
            table_names = list(self.table_data.keys())
            if table_names:
                latest_table = table_names[-1]  # Assuming tables are added in order
                data_chunk = self.table_data[latest_table]
                source_table = latest_table
                print(f"DEBUG: Using most recent table '{latest_table}' as fallback")

        return source_table, data_chunk

    def _execute_export(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an export step.

        Args:
            step: Export step configuration

        Returns:
            Dictionary with execution status

        Raises:
            RuntimeError: If no data found for export
        """
        print(f"DEBUG: Starting export step {step['id']}")
        source_table = step.get("source_table")
        data_chunk = None

        print(f"DEBUG: Export step initial source_table: {source_table}")
        print(f"DEBUG: All available tables: {list(self.table_data.keys())}")
        print(f"DEBUG: Step-table mapping: {self.step_table_map}")
        print(f"DEBUG: Step output mapping: {self.step_output_mapping}")

        # First try to extract from SQL query
        if not source_table or source_table == "unknown":
            extracted_table = self._extract_source_table(step)
            if extracted_table:
                source_table = extracted_table

        # If still unknown, try dependencies
        if not source_table or source_table == "unknown":
            source_table, data_chunk = self._resolve_table_from_dependencies(step)

        # If we have a table name but no data chunk, try to get it directly
        if source_table and not data_chunk:
            source_table, data_chunk = self._get_table_by_name(source_table)

        # As a final fallback, use the most recently created table
        if data_chunk is None:
            source_table, data_chunk = self._get_latest_table()

        if data_chunk is None:
            error_msg = f"No data found for export step: {step['id']}"
            print(f"DEBUG: {error_msg}")
            raise RuntimeError(error_msg)

        print(
            f"DEBUG: Exporting {len(data_chunk.pandas_df)} rows of data from table '{source_table}'"
        )

        # Export the data
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
            True if executor supports resuming, False otherwise
        """
        return True

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
            return {"status": "failed", "error": str(e)}

        return self.results
