"""Local executor for SQLFlow pipelines."""

import json
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
from sqlflow.core.sql_generator import SQLGenerator
from sqlflow.core.storage.artifact_manager import ArtifactManager
from sqlflow.project import Project

logger = logging.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Executes pipelines sequentially in a single process."""

    def __init__(self, profile_name: str = "dev", project_dir: Optional[str] = None):
        """Initialize a LocalExecutor with a given profile.

        Args:
            profile_name: Name of the profile to use
            project_dir: Project directory (default: current working directory)
        """
        self.project = Project(project_dir or os.getcwd(), profile_name=profile_name)
        self.profile = self.project.get_profile()
        self.duckdb_engine = DuckDBEngine(self.profile)
        self.connector_engine = ConnectorEngine()
        self.sql_generator = SQLGenerator(dialect="duckdb")
        self.results: Dict[str, Any] = {}
        self.step_table_map: Dict[str, str] = {}
        self.step_output_mapping: Dict[str, List[str]] = {}
        self.table_data: Dict[str, DataChunk] = {}
        self.source_connectors: Dict[str, str] = {}
        self.executed_steps: Set[str] = set()
        self.failed_step: Optional[Dict[str, Any]] = None
        self.logger = logger

        # Attributes to be set by the calling context (e.g., CLI run command)
        self.artifact_manager: Optional[ArtifactManager] = None
        self.execution_id: Optional[str] = None

        # Load DuckDB config from profile only
        duckdb_config = self.profile.get("engines", {}).get("duckdb", {})
        self.duckdb_mode = duckdb_config.get("mode", "memory")
        duckdb_path = duckdb_config.get("path", None)

        # Handle DuckDB database path
        if self.duckdb_mode == "memory":
            logger.info("[SQLFlow] LocalExecutor: DuckDB running in memory mode.")
            duckdb_path = ":memory:"
        else:
            logger.info(
                f"[SQLFlow] LocalExecutor: DuckDB persistent mode, path={duckdb_path}"
            )
            if not duckdb_path:
                raise ValueError(
                    "DuckDB persistent mode requires a 'path' in profile config."
                )
            # Ensure the directory for the DuckDB file exists
            db_dir = os.path.dirname(duckdb_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            # Use the exact path from the profile
            logger.info(
                f"[SQLFlow] Using exact database path from profile: {duckdb_path}"
            )

        try:
            self.duckdb_engine = DuckDBEngine(duckdb_path)
        except Exception as e:
            logger.error(f"Error initializing DuckDB engine: {e}")
            # Fall back to in-memory if we can't use the file-based DB
            logger.info(
                "[SQLFlow] Failed to initialize persistent database, falling back to in-memory mode"
            )
            self.duckdb_engine = DuckDBEngine(":memory:")

        self.table_data: Dict[str, DataChunk] = {}  # In-memory table state
        self.source_connectors: Dict[str, str] = {}  # Map source name to connector type
        self.step_table_map: Dict[str, str] = {}  # Map step ID to table name
        self.step_output_mapping: Dict[str, List[str]] = (
            {}
        )  # Map step ID to list of table names it produces

        # Attributes to be set by the calling context (e.g., CLI run command)
        self.execution_id: Optional[str] = None

    def _cleanup_old_database_files(
        self, directory: str, base_name: str, keep_last: int = 5
    ) -> None:
        """Clean up old database files to prevent accumulation.
        This method is no longer used if exact db paths are enforced.
        Kept for potential future use if naming strategy changes.

        Args:
            directory: Directory containing database files
            base_name: Base name of the database files
            keep_last: Number of most recent files to keep
        """
        logger.debug(
            "[SQLFlow] _cleanup_old_database_files is currently not active due to exact DB path usage."
        )
        return  # No longer cleaning up if using exact path from profile
        # try:
        #     import glob
        #     import os

        #     # Find all files matching the pattern
        #     pattern = os.path.join(directory, f"{base_name}_*")
        #     db_files = glob.glob(pattern)

        #     # Sort by modification time (new
        #     db_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        #     # Keep the most recent files, delete the rest
        #     if len(db_files) > keep_last:
        #         for old_file in db_files[keep_last:]:
        #             try:
        #                 logger.info(f"[SQLFlow] Removing old database file: {old_file}")
        #                 os.remove(old_file)
        #             except Exception as e:
        #                 logger.warning(f"[SQLFlow] Failed to remove old database file {old_file}: {e}")
        # except Exception as e:
        #     logger.warning(f"[SQLFlow] Error during database cleanup: {e}")

    def _execute_steps_in_original_order(
        self, steps, pipeline_name: Optional[str] = None
    ):
        for step in steps:
            step_id = step["id"]
            logger.debug(
                "Executing step %s (%s) in original order", step_id, step["type"]
            )
            result = self.execute_step(step, pipeline_name)
            self.results[step_id] = result
            logger.debug("Step %s execution result: %s", step_id, result)
            if result.get("status") == "failed":
                self.failed_step = step
                self.results["error"] = result.get("error", "Unknown error")
                self.results["failed_step"] = step_id
                logger.debug(
                    "Step %s failed: %s",
                    step_id,
                    result.get("error", "Unknown error"),
                )
            self.executed_steps.add(step_id)

    def _execute_steps_in_dependency_order(
        self, steps, dependency_resolver, pipeline_name: Optional[str] = None
    ):
        for step_id in dependency_resolver.last_resolved_order:
            step = next((s for s in steps if s["id"] == step_id), None)
            if step is None:
                logger.warning(f"Step {step_id} not found")
                logger.debug("Step %s not found in plan", step_id)
                continue
            logger.debug("Executing step %s (%s)", step_id, step["type"])
            result = self.execute_step(step, pipeline_name)
            self.results[step_id] = result
            logger.debug("Step %s execution result: %s", step_id, result)
            if result.get("status") == "failed":
                self.failed_step = step
                self.results["error"] = result.get("error", "Unknown error")
                self.results["failed_step"] = step_id
                logger.debug(
                    "Step %s failed: %s",
                    step_id,
                    result.get("error", "Unknown error"),
                )
            self.executed_steps.add(step_id)

    def _generate_step_summary(self, steps):
        step_types = {}
        for step in steps:
            step_type = step.get("type", "unknown")
            if step_type not in step_types:
                step_types[step_type] = {
                    "total": 0,
                    "success": 0,
                    "failed": 0,
                    "steps": [],
                }
            step_types[step_type]["total"] += 1
            step_id = step.get("id", "unknown")
            result = self.results.get(step_id, {})
            status = result.get("status", "unknown")
            if status == "success":
                step_types[step_type]["success"] += 1
            elif status == "failed":
                step_types[step_type]["failed"] += 1
            step_info = {
                "id": step_id,
                "status": status,
                "error": result.get("error", None),
            }
            step_types[step_type]["steps"].append(step_info)
        self.results["summary"] = {
            "total_steps": len(steps),
            "successful_steps": len(
                [
                    s
                    for s in steps
                    if self.results.get(s["id"], {}).get("status") == "success"
                ]
            ),
            "failed_steps": len(
                [
                    s
                    for s in steps
                    if self.results.get(s["id"], {}).get("status") == "failed"
                ]
            ),
            "by_type": step_types,
        }

    def execute(
        self,
        steps: List[Dict[str, Any]],
        dependency_resolver: Optional[DependencyResolver] = None,
        pipeline_name: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline.

        Args:
            steps: List of operations to execute
            dependency_resolver: Optional DependencyResolver
            pipeline_name: Name of the pipeline
            variables: Variables for variable substitution

        Returns:
            Execution results
        """
        self.executed_steps = set()
        self.failed_step = None
        self.results = {}
        self.dependency_resolver = dependency_resolver
        self.step_table_map = {}
        self.step_output_mapping = {}

        # Initialize execution tracking if pipeline_name is provided
        if pipeline_name:
            self.execution_id, _ = self.artifact_manager.initialize_execution(
                pipeline_name, variables or {}, self.profile_name
            )
        else:
            self.execution_id = None

        # Check if we have a valid dependency order
        if (
            dependency_resolver is not None
            and dependency_resolver.last_resolved_order is not None
            and not dependency_resolver.last_resolved_order
        ):
            logger.warning("No steps to execute in pipeline")
            return {"status": "no_steps"}

        logger.debug("Dependency resolver: %s", dependency_resolver)
        logger.debug(
            "Dependencies: %s",
            dependency_resolver.dependencies if dependency_resolver else "None",
        )
        logger.debug(
            "Last resolved order: %s",
            dependency_resolver.last_resolved_order if dependency_resolver else "None",
        )

        # Execute operations based on dependency order
        if not dependency_resolver or not dependency_resolver.last_resolved_order:
            logger.debug(
                "No valid dependency order - executing all steps in original order"
            )
            self._execute_steps_in_original_order(steps, pipeline_name)
        elif dependency_resolver is not None:
            logger.debug("Steps to execute: %d", len(steps))
            step_ids = [step["id"] for step in steps]
            logger.debug("All step IDs: %s", step_ids)
            logger.debug(
                "Dependency resolver execution order: %s",
                dependency_resolver.last_resolved_order,
            )
            if dependency_resolver.last_resolved_order:
                plan_order = [step["id"] for step in steps]
                resolved_order = dependency_resolver.last_resolved_order
                filtered_plan_order = [s for s in plan_order if s in resolved_order]
                if filtered_plan_order != resolved_order:
                    logger.warning(
                        "Execution order mismatch detected. Plan order: %s, Resolved dependency order: %s",
                        filtered_plan_order,
                        resolved_order,
                    )
                    logger.debug("⚠️ Execution order mismatch detected.")
                    logger.debug("Plan order: %s", filtered_plan_order)
                    logger.debug("Resolved dependency order: %s", resolved_order)
                self._execute_steps_in_dependency_order(
                    steps, dependency_resolver, pipeline_name
                )
            else:
                logger.debug(
                    "⚠️ No resolved order in dependency resolver. Will execute all steps in their original order."
                )
                self._execute_steps_in_original_order(steps, pipeline_name)
        else:
            logger.debug(
                "No dependency resolver provided. Executing steps in original order."
            )
            self._execute_steps_in_original_order(steps, pipeline_name)

        # Generate execution summary
        self._generate_step_summary(steps)

        # Record execution completion if tracking
        if pipeline_name and self.execution_id:
            success = "error" not in self.results
            self.artifact_manager.finalize_execution(pipeline_name, success)

        return self.results

    def _handle_artifact_recording(
        self,
        event_type: str,  # "start" or "completion"
        pipeline_name: Optional[str],
        step_id: str,
        step_type: str,
        sql: Optional[str] = None,  # For start event
        result: Optional[Dict[str, Any]] = None,  # For completion event
        success_status: Optional[bool] = None,  # For completion event
    ):
        """Helper to record operation start or completion if tracking is active."""
        if not (pipeline_name and self.execution_id and self.artifact_manager):
            return

        if event_type == "start":
            if sql is not None:
                self.artifact_manager.record_operation_start(
                    pipeline_name, step_id, step_type, sql
                )
            else:
                logger.warning(
                    f"SQL not provided for artifact recording of step start: {step_id}"
                )
        elif event_type == "completion":
            if result is None or success_status is None:
                logger.warning(
                    f"Result or success_status not provided for artifact recording of step completion: {step_id}"
                )
                # Fallback to a generic error if crucial info is missing
                _result = result or {}
                _success = success_status if success_status is not None else False
                _error_payload = {
                    "error": _result.get(
                        "error", "Unknown error during artifact recording"
                    )
                }
                self.artifact_manager.record_operation_completion(
                    pipeline_name,
                    step_id,
                    _success,
                    _error_payload if not _success else _result,
                )
                return

            if success_status:
                self.artifact_manager.record_operation_completion(
                    pipeline_name, step_id, True, result
                )
            else:
                error_payload = {
                    "error": result.get("error", "Unknown error"),
                }
                self.artifact_manager.record_operation_completion(
                    pipeline_name, step_id, False, error_payload
                )
        else:
            logger.warning(
                f"Unknown artifact event_type: {event_type} for step {step_id}"
            )

    def execute_step(
        self, step: Dict[str, Any], pipeline_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute a single step."""
        step_id = step.get("id", "unknown")
        step_type = step.get("type", "unknown")
        current_step_result: Dict[str, Any] = {}
        sql_for_artifact: Optional[str] = None

        try:
            # Generate SQL for artifact recording if needed, outside the main execution try-catch
            if pipeline_name and self.execution_id and self.artifact_manager:
                variables = (
                    {}
                )  # Should be passed from execute() context or class member
                context = {"variables": variables, "execution_id": self.execution_id}
                sql_for_artifact = self.sql_generator.generate_operation_sql(
                    step, context
                )

            self._handle_artifact_recording(
                "start", pipeline_name, step_id, step_type, sql=sql_for_artifact
            )

            # Execute the step based on its type
            if step_type == "source_definition":
                current_step_result = self._execute_source_definition(step)
            elif step_type == "load":
                current_step_result = self._execute_load(step)
            elif step_type == "transform":
                current_step_result = self._execute_transform(step)
            elif step_type == "export":
                current_step_result = self._execute_export(step)
            else:
                logger.warning(f"Unknown step type: {step_type} for step {step_id}")
                current_step_result = {
                    "status": "failed",
                    "error": f"Unknown step type: {step_type}",
                }

            # Determine success status for artifact recording
            success_status = current_step_result.get("status") == "success"
            self._handle_artifact_recording(
                "completion",
                pipeline_name,
                step_id,
                step_type,
                result=current_step_result,
                success_status=success_status,
            )

        except Exception as e:
            logger.exception(f"Unhandled error executing step {step_id}: {str(e)}")
            current_step_result = {
                "status": "failed",
                "error": f"Unhandled error: {str(e)}",
            }
            # Record operation failure if an overarching exception occurred
            # Ensure we pass a valid result structure for artifact recording
            self._handle_artifact_recording(
                "completion",
                pipeline_name,
                step_id,
                step_type,
                result=current_step_result,
                success_status=False,
            )

        # Update executor's internal state
        self.results[step_id] = current_step_result
        self.executed_steps.add(step_id)
        if current_step_result.get("status") == "failed":
            self.failed_step = step

        return current_step_result

    def _execute_source_definition(self, step: Dict[str, Any]) -> Dict[str, Any]:
        name = step["name"]
        connector_type = step["source_connector_type"]
        params = step["query"]
        self.connector_engine.register_connector(name, connector_type, params)
        self.source_connectors[name] = connector_type
        return {"status": "success"}

    def _execute_load(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a load step to import data from a source connector.

        Args:
            step: Load step configuration

        Returns:
            Dictionary with execution status
        """
        source_name = step["query"]["source_name"]
        table_name = step["query"]["table_name"]

        logger.debug(
            "Executing LOAD step for source '%s' into table '%s'",
            source_name,
            table_name,
        )

        # Before attempting to load data, ensure the source connector is registered
        if source_name not in self.connector_engine.registered_connectors:
            # If source definition step executed but connector not registered, try to find it
            # This is needed because the dependency resolution didn't properly establish relationships
            for src_step in [
                s for s in self.step_table_map.keys() if s.startswith("source_")
            ]:
                if src_step == f"source_{source_name}":
                    logger.debug(
                        "Found matching source step '%s' for source '%s'",
                        src_step,
                        source_name,
                    )
                    # Connector found but not registered - likely execution order issue
                    # Re-run source definition step
                    return {
                        "status": "failed",
                        "error": f"Connector '{source_name}' not registered. The source definition step may not have run successfully.",
                    }

            # No matching source step found
            return {
                "status": "failed",
                "error": f"Connector '{source_name}' not registered. No matching source definition found.",
            }

        try:
            connector_type = self.source_connectors.get(source_name, "unknown")
            logger.debug(
                "Using connector of type '%s' for source '%s'",
                connector_type,
                source_name,
            )

            # Get data from the connector
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
                logger.debug(
                    "Load step %s loaded table '%s' with %d rows",
                    step["id"],
                    table_name,
                    len(table),
                )
                return {"status": "success"}
            else:
                # Create an empty table
                self.table_data[table_name] = DataChunk(pd.DataFrame())
                self.step_table_map[step["id"]] = table_name
                if step["id"] not in self.step_output_mapping:
                    self.step_output_mapping[step["id"]] = []
                self.step_output_mapping[step["id"]].append(table_name)
                logger.debug(
                    "Load step %s created empty table '%s'", step["id"], table_name
                )
                return {
                    "status": "success",
                    "warning": "No data was loaded, created an empty table",
                }
        except Exception as e:
            logger.debug("Error loading data for step %s: %s", step["id"], str(e))
            return {"status": "failed", "error": f"Error loading data: {str(e)}"}

    def _execute_transform(self, step: Dict[str, Any]) -> Dict[str, Any]:
        sql = step["query"]
        table_name = step["name"]
        # Register all tables with DuckDB
        for tbl, chunk in self.table_data.items():
            df = chunk.pandas_df
            self.duckdb_engine.connection.register(tbl, df)

        # In persistent mode, ensure all transform tables are persistent
        if self.duckdb_mode == "persistent":
            sql_stripped = sql.strip().lower()
            # If the SQL is a SELECT, wrap it as CREATE TABLE ... AS (...)
            if sql_stripped.startswith("select"):
                sql = f"CREATE TABLE {table_name} AS {sql}"
            # If the SQL is CREATE TEMP or CREATE TEMPORARY, replace with CREATE TABLE
            elif sql_stripped.startswith(
                "create temporary table"
            ) or sql_stripped.startswith("create temp table"):
                sql = re.sub(
                    r"create\s+temp(orary)?\s+table",
                    "CREATE TABLE",
                    sql,
                    flags=re.IGNORECASE,
                )
            # If the SQL is CREATE TABLE, leave as is
            # Otherwise, leave as is (user may have written a CTE or other statement)

        # Execute the query and store result
        result = self.duckdb_engine.execute_query(sql).fetchdf()
        self.table_data[table_name] = DataChunk(result)
        self.step_table_map[step["id"]] = table_name
        if step["id"] not in self.step_output_mapping:
            self.step_output_mapping[step["id"]] = []
        self.step_output_mapping[step["id"]].append(table_name)
        logger.debug(
            "Transform step %s created table '%s' with %d rows",
            step["id"],
            table_name,
            len(result),
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
                logger.debug("Extracted source_table from SQL query: %s", table_name)
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
            logger.debug("Found table '%s' for dependency %s", table_name, dep_id)
            data_chunk = self.table_data.get(table_name)
            source_table = table_name
            if data_chunk:
                logger.debug(
                    "Using data from table '%s' for export (%d rows)",
                    table_name,
                    len(data_chunk.pandas_df),
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
                    logger.debug(
                        "Using data from output table '%s' for export (%d rows)",
                        table,
                        len(data_chunk.pandas_df),
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
                    logger.debug(
                        "Matched table '%s' with dependency name %s",
                        table_name,
                        dep_name,
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
        logger.debug("Export step depends_on: %s", depends_on)

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
            logger.debug(
                "Using explicit table '%s' with %d rows for export",
                source_table,
                len(data_chunk.pandas_df),
            )
        else:
            logger.debug("Table '%s' not found in available tables", source_table)
            # Try partial matches on table name
            for table_name in self.table_data.keys():
                if source_table.lower() in table_name.lower():
                    data_chunk = self.table_data[table_name]
                    logger.debug(
                        "Found partial match '%s' for '%s'", table_name, source_table
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
                logger.debug("Using most recent table '%s' as fallback", latest_table)

        return source_table, data_chunk

    def _resolve_export_source(self, step):
        source_table = step.get("source_table")
        data_chunk = None
        logger.debug("Export step initial source_table: %s", source_table)
        logger.debug("All available tables: %s", list(self.table_data.keys()))
        logger.debug("Step-table mapping: %s", self.step_table_map)
        logger.debug("Step output mapping: %s", self.step_output_mapping)
        # First try to extract from SQL query
        if not source_table or source_table == "unknown":
            extracted_table = self._extract_source_table(step)
            if extracted_table:
                source_table = extracted_table
                logger.debug("Extracted source table from SQL: %s", source_table)
        # If still unknown, try dependencies
        if not source_table or source_table == "unknown":
            logger.debug("Looking for source table via dependencies")
            source_table, data_chunk = self._resolve_table_from_dependencies(step)
            if source_table:
                logger.debug("Found source table via dependencies: %s", source_table)
        # If we have a table name but no data chunk, try to get it directly
        if source_table and not data_chunk:
            logger.debug("Looking for table by name: %s", source_table)
            source_table, data_chunk = self._get_table_by_name(source_table)
            if data_chunk:
                logger.debug("Found data chunk for table %s", source_table)
        # As a final fallback, use the most recently created table
        if data_chunk is None:
            logger.debug("Using latest table as fallback")
            source_table, data_chunk = self._get_latest_table()
            if data_chunk:
                logger.debug("Found data in latest table: %s", source_table)
        return source_table, data_chunk

    def _report_export_error(self, step, error_msg):
        logger.debug("❌ %s", error_msg)
        return {"status": "failed", "error": error_msg}

    def _substitute_variables(self, template: str) -> str:
        """Substitute variables in the template string.

        Args:
            template: String with variables in ${var} or ${var|default} format

        Returns:
            String with variables substituted
        """
        import re

        def replace_var(match):
            var_expr = match.group(1)
            if "|" in var_expr:
                # Handle default value
                var_name, default = var_expr.split("|", 1)
                var_name = var_name.strip()
                default = default.strip()

                if hasattr(self, "profile") and hasattr(self.profile, "get"):
                    profile_vars = self.profile.get("variables", {})
                    if var_name in profile_vars:
                        return str(profile_vars[var_name])

                return default
            else:
                var_name = var_expr.strip()
                if hasattr(self, "profile") and hasattr(self.profile, "get"):
                    profile_vars = self.profile.get("variables", {})
                    if var_name in profile_vars:
                        return str(profile_vars[var_name])

                return match.group(0)  # Keep the original if not found

        return re.sub(r"\$\{([^}]+)\}", replace_var, template)

    def _substitute_variables_in_dict(self, data: dict) -> dict:
        """Substitute variables in a dictionary (recursively).

        Args:
            data: Dictionary potentially containing variables in values

        Returns:
            Dictionary with variables substituted in values
        """
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = self._substitute_variables_in_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    (
                        self._substitute_variables_in_dict(item)
                        if isinstance(item, dict)
                        else (
                            self._substitute_variables(item)
                            if isinstance(item, str)
                            else item
                        )
                    )
                    for item in value
                ]
            elif isinstance(value, str):
                result[key] = self._substitute_variables(value)
            else:
                result[key] = value
        return result

    def _invoke_export(self, step, data_chunk, source_table):
        connector_type = step["source_connector_type"]
        try:
            original_destination = step["query"]["destination_uri"]
            original_options = step["query"].get("options", {})

            # Apply variable substitution to the destination URI
            destination = self._substitute_variables(original_destination)

            # Apply variable substitution to options if they're strings
            options = self._substitute_variables_in_dict(original_options)

            logger.debug(
                "Destination after variable substitution: %s -> %s",
                original_destination,
                destination,
            )

            if options != original_options:
                logger.debug(
                    "Options after variable substitution: %s -> %s",
                    original_options,
                    options,
                )

            logger.debug("Attempting to export to: %s", destination)
            logger.debug("Using connector type: %s", connector_type)
            logger.debug("With options: %s", options)
            self.connector_engine.export_data(
                data=data_chunk,
                destination=destination,
                connector_type=connector_type,
                options=options,
            )
            logger.debug(
                "✅ EXPORT SUCCESSFUL: %s to %s",
                step["id"],
                destination,
            )
            return {"status": "success"}
        except Exception as e:
            logger.debug("❌ EXPORT FAILED: %s", str(e))
            return {"status": "failed", "error": f"Export failed: {str(e)}"}

    def _execute_export(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an export step."""
        logger.debug("Starting export step %s", step["id"])
        logger.debug("EXPORT STEP DETAILS: %s", json.dumps(step, indent=2))
        try:
            source_table, data_chunk = self._resolve_export_source(step)
            if data_chunk is None:
                error_msg = f"No data found for export step: {step['id']}"
                return self._report_export_error(step, error_msg)
            logger.debug(
                "✅ Exporting %d rows of data from table '%s'",
                len(data_chunk.pandas_df),
                source_table,
            )
            return self._invoke_export(step, data_chunk, source_table)
        except Exception as e:
            logger.debug("❌ Export step %s error: %s", step["id"], str(e))
            return {"status": "failed", "error": str(e)}

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
