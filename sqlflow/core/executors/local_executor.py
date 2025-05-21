"""Local executor for SQLFlow pipelines."""

import datetime
import importlib.util
import json
import logging
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd
import typer

from sqlflow.connectors.connector_engine import ConnectorEngine
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.engines.duckdb_engine import DuckDBEngine, UDFError
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
        # Initialize base executor
        super().__init__()

        self.project = Project(project_dir or os.getcwd(), profile_name=profile_name)
        self.profile = self.project.get_profile()
        self.profile_name = profile_name
        self.duckdb_engine = None  # Will be initialized below
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

        # Initialize UDF manager with project directory
        self.udf_manager.project_dir = self.project.project_dir

        # Discover all UDFs in the project
        self.discover_udfs()
        logger.info(f"Discovered {len(self.discovered_udfs)} Python UDFs")

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
            if not duckdb_path:
                raise ValueError(
                    "DuckDB persistent mode requires a 'path' in profile config."
                )
            logger.info(
                f"[SQLFlow] LocalExecutor: DuckDB persistent mode, path={duckdb_path}"
            )
            # Ensure the directory for the DuckDB file exists
            db_dir = os.path.dirname(duckdb_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            # Use the exact path from the profile
            logger.info(
                "[SQLFlow] Using exact database path from profile: " + duckdb_path
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

    def _setup_execution_environment(
        self, variables: Optional[Dict[str, Any]] = None
    ) -> None:
        """Set up the execution environment.

        Args:
            variables: Optional dictionary of variables for substitution
        """
        # Initialize variables if not provided
        self.variables = variables or {}

        # Register variables with the SQL generator
        self.sql_generator.variables = self.variables.copy()

        # Register variables with the DuckDB engine
        if self.duckdb_engine is not None:
            for name, value in self.variables.items():
                self.duckdb_engine.register_variable(name, value)

        logger.debug(f"Execution environment set up with variables: {self.variables}")

    def _handle_external_resolver(
        self,
        steps: List[Dict[str, Any]],
        external_resolver: DependencyResolver,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Handle execution with an external dependency resolver.

        Args:
            steps: List of steps to execute
            external_resolver: External dependency resolver to use

        Returns:
            Tuple of (resolved_steps, resolved_order)
        """
        # Compare plan order to resolved order from resolver
        plan_ids = [step["id"] for step in steps]
        expected_order = external_resolver.last_resolved_order or []
        if plan_ids != expected_order:
            # Warn about execution order mismatch
            logger.warning(
                "Execution order mismatch detected", plan_ids, expected_order
            )
        # Use steps as is and resolver order for execution
        return steps, expected_order

    def execute(
        self,
        steps: List[Dict[str, Any]],
        pipeline_name: str = None,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline plan.

        Args:
            steps: List of operations to execute
            pipeline_name: Name of the pipeline
            variables: Optional dictionary of variables for substitution

        Returns:
            Dict containing execution results
        """
        logger.info(f"Executing plan with {len(steps)} steps")

        # Initialize execution state
        self._init_execution_state()

        # Reset the SQL generator's warning tracking to prevent warnings from previous runs
        self.sql_generator.reset_warning_tracking()

        # Set up execution environment
        self._setup_execution_environment(variables)

        try:
            # Detect external DependencyResolver passed in place of pipeline_name
            external_resolver = None
            if isinstance(pipeline_name, DependencyResolver):
                external_resolver = pipeline_name
                # Clear pipeline_name since it's not a name
                pipeline_name = None

            # Use external resolver if provided, else build one
            if external_resolver is not None:
                # Use provided DependencyResolver for execution order
                dependency_resolver = external_resolver
                resolved_steps, resolved_order = self._handle_external_resolver(
                    steps, external_resolver
                )
            else:
                logger.debug("Building dependency resolver")
                dependency_resolver = self._build_dependency_resolver(steps)
                resolved_steps, resolved_order = self._process_steps_and_create_order(
                    steps, dependency_resolver
                )

            # Map steps by ID for quick lookup
            steps_by_id = {step["id"]: step for step in resolved_steps}

            # Execute the steps
            logger.debug(f"Executing {len(resolved_order)} steps in dependency order")
            self._execute_steps_in_order(resolved_order, steps_by_id)

            # Add execution summary
            self._generate_step_summary(steps)

            # Log warning summary from SQL generator
            warning_counts = self.sql_generator.get_warning_summary()
            if warning_counts:
                total_warnings = sum(warning_counts.values())
                suppressed_warnings = total_warnings - len(warning_counts)
                # Add warning summary to results
                self.results["warnings"] = {
                    "total": total_warnings,
                    "unique": len(warning_counts),
                    "suppressed": suppressed_warnings,
                    "by_type": warning_counts,
                }

                if suppressed_warnings > 0:
                    logger.info(
                        f"Execution completed with {total_warnings} warnings "
                        f"({len(warning_counts)} unique, {suppressed_warnings} suppressed)"
                    )
                else:
                    logger.info(f"Execution completed with {total_warnings} warnings")

            # Log successful completion
            logger.info(
                f"Plan execution completed with status: {self.results.get('status', 'success')}"
            )
            return self.results

        except Exception as e:
            logger.error(f"Error executing plan: {str(e)}")
            self.results["status"] = "failed"
            self.results["error"] = str(e)
            return self.results

    def _init_execution_state(self) -> None:
        """Initialize execution state."""
        self.execution_id = str(uuid.uuid4())
        self.table_data = {}  # Map of table name to DataChunk
        self.step_table_map = {}  # Map of step ID to table name
        self.step_output_mapping = {}  # Map of step ID to list of output tables
        self.step_results = {}  # Map of step ID to execution result
        self.failed_step = None

    def _build_dependency_resolver(
        self, steps: List[Dict[str, Any]]
    ) -> DependencyResolver:
        """Build dependency resolver for the given steps.

        Args:
            steps: List of steps to build dependencies for

        Returns:
            Initialized dependency resolver
        """
        dependency_resolver = DependencyResolver()
        logger.debug("Dependency resolver: %s", dependency_resolver)

        for step in steps:
            step_id = step["id"]
            for dependency in step.get("depends_on", []):
                dependency_resolver.add_dependency(step_id, dependency)

        return dependency_resolver

    def _filter_unique_steps(
        self,
        steps: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Filter steps to ensure unique IDs.

        Args:
            steps: List of steps to filter

        Returns:
            List of steps with unique IDs
        """
        step_ids = set(step["id"] for step in steps)
        filtered_plan = [step for step in steps if step["id"] in step_ids]
        logger.debug("Plan contains %d unique steps", len(filtered_plan))
        return filtered_plan

    def _ensure_dependency_entries(
        self,
        filtered_plan: List[Dict[str, Any]],
        dependency_resolver: DependencyResolver,
    ) -> None:
        """Ensure all steps have an entry in the dependency graph.

        Args:
            filtered_plan: List of filtered steps
            dependency_resolver: Dependency resolver to update
        """
        for step in filtered_plan:
            if (
                step["id"] not in dependency_resolver.dependencies
                and "depends_on" not in step
            ):
                logger.debug(
                    f"Adding missing step to dependency resolver: {step['id']}"
                )
                dependency_resolver.dependencies[step["id"]] = []

    def _build_source_dependencies(
        self,
        filtered_plan: List[Dict[str, Any]],
        dependency_resolver: DependencyResolver,
    ) -> List[str]:
        """Build dependencies starting from source steps.

        Args:
            filtered_plan: List of filtered steps
            dependency_resolver: Dependency resolver to use

        Returns:
            List of resolved step IDs in execution order
        """
        # Get source steps
        source_steps = [
            step for step in filtered_plan if step["type"] == "source_definition"
        ]
        source_step_ids = [step["id"] for step in source_steps]

        # If no source steps, use first step as entry point
        if not source_step_ids:
            logger.debug("No source steps found, using first step as entry point")
            return [filtered_plan[0]["id"]] if filtered_plan else []

        # Build dependencies from source steps
        logger.debug(f"Using source steps as entry points: {source_step_ids}")
        resolved_order = []
        try:
            for source_id in source_step_ids:
                source_deps = dependency_resolver.resolve_dependencies(source_id)
                for dep in source_deps:
                    if dep not in resolved_order:
                        resolved_order.append(dep)
        except Exception as e:
            logger.error(f"Error resolving dependencies: {e}")
            raise

        return resolved_order

    def _process_steps_and_create_order(
        self, steps: List[Dict[str, Any]], dependency_resolver: DependencyResolver
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Process steps and create execution order.

        Args:
            steps: List of steps to process
            dependency_resolver: Dependency resolver to use

        Returns:
            Tuple of (filtered_plan, resolved_order)
        """
        # Filter steps with unique IDs
        filtered_plan = self._filter_unique_steps(steps)

        # Ensure all steps have dependency entries
        self._ensure_dependency_entries(filtered_plan, dependency_resolver)

        # Build dependencies from source steps
        resolved_order = self._build_source_dependencies(
            filtered_plan, dependency_resolver
        )

        # Ensure all steps are in the resolved order
        for step in filtered_plan:
            if step["id"] not in resolved_order:
                logger.debug(f"Adding missing step to execution order: {step['id']}")
                resolved_order.append(step["id"])

        return filtered_plan, resolved_order

    def _execute_steps_in_order(
        self, resolved_order: List[str], steps_by_id: Dict[str, Dict[str, Any]]
    ) -> bool:
        """Execute steps in the resolved dependency order.

        Args:
            resolved_order: List of step IDs in execution order
            steps_by_id: Map of step IDs to step definitions

        Returns:
            True if all steps executed successfully, False otherwise
        """
        self.results["status"] = "success"

        for step_id in resolved_order:
            logger.debug(f"Executing step {step_id}")
            if step_id not in steps_by_id:
                logger.warning(f"Step {step_id} not found in plan")
                continue

            step = steps_by_id[step_id]

            # Check if dependencies are satisfied
            if not self._check_dependencies_satisfied(step):
                self.results["status"] = "failed"
                self.results["error"] = f"Dependencies not satisfied for step {step_id}"
                return False

            # Execute the step
            success = self._execute_single_step(step_id, step)
            if not success:
                self.results["status"] = "failed"
                return False

        return True

    def _check_dependencies_satisfied(self, step: Dict[str, Any]) -> bool:
        """Check if all dependencies for a step are satisfied.

        Args:
            step: Step to check

        Returns:
            True if all dependencies are satisfied, False otherwise
        """
        # Always allow source definition and load steps to run (no dependencies)
        step_type = step.get("type")
        if step_type == "source_definition" or step_type == "load":
            return True

        # For steps with dependencies, check that they've all been executed successfully
        dependencies = step.get("depends_on", [])
        if not dependencies:
            # No dependencies, so all are satisfied
            return True

        for dependency in dependencies:
            # Check if dependency result exists and was successful
            dep_result = self.results.get(dependency)
            if not dep_result or dep_result.get("status") != "success":
                logger.warning(
                    f"Dependency {dependency} for step {step['id']} failed or not executed"
                )
                return False

        return True

    def _execute_single_step(self, step_id: str, step: Dict[str, Any]) -> bool:
        """Execute a single step.

        Args:
            step_id: ID of the step to execute
            step: Step definition

        Returns:
            True if step executed successfully, False otherwise
        """
        logger.info(f"Executing step {step_id} ({step['type']})")

        try:
            # Log start of execution for artifacts
            self._handle_artifact_recording(
                event_type="start",
                pipeline_name=None,
                step_id=step_id,
                step_type=step["type"],
            )

            # Execute based on step type
            if step["type"] == "source_definition":
                result = self._execute_source_definition(step)
            elif step["type"] == "load":
                result = self._execute_load(step)
            elif step["type"] == "transform":
                result = self._execute_transform(step)
            elif step["type"] == "export":
                result = self._execute_export(step)
            else:
                # Unknown step type - log at debug level to avoid spurious warnings
                logger.debug(f"Unknown step type: {step['type']}")
                result = {
                    "status": "failed",
                    "error": f"Unknown step type: {step['type']}",
                }

            # Store the result
            self.results[step_id] = result

            # Log completion of execution for artifacts
            self._handle_artifact_recording(
                event_type="completion",
                pipeline_name=None,
                step_id=step_id,
                step_type=step["type"],
                result=result,
                success_status=result.get("status") == "success",
            )

            # Return success status
            if result.get("status") != "success":
                logger.error(
                    f"Step {step_id} failed: {result.get('error', 'Unknown error')}"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Error executing step {step_id}: {str(e)}")
            self.results[step_id] = {"status": "failed", "error": str(e)}
            return False

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
        """Execute a single step of a pipeline.

        Args:
            step: The step to execute
            pipeline_name: Optional name of the pipeline (for artifact recording)

        Returns:
            Result of the execution
        """
        # Log what we're about to execute to make it clear to the user
        step_id = step.get("id", "unknown")
        step_type = step.get("type", "unknown")
        self.current_step_id = step_id

        # More focused, user-friendly output format
        typer.echo(f"  ▶️ Executing {step_id} ({step_type})")

        result = None
        try:
            # Start tracking this operation if pipeline_name is provided
            if pipeline_name:
                self.artifact_manager.record_operation_start(
                    pipeline_name, step_id, step_type, ""
                )

            # Route execution based on step type
            if step_type == "source_definition":
                result = self._handle_source_step(step)
            elif step_type == "load":
                result = self._handle_load_step(step)
            elif step_type == "transform":
                result = self._handle_transform_step(step)
            elif step_type == "export":
                result = self._handle_export_step(step)
            else:
                logger.error(f"Unknown step type: {step_type}")
                result = {
                    "status": "failed",
                    "error": f"Unknown step type: {step_type}",
                }

            # Record the result in our data structure
            self.results[step_id] = result

            # Record completion for the operation
            if pipeline_name:
                self.artifact_manager.record_operation_completion(
                    pipeline_name, step_id, step_type, result.get("status", "unknown")
                )

            # Update UI with success/failure
            if result.get("status") == "success":
                typer.echo(f"    ✅ {step_type.capitalize()} completed successfully")
            else:
                error_msg = result.get("error", "Unknown error")
                typer.echo(f"    ❌ Failed: {error_msg}")

            return result

        except Exception as e:
            logger.exception(f"Error executing step {step_id}: {e}")
            result = {"status": "failed", "error": str(e)}

            # Record the failure in our data structure
            self.results[step_id] = result

            # Record failure for the operation
            if pipeline_name:
                self.artifact_manager.record_operation_completion(
                    pipeline_name, step_id, step_type, "failed"
                )

            typer.echo(f"    ❌ Failed: {str(e)}")
            return result

    def _execute_source_definition(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a source definition step."""
        step_id = step.get("id", "unknown")
        source_name = step.get("name", None)

        if not source_name:
            error_msg = f"Source step {step_id} is missing a name property"
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        try:
            # Check if this is a profile-based source definition (FROM syntax)
            if step.get("is_from_profile", False):
                return self._handle_profile_based_source(step, step_id, source_name)
            else:
                return self._handle_traditional_source(step, step_id, source_name)

        except Exception as e:
            error_msg = f"Error executing source definition step {step_id}: {str(e)}"
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

    def _handle_profile_based_source(
        self, step: Dict[str, Any], step_id: str, source_name: str
    ) -> Dict[str, Any]:
        """Handle a profile-based source definition using FROM syntax."""
        profile_connector_name = step.get("profile_connector_name", None)
        if not profile_connector_name:
            error_msg = (
                f"Source step {step_id} is missing a profile_connector_name property.\n\n"
                "Correct syntax for profile-based sources:\n"
                'SOURCE name FROM "connector_name" OPTIONS { ... };'
            )
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        # Check for mistakenly set source_connector_type (mixing FROM and TYPE syntax)
        if step.get("source_connector_type"):
            error_msg = (
                f"Source step {step_id} has both profile_connector_name and source_connector_type set.\n\n"
                "This indicates mixing of FROM and TYPE syntax, which is not allowed.\n"
                "Choose one of these formats:\n"
                '1. SOURCE name FROM "connector_name" OPTIONS { ... };\n'
                "2. SOURCE name TYPE connector_type PARAMS { ... };\n"
            )
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        logger.debug(
            f"Processing profile-based source definition: {source_name} FROM {profile_connector_name}"
        )

        # Get connectors section from the profile
        profile_dict = self.project.get_profile()
        profile_connectors = profile_dict.get("connectors", {})

        if not profile_connectors:
            error_msg = (
                f"No connectors defined in profile '{self.profile_name}'.\n\n"
                f"Make sure your profile has a 'connectors' section with the connector '{profile_connector_name}' defined."
            )
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        # Check if the referenced connector exists in the profile
        if profile_connector_name not in profile_connectors:
            # Build a list of available connectors as a hint
            available_connectors = list(profile_connectors.keys())
            connector_list = ", ".join(f"'{conn}'" for conn in available_connectors)

            error_msg = (
                f"Connector '{profile_connector_name}' not found in profile '{self.profile_name}'.\n\n"
                f"Available connectors in profile: {connector_list}.\n"
                "Check your profile configuration and make sure the connector name matches exactly."
            )
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        # Get the connector from the profile
        profile_connector = profile_connectors.get(profile_connector_name, {})

        # Check that the connector has a type
        connector_type = profile_connector.get("type")
        if not connector_type:
            error_msg = (
                f"Connector '{profile_connector_name}' in profile '{self.profile_name}' does not have a 'type' property.\n\n"
                "Make sure your connector definition includes a 'type' property, such as:\n"
                "connectors:\n"
                f"  {profile_connector_name}:\n"
                "    type: postgres  # or csv, duckdb, etc."
            )
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        # Options are the parameters for this specific source
        options = step.get("query", {})

        try:
            # Store the connector type in the step for SQL generator to use
            step["source_connector_type"] = connector_type

            # Register the connector using the profile definition plus options
            self.connector_engine.register_profile_connector(
                source_name, profile_connector_name, profile_connectors, options
            )
            logger.debug(
                f"Registered profile-based connector '{source_name}' referencing profile connector '{profile_connector_name}'"
            )

            # Track this connector type
            self.source_connectors[source_name] = connector_type
            logger.debug(
                f"Source {source_name} using connector type {connector_type} from profile"
            )

        except Exception as e:
            logger.error(
                f"Failed to register profile-based connector '{source_name}': {e}"
            )
            return {
                "status": "failed",
                "error": f"Failed to register profile-based connector '{source_name}': {e}",
            }

        # Save the step mapping for this source
        self.step_table_map[step_id] = source_name
        logger.debug(f"Mapped step {step_id} to table {source_name}")

        return {"status": "success"}

    def _handle_traditional_source(
        self, step: Dict[str, Any], step_id: str, source_name: str
    ) -> Dict[str, Any]:
        """Handle a traditional source definition with TYPE PARAMS syntax."""
        # Extract connector type
        connector_type = step.get("source_connector_type", "").upper()
        if not connector_type:
            error_msg = (
                f"Missing connector type in source step {step_id}.\n\n"
                "Correct syntax:\n"
                "SOURCE name TYPE connector_type PARAMS { ... };\n"
            )
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        # Check if connector type is supported
        supported_connector_types = [
            "CSV",
            "POSTGRES",
            "POSTGRESQL",
            # Add other supported types here
        ]

        if connector_type.upper() not in supported_connector_types:
            error_msg = (
                f"Unsupported connector type '{connector_type}' in source step {step_id}.\n\n"
                f"Supported connector types: {', '.join(supported_connector_types)}.\n\n"
                "If you need to use a custom connector, use the FROM syntax with a profile connector:\n"
                'SOURCE name FROM "connector_name" OPTIONS { ... };\n'
            )
            logger.warning(error_msg)
            # Don't fail here, as the SQLGenerator will handle the unknown type

        # Track connector type for this source
        self.source_connectors[source_name] = connector_type
        logger.debug(
            f"Registered source {source_name} with connector type {connector_type}"
        )

        # Register connector with connector engine for data loading
        params = step.get("query", {})
        try:
            self.connector_engine.register_connector(
                source_name, connector_type, params
            )
            logger.debug(
                f"Connector engine registered connector '{source_name}' of type '{connector_type}' with params: {params}"
            )
        except Exception as e:
            error_msg = f"Failed to register connector '{source_name}': {e}"
            logger.error(error_msg)
            return {
                "status": "failed",
                "error": error_msg,
            }

        # Save the step mapping for this source
        self.step_table_map[step_id] = source_name
        logger.debug(f"Mapped step {step_id} to table {source_name}")

        return {"status": "success"}

    def _execute_load(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a load step to import data from a source connector.

        Args:
            step: Load step configuration

        Returns:
            Dictionary with execution status
        """
        # flake8: noqa: C901
        source_name = step["query"]["source_name"]
        table_name = step["query"]["table_name"]

        logger.debug(
            "Executing LOAD step for source '%s' into table '%s'",
            source_name,
            table_name,
        )

        # Add debug output
        print(
            f"DEBUG: Executing LOAD step for source '{source_name}' into table '{table_name}'"
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
            print(
                f"DEBUG: Using connector of type '{connector_type}' for source '{source_name}'"
            )

            # Get data from the connector
            data_iter = self.connector_engine.load_data(source_name, table_name)
            all_batches = [chunk.arrow_table for chunk in data_iter]

            if all_batches:
                import pyarrow as pa

                # Concatenate all arrow batches
                table = pa.concat_tables(all_batches)

                # Get the connector to check if it's a CSV with headers
                connector = self.connector_engine.registered_connectors.get(source_name)
                has_headers = False
                if connector_type.upper() == "CSV":
                    # For CSV connectors, check has_header property
                    if hasattr(connector, "has_header"):
                        has_headers = connector.has_header
                    print(f"DEBUG: CSV connector has_header: {has_headers}")

                # Print debugging info about the Arrow table
                print(f"DEBUG: Arrow table column names: {table.column_names}")
                print(f"DEBUG: Arrow table schema: {table.schema}")

                # Store data in memory as Arrow table
                self.table_data[table_name] = DataChunk(table)
                self.step_table_map[step["id"]] = table_name
                if step["id"] not in self.step_output_mapping:
                    self.step_output_mapping[step["id"]] = []
                self.step_output_mapping[step["id"]].append(table_name)

                # Add direct DuckDB table creation for persistence
                if (
                    self.duckdb_mode == "persistent"
                    and self.duckdb_engine is not None
                    and self.duckdb_engine.database_path != ":memory:"
                ):
                    try:
                        print(
                            f"DEBUG: Creating persistent table '{table_name}' in DuckDB"
                        )

                        # Begin transaction for this operation
                        self.duckdb_engine.connection.begin()
                        transaction_started = True

                        # First, explicitly drop the existing table if it exists
                        try:
                            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
                            print(f"DEBUG: Executing: {drop_sql}")
                            self.duckdb_engine.connection.execute(drop_sql)
                        except Exception as drop_error:
                            print(f"DEBUG: Error dropping table: {drop_error}")
                            # Continue anyway as we're using IF EXISTS

                        # Get the original column names if available
                        original_column_names = None
                        data_chunk = self.table_data.get(table_name)
                        if data_chunk and hasattr(data_chunk, "original_column_names"):
                            original_column_names = data_chunk.original_column_names

                        # Convert arrow table to pandas DataFrame
                        df = table.to_pandas()
                        print(f"DEBUG: DataFrame columns: {df.columns.tolist()}")
                        print(f"DEBUG: Original column names: {original_column_names}")

                        # Register the pandas DataFrame with DuckDB
                        self.duckdb_engine.connection.register(table_name, df)

                        # Create the table with explicit column names
                        if original_column_names:
                            # Use original column names from the CSV header
                            columns_sql = ", ".join(
                                [
                                    f'"{df.columns[i]}" AS "{original_column_names[i]}"'
                                    for i in range(len(df.columns))
                                    if i < len(original_column_names)
                                ]
                            )
                        else:
                            # Use the existing column names
                            columns_sql = ", ".join(
                                [f'"{col}" AS "{col}"' for col in df.columns]
                            )

                        create_sql = f"CREATE TABLE {table_name} AS SELECT {columns_sql} FROM {table_name}"
                        print(f"DEBUG: Creating table with SQL: {create_sql}")
                        self.duckdb_engine.connection.execute(create_sql)

                        # Commit the transaction
                        self.duckdb_engine.connection.commit()
                        print(f"DEBUG: Transaction committed for table '{table_name}'")

                        # Check table structure after creation
                        try:
                            desc_result = self.duckdb_engine.connection.execute(
                                f"DESCRIBE {table_name}"
                            ).fetchdf()
                            print(
                                f"DEBUG: Table '{table_name}' after creation: {desc_result}"
                            )
                        except Exception as e:
                            print(f"DEBUG: Error describing table after creation: {e}")

                        # Execute CHECKPOINT after transaction
                        try:
                            self.duckdb_engine.execute_query("CHECKPOINT")
                            logger.debug(
                                "Checkpoint executed to persist data after commit"
                            )
                        except Exception as e:
                            logger.debug(f"Error performing checkpoint: {e}")
                        # Don't raise an error here - checkpoint may not be supported

                    except Exception as e:
                        # Rollback on error
                        try:
                            if transaction_started:
                                self.duckdb_engine.connection.rollback()
                        except Exception as rollback_error:
                            logger.debug(f"Error during rollback: {rollback_error}")

                        logger.error(
                            f"Error creating persistent table {table_name}: {e}"
                        )
                        print(
                            f"DEBUG: Error creating persistent table {table_name}: {e}"
                        )
                        return {
                            "status": "failed",
                            "error": f"Error creating persistent table: {str(e)}",
                        }

                logger.debug(
                    "Load step %s loaded table '%s' with %d rows",
                    step["id"],
                    table_name,
                    table.num_rows,
                )
                return {"status": "success"}
            else:
                # Create an empty table
                import pyarrow as pa

                # Create an empty Arrow table with the same schema as expected
                empty_table = pa.table({})

                self.table_data[table_name] = DataChunk(empty_table)
                self.step_table_map[step["id"]] = table_name
                if step["id"] not in self.step_output_mapping:
                    self.step_output_mapping[step["id"]] = []
                self.step_output_mapping[step["id"]].append(table_name)

                # Add direct DuckDB CREATE TABLE for empty table
                if (
                    self.duckdb_mode == "persistent"
                    and self.duckdb_engine is not None
                    and self.duckdb_engine.database_path != ":memory:"
                ):
                    try:
                        # Begin transaction for this operation
                        self.duckdb_engine.connection.begin()
                        transaction_started = True

                        # First, drop the table if it exists
                        try:
                            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
                            print(f"DEBUG: Executing: {drop_sql}")
                            self.duckdb_engine.connection.execute(drop_sql)
                        except Exception as drop_error:
                            print(f"DEBUG: Error dropping table: {drop_error}")
                            # Continue anyway as we're using IF EXISTS

                        # Create a persistent table with single column for empty data
                        self.duckdb_engine.execute_query(
                            f"CREATE TABLE {table_name} (dummy INTEGER)"
                        )

                        # Commit the transaction
                        self.duckdb_engine.connection.commit()

                        # Execute CHECKPOINT after transaction
                        try:
                            self.duckdb_engine.execute_query("CHECKPOINT")
                            logger.debug(
                                "Checkpoint executed to persist data after commit"
                            )
                        except Exception as e:
                            logger.debug(f"Error performing checkpoint: {e}")

                        logger.debug("Created empty persistent table %s", table_name)
                    except Exception as e:
                        # Rollback on error
                        try:
                            if transaction_started:
                                self.duckdb_engine.connection.rollback()
                        except Exception as rollback_error:
                            logger.debug(f"Error during rollback: {rollback_error}")

                        logger.error(
                            f"Error creating empty persistent table {table_name}: {e}"
                        )
                        return {
                            "status": "failed",
                            "error": f"Error creating empty table: {str(e)}",
                        }

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
        """Execute a transform step to create a table from a SQL query."""
        sql = step["query"]
        table_name = step["name"]
        # Get the mode parameter with default REPLACE
        mode = step.get("mode", "REPLACE").upper()
        merge_keys = step.get("merge_keys", [])

        logger.debug("Transform step query: %s", sql)
        logger.debug("Target table name: %s", table_name)
        logger.debug("Mode: %s", mode)

        if mode not in ["REPLACE", "APPEND", "MERGE"]:
            return {
                "status": "failed",
                "error": f"Invalid mode: {mode}. Supported modes are REPLACE, APPEND, and MERGE.",
            }

        if mode == "MERGE" and not merge_keys:
            return {
                "status": "failed",
                "error": "MERGE mode requires merge_keys to be specified.",
            }

        # Register all tables with DuckDB
        self._register_tables_with_engine()

        # First, check if this is a direct table UDF call
        # Pattern: SELECT * FROM PYTHON_FUNC("module.function", table_name)
        direct_udf_match = re.match(
            r'^\s*SELECT\s+\*\s+FROM\s+PYTHON_FUNC\s*\(\s*[\'"]([a-zA-Z0-9_\.]+)[\'"]\s*,\s*([^)]+)\)\s*$',
            sql,
            re.IGNORECASE,
        )

        if direct_udf_match:
            logger.debug("Detected direct table UDF call pattern")
            return self._execute_direct_table_udf(
                direct_udf_match, table_name, step["id"]
            )

        # If not a direct UDF call, proceed with regular SQL execution using the mode
        return self._execute_sql_transform(
            sql, table_name, step["id"], mode, merge_keys
        )

    def _execute_direct_table_udf(
        self, match_obj, table_name: str, step_id: str
    ) -> Dict[str, Any]:
        """Execute a table UDF directly without going through SQL."""
        # Extract the UDF name and arguments
        udf_name = match_obj.group(1)
        udf_args = match_obj.group(2).strip()

        logger.debug("Direct UDF execution - UDF: %s, args: %s", udf_name, udf_args)

        try:
            # Fetch the UDF from the manager
            udf_func = self.udf_manager.get_udf(udf_name)
            if not udf_func:
                error_msg = "UDF " + udf_name + " not found"
                logger.debug(error_msg)
                return {"status": "failed", "error": error_msg}

            # Verify this is a table UDF
            udf_type = getattr(udf_func, "_udf_type", "unknown")
            if udf_type != "table":
                error_msg = (
                    "Expected table UDF but got " + str(udf_type) + " UDF: " + udf_name
                )
                logger.debug(error_msg)
                return {"status": "failed", "error": error_msg}

            # Get the input data
            input_data = self._get_input_data_for_direct_udf(udf_args)
            if isinstance(input_data, dict) and input_data.get("status") == "failed":
                return input_data

            # Safely check for shape attribute
            input_shape = "unknown"
            if hasattr(input_data, "shape"):
                input_shape = input_data.shape

            logger.debug(
                "Executing UDF %s directly with input shape %s",
                udf_name,
                input_shape,
            )
            output_schema = getattr(udf_func, "_output_schema", None)
            logger.debug("UDF output schema: %s", output_schema)

            # Execute the UDF directly
            result_df = udf_func(input_data)

            # Safely get result shape
            result_shape = "unknown"
            result_columns = []
            if hasattr(result_df, "shape"):
                result_shape = result_df.shape
            if hasattr(result_df, "columns"):
                result_columns = list(result_df.columns)

            logger.debug(
                "UDF result shape: %s, columns: %s",
                result_shape,
                result_columns,
            )

            # Store the result
            self.table_data[table_name] = DataChunk(result_df)
            self.step_table_map[step_id] = table_name
            if step_id not in self.step_output_mapping:
                self.step_output_mapping[step_id] = []
            self.step_output_mapping[step_id].append(table_name)

            logger.debug("Direct UDF execution successful: %s", udf_name)
            return {"status": "success"}

        except Exception as e:
            import traceback

            error_msg = (
                "Error executing table UDF "
                + udf_name
                + " directly: "
                + str(e)
                + "\n"
                + traceback.format_exc()
            )
            logger.debug("%s", error_msg)
            return {"status": "failed", "error": error_msg}

    def _execute_sql_transform(
        self,
        sql: str,
        table_name: str,
        step_id: str,
        mode: str = "REPLACE",
        merge_keys: List[str] = [],
    ) -> Dict[str, Any]:
        """Execute a transform using SQL through the DuckDB engine."""
        # flake8: noqa: C901
        try:
            if self.duckdb_engine is None:
                return {"status": "failed", "error": "DuckDB engine is not initialized"}

            # Check for UDF references
            udfs = self.udf_manager.get_udfs_for_query(sql)
            if udfs:
                logger.debug(
                    f"DEBUG: Found {len(udfs)} UDFs in query: {list(udfs.keys())}"
                )

                # Register UDFs with the engine
                self.udf_manager.register_udfs_with_engine(
                    self.duckdb_engine, list(udfs.keys())
                )

            # Process the query to update UDF references
            if self.duckdb_engine is not None:  # Recheck to satisfy linter
                processed_sql = self.duckdb_engine.process_query_for_udfs(sql, udfs)
                logger.debug("DEBUG: Processed SQL: %s", processed_sql)
                sql = processed_sql

            # Handle persistence based on mode
            if (
                self.duckdb_engine is not None
                and self.duckdb_engine.database_path != ":memory:"
            ):
                # Use a single transaction for the entire operation
                transaction_started = False
                try:
                    # Begin transaction
                    self.duckdb_engine.connection.begin()
                    transaction_started = True

                    if mode == "REPLACE":
                        # Use CREATE OR REPLACE TABLE for REPLACE mode
                        create_sql = f"CREATE OR REPLACE TABLE {table_name} AS {sql}"
                        self.duckdb_engine.execute_query(create_sql)
                    elif mode == "APPEND":
                        # First check if the table exists
                        table_exists = self.duckdb_engine.table_exists(table_name)

                        if not table_exists:
                            # If table doesn't exist, create it
                            create_sql = f"CREATE TABLE {table_name} AS {sql}"
                            self.duckdb_engine.execute_query(create_sql)
                        else:
                            # If table exists, append to it
                            append_sql = f"INSERT INTO {table_name} {sql}"
                            self.duckdb_engine.execute_query(append_sql)
                    elif mode == "MERGE":
                        # First check if the table exists
                        table_exists = self.duckdb_engine.table_exists(table_name)

                        if not table_exists:
                            # If table doesn't exist, create it
                            create_sql = f"CREATE TABLE {table_name} AS {sql}"
                            self.duckdb_engine.execute_query(create_sql)
                        else:
                            # Create a temporary table for the new data
                            temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
                            self.duckdb_engine.execute_query(
                                f"CREATE TEMP TABLE {temp_table} AS {sql}"
                            )

                            # Build the merge key conditions
                            join_conditions = " AND ".join(
                                [f"target.{key} = source.{key}" for key in merge_keys]
                            )

                            # Build the SET clause for updates
                            # Get column names from the temporary table
                            col_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{temp_table}'"
                            columns_result = self.duckdb_engine.execute_query(
                                col_query
                            ).fetchdf()
                            columns = columns_result["column_name"].tolist()

                            # Remove merge keys from update columns
                            update_columns = [c for c in columns if c not in merge_keys]

                            # Perform the merge operation
                            # DuckDB doesn't have a native MERGE, so we use DELETE + INSERT approach
                            if (
                                update_columns
                            ):  # Only update if there are non-key columns
                                # Delete rows that will be updated
                                delete_sql = f"""
                                DELETE FROM {table_name} 
                                WHERE EXISTS (
                                    SELECT 1 FROM {temp_table} source
                                    WHERE {join_conditions}
                                )
                                """
                                self.duckdb_engine.execute_query(delete_sql)

                            # Insert all rows from source
                            insert_sql = (
                                f"INSERT INTO {table_name} SELECT * FROM {temp_table}"
                            )
                            self.duckdb_engine.execute_query(insert_sql)

                            # Drop the temporary table
                            self.duckdb_engine.execute_query(f"DROP TABLE {temp_table}")

                    # Fetch the result for in-memory tracking
                    result = self.duckdb_engine.execute_query(
                        f"SELECT * FROM {table_name}"
                    ).fetchdf()

                    # Commit the transaction
                    self.duckdb_engine.connection.commit()

                    # IMPORTANT NOTE FOR DEVELOPERS:
                    # DuckDB CHECKPOINT must be executed OUTSIDE of a transaction.
                    # This operation ensures data is properly persisted to disk.
                    # The sequence is critical:
                    # 1. Begin transaction
                    # 2. Perform all data operations
                    # 3. Commit transaction (makes changes logically permanent)
                    # 4. CHECKPOINT (makes changes physically permanent on disk)
                    #
                    # If you try to CHECKPOINT within a transaction, you'll get:
                    # "Cannot CHECKPOINT: the current transaction has transaction local changes"
                    try:
                        self.duckdb_engine.execute_query("CHECKPOINT")
                        logger.debug("Checkpoint executed to persist data after commit")
                    except Exception as e:
                        logger.debug(f"Error performing checkpoint: {e}")
                        # Don't raise an error here - checkpoint may not be supported

                except Exception as e:
                    # Rollback on error
                    if transaction_started:
                        try:
                            self.duckdb_engine.connection.rollback()
                        except Exception as rollback_error:
                            logger.debug(f"Error during rollback: {rollback_error}")
                    logger.error(f"SQL transform error: {e}")
                    raise
            else:
                # In memory mode, just run the query and store result
                result = self.duckdb_engine.execute_query(sql).fetchdf()

            logger.debug(
                "DEBUG: SQL execution successful, result shape: %s",
                result.shape if hasattr(result, "shape") else "unknown",
            )

            # Store the result
            self.table_data[table_name] = DataChunk(result)
            self.step_table_map[step_id] = table_name
            if step_id not in self.step_output_mapping:
                self.step_output_mapping[step_id] = []
            self.step_output_mapping[step_id].append(table_name)

            return {"status": "success"}

        except Exception as e:
            import traceback

            error_msg = (
                "Error executing SQL transform: "
                + str(e)
                + "\n"
                + traceback.format_exc()
            )
            logger.debug("%s", error_msg)
            return {"status": "failed", "error": error_msg}

    def _get_input_data_for_direct_udf(
        self, udf_args: str
    ) -> Union[pd.DataFrame, Dict[str, Any]]:
        """Get input data for a directly executed table UDF."""
        logger.debug("Getting input data for direct UDF args: %s", udf_args)

        # Check if arg is a SQL query
        if udf_args.strip().upper().startswith("SELECT "):
            logger.debug("UDF argument is a SQL query")
            try:
                if self.duckdb_engine is None:
                    return {
                        "status": "failed",
                        "error": "DuckDB engine is not initialized",
                    }
                result = self.duckdb_engine.execute_query(udf_args).fetchdf()
                logger.debug(
                    "SQL subquery result shape: %s",
                    result.shape if hasattr(result, "shape") else "unknown",
                )
                return result
            except Exception as e:
                error_msg = "Error executing SQL subquery for UDF: " + str(e)
                logger.debug("%s", error_msg)
                return {"status": "failed", "error": error_msg}

        # Otherwise, assume it's a table name
        table_name = udf_args.strip().strip("'\"")  # Remove quotes if present
        logger.debug("Looking for table: %s", table_name)

        if table_name in self.table_data:
            result = self.table_data[table_name].pandas_df
            logger.debug(
                "Found table %s, shape: %s",
                table_name,
                result.shape if hasattr(result, "shape") else "unknown",
            )
            return result

        # Table not found, check for partial matches
        for name in self.table_data.keys():
            if table_name.lower() in name.lower():
                result = self.table_data[name].pandas_df
                logger.debug(
                    "Found partial match %s for %s, shape: %s",
                    name,
                    table_name,
                    result.shape if hasattr(result, "shape") else "unknown",
                )
                return result

        # No table found
        error_msg = "Table not found for UDF input: " + table_name
        logger.debug("%s", error_msg)
        logger.debug("Available tables: %s", list(self.table_data.keys()))
        return {"status": "failed", "error": error_msg}

    def _register_tables_with_engine(self) -> None:
        """Register all tables with the DuckDB engine."""
        if self.duckdb_engine is None:
            logger.warning("Cannot register tables: DuckDB engine is not initialized")
            return

        for tbl, chunk in self.table_data.items():
            # Check if the DataChunk contains an Arrow table or pandas DataFrame
            if hasattr(chunk, "arrow_table") and chunk.arrow_table is not None:
                # Don't let register_arrow manage transactions, we'll handle them at a higher level
                self.duckdb_engine.register_arrow(
                    tbl, chunk.arrow_table, manage_transaction=False
                )
            elif hasattr(chunk, "pandas_df") and chunk.pandas_df is not None:
                # Don't let register_table manage transactions, we'll handle them at a higher level
                self.duckdb_engine.register_table(
                    tbl, chunk.pandas_df, manage_transaction=False
                )
            else:
                logger.warning(
                    f"Could not register table {tbl}: no valid data format found"
                )

    def _is_direct_table_udf_call(self, sql: str) -> bool:
        """Check if the SQL is a direct table UDF call.

        This checks for SQL patterns like:
        SELECT * FROM PYTHON_FUNC("module.function", table_name)
        """
        logger.debug(f"DEBUG: Checking if direct table UDF call: {sql}")
        direct_table_udf_pattern = r'^\s*SELECT\s+\*\s+FROM\s+PYTHON_FUNC\(\s*[\'"]([a-zA-Z0-9_\.]+)[\'"]\s*,\s*(.*?)\)\s*$'
        result = bool(re.match(direct_table_udf_pattern, sql, re.IGNORECASE))
        logger.debug(f"DEBUG: Is direct table UDF call: {result}")
        return result

    def _extract_table_udf_info(self, sql: str) -> tuple[str, str]:
        """Extract UDF name and arguments from a direct table UDF call."""
        logger.debug(f"DEBUG: Extracting table UDF info from: {sql}")
        direct_table_udf_pattern = r'^\s*SELECT\s+\*\s+FROM\s+PYTHON_FUNC\(\s*[\'"]([a-zA-Z0-9_\.]+)[\'"]\s*,\s*(.*?)\)\s*$'
        match = re.match(direct_table_udf_pattern, sql, re.IGNORECASE)
        if not match:
            logger.debug("DEBUG: No match found for table UDF pattern")
            return "", ""
        udf_name = match.group(1)
        args = match.group(2).strip()
        logger.debug("Extracted UDF name: %s, args: %s", udf_name, args)
        return udf_name, args

    def _execute_and_store_result(
        self, sql: str, table_name: str, step_id: str
    ) -> Dict[str, Any]:
        """Execute the SQL query and store the result."""
        try:
            if self.duckdb_engine is None:
                return {"status": "failed", "error": "DuckDB engine is not initialized"}

            result = self.duckdb_engine.execute_query(sql).fetchdf()
            self.table_data[table_name] = DataChunk(result)
            self.step_table_map[step_id] = table_name
            if step_id not in self.step_output_mapping:
                self.step_output_mapping[step_id] = []
            self.step_output_mapping[step_id].append(table_name)
            logger.debug(
                "Transform step %s created table '%s' with %d rows",
                step_id,
                table_name,
                len(result),
            )
            return {"status": "success"}
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return {"status": "failed", "error": f"Error executing query: {str(e)}"}

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

                # Get variables from profile
                profile_vars = (
                    self.profile.get("variables", {})
                    if isinstance(self.profile, dict)
                    else {}
                )
                if var_name in profile_vars:
                    return str(profile_vars[var_name])

                return default
            else:
                var_name = var_expr.strip()
                # Get variables from profile
                profile_vars = (
                    self.profile.get("variables", {})
                    if isinstance(self.profile, dict)
                    else {}
                )
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
            # Get the destination URI and options from the step
            # These should already have variables substituted by _execute_export
            destination = step["query"]["destination_uri"]
            options = step["query"].get("options", {})

            # Log what we're exporting
            logger.info(
                f"Exporting to: {destination} with connector type: {connector_type}"
            )

            # Create parent directories for the destination if needed
            if connector_type.upper() == "CSV" and not destination.startswith("s3://"):
                # Only create directories for local file paths (not S3 URIs)
                dirname = os.path.dirname(destination)
                if dirname:
                    os.makedirs(dirname, exist_ok=True)

            # Export the data
            self.connector_engine.export_data(
                data=data_chunk,
                destination=destination,
                connector_type=connector_type,
                options=options,
            )

            logger.info(f"Export successful: {step['id']} to {destination}")
            return {"status": "success"}
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            return {"status": "failed", "error": f"Export failed: {str(e)}"}

    def _execute_export(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an export step."""
        logger.debug("Starting export step %s", step["id"])
        logger.debug("EXPORT STEP DETAILS: %s", json.dumps(step, indent=2))
        try:
            # First substitute variables in destination_uri
            if "query" in step and "destination_uri" in step["query"]:
                original_destination = step["query"]["destination_uri"]
                substituted_destination = self._substitute_variables(
                    original_destination
                )
                step["query"]["destination_uri"] = substituted_destination

            # Also substitute variables in options
            if "query" in step and "options" in step["query"]:
                original_options = step["query"]["options"]
                substituted_options = self._substitute_variables_in_dict(
                    original_options
                )
                step["query"]["options"] = substituted_options

            source_table, data_chunk = self._resolve_export_source(step)
            if data_chunk is None:
                error_msg = f"No data found for export step: {step['id']}"
                return self._report_export_error(step, error_msg)
            logger.debug(
                "Exporting %d rows of data from table '%s'",
                len(data_chunk.pandas_df),
                source_table,
            )
            return self._invoke_export(step, data_chunk, source_table)
        except Exception as e:
            logger.debug("Export step %s error: %s", step["id"], str(e))
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

    def _execute_include(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an INCLUDE step to import Python UDFs or external SQLFlow files.

        Args:
            step: INCLUDE step definition

        Returns:
            Dict containing execution results
        """
        file_path = step["file_path"]
        alias = step["alias"]

        logger.info(f"Executing INCLUDE step for file: {file_path}")

        # Get the absolute path for the included file
        abs_file_path = os.path.join(self.project.project_dir, file_path)

        if not os.path.exists(abs_file_path):
            error_msg = f"Included file not found: {file_path}"
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

        # Check file extension to determine handling
        _, ext = os.path.splitext(file_path)

        if ext.lower() == ".py":
            # Python file - import UDFs
            try:
                logger.info(f"Including Python file for UDFs: {file_path}")

                # Load the module from file path
                spec = importlib.util.spec_from_file_location(alias, abs_file_path)
                if not spec or not spec.loader:
                    return {
                        "status": "failed",
                        "error": f"Could not load Python module: {file_path}",
                    }

                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Register the module with the UDF manager
                # Here we rely on UDF decorators to mark functions in the module
                # The UDF manager will discover UDFs during execution
                logger.info(f"Reloading UDFs after including {file_path}")
                self.discover_udfs()

                # Log all discovered UDFs after including this file
                logger.info(
                    f"Current UDFs after include: {list(self.discovered_udfs.keys())}"
                )
                logger.info(
                    f"Now have {len(self.discovered_udfs)} total UDFs available"
                )

                return {"status": "success"}

            except Exception as e:
                error_msg = f"Error including Python file {file_path}: {str(e)}"
                logger.error(error_msg)
                return {"status": "failed", "error": error_msg}

        elif ext.lower() == ".sf":
            # SQLFlow file - to be implemented
            # This would require parsing the included SQLFlow file and executing its steps
            # For now, we just log a message
            logger.warning(
                f"Including SQLFlow files not fully implemented yet: {file_path}"
            )
            return {
                "status": "success",
                "warning": "Including SQLFlow files not fully implemented",
            }

        else:
            error_msg = f"Unsupported file type for INCLUDE: {ext}"
            logger.error(error_msg)
            return {"status": "failed", "error": error_msg}

    def process_udfs_in_query(self, query: str) -> str:
        """Process UDFs in a query, registering them with the engine and replacing references.

        Args:
            query: SQL query that might contain UDF references

        Returns:
            Processed query with UDF references replaced with engine-specific syntax

        Raises:
            UDFError: If processing UDFs fails
        """
        try:
            # Extract UDF references from query
            udfs_for_query = self.udf_manager.get_udfs_for_query(query)
            logger.debug(f"DEBUG: UDFs for query: {list(udfs_for_query.keys())}")

            if not udfs_for_query:
                return query

            # If the engine is not initialized, just return the original query
            if self.duckdb_engine is None:
                logger.warning("Cannot process UDFs: DuckDB engine is not initialized")
                return query

            # For each UDF referenced in the query
            for udf_name, udf in udfs_for_query.items():
                logger.debug(
                    "DEBUG: Checking UDF %s before engine registration", udf_name
                )
                logger.debug(
                    "UDF _output_schema: %s", getattr(udf, "_output_schema", None)
                )
                logger.debug(
                    "UDF _infer_schema: %s", getattr(udf, "_infer_schema", False)
                )
                logger.debug("UDF _udf_type: %s", getattr(udf, "_udf_type", "unknown"))

            # Register UDFs with engine
            self.udf_manager.register_udfs_with_engine(
                self.duckdb_engine, list(udfs_for_query.keys())
            )

            # Create SQL query with UDF references updated
            processed_query = self.duckdb_engine.process_query_for_udfs(
                query, udfs_for_query
            )

            return processed_query
        except Exception as e:
            raise UDFError(f"Error processing UDFs in query: {str(e)}") from e

    def _handle_source_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a source definition step.

        Args:
            step: The source definition step to execute

        Returns:
            Result of the execution
        """
        return self._execute_source_definition(step)

    def _handle_load_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a load step.

        Args:
            step: The load step to execute

        Returns:
            Result of the execution
        """
        return self._execute_load(step)

    def _handle_transform_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a transform step.

        Args:
            step: The transform step to execute

        Returns:
            Result of the execution
        """
        return self._execute_transform(step)

    def _handle_export_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Handle an export step.

        Args:
            step: The export step to execute

        Returns:
            Result of the execution
        """
        return self._execute_export(step)

    def _interpolate_variables(self, text: str, variables: Dict[str, Any]) -> str:
        """Replace variable placeholders in SQL with their values.

        Args:
            text: SQL query with variable placeholders
            variables: Dictionary of variable names and values

        Returns:
            SQL query with variables replaced
        """
        if not variables:
            return text

        # Debug output of variables
        print(f"DEBUG: Variables for interpolation: {variables}")

        result = text
        for var_name, var_value in variables.items():
            # Convert Python values to SQL-compatible values
            if var_value is None:
                sql_value = "NULL"
            elif isinstance(var_value, bool):
                sql_value = str(var_value).lower()
            elif isinstance(var_value, (int, float)):
                sql_value = str(var_value)
            elif isinstance(var_value, datetime.datetime):
                sql_value = f"'{var_value.strftime('%Y-%m-%d %H:%M:%S')}'"
            elif isinstance(var_value, datetime.date):
                sql_value = f"'{var_value.strftime('%Y-%m-%d')}'"
            else:
                # Strings and other types - quote them
                sql_value = f"'{str(var_value)}'"

            # Replace ${var_name} with the value
            pattern = rf"\$\{{{var_name}\}}"
            replacement = sql_value
            old_result = result
            result = re.sub(pattern, replacement, result)

            # Check if replacement was made
            if old_result != result:
                print(f"DEBUG: Replaced ${{{var_name}}} with {sql_value}")

        # Debug output of final SQL
        print(f"DEBUG: Final interpolated text: {result}")

        return result
