"""Artifact Manager for SQLFlow.

This module provides classes for managing SQLFlow artifacts such as compiled plans,
execution logs, and generated SQL. It follows a design similar to dbt's artifact
structure with compiled/ and run/ directories.
"""

import json
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class ArtifactManager:
    """Manages the creation and retrieval of SQLFlow artifacts.

    This class coordinates the storage and retrieval of compilation
    and execution artifacts in the target directory structure.

    Args:
        project_dir: Root directory of the SQLFlow project.
    """

    def __init__(self, project_dir: str):
        """Initialize the artifact manager.

        Args:
            project_dir: Root directory of the SQLFlow project.
        """
        self.project_dir = project_dir
        self.setup_directories()

    def setup_directories(self) -> List[str]:
        """Create standard artifact directories.

        Returns:
            List of created directory paths.
        """
        directories = [
            os.path.join(self.project_dir, "target", "compiled"),
            os.path.join(self.project_dir, "target", "run"),
            os.path.join(self.project_dir, "target", "logs"),
        ]
        for directory in directories:
            logger.debug(f"Creating directory: {directory}")
            os.makedirs(directory, exist_ok=True)
        return directories

    def get_compiled_path(self, pipeline_name: str) -> str:
        """Get path to the compiled plan file.

        Args:
            pipeline_name: Name of the pipeline.

        Returns:
            Path to the compiled plan file.
        """
        return os.path.join(
            self.project_dir, "target", "compiled", f"{pipeline_name}.json"
        )

    def get_run_dir(self, pipeline_name: str) -> str:
        """Get path to the run directory for a pipeline.

        Args:
            pipeline_name: Name of the pipeline.

        Returns:
            Path to the run directory.
        """
        run_dir = os.path.join(self.project_dir, "target", "run", pipeline_name)
        os.makedirs(run_dir, exist_ok=True)
        return run_dir

    def clean_run_dir(self, pipeline_name: str) -> None:
        """Clean (delete and recreate) the run directory for a pipeline.

        Args:
            pipeline_name: Name of the pipeline.
        """
        run_dir = os.path.join(self.project_dir, "target", "run", pipeline_name)
        if os.path.exists(run_dir):
            import shutil

            try:
                shutil.rmtree(run_dir)
                logger.info(f"Cleaned run directory: {run_dir}")
            except Exception as e:
                logger.error(f"Error cleaning run directory {run_dir}: {e}")
        # Recreate the directory after cleaning
        os.makedirs(run_dir, exist_ok=True)

    def save_compiled_plan(self, pipeline_name: str, plan: Dict[str, Any]) -> str:
        """Save the compiled plan to disk.

        Args:
            pipeline_name: Name of the pipeline.
            plan: Execution plan dictionary.

        Returns:
            Path to the saved plan file.
        """
        path = self.get_compiled_path(pipeline_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(plan, f, indent=2)
        return path

    def load_compiled_plan(self, pipeline_name: str) -> Dict[str, Any]:
        """Load a compiled plan from disk.

        Args:
            pipeline_name: Name of the pipeline.

        Returns:
            Execution plan dictionary.

        Raises:
            FileNotFoundError: If compiled plan doesn't exist.
        """
        path = self.get_compiled_path(pipeline_name)
        with open(path, "r") as f:
            return json.load(f)

    def initialize_execution(
        self, pipeline_name: str, variables: Dict[str, Any], profile: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Initialize execution tracking for a pipeline.

        Args:
            pipeline_name: Name of the pipeline.
            variables: Variables used in execution.
            profile: Profile name used.

        Returns:
            Tuple of (execution_id, metadata_dict).
        """
        run_dir = self.get_run_dir(pipeline_name)
        execution_id = (
            f"exec-{datetime.now().strftime('%Y%m%d%H%M')}-{uuid.uuid4().hex[:8]}"
        )

        metadata = {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "duration_ms": None,
            "status": "running",
            "variables": variables,
            "profile": profile,
            "operations_summary": {"total": 0, "success": 0, "failed": 0, "skipped": 0},
            "operation_types": {},
        }

        # Save initial metadata
        metadata_path = os.path.join(run_dir, "metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return execution_id, metadata

    def record_operation_start(
        self,
        pipeline_name: str,
        operation_id: str,
        operation_type: str,
        sql: str,
        operation_details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record the start of an operation execution.

        Args:
            pipeline_name: Name of the pipeline.
            operation_id: ID of the operation.
            operation_type: Type of the operation.
            sql: SQL to be executed.
            operation_details: Additional details about the operation.

        Returns:
            Operation metadata dictionary.
        """
        run_dir = self.get_run_dir(pipeline_name)

        # Generate a more descriptive filename
        file_name_base = self._generate_descriptive_name(
            operation_id, operation_type, operation_details
        )

        # Save the SQL
        sql_path = os.path.join(run_dir, f"{file_name_base}.sql")
        with open(sql_path, "w") as f:
            f.write(sql)

        # Create operation metadata
        op_metadata = {
            "operation_id": operation_id,
            "file_name_base": file_name_base,
            "operation_type": operation_type,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "duration_ms": None,
            "rows_processed": None,
            "rows_affected": None,
            "rows_rejected": None,
            "database_info": None,
            "error_details": None,
        }

        # Save initial operation metadata
        op_path = os.path.join(run_dir, f"{file_name_base}.json")
        with open(op_path, "w") as f:
            json.dump(op_metadata, f, indent=2)

        # Update pipeline metadata to count operations
        metadata_path = os.path.join(run_dir, "metadata.json")
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        # Add operation mapping to metadata for lookup
        if "operation_mapping" not in metadata:
            metadata["operation_mapping"] = {}
        metadata["operation_mapping"][operation_id] = file_name_base

        metadata["operations_summary"]["total"] += 1
        if operation_type not in metadata["operation_types"]:
            metadata["operation_types"][operation_type] = 0
        metadata["operation_types"][operation_type] += 1

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return op_metadata

    # --- Helper methods for _generate_descriptive_name --- #
    def _extract_name_for_source(self, details: Dict[str, Any]) -> Optional[str]:
        return f"source_{details['name']}" if "name" in details else None

    def _extract_name_for_load(self, details: Dict[str, Any]) -> Optional[str]:
        return f"load_{details['name']}" if "name" in details else None

    def _extract_name_for_transform(self, details: Dict[str, Any]) -> Optional[str]:
        return f"transform_{details['name']}" if "name" in details else None

    def _extract_name_for_export(self, details: Dict[str, Any]) -> Optional[str]:
        if "query" in details and isinstance(details["query"], dict):
            query_details = details["query"]
            uri = query_details.get("destination_uri", "")
            if uri:
                parts = uri.split("/")
                if parts and parts[-1]:
                    base_name = os.path.splitext(parts[-1])[0]
                    clean_name = base_name.replace("${", "").replace("}", "")
                    if clean_name:  # Ensure not empty after cleaning placeholders
                        return f"export_{clean_name}"

            if "sql_query" in query_details:
                sql = query_details["sql_query"]
                match = re.search(
                    r"FROM\s+([a-zA-Z0-9_.]+)|JOIN\s+([a-zA-Z0-9_.]+)",
                    sql,
                    re.IGNORECASE,
                )
                if match:
                    table_name = match.group(1) or match.group(2)
                    if table_name:
                        return f"export_{table_name.split('.')[-1]}"
        return None

    # --- End of helper methods --- #

    _NAME_EXTRACTORS: Dict[str, Callable[[Any, Dict[str, Any]], Optional[str]]] = {
        "source_definition": _extract_name_for_source,
        "load": _extract_name_for_load,
        "transform": _extract_name_for_transform,
        "export": _extract_name_for_export,
    }

    def _generate_descriptive_name(
        self,
        operation_id: str,
        operation_type: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate a descriptive file name for an operation.

        Args:
            operation_id: Original operation ID.
            operation_type: Type of operation.
            details: Additional operation details.

        Returns:
            A descriptive file name base (without extension).
        """
        descriptive_part: Optional[str] = None
        if details:
            extractor = self._NAME_EXTRACTORS.get(operation_type)
            if extractor:
                # Type hint for extractor implies it's a method bound to an instance of this class.
                # So, it should be called as extractor(self, details)
                # if defined as methods, or just extractor(details) if they are static/standalone.
                # Based on how they are defined (e.g. self._extract_name_for_source),
                # they are instance methods, so self is passed implicitly when called via self.extractor
                # but here extractor is a direct reference to the method *object*.
                # Thus, self needs to be passed explicitly to the method object.
                descriptive_part = extractor(self, details)

        # Use descriptive part if available and valid, otherwise fallback to operation_id
        base_name_candidate = descriptive_part if descriptive_part else operation_id

        # Sanitize the chosen name candidate
        # Replace common invalid characters, collapse multiple underscores
        sanitized_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", base_name_candidate)
        sanitized_name = re.sub(
            r"_{2,}", "_", sanitized_name
        )  # Collapse multiple underscores
        sanitized_name = sanitized_name.strip(
            "_"
        )  # Remove leading/trailing underscores

        return (
            sanitized_name or f"{operation_type}_fallback_{uuid.uuid4().hex[:4]}"
        )  # Ensure not empty, add type for clarity on fallback

    def _get_operation_file_name(self, pipeline_name: str, operation_id: str) -> str:
        """Get the file name for an operation from metadata or fallback to operation_id.

        Args:
            pipeline_name: Name of the pipeline
            operation_id: ID of the operation

        Returns:
            File name base for the operation
        """
        run_dir = self.get_run_dir(pipeline_name)
        metadata_path = os.path.join(run_dir, "metadata.json")
        try:
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            operation_mapping = metadata.get("operation_mapping", {})
            return operation_mapping.get(operation_id, operation_id)
        except Exception:
            return operation_id

    def _load_operation_metadata(
        self, run_dir: str, file_name_base: str
    ) -> Dict[str, Any]:
        """Load existing operation metadata or create new metadata.

        Args:
            run_dir: Directory containing operation files
            file_name_base: Base name for operation files

        Returns:
            Operation metadata dictionary
        """
        op_path = os.path.join(run_dir, f"{file_name_base}.json")
        try:
            with open(op_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {
                "operation_id": file_name_base,
                "file_name_base": file_name_base,
                "started_at": datetime.now().isoformat(),
            }

    def _process_operation_status(
        self,
        status: Union[bool, str, Dict[str, Any]],
        results: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Process operation status and results.

        Args:
            status: Operation status (bool, str, or dict)
            results: Optional operation results

        Returns:
            Tuple of (success: bool, error_details: Optional[str], result_values: Dict[str, Any])
        """
        success = False
        error_details = None
        result_values = {}

        if isinstance(status, bool):
            success = status
        elif isinstance(status, str):
            success = status == "success"
            if not success:
                error_details = status
        elif isinstance(status, dict):
            results = status
            success = results.get("status") == "success"
            if not success:
                error_details = results.get("error", "Unknown error")
            result_values = results

        # If we have separate results dict and didn't use status as results
        if results is not None and status is not results:
            result_values = results
            if "status" in results and not isinstance(status, bool):
                success = results["status"] == "success"
            if not success and "error" in results:
                error_details = results["error"]

        return success, error_details, result_values

    def _update_operation_metadata(
        self,
        op_metadata: Dict[str, Any],
        success: bool,
        error_details: Optional[str],
        result_values: Dict[str, Any],
        started_at: datetime,
    ) -> Dict[str, Any]:
        """Update operation metadata with completion information.

        Args:
            op_metadata: Existing operation metadata
            success: Whether operation succeeded
            error_details: Error details if operation failed
            result_values: Operation result values
            started_at: Operation start time

        Returns:
            Updated operation metadata
        """
        now = datetime.now()
        duration_ms = int((now - started_at).total_seconds() * 1000)

        op_metadata.update(
            {
                "status": "success" if success else "failed",
                "completed_at": now.isoformat(),
                "duration_ms": duration_ms,
                "rows_processed": result_values.get("rows_processed", 0),
                "rows_affected": result_values.get("rows_affected", 0),
                "rows_rejected": result_values.get("rows_rejected", 0),
                "database_info": result_values.get("database_info", {}),
                "error_details": (
                    None if success else (error_details or "Unknown error")
                ),
            }
        )

        return op_metadata

    def _update_pipeline_metadata(self, pipeline_name: str, success: bool) -> None:
        """Update pipeline metadata with operation completion information.

        Args:
            pipeline_name: Name of the pipeline
            success: Whether operation succeeded
        """
        run_dir = self.get_run_dir(pipeline_name)
        metadata_path = os.path.join(run_dir, "metadata.json")

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        if success:
            metadata["operations_summary"]["success"] += 1
        else:
            metadata["operations_summary"]["failed"] += 1

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def record_operation_completion(
        self,
        pipeline_name: str,
        operation_id: str,
        operation_type: str,
        status: Union[bool, str, Dict[str, Any]],
        results: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record completion of an operation.

        Args:
            pipeline_name: Name of the pipeline
            operation_id: ID of the operation
            operation_type: Type of the operation
            status: Either a boolean (success/failure), string status, or a results dict
            results: Optional results of the operation (if status is not a dict)

        Returns:
            Updated operation metadata
        """
        run_dir = self.get_run_dir(pipeline_name)
        file_name_base = self._get_operation_file_name(pipeline_name, operation_id)
        op_metadata = self._load_operation_metadata(run_dir, file_name_base)

        # Process operation status and results
        success, error_details, result_values = self._process_operation_status(
            status, results
        )

        # Update operation metadata
        started_at = datetime.fromisoformat(
            op_metadata.get("started_at", datetime.now().isoformat())
        )
        op_metadata = self._update_operation_metadata(
            op_metadata, success, error_details, result_values, started_at
        )

        # Save updated operation metadata
        op_path = os.path.join(run_dir, f"{file_name_base}.json")
        with open(op_path, "w") as f:
            json.dump(op_metadata, f, indent=2)

        # Update pipeline metadata
        self._update_pipeline_metadata(pipeline_name, success)

        return op_metadata

    def finalize_execution(self, pipeline_name: str, success: bool) -> Dict[str, Any]:
        """Finalize the execution tracking.

        Args:
            pipeline_name: Name of the pipeline.
            success: Whether pipeline execution succeeded.

        Returns:
            Final pipeline execution metadata.
        """
        run_dir = self.get_run_dir(pipeline_name)
        metadata_path = os.path.join(run_dir, "metadata.json")

        # Load existing metadata
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        # Update with completion information
        now = datetime.now()
        started = datetime.fromisoformat(metadata["started_at"])
        duration_ms = int((now - started).total_seconds() * 1000)

        metadata.update(
            {
                "status": "success" if success else "failed",
                "completed_at": now.isoformat(),
                "duration_ms": duration_ms,
            }
        )

        # Save updated metadata
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return metadata
