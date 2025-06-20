"""Export Step Handler for V2 Executor.

This module implements the ExportStepHandler that handles data export operations
from SQL engine tables to various destinations with comprehensive observability.
"""

import os
import time
from datetime import datetime
from typing import Any, Dict

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep, ExportStep
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ExportStepHandler(StepHandler):
    """
    Handles the execution of data export operations.

    This handler is responsible for:
    - Extracting data from SQL engine tables or queries
    - Creating appropriate destination connectors
    - Exporting data to various formats (CSV, Parquet, JSON, etc.)
    - Supporting S3, local files, and other destination types
    - Providing detailed performance metrics and observability
    - Handling variable substitution in destination paths

    The handler follows the Single Responsibility Principle by focusing solely
    on data export logic while delegating data extraction to the SQL engine
    and destination writing to connector implementations.
    """

    STEP_TYPE = "export"

    @observed_execution("export")
    def execute(self, step: BaseStep, context: ExecutionContext) -> StepExecutionResult:
        """
        Execute a data export operation with comprehensive observability.

        Args:
            step: ExportStep containing source, target, and configuration
            context: ExecutionContext with shared services

        Returns:
            StepExecutionResult with detailed execution metrics
        """
        # Type check and cast to ExportStep
        if not isinstance(step, ExportStep):
            raise ValueError(
                f"ExportStepHandler can only handle ExportStep, got {type(step)}"
            )

        export_step = step  # Now we know it's an ExportStep
        start_time = datetime.utcnow()
        logger.debug(
            f"Executing ExportStep {export_step.id}: {export_step.source_table} -> {export_step.target}"
        )

        try:
            # Step 1: Validate step configuration
            self._validate_export_step(export_step)

            # Step 2: Resolve source data (table or SQL query)
            source_sql = self._resolve_source_data(export_step, context)

            # Step 3: Prepare destination configuration with variable substitution
            destination_config = self._prepare_destination_config(export_step, context)

            # Step 4: Extract data from source
            extraction_metrics = self._extract_data_with_metrics(source_sql, context)

            # Step 5: Execute export operation
            export_metrics = self._execute_export_operation(
                extraction_metrics["data"], destination_config, context
            )

            # Step 6: Provide user feedback
            self._provide_user_feedback(export_step, destination_config["target"])

            end_time = datetime.utcnow()

            # Combine metrics
            combined_metrics = {**extraction_metrics, **export_metrics}

            return StepExecutionResult.success(
                step_id=export_step.id,
                step_type="export",
                start_time=start_time,
                end_time=end_time,
                rows_affected=combined_metrics.get("rows_exported"),
                performance_metrics=combined_metrics,
                data_lineage={
                    "source_table": export_step.source_table,
                    "target": destination_config["target"],
                    "export_format": export_step.export_format,
                    "compression": export_step.compression,
                },
            )

        except Exception as e:
            # Use common error handling pattern
            return self._handle_execution_error(export_step, start_time, e)

    def _validate_export_step(self, step: ExportStep) -> None:
        """
        Validate ExportStep configuration before execution.

        Args:
            step: ExportStep to validate

        Raises:
            ValueError: If step configuration is invalid
        """
        if not step.source_table or not step.source_table.strip():
            raise ValueError(f"ExportStep {step.id}: source_table cannot be empty")

        if not step.target or not step.target.strip():
            raise ValueError(f"ExportStep {step.id}: target cannot be empty")

    def _resolve_source_data(self, step: ExportStep, context: ExecutionContext) -> str:
        """
        Resolve source data from table name or SQL query.

        Args:
            step: ExportStep with source configuration
            context: ExecutionContext with SQL engine

        Returns:
            SQL query to extract the source data
        """
        source = step.source_table.strip()

        # If source contains SQL keywords, treat it as a query
        if any(keyword in source.upper() for keyword in ["SELECT", "WITH", "FROM"]):
            logger.debug(f"Treating source as SQL query: {source}")
            return source
        else:
            # Treat as table name - generate SELECT * query
            table_sql = f"SELECT * FROM {source}"
            logger.debug(f"Treating source as table name, generated query: {table_sql}")
            return table_sql

    def _prepare_destination_config(
        self, step: ExportStep, context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Prepare destination configuration with variable substitution.

        Args:
            step: ExportStep with destination configuration
            context: ExecutionContext with variable manager

        Returns:
            Dictionary containing resolved destination configuration
        """
        # Perform variable substitution on target path
        resolved_target = step.target
        if hasattr(context, "variable_manager"):
            try:
                # Use getattr with default for cleaner method resolution
                substitute_method = getattr(
                    context.variable_manager,
                    "substitute",
                    getattr(context.variable_manager, "substitute_variables", None),
                )
                if substitute_method:
                    resolved_target = substitute_method(step.target)
                    if resolved_target != step.target:
                        logger.debug(
                            f"Variable substitution in target: {step.target} -> {resolved_target}"
                        )
            except Exception as e:
                logger.warning(f"Variable substitution failed for target: {e}")

        # Prepare destination configuration
        config = {
            "target": resolved_target,
            "format": step.export_format,
            "options": step.options.copy() if step.options else {},
        }

        # Add compression if specified
        if step.compression:
            config["options"]["compression"] = step.compression

        # Add partitioning if specified
        if step.partitioning:
            config["options"]["partitioning"] = step.partitioning

        return config

    def _extract_data_with_metrics(
        self, sql: str, context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Extract data from source and collect performance metrics.

        Args:
            sql: SQL query to extract data
            context: ExecutionContext with SQL engine

        Returns:
            Dictionary containing extracted data and metrics
        """
        extraction_start_time = time.monotonic()

        try:
            # Execute SQL query to get data
            result = context.sql_engine.execute_query(sql)

            # Extract both data and column information
            rows = result.fetchall()
            columns = (
                [desc[0] for desc in result.description]
                if hasattr(result, "description") and result.description
                else []
            )

            # Convert to DataFrame immediately with proper column names
            import pandas as pd

            if rows and columns:
                # Use dictionary approach for type safety
                data_dict = {
                    col: [row[i] for row in rows] for i, col in enumerate(columns)
                }
                data = pd.DataFrame(data_dict)
            elif rows:
                # Fallback if no column info available
                data = pd.DataFrame(rows)
            else:
                # Empty result
                data = pd.DataFrame()

            extraction_time = (time.monotonic() - extraction_start_time) * 1000

            metrics = {
                "data": data,
                "data_extraction_time_ms": extraction_time,
                "rows_extracted": len(data) if not data.empty else 0,
                "source_sql": sql,
                "columns": columns,
            }

            logger.debug(
                f"Data extraction completed in {extraction_time:.2f}ms, {len(data) if not data.empty else 0} rows, {len(columns)} columns"
            )
            return metrics

        except Exception as e:
            extraction_time = (time.monotonic() - extraction_start_time) * 1000
            logger.error(f"Data extraction failed after {extraction_time:.2f}ms: {e}")
            raise

    def _execute_export_operation(
        self, data: Any, destination_config: Dict[str, Any], context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Execute the actual export operation using destination connector.

        Args:
            data: Data to export
            destination_config: Destination configuration
            context: ExecutionContext with connector registry

        Returns:
            Dictionary containing export metrics
        """
        export_start_time = time.monotonic()

        try:
            target = destination_config["target"]

            # Detect connector type from target
            connector_type = self._detect_connector_type_from_target(target)

            # Prepare connector options with target path (defensive programming)
            options = destination_config.get("options", {})
            if not isinstance(options, dict):
                options = {}  # Reset to empty dict if not a dictionary

            connector_options = options.copy()
            connector_options["path"] = (
                target  # Key fix: provide path parameter for connectors
            )

            # Create destination connector with proper type and options
            connector = context.connector_registry.create_destination_connector(
                connector_type, connector_options
            )

            # Write data to destination - use proper connector interface
            # Most destination connectors expect (data, options) not (data, format)
            write_options = (
                connector_options.copy() if isinstance(connector_options, dict) else {}
            )
            if "format" in destination_config and isinstance(write_options, dict):
                write_options["format"] = destination_config["format"]

            # Remove path from write_options since connectors handle path separately
            # and pandas methods don't accept path as a parameter
            if isinstance(write_options, dict):
                write_options.pop("path", None)
                # Remove format from write_options since pandas to_csv doesn't accept it
                write_options.pop("format", None)

            # Data should already be a DataFrame from extraction
            rows_exported = self._write_data_to_connector(
                data, connector, write_options
            )

            export_time = (time.monotonic() - export_start_time) * 1000

            metrics = {
                "export_time_ms": export_time,
                "rows_exported": rows_exported,
                "export_format": destination_config["format"],
                "destination": target,
                "connector_type": connector_type,
            }

            logger.debug(f"Export operation completed in {export_time:.2f}ms")
            return metrics

        except Exception as e:
            export_time = (time.monotonic() - export_start_time) * 1000
            logger.error(f"Export operation failed after {export_time:.2f}ms: {e}")
            raise

    def _write_data_to_connector(
        self, data: Any, connector: Any, write_options: Dict[str, Any]
    ) -> int:
        """
        Write data to connector, handling different data types gracefully.

        Args:
            data: Data to write (should be DataFrame)
            connector: Destination connector
            write_options: Options for writing

        Returns:
            Number of rows exported
        """
        import pandas as pd

        # Handle DataFrame data
        if isinstance(data, pd.DataFrame):
            if not data.empty:
                connector.write(data, options=write_options)
                return len(data)
            else:
                logger.info("No data to export - empty DataFrame")
                return 0

        # Handle other iterable data types
        elif hasattr(data, "__len__") and len(data) > 0:
            connector.write(data, options=write_options)
            return len(data)

        else:
            logger.info("No data to export - skipping write operation")
            return 0

    def _detect_connector_type_from_target(self, target: str) -> str:
        """
        Detect connector type from target path.

        Args:
            target: Target path or URI

        Returns:
            Connector type string
        """
        # Handle S3 URIs
        if target.startswith("s3://"):
            return "s3"

        # Handle file paths by extension
        _, ext = os.path.splitext(target)
        ext = ext.lower()

        if ext == ".csv":
            return "csv"
        elif ext in [".parquet", ".pq"]:
            return "parquet"
        elif ext in [".json", ".jsonl"]:
            return "json"
        else:
            # Default to CSV for unknown extensions
            return "csv"

    def _provide_user_feedback(self, step: ExportStep, resolved_target: str) -> None:
        """
        Provide user-friendly feedback about the export operation.

        Args:
            step: ExportStep that was executed
            resolved_target: The actual resolved target path after variable substitution
        """
        print(f"📤 Exported {step.source_table} to {resolved_target}")
        logger.debug(f"Export step {step.id} completed successfully")
