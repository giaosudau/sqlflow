"""Load Step Handler for V2 Executor.

This module implements the LoadStepHandler that handles data loading operations
from various sources into SQL tables. It provides comprehensive support for:
- Multiple load modes (REPLACE, APPEND, UPSERT)
- Incremental loading with watermarks
- Profile-based source configuration
- Rich observability and performance metrics
- Schema validation and evolution
"""

import time
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import LoadStep
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class LoadStepHandler(StepHandler):
    """
    Handles the execution of LoadStep operations with comprehensive observability.

    This handler is responsible for:
    - Loading data from various sources (files, APIs, databases)
    - Supporting different load modes (REPLACE, APPEND, UPSERT)
    - Managing incremental loading with watermarks
    - Providing detailed performance metrics and schema tracking
    - Ensuring data consistency and error recovery

    The handler follows the Single Responsibility Principle by focusing solely
    on load operations while delegating source connectivity to the connector
    registry and SQL operations to the SQL engine.
    """

    STEP_TYPE = "load"

    @observed_execution("load")
    def execute(self, step: LoadStep, context: ExecutionContext) -> StepExecutionResult:
        """
        Execute a load operation with comprehensive observability.

        Args:
            step: LoadStep containing source, target, and configuration
            context: ExecutionContext with shared services

        Returns:
            StepExecutionResult with detailed execution metrics
        """
        start_time = datetime.utcnow()
        logger.debug(
            f"Executing LoadStep {step.id}: {step.source} -> {step.target_table}"
        )

        try:
            # Step 1: Validate step configuration
            self._validate_load_step(step)

            # Step 2: Resolve source configuration and create connector
            connector = self._create_source_connector(step, context)

            # Step 3: Load data from source
            data_chunks, total_rows = self._load_data_from_source(
                connector, step, context
            )

            # Step 4: Process data according to load mode
            result_info = self._process_load_mode(
                step, context, data_chunks, total_rows
            )

            # Step 5: Update watermarks for incremental loading if needed
            self._update_watermarks_if_needed(step, context, data_chunks)

            # Step 6: Collect performance metrics and schema information
            performance_metrics = self._collect_performance_metrics(
                result_info, start_time
            )
            schema_info = self._collect_schema_information(step, context, data_chunks)

            end_time = datetime.utcnow()
            (end_time - start_time).total_seconds() * 1000

            # Step 7: Provide user feedback
            self._provide_user_feedback(step, total_rows)

            return StepExecutionResult.success(
                step_id=step.id,
                step_type="load",
                start_time=start_time,
                end_time=end_time,
                rows_affected=total_rows,
                output_schema=schema_info.get("output_schema"),
                input_schemas=schema_info.get("input_schemas", {}),
                performance_metrics=performance_metrics,
                data_lineage={
                    "source": step.source,
                    "target_table": step.target_table,
                    "load_mode": step.load_mode,
                    "source_rows": total_rows,
                },
            )

        except Exception as e:
            end_time = datetime.utcnow()
            error_message = f"Load operation failed: {str(e)}"
            logger.error(f"LoadStep {step.id} failed: {error_message}")

            return StepExecutionResult.failure(
                step_id=step.id,
                step_type="load",
                start_time=start_time,
                end_time=end_time,
                error_message=error_message,
                error_code="LOAD_EXECUTION_ERROR",
            )

    def _validate_load_step(self, step: LoadStep) -> None:
        """
        Validate LoadStep configuration before execution.

        Args:
            step: LoadStep to validate

        Raises:
            ValueError: If step configuration is invalid
        """
        if not step.source:
            raise ValueError(f"LoadStep {step.id}: source cannot be empty")

        if not step.target_table:
            raise ValueError(f"LoadStep {step.id}: target_table cannot be empty")

        if step.load_mode not in ["replace", "append", "upsert"]:
            raise ValueError(
                f"LoadStep {step.id}: invalid load_mode '{step.load_mode}'. "
                f"Must be one of: replace, append, upsert"
            )

        if step.load_mode == "upsert" and step.incremental_config:
            upsert_keys = step.incremental_config.get("upsert_keys", [])
            if not upsert_keys:
                raise ValueError(
                    f"LoadStep {step.id}: UPSERT mode requires upsert_keys "
                    f"in incremental_config"
                )

    def _create_source_connector(self, step: LoadStep, context: ExecutionContext):
        """
        Create and configure the source connector for this load step.

        Args:
            step: LoadStep configuration
            context: ExecutionContext with connector registry

        Returns:
            Configured source connector instance
        """
        try:
            # Determine connector type and options
            connector_type = self._detect_connector_type(step)
            connector_options = self._prepare_connector_options(step)

            return context.connector_registry.create_source_connector(
                connector_type, connector_options
            )

        except Exception as e:
            raise ValueError(
                f"Failed to create connector for source '{step.source}': {e}"
            ) from e

    def _detect_connector_type(self, step: LoadStep) -> str:
        """Detect connector type from step configuration."""
        # Explicit connector type in options
        if "connector_type" in step.options:
            return step.options["connector_type"]

        # Auto-detect from file extension
        if step.source.endswith(".csv"):
            return "csv"
        elif step.source.endswith(".parquet"):
            return "parquet"

        # Default fallback
        return "csv"

    def _prepare_connector_options(self, step: LoadStep) -> Dict[str, Any]:
        """Prepare connector options with source path."""
        connector_options = dict(step.options)
        connector_options["path"] = step.source
        return connector_options

    def _load_data_from_source(
        self, connector, step: LoadStep, context: ExecutionContext
    ):
        """
        Load data from the source connector.

        Args:
            connector: Source connector instance
            step: LoadStep configuration
            context: ExecutionContext

        Returns:
            Tuple of (data_chunks, total_rows)
        """
        try:
            # Use connector to read data - handle different connector interfaces
            data_chunks = self._read_data_chunks_from_connector(connector)

            if not data_chunks:
                logger.warning(f"No data loaded from source '{step.source}'")
                return [], 0

            # Calculate total rows
            total_rows = sum(len(chunk) for chunk in data_chunks)
            logger.debug(f"Loaded {total_rows} rows from {len(data_chunks)} chunks")

            return data_chunks, total_rows

        except Exception as e:
            raise ValueError(
                f"Failed to load data from source '{step.source}': {e}"
            ) from e

    def _read_data_chunks_from_connector(self, connector):
        """Read data chunks from connector using appropriate interface."""
        if hasattr(connector, "read_chunks"):
            # New chunked interface
            return list(connector.read_chunks())
        elif hasattr(connector, "read"):
            # Traditional interface - convert to chunks
            data_result = connector.read()
            return self._normalize_to_chunks(data_result)
        else:
            raise ValueError(
                f"Connector {type(connector)} does not support data reading"
            )

    def _normalize_to_chunks(self, data_result):
        """
        Normalize any data result to DataChunk objects.

        Follows "Simple is better than complex" - one clear conversion path.
        """
        # Handle different return types from connectors
        if isinstance(data_result, DataChunk):
            return [data_result]

        if isinstance(data_result, pd.DataFrame):
            return [DataChunk(data_result)]

        # Handle iterables (generators, lists of data)
        if hasattr(data_result, "__iter__") and not isinstance(
            data_result, (str, bytes)
        ):
            chunks = []
            for item in data_result:
                if isinstance(item, DataChunk):
                    chunks.append(item)
                elif isinstance(item, pd.DataFrame):
                    chunks.append(DataChunk(item))
                else:
                    # Fallback: let DataChunk handle it
                    chunks.append(DataChunk(item))
            return chunks

        # Default: wrap in DataChunk
        return [DataChunk(data_result)]

    def _process_load_mode(
        self,
        step: LoadStep,
        context: ExecutionContext,
        data_chunks: List[DataChunk],
        total_rows: int,
    ) -> Dict[str, Any]:
        """
        Process the data according to the specified load mode.

        Args:
            step: LoadStep configuration
            context: ExecutionContext with SQL engine
            data_chunks: List of DataChunk objects
            total_rows: Total number of rows

        Returns:
            Dictionary with processing results and metrics
        """
        if not data_chunks:
            return {"processed_rows": 0, "operation": "skip"}

        result_info = {
            "processed_rows": total_rows,
            "chunks_processed": len(data_chunks),
            "operation": step.load_mode.upper(),
        }

        if step.load_mode == "replace":
            result_info.update(self._process_replace_mode(step, context, data_chunks))
        elif step.load_mode == "append":
            result_info.update(self._process_append_mode(step, context, data_chunks))
        elif step.load_mode == "upsert":
            result_info.update(self._process_upsert_mode(step, context, data_chunks))
        else:
            raise ValueError(f"Unsupported load mode: {step.load_mode}")

        return result_info

    def _process_replace_mode(
        self, step: LoadStep, context: ExecutionContext, data_chunks: List[DataChunk]
    ) -> Dict[str, Any]:
        """Process REPLACE mode - recreate table with new data."""
        table_exists = context.sql_engine.table_exists(step.target_table)

        for i, chunk in enumerate(data_chunks):
            if i == 0:
                # First chunk: CREATE TABLE or CREATE OR REPLACE TABLE
                temp_table_name = f"temp_{step.target_table}_{int(time.time())}"
                context.sql_engine.register_arrow(temp_table_name, chunk.arrow_table)

                if table_exists:
                    context.sql_engine.execute_query(
                        f"CREATE OR REPLACE TABLE {step.target_table} AS SELECT * FROM {temp_table_name}"
                    )
                else:
                    context.sql_engine.execute_query(
                        f"CREATE TABLE {step.target_table} AS SELECT * FROM {temp_table_name}"
                    )
                table_exists = True  # Now it exists
            else:
                # Subsequent chunks: INSERT
                temp_table_name = f"temp_{step.target_table}_{int(time.time())}_{i}"
                context.sql_engine.register_arrow(temp_table_name, chunk.arrow_table)
                context.sql_engine.execute_query(
                    f"INSERT INTO {step.target_table} SELECT * FROM {temp_table_name}"
                )

        return {"strategy": "create_or_replace_then_insert"}

    def _process_append_mode(
        self, step: LoadStep, context: ExecutionContext, data_chunks: List[DataChunk]
    ) -> Dict[str, Any]:
        """Process APPEND mode - add data to existing table."""
        table_exists = context.sql_engine.table_exists(step.target_table)

        for i, chunk in enumerate(data_chunks):
            temp_table_name = f"temp_{step.target_table}_{int(time.time())}_{i}"
            context.sql_engine.register_arrow(temp_table_name, chunk.arrow_table)

            if not table_exists and i == 0:
                # Create table from first chunk
                context.sql_engine.execute_query(
                    f"CREATE TABLE {step.target_table} AS SELECT * FROM {temp_table_name}"
                )
                table_exists = True
            else:
                # Insert into existing table
                context.sql_engine.execute_query(
                    f"INSERT INTO {step.target_table} SELECT * FROM {temp_table_name}"
                )

        return {"strategy": "insert_append"}

    def _process_upsert_mode(
        self, step: LoadStep, context: ExecutionContext, data_chunks: List[DataChunk]
    ) -> Dict[str, Any]:
        """Process UPSERT mode - merge data based on key columns."""
        if not step.incremental_config or not step.incremental_config.get(
            "upsert_keys"
        ):
            raise ValueError("UPSERT mode requires upsert_keys in incremental_config")

        upsert_keys = step.incremental_config["upsert_keys"]
        table_exists = context.sql_engine.table_exists(step.target_table)

        if not table_exists:
            # If table doesn't exist, create it from first chunk
            first_chunk = data_chunks[0]
            temp_table_name = f"temp_{step.target_table}_{int(time.time())}"
            context.sql_engine.register_arrow(temp_table_name, first_chunk.arrow_table)
            context.sql_engine.execute_query(
                f"CREATE TABLE {step.target_table} AS SELECT * FROM {temp_table_name}"
            )
            # Process remaining chunks as upserts
            remaining_chunks = data_chunks[1:]
        else:
            remaining_chunks = data_chunks

        upserted_rows = 0
        for i, chunk in enumerate(remaining_chunks):
            temp_table_name = f"temp_upsert_{step.target_table}_{int(time.time())}_{i}"
            context.sql_engine.register_arrow(temp_table_name, chunk.arrow_table)

            # Build UPSERT SQL (INSERT OR REPLACE for DuckDB)
            key_conditions = " AND ".join(
                [
                    f"{step.target_table}.{key} = {temp_table_name}.{key}"
                    for key in upsert_keys
                ]
            )

            # Delete existing rows that match keys
            context.sql_engine.execute_query(
                f"""
                DELETE FROM {step.target_table} 
                WHERE EXISTS (
                    SELECT 1 FROM {temp_table_name} 
                    WHERE {key_conditions}
                )
            """
            )

            # Insert all rows from temp table
            context.sql_engine.execute_query(
                f"INSERT INTO {step.target_table} SELECT * FROM {temp_table_name}"
            )

            upserted_rows += len(chunk)

        return {
            "strategy": "delete_then_insert_upsert",
            "upsert_keys": upsert_keys,
            "upserted_rows": upserted_rows,
        }

    def _update_watermarks_if_needed(
        self, step: LoadStep, context: ExecutionContext, data_chunks: List[DataChunk]
    ) -> None:
        """Update watermarks for incremental loading if configured."""
        if not step.incremental_config:
            return

        cursor_field = step.incremental_config.get("cursor_field")
        if not cursor_field or not data_chunks:
            return

        try:
            # Find the maximum cursor value from all chunks
            max_cursor_value = None

            for chunk in data_chunks:
                df = chunk.pandas_df
                if cursor_field in df.columns:
                    chunk_max = df[cursor_field].max()
                    if max_cursor_value is None or chunk_max > max_cursor_value:
                        max_cursor_value = chunk_max

            if max_cursor_value is not None:
                # Update watermark using the watermark manager
                context.watermark_manager.update_watermark_atomic(
                    pipeline=step.metadata.get("pipeline_name", "default"),
                    source=step.source,
                    target=step.target_table,
                    column=cursor_field,
                    value=max_cursor_value,
                )
                logger.debug(
                    f"Updated watermark for {step.id}: {cursor_field} = {max_cursor_value}"
                )

        except Exception as e:
            logger.warning(f"Failed to update watermarks for step {step.id}: {e}")

    def _collect_performance_metrics(
        self, result_info: Dict[str, Any], start_time: datetime
    ) -> Dict[str, Any]:
        """Collect performance metrics for observability."""
        execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        processed_rows = result_info.get("processed_rows", 0)
        chunks_processed = result_info.get("chunks_processed", 0)

        metrics = {
            "execution_time_ms": execution_time_ms,
            "processed_rows": processed_rows,
            "chunks_processed": chunks_processed,
            "operation": result_info.get("operation", "unknown"),
            "strategy": result_info.get("strategy", "unknown"),
        }

        # Calculate throughput if we have data
        if processed_rows > 0 and execution_time_ms > 0:
            metrics["rows_per_second"] = processed_rows / (execution_time_ms / 1000)
            metrics["avg_chunk_size"] = processed_rows / max(chunks_processed, 1)

        # Add UPSERT-specific metrics
        if "upserted_rows" in result_info:
            metrics["upserted_rows"] = result_info["upserted_rows"]
            metrics["upsert_keys"] = result_info.get("upsert_keys", [])

        return metrics

    def _collect_schema_information(
        self, step: LoadStep, context: ExecutionContext, data_chunks: List[DataChunk]
    ) -> Dict[str, Any]:
        """Collect schema information for data lineage."""
        schema_info = {"input_schemas": {}, "output_schema": None}

        try:
            # Input schema from first chunk
            if data_chunks:
                first_chunk = data_chunks[0]
                schema_info["input_schemas"][step.source] = list(
                    first_chunk.schema.names
                )

            # Output schema from target table
            if context.sql_engine.table_exists(step.target_table):
                output_schema = context.sql_engine.get_table_schema(step.target_table)
                if output_schema:
                    schema_info["output_schema"] = output_schema

        except Exception as e:
            logger.warning(
                f"Failed to collect schema information for step {step.id}: {e}"
            )

        return schema_info

    def _provide_user_feedback(self, step: LoadStep, total_rows: int) -> None:
        """Provide user-friendly feedback about the load operation."""
        # Clean up table name for display
        display_name = step.target_table
        if display_name.startswith("transform_"):
            display_name = display_name.replace("transform_", "")

        # Print user-friendly message
        print(f"📥 Loaded {display_name} ({total_rows:,} rows)")
        logger.debug(
            f"Load operation completed: {step.source} -> {step.target_table} ({total_rows} rows)"
        )
