"""Source Operations for V2 Executor.

This module handles source definition and execution operations,
following the principle of "Simple is better than complex".

Key refactoring patterns applied:
- Strategy Pattern: Different execution strategies for incremental vs full refresh
- Template Method: Common structure with specialized implementations
- Single Responsibility: Each class handles one aspect of source operations
"""

import time
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class SourceExecutionContext:
    """Context object for source execution (Martin Fowler: Parameter Object pattern)."""

    def __init__(
        self, step: Dict[str, Any], source_name: str, connector, params: Dict[str, Any]
    ):
        self.step = step
        self.source_name = source_name
        self.connector = connector
        self.params = params
        self.cursor_field = step.get("cursor_field")
        self.sync_mode = step.get("sync_mode", "full_refresh")
        self.table_data_storage: Optional[Dict[str, Any]] = None
        # Extract connector_type from step for consistency
        self.connector_type = (
            step.get("source_connector_type", "")
            or step.get("connector_type", "")
            or "csv"
        ).lower()
        # Optional watermark manager for incremental loading
        self.watermark_manager = None


class SourceExecutionStrategy:
    """Base strategy for source execution (Strategy Pattern)."""

    def execute(
        self, context: SourceExecutionContext, observability_manager=None
    ) -> Dict[str, Any]:
        """Template method for source execution."""
        start_time = time.time()

        try:
            # Record start if observability is available
            if observability_manager:
                observability_manager.record_step_start(
                    context.step.get("id", context.source_name), self._get_step_type()
                )

            # Execute the specific strategy
            result = self._execute_strategy(context)

            # Record success if observability is available
            if observability_manager:
                duration_ms = (time.time() - start_time) * 1000
                observability_manager.record_step_success(
                    context.step.get("id", context.source_name),
                    {
                        "duration_ms": duration_ms,
                        "rows_affected": result.get("rows_processed", 0),
                        "step_type": self._get_step_type(),
                        "performance_metrics": self._get_performance_metrics(
                            result, duration_ms
                        ),
                    },
                )

            return result

        except Exception as e:
            # Record failure if observability is available
            if observability_manager:
                duration_ms = (time.time() - start_time) * 1000
                observability_manager.record_step_failure(
                    context.step.get("id", context.source_name),
                    self._get_step_type(),
                    str(e),
                    duration_ms,
                )
            raise

    def _execute_strategy(self, context: SourceExecutionContext) -> Dict[str, Any]:
        """Override in subclasses for specific execution logic."""
        raise NotImplementedError

    def _get_step_type(self) -> str:
        """Get the step type for observability."""
        return "source_execution"

    def _get_performance_metrics(
        self, result: Dict[str, Any], duration_ms: float
    ) -> Dict[str, Any]:
        """Extract performance metrics from result."""
        rows = result.get("rows_processed", 0)
        return {
            "rows_per_second": rows / (duration_ms / 1000) if duration_ms > 0 else 0,
        }


class IncrementalSourceStrategy(SourceExecutionStrategy):
    """Strategy for incremental source execution."""

    def _execute_strategy(self, context: SourceExecutionContext) -> Dict[str, Any]:
        """Execute incremental source loading with watermark management."""
        pipeline_name = "default_pipeline"  # Could be passed in context

        # Get previous watermark
        previous_watermark = self._get_previous_watermark(context, pipeline_name)

        # Read incremental data
        object_name = self._extract_object_name(context)
        data_chunks = self._read_incremental_data(
            context, object_name, previous_watermark
        )

        # Process data and calculate metrics
        total_rows = self._process_data_chunks(context, data_chunks)

        # Update watermark if successful
        new_watermark = self._update_watermark(context, data_chunks, pipeline_name)

        return {
            "status": "success",
            "step_id": context.step.get("id", context.source_name),
            "source_name": context.source_name,
            "connector_type": context.connector_type,
            "sync_mode": "incremental",
            "rows_processed": total_rows,
            "previous_watermark": previous_watermark,
            "new_watermark": new_watermark,
            "execution_time_ms": 0,  # Will be set by parent
        }

    def _get_step_type(self) -> str:
        return "incremental_source"

    def _get_previous_watermark(
        self, context: SourceExecutionContext, pipeline_name: str
    ) -> Optional[str]:
        """Get previous watermark value."""
        # Use watermark manager if available in context
        if hasattr(context, "watermark_manager") and context.watermark_manager:
            return context.watermark_manager.get_source_watermark(
                pipeline=pipeline_name,
                source=context.source_name,
                cursor_field=context.cursor_field,
            )
        return None

    def _extract_object_name(self, context: SourceExecutionContext) -> str:
        """Extract object name from parameters."""
        return (
            context.params.get("object_name")
            or context.params.get("table")
            or context.params.get("path")
            or context.source_name
        )

    def _read_incremental_data(
        self,
        context: SourceExecutionContext,
        object_name: str,
        previous_watermark: Optional[str],
    ) -> List[Any]:
        """Read incremental data from connector."""
        if hasattr(context.connector, "read_incremental") and previous_watermark:
            return list(
                context.connector.read_incremental(
                    object_name=object_name,
                    cursor_field=context.cursor_field,
                    cursor_value=previous_watermark,
                    batch_size=10000,
                )
            )
        else:
            # Fallback to full read for first time or unsupported connectors
            return list(context.connector.read(object_name=object_name))

    def _process_data_chunks(
        self, context: SourceExecutionContext, data_chunks: List[Any]
    ) -> int:
        """Process data chunks and return total row count."""
        total_rows = 0
        combined_data = None

        for chunk in data_chunks:
            if hasattr(chunk, "arrow_table"):
                total_rows += len(chunk.arrow_table)
                if combined_data is None:
                    combined_data = chunk
                else:
                    # Combine arrow tables
                    import pyarrow as pa

                    combined_table = pa.concat_tables(
                        [combined_data.arrow_table, chunk.arrow_table]
                    )
                    combined_data = type(chunk)(combined_table)
            elif hasattr(chunk, "pandas_df"):
                total_rows += len(chunk.pandas_df)
                if combined_data is None:
                    combined_data = chunk
                else:
                    # Combine pandas dataframes
                    import pandas as pd

                    combined_df = pd.concat(
                        [combined_data.pandas_df, chunk.pandas_df], ignore_index=True
                    )
                    combined_data = type(chunk)(combined_df)

        # Store combined data for access by other components
        if combined_data and context.table_data_storage is not None:
            context.table_data_storage[context.source_name] = combined_data

        return total_rows

    def _update_watermark(
        self,
        context: SourceExecutionContext,
        data_chunks: List[Any],
        pipeline_name: str,
    ) -> Optional[str]:
        """Update watermark with new value."""
        if not data_chunks or not context.cursor_field:
            return None

        # Find the maximum value of the cursor field from processed data
        max_cursor_value = None

        for chunk in data_chunks:
            if hasattr(chunk, "pandas_df"):
                df = chunk.pandas_df
                if context.cursor_field in df.columns and not df.empty:
                    # Get max value from this chunk
                    chunk_max = df[context.cursor_field].max()
                    if max_cursor_value is None or chunk_max > max_cursor_value:
                        max_cursor_value = chunk_max

        # Convert to string for watermark storage
        if max_cursor_value is not None:
            # Handle datetime objects or strings
            if hasattr(max_cursor_value, "strftime"):
                return max_cursor_value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return str(max_cursor_value)

        return None


class FullRefreshSourceStrategy(SourceExecutionStrategy):
    """Strategy for full refresh source execution."""

    def _execute_strategy(self, context: SourceExecutionContext) -> Dict[str, Any]:
        """Execute full refresh source loading."""
        # Read all data
        object_name = self._extract_object_name(context)
        data_chunks = list(context.connector.read(object_name=object_name))

        # Process data
        total_rows = self._process_data_chunks(context, data_chunks)

        return {
            "status": "success",
            "step_id": context.step.get("id", context.source_name),
            "source_name": context.source_name,
            "connector_type": context.connector_type,
            "sync_mode": "full_refresh",
            "rows_processed": total_rows,
            "execution_time_ms": 0,  # Will be set by parent
        }

    def _get_step_type(self) -> str:
        return "full_refresh_source"

    def _extract_object_name(self, context: SourceExecutionContext) -> str:
        """Extract object name from parameters."""
        return (
            context.params.get("object_name")
            or context.params.get("table")
            or context.params.get("path")
            or context.source_name
        )

    def _process_data_chunks(
        self, context: SourceExecutionContext, data_chunks: List[Any]
    ) -> int:
        """Process data chunks and return total row count."""
        total_rows = 0
        combined_data = None

        for chunk in data_chunks:
            if hasattr(chunk, "arrow_table"):
                total_rows += len(chunk.arrow_table)
                if combined_data is None:
                    combined_data = chunk
                else:
                    import pyarrow as pa

                    combined_table = pa.concat_tables(
                        [combined_data.arrow_table, chunk.arrow_table]
                    )
                    combined_data = type(chunk)(combined_table)
            elif hasattr(chunk, "pandas_df"):
                total_rows += len(chunk.pandas_df)
                if combined_data is None:
                    combined_data = chunk
                else:
                    import pandas as pd

                    combined_df = pd.concat(
                        [combined_data.pandas_df, chunk.pandas_df], ignore_index=True
                    )
                    combined_data = type(chunk)(combined_df)

        # Store combined data for access by other components
        if combined_data and context.table_data_storage is not None:
            context.table_data_storage[context.source_name] = combined_data

        return total_rows


class SourceExecutionOrchestrator:
    """Orchestrates source execution using appropriate strategy."""

    def __init__(self):
        self.strategies = {
            "incremental": IncrementalSourceStrategy(),
            "full_refresh": FullRefreshSourceStrategy(),
        }

    def execute_source_with_patterns(
        self,
        step: Dict[str, Any],
        source_name: str,
        connector,
        params: Dict[str, Any],
        observability_manager=None,
        table_data_storage=None,
        watermark_manager=None,
    ) -> Dict[str, Any]:
        """Execute source operation using appropriate strategy."""

        # Create execution context
        context = SourceExecutionContext(step, source_name, connector, params)

        # Add table data storage if provided
        if table_data_storage is not None:
            context.table_data_storage = table_data_storage

        # Add watermark manager if provided
        context.watermark_manager = watermark_manager

        # Determine strategy based on sync mode
        sync_mode = step.get("sync_mode", "full_refresh")
        cursor_field = step.get("cursor_field")

        # Use incremental strategy if cursor field is provided and sync mode is incremental
        strategy_key = (
            "incremental"
            if (sync_mode == "incremental" and cursor_field)
            else "full_refresh"
        )

        strategy = self.strategies.get(strategy_key)
        if not strategy:
            raise ValueError(f"Unknown strategy: {strategy_key}")

        try:
            result = strategy.execute(context, observability_manager)

            # Handle watermark updates for incremental loads
            if (
                strategy_key == "incremental"
                and result.get("status") == "success"
                and result.get("new_watermark")
                and watermark_manager
            ):

                watermark_manager.update_source_watermark(
                    pipeline="default_pipeline",
                    source=source_name,
                    cursor_field=cursor_field,
                    value=result["new_watermark"],
                )

            return result
        except FileNotFoundError as e:
            # Handle file not found gracefully for source definitions
            return self._handle_file_not_found(step, source_name, str(e))
        except Exception as e:
            # Handle other exceptions
            return self._handle_execution_error(step, source_name, str(e))

    def _handle_file_not_found(
        self, step: Dict[str, Any], source_name: str, error: str
    ) -> Dict[str, Any]:
        """Handle file not found errors gracefully."""
        # Extract connector_type for consistency
        connector_type = (
            step.get("source_connector_type", "")
            or step.get("connector_type", "")
            or "csv"
        ).lower()

        return {
            "status": "success",  # Source definition succeeds even if file not accessible yet
            "step_id": step.get("id", source_name),
            "source_name": source_name,
            "connector_type": connector_type,  # Include connector_type
            "sync_mode": step.get("sync_mode", "full_refresh"),
            "rows_processed": 0,
            "message": "Source definition stored successfully (file access deferred)",
        }

    def _handle_execution_error(
        self, step: Dict[str, Any], source_name: str, error: str
    ) -> Dict[str, Any]:
        """Handle execution errors."""
        # Extract connector_type for consistency
        connector_type = (
            step.get("source_connector_type", "")
            or step.get("connector_type", "")
            or "csv"
        ).lower()

        return {
            "status": "error",
            "step_id": step.get("id", source_name),
            "source_name": source_name,
            "connector_type": connector_type,  # Include connector_type
            "error": error,
        }
