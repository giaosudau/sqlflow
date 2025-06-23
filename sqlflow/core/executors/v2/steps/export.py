"""Export Step Executor following Single Responsibility Principle.

Handles data export from tables to external destinations.
Clean, focused implementation with proper error handling.
"""

import time
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step
from ..results.models import StepResult
from .base import BaseStepExecutor

logger = get_logger(__name__)


@dataclass
class ExportStepExecutor(BaseStepExecutor):
    """Executes export steps - SINGLE responsibility.

    Handles only export operations:
    - Reading data from tables
    - Writing to external destinations
    - Format conversion and validation
    """

    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the step."""
        return hasattr(step, "step_type") and step.step_type == "export"

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute export step with focused implementation."""
        start_time = time.time()
        step_id = getattr(step, "id", "unknown")

        try:
            logger.info(f"Executing export step: {step_id}")

            # Extract step details
            source_table, destination, export_format = self._extract_export_details(
                step
            )

            # Read data from source table
            data = self._read_source_data(source_table, context)

            # Create destination connector and export
            with self._observability_scope(context, "export_step"):
                connector = self._create_destination_connector(step, context)
                rows_exported = connector.write(data)

            execution_time = time.time() - start_time

            return self._create_result(
                step_id=step_id,
                status="success",
                message=f"Successfully exported {rows_exported} rows to {destination}",
                execution_time=execution_time,
                rows_affected=rows_exported,
                metadata={
                    "source_table": source_table,
                    "destination": destination,
                    "format": export_format,
                },
            )

        except Exception as error:
            execution_time = time.time() - start_time
            logger.error(f"Export step {step_id} failed: {error}")
            return self._handle_execution_error(step_id, error, execution_time)

    def _extract_export_details(self, step: Step) -> tuple[str, str, str]:
        """Extract export step details with validation."""
        source_table = getattr(step, "source_table", "")
        destination = getattr(step, "destination", "")
        export_format = getattr(step, "format", "csv")

        # Validation
        if not source_table:
            raise ValueError("Export step source_table cannot be empty")
        if not destination:
            raise ValueError("Export step destination cannot be empty")

        return source_table, destination, export_format

    def _read_source_data(self, source_table: str, context: ExecutionContext):
        """Read data from the source table."""
        engine = context.engine

        if not engine:
            raise ValueError("No database engine available in execution context")

        # Use engine's execute_query method to get results, then convert to DataFrame
        query = f"SELECT * FROM {source_table}"
        result = engine.execute_query(query)

        # Convert result to pandas DataFrame
        try:
            import pandas as pd

            # For DuckDB engine, the result should have a df() method
            if hasattr(result, "df"):
                return result.df()
            # If result is already a DataFrame, return it
            elif hasattr(result, "columns") and hasattr(result, "values"):
                return result
            # If result has fetchall method (like cursor), convert to DataFrame
            elif hasattr(result, "fetchall"):
                rows = result.fetchall()
                if hasattr(result, "description") and result.description:
                    columns = [desc[0] for desc in result.description]
                    return pd.DataFrame(rows, columns=columns)
                else:
                    return pd.DataFrame(rows)
            else:
                # Last resort: try to convert whatever we got to DataFrame
                return pd.DataFrame(result)

        except ImportError:
            raise RuntimeError("pandas is required for export operations")
        except Exception as e:
            raise RuntimeError(f"Failed to convert query result to DataFrame: {e}")

    def _create_destination_connector(self, step: Step, context: ExecutionContext):
        """Create appropriate connector for the destination."""
        if not hasattr(context, "connector_registry"):
            raise RuntimeError("Context missing connector_registry")

        # Extract destination configuration
        destination_path = getattr(step, "destination", "")
        export_format = getattr(step, "format", "csv")
        options = getattr(step, "options", {})

        # Determine connector type based on file extension
        if destination_path.endswith(".csv"):
            connector_type = "csv"
        elif destination_path.endswith(".parquet"):
            connector_type = "parquet"
        elif destination_path.endswith(".json"):
            connector_type = "json"
        else:
            # Default to CSV for unknown extensions
            connector_type = "csv"

        # Create resolved configuration
        resolved_config = {
            "path": destination_path,  # CSV connector expects 'path', not 'destination'
            "format": export_format,
            **options,
        }

        return context.connector_registry.create_destination_connector(
            connector_type, resolved_config
        )

    def _observability_scope(self, context: ExecutionContext, scope_name: str):
        """Create observability scope for measurements."""
        if hasattr(context, "observability") and context.observability is not None:
            return context.observability.measure_scope(scope_name)
        else:
            # Fallback no-op context manager
            return nullcontext()
