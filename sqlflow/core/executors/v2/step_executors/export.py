"""Export Step Executor following Single Responsibility Principle.

Handles data export from tables to external destinations.
Clean, focused implementation with proper error handling.
"""

import time
from dataclasses import dataclass
from typing import Any

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step
from .base import BaseStepExecutor, StepExecutionResult

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
        if hasattr(step, "type"):
            return step.type == "export"
        elif isinstance(step, dict):
            return step.get("type") == "export"
        return False

    def execute(self, step: Step, context: ExecutionContext) -> StepExecutionResult:
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
        if hasattr(step, "source_table"):
            source_table = step.source_table
            destination = getattr(step, "destination", "")
            export_format = getattr(step, "format", "csv")
        elif isinstance(step, dict):
            source_table = step.get("source_table", "")
            destination = step.get("destination", "")
            export_format = step.get("format", "csv")
        else:
            raise ValueError(f"Invalid step format: {type(step)}")

        # Validation
        if not source_table:
            raise ValueError("Export step source_table cannot be empty")
        if not destination:
            raise ValueError("Export step destination cannot be empty")

        return source_table, destination, export_format

    def _read_source_data(self, source_table: str, context: ExecutionContext):
        """Read data from the source table."""
        session = context.session

        # Execute query to get data
        query = f"SELECT * FROM {source_table}"
        result = session.execute(query)

        # Convert to appropriate format for export
        if hasattr(result, "fetchall"):
            try:
                # Try to get raw data first
                data = result.fetchall()
                return data
            except Exception:
                # If fetchall fails, try pandas
                try:
                    import pandas as pd

                    data = pd.read_sql(query, session.engine)
                    return data
                except (ImportError, Exception):
                    # Fallback to result itself
                    return result
        else:
            return result

    def _create_destination_connector(self, step: Step, context: ExecutionContext):
        """Create appropriate connector for the destination."""
        # Get connector registry from context
        if not hasattr(context, "connector_registry"):
            raise RuntimeError("Context missing connector_registry")

        # Extract destination configuration
        if hasattr(step, "destination"):
            dest_config = {
                "destination": step.destination,
                "format": getattr(step, "format", "csv"),
                "options": getattr(step, "options", {}),
            }
        else:
            dest_config = step

        return context.connector_registry.create_destination_connector(dest_config)

    def _observability_scope(self, context: ExecutionContext, scope_name: str):
        """Create observability scope for measurements."""
        if hasattr(context, "observability"):
            return context.observability.measure_scope(scope_name)
        else:
            # Fallback no-op context manager
            from contextlib import nullcontext

            return nullcontext()
