"""Load Step Executor following Single Responsibility Principle.

Handles data loading from various sources into target tables.
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
class LoadStepExecutor(BaseStepExecutor):
    """Executes load steps - SINGLE responsibility.

    Handles only load operations:
    - Data ingestion from various sources
    - Target table creation and population
    - Load mode handling (replace, append, upsert)
    """

    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the step."""
        if hasattr(step, "type"):
            return step.type == "load"
        elif isinstance(step, dict):
            return step.get("type") == "load"
        return False

    def execute(self, step: Step, context: ExecutionContext) -> StepExecutionResult:
        """Execute load step with focused implementation."""
        start_time = time.time()
        step_id = getattr(step, "id", "unknown")

        try:
            logger.info(f"Executing load step: {step_id}")

            # Extract step details
            source, target_table, load_mode = self._extract_load_details(step)

            # Create connector and load data
            with self._observability_scope(context, "load_step"):
                connector = self._create_connector(step, context)
                data = connector.read()
                rows_loaded = self._load_data_to_table(
                    data, target_table, load_mode, context
                )

            execution_time = time.time() - start_time

            return self._create_result(
                step_id=step_id,
                status="success",
                message=f"Successfully loaded {rows_loaded} rows to {target_table}",
                execution_time=execution_time,
                rows_affected=rows_loaded,
                table_name=target_table,
            )

        except Exception as error:
            execution_time = time.time() - start_time
            logger.error(f"Load step {step_id} failed: {error}")
            return self._handle_execution_error(step_id, error, execution_time)

    def _extract_load_details(self, step: Step) -> tuple[str, str, str]:
        """Extract load step details with validation."""
        if hasattr(step, "source"):
            source = step.source
            target_table = step.target_table
            load_mode = getattr(step, "load_mode", "replace")
        elif isinstance(step, dict):
            source = step.get("source", "")
            target_table = step.get("target_table", "")
            load_mode = step.get("load_mode", "replace")
        else:
            raise ValueError(f"Invalid step format: {type(step)}")

        # Validation
        if not source:
            raise ValueError("Load step source cannot be empty")
        if not target_table:
            raise ValueError("Load step target_table cannot be empty")

        return source, target_table, load_mode

    def _create_connector(self, step: Step, context: ExecutionContext):
        """Create appropriate connector for the data source."""
        # Get connector registry from context
        if not hasattr(context, "connector_registry"):
            raise RuntimeError("Context missing connector_registry")

        # Extract source configuration
        if hasattr(step, "source"):
            source_config = {
                "source": step.source,
                "options": getattr(step, "options", {}),
            }
        else:
            source_config = step

        return context.connector_registry.create_source_connector(source_config)

    def _load_data_to_table(
        self, data: Any, target_table: str, load_mode: str, context: ExecutionContext
    ) -> int:
        """Load data to target table using the database session."""
        session = context.session

        if load_mode == "replace":
            # Drop and recreate table
            session.execute(f"DROP TABLE IF EXISTS {target_table}")

        # Load data using the database engine
        if hasattr(data, "__iter__"):
            # Handle iterable data
            rows_loaded = 0
            for chunk in data:
                if hasattr(chunk, "to_sql"):
                    # Pandas DataFrame
                    chunk.to_sql(
                        target_table, session.engine, if_exists="append", index=False
                    )
                    rows_loaded += len(chunk)
                else:
                    # Other formats - use engine's load method
                    result = session.load_data(chunk, target_table)
                    rows_loaded += result
        else:
            # Single data object
            if hasattr(data, "to_sql"):
                data.to_sql(
                    target_table, session.engine, if_exists="append", index=False
                )
                rows_loaded = len(data)
            else:
                rows_loaded = session.load_data(data, target_table)

        return rows_loaded

    def _observability_scope(self, context: ExecutionContext, scope_name: str):
        """Create observability scope for measurements."""
        if hasattr(context, "observability"):
            return context.observability.measure_scope(scope_name)
        else:
            # Fallback no-op context manager
            from contextlib import nullcontext

            return nullcontext()
