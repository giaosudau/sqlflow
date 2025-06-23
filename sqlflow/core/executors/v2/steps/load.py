"""Load Step Executor using functional decomposition.

Following Raymond Hettinger's guidance:
- "Simple is better than complex"
- "Explicit is better than implicit"
- Small, focused functions instead of monster methods
- Pure functions where possible
- Clear data flow
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step
from ..results.models import StepResult
from .base import BaseStepExecutor
from .definitions import LoadStep

logger = get_logger(__name__)


# Pure functions for data processing
def extract_load_details(step: LoadStep) -> Tuple[str, str, str]:
    """Extract load step details from typed step - pure function.

    Args:
        step: Typed LoadStep object

    Returns:
        Tuple of (source, target_table, mode)
    """
    return step.source, step.target_table, step.mode.value


def validate_load_inputs(source: str, target_table: str) -> None:
    """Validate load inputs - pure function."""
    if not source.strip():
        raise ValueError("Load step source cannot be empty")
    if not target_table.strip():
        raise ValueError("Load step target_table cannot be empty")


def determine_connector_type(source_name: str) -> str:
    """Determine connector type from file extension - pure function."""
    if source_name.endswith(".csv"):
        return "csv"
    elif source_name.endswith(".parquet"):
        return "parquet"
    elif source_name.endswith(".json"):
        return "json"
    else:
        return "csv"  # Default


def create_connector_config(source_name: str) -> Dict[str, Any]:
    """Create connector configuration - pure function."""
    return {"path": source_name}


def get_source_definition(
    context: ExecutionContext, source_name: str
) -> Dict[str, Any]:
    """Get source definition from context."""
    if hasattr(context, "source_definitions"):
        return context.source_definitions.get(source_name, {})
    return {}


def create_connector(step: LoadStep, context: ExecutionContext):
    """Create appropriate connector for the data source."""
    # Use getattr with a reasonable error message for missing connector_registry
    connector_registry = getattr(context, "connector_registry", None)
    if connector_registry is None:
        raise RuntimeError("Context missing connector_registry")

    source_name = extract_load_details(step)[0]

    # Check for source definition first
    source_definition = get_source_definition(context, source_name)

    if source_definition:
        connector_type = source_definition.get("connector_type", "csv")
        configuration = source_definition.get("configuration", {})
    else:
        # Fallback to file extension detection
        connector_type = determine_connector_type(source_name)
        configuration = create_connector_config(source_name)
        logger.warning(
            f"No source definition found for '{source_name}', using fallback"
        )

    return connector_registry.create_source_connector(connector_type, configuration)


def load_data_replace(engine: Any, data: Any, target_table: str, temp_view: str) -> int:
    """Handle REPLACE load mode - pure business logic."""
    engine.execute_query(f"DROP TABLE IF EXISTS {target_table}")
    engine.execute_query(f"CREATE TABLE {target_table} AS SELECT * FROM {temp_view}")
    return len(data)


def load_data_append(engine: Any, data: Any, target_table: str, temp_view: str) -> int:
    """Handle APPEND load mode - pure business logic."""
    if not engine.table_exists(target_table):
        engine.execute_query(
            f"CREATE TABLE {target_table} AS SELECT * FROM {temp_view}"
        )
    else:
        engine.execute_query(f"INSERT INTO {target_table} SELECT * FROM {temp_view}")
    return len(data)


def get_upsert_keys(step: LoadStep) -> List[str]:
    """Get upsert keys from step definition.

    Raises ValueError if upsert_keys are not explicitly provided for UPSERT mode.
    This follows the principle "Explicit is better than implicit".
    """
    if not step.upsert_keys:
        raise ValueError("upsert_keys cannot be empty for UPSERT mode")

    return step.upsert_keys


def load_data_upsert(
    engine: Any, data: Any, target_table: str, temp_view: str, upsert_keys: List[str]
) -> int:
    """Handle UPSERT load mode - pure business logic."""
    if not engine.table_exists(target_table):
        engine.execute_query(
            f"CREATE TABLE {target_table} AS SELECT * FROM {temp_view}"
        )
        return len(data)

    # Build delete query based on number of keys
    if len(upsert_keys) == 1:
        key = upsert_keys[0]
        delete_query = (
            f"DELETE FROM {target_table} WHERE {key} IN (SELECT {key} FROM {temp_view})"
        )
    else:
        key_list = ", ".join(upsert_keys)
        delete_query = f"DELETE FROM {target_table} WHERE ({key_list}) IN (SELECT {key_list} FROM {temp_view})"

    engine.execute_query(delete_query)
    engine.execute_query(f"INSERT INTO {target_table} SELECT * FROM {temp_view}")

    return len(data)


def load_dataframe_to_table(
    data: Any,
    target_table: str,
    load_mode: str,
    step: LoadStep,
    context: ExecutionContext,
) -> int:
    """Load pandas DataFrame to table - focused function."""
    engine = context.engine
    if not engine:
        raise ValueError("No database engine available")

    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError("pandas is required for DataFrame operations")

    if not isinstance(data, pd.DataFrame):
        raise ValueError("Expected pandas DataFrame")

    # Create temporary view
    temp_view = f"{target_table}_temp_{int(time.time() * 1000)}"
    # Use getattr to access register_table method safely
    register_table = getattr(engine, "register_table", None)
    if register_table is None:
        raise RuntimeError("Database engine does not support register_table method")
    register_table(temp_view, data)

    try:
        # Dispatch to appropriate load function
        if load_mode.upper() == "REPLACE":
            rows_loaded = load_data_replace(engine, data, target_table, temp_view)
        elif load_mode.upper() == "APPEND":
            rows_loaded = load_data_append(engine, data, target_table, temp_view)
        elif load_mode.upper() == "UPSERT":
            upsert_keys = get_upsert_keys(step)
            rows_loaded = load_data_upsert(
                engine, data, target_table, temp_view, upsert_keys
            )
        else:
            # Default to REPLACE
            rows_loaded = load_data_replace(engine, data, target_table, temp_view)

        return rows_loaded

    finally:
        # Always cleanup
        engine.execute_query(f"DROP VIEW IF EXISTS {temp_view}")


@dataclass
class LoadStepExecutor(BaseStepExecutor):
    """Executes load steps using functional decomposition.

    This executor only works with typed LoadStep objects.
    Dictionary-based steps must be converted to typed steps before execution.
    """

    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the step.

        Only accepts typed LoadStep objects with step_type attribute.
        """
        return hasattr(step, "step_type") and step.step_type == "load"

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute load step using functional approach.

        Args:
            step: Step object (should be LoadStep)
            context: Execution context

        Returns:
            StepResult with execution details
        """
        start_time = time.time()
        step_id = getattr(step, "id", "unknown")

        try:
            logger.info(f"Executing load step: {step_id}")

            # Ensure we have a LoadStep
            if not isinstance(step, LoadStep):
                raise ValueError(
                    f"LoadStepExecutor requires LoadStep, got {type(step)}"
                )

            # Extract and validate inputs
            source, target_table, load_mode = extract_load_details(step)
            validate_load_inputs(source, target_table)

            # Create connector and load data
            connector = create_connector(step, context)
            data = connector.read()

            # Load data using functional approach
            rows_loaded = load_dataframe_to_table(
                data, target_table, load_mode, step, context
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
