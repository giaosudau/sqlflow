"""Load Handler for V2 Executor.

Handles load operations with clean separation of concerns.
"""

from typing import Any, Dict, cast

from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import LoadStep
from sqlflow.logging import get_logger

from ..load_operations import LoadStepExecutor
from .base import StepHandler

logger = get_logger(__name__)


class LoadStepHandler(StepHandler):
    """Handler for load operations with performance optimizations."""

    STEP_TYPE = "load"

    def __init__(self, enable_optimizations: bool = True):
        """Initialize with optimization settings."""
        super().__init__()
        self.load_executor = LoadStepExecutor(enable_optimizations=enable_optimizations)

    def can_handle(self, step) -> bool:
        """Check if this handler can process the step."""
        if hasattr(step, "type"):
            return step.type == "load"
        elif isinstance(step, dict):
            return step.get("type") == "load"
        return False

    def execute(self, step, context) -> StepExecutionResult:
        """Execute load step with performance optimizations."""
        from datetime import datetime

        # Extract step information
        step_info = self._extract_step_info(step)
        if isinstance(step_info, StepExecutionResult):  # Validation error
            return step_info

        step_id, step_dict = step_info
        logger.info(f"Executing load step: {step_id}")

        start_time = datetime.utcnow()

        try:
            # Execute the load operation
            result = self._execute_load_operation(step_dict, context)

            # Convert result to StepExecutionResult
            return self._convert_to_step_result(
                result, step_id, step_dict, start_time, context
            )

        except Exception as e:
            logger.error(f"Load step failed: {e}")
            return self._handle_execution_error(step, start_time, e)

    def _extract_step_info(self, step):
        """Extract step information and validate."""
        from datetime import datetime

        if hasattr(step, "id") and hasattr(step, "source"):
            # This is a LoadStep object
            load_step = cast(LoadStep, step)
            step_dict = {
                "id": load_step.id,
                "source_name": load_step.source,
                "target_table": load_step.target_table,
                "mode": load_step.load_mode.upper(),
                "type": "load",
            }
            if (
                hasattr(load_step, "incremental_config")
                and load_step.incremental_config
            ):
                step_dict["incremental_config"] = dict(load_step.incremental_config)
            return load_step.id, step_dict

        elif isinstance(step, dict):
            # Validate dictionary step by creating LoadStep
            try:
                load_step = LoadStep(
                    id=step.get("id", "unnamed_load"),
                    source=step.get("source", ""),
                    target_table=step.get("target_table", ""),
                    load_mode=step.get("load_mode", step.get("mode", "replace")),
                    incremental_config=step.get("incremental_config"),
                    options=step.get("options", {}),
                )
                return load_step.id, step
            except ValueError as e:
                return StepExecutionResult.failure(
                    step_id=step.get("id", "unnamed_load"),
                    step_type="load",
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow(),
                    error_message=str(e),
                    error_code="VALIDATION_ERROR",
                )
        else:
            # Fallback
            step_id = "unnamed_load"
            step_dict = {"id": step_id, "type": "load"}
            return step_id, step_dict

    def _execute_load_operation(self, step_dict, context):
        """Execute the actual load operation."""
        table_data = getattr(context, "table_data", {})
        engine = getattr(context, "sql_engine", None)
        return self.load_executor.execute_load_step(step_dict, table_data, engine)

    def _convert_to_step_result(self, result, step_id, step_dict, start_time, context):
        """Convert load executor result to StepExecutionResult."""
        from datetime import datetime

        if result.get("status") == "success":
            output_schema = self._get_output_schema(result, context)
            return StepExecutionResult.success(
                step_id=step_id,
                step_type="load",
                start_time=start_time,
                end_time=datetime.utcnow(),
                rows_affected=result.get("rows_loaded", 0),
                performance_metrics=result.get("transfer_metrics", {}),
                output_schema=output_schema,
                data_lineage=self._build_data_lineage(result, step_dict),
            )
        else:
            return StepExecutionResult.failure(
                step_id=step_id,
                step_type="load",
                start_time=start_time,
                end_time=datetime.utcnow(),
                error_message=result.get("error", "Load operation failed"),
                error_code="LOAD_EXECUTION_ERROR",
            )

    def _get_output_schema(self, result, context):
        """Try to get schema information from the engine."""
        output_schema = None
        try:
            target_table = result.get("target_table", "")
            engine = getattr(context, "sql_engine", None)
            if target_table and engine:
                output_schema = self._extract_table_schema(engine, target_table)
        except Exception:
            pass  # Schema detection failed, continue without it
        return output_schema

    def _extract_table_schema(self, engine, target_table):
        """Extract schema using various methods."""
        # Try engine's get_table_schema method first
        if hasattr(engine, "get_table_schema"):
            try:
                schema_info = engine.get_table_schema(target_table)
                if schema_info:
                    return {col["name"]: col["type"] for col in schema_info}
            except Exception:
                pass

        # Fallback: use DESCRIBE
        try:
            schema_result = engine.execute_query(f"DESCRIBE {target_table}").fetchdf()
            if not schema_result.empty and "column_name" in schema_result.columns:
                return dict(
                    zip(schema_result["column_name"], schema_result["column_type"])
                )
        except Exception:
            pass

        return None

    def _build_data_lineage(self, result, step_dict):
        """Build data lineage information."""
        return {
            "target_table": result.get("target_table", ""),
            "load_strategy": result.get("load_strategy", "standard"),
            "load_mode": result.get("mode", step_dict.get("mode", "replace")).lower(),
            "source": result.get(
                "source", step_dict.get("source_name", step_dict.get("source", ""))
            ),
        }

    def get_transfer_metrics(self) -> Dict[str, Any]:
        """Get transfer performance metrics from the load executor."""
        return self.load_executor.get_transfer_performance_summary()
