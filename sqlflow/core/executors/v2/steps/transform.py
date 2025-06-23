"""Transform Step Executor following Single Responsibility Principle.

Handles SQL transformation with UDF support and variable substitution.
Clean, focused implementation with proper error handling.
"Explicit is better than implicit"
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step
from ..results.models import StepResult
from .base import BaseStepExecutor
from .definitions import TransformStep

logger = get_logger(__name__)


@dataclass
class TransformStepExecutor(BaseStepExecutor):
    """Executes transform steps - SINGLE responsibility.

    This executor only works with typed TransformStep objects.
    Dictionary-based steps must be converted to typed steps before execution.

    Handles only transform operations:
    - SQL execution with UDF support
    - Variable substitution in SQL
    - Target table creation from queries
    """

    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the step.

        Only accepts typed TransformStep objects with step_type attribute.
        """
        return hasattr(step, "step_type") and step.step_type == "transform"

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute transform step with focused implementation.

        Args:
            step: Step object (should be TransformStep)
            context: Execution context

        Returns:
            StepResult with execution details
        """
        start_time = time.time()
        step_id = getattr(step, "id", "unknown")

        try:
            logger.info(f"Executing transform step: {step_id}")

            # Ensure we have a TransformStep
            if not isinstance(step, TransformStep):
                raise ValueError(
                    f"TransformStepExecutor requires TransformStep, got {type(step)}"
                )

            # Extract and validate SQL
            sql, target_table, udf_dependencies = self._extract_transform_details(step)

            with self._observability_scope(context, "transform_step"):
                # Register UDFs if needed
                self._register_udfs(udf_dependencies, context)

                # Process SQL with variable substitution
                processed_sql = self._substitute_variables(sql, context)

                # Execute SQL
                result = self._execute_sql(processed_sql, target_table, context)

            execution_time = time.time() - start_time

            return self._create_result(
                step_id=step_id,
                status="success",
                message=f"Successfully executed transform: {result.get('rows_affected', 0)} rows affected",
                execution_time=execution_time,
                rows_affected=result.get("rows_affected", 0),
                table_name=target_table,
                metadata={"sql_executed": processed_sql},
            )

        except Exception as error:
            execution_time = time.time() - start_time
            logger.error(f"Transform step {step_id} failed: {error}")
            return self._handle_execution_error(step_id, error, execution_time)

    def _extract_transform_details(
        self, step: TransformStep
    ) -> Tuple[str, Optional[str], List[Any]]:
        """Extract transform step details from typed step.

        Args:
            step: Typed TransformStep object

        Returns:
            Tuple of (sql, target_table, udf_dependencies)
        """
        sql = step.sql
        target_table = step.target_table if step.target_table else None
        udf_dependencies = getattr(step, "udf_dependencies", [])

        # Validation
        if not sql or not sql.strip():
            raise ValueError("Transform step SQL cannot be empty")

        return sql, target_table, udf_dependencies

    def _register_udfs(self, udf_dependencies: List[str], context: ExecutionContext):
        """Register required UDFs for this transformation."""
        if not udf_dependencies:
            return

        # The UDFs should already be registered with the engine by the context setup
        # This is handled by the PythonUDFManager in conftest.py or project setup
        # We don't need to re-register them here, just verify they exist
        engine = context.engine

        if engine is None:
            logger.warning("Engine not available in context")
            return

        # Check if UDFs are already registered with the engine
        if hasattr(engine, "registered_udfs"):
            registered_udfs = getattr(engine, "registered_udfs", {})
            registered_udf_names = list(registered_udfs.keys())
            logger.debug(f"Engine has registered UDFs: {registered_udf_names}")

            for udf_name in udf_dependencies:
                # Check both full name and short name
                short_name = udf_name.split(".")[-1] if "." in udf_name else udf_name
                if (
                    udf_name not in registered_udfs
                    and short_name not in registered_udfs
                ):
                    logger.warning(
                        f"UDF dependency {udf_name} not found in engine registry"
                    )
                else:
                    logger.debug(f"UDF dependency {udf_name} is available")
        else:
            logger.debug("Engine does not have registered_udfs attribute")

    def _substitute_variables(self, sql: str, context: ExecutionContext) -> str:
        """Substitute variables in SQL using pure function approach."""
        variables = context.variables
        if not variables:
            return sql

        # Use functional variable substitution
        try:
            from ..variables.substitution import substitute_variables

            return substitute_variables(sql, variables)
        except ImportError:
            # Fallback to simple string substitution
            logger.warning("Using fallback variable substitution")
            processed_sql = sql
            for key, value in variables.items():
                # Handle both ${variable} and {{variable}} syntax
                placeholder_dollar = f"${{{key}}}"
                placeholder_jinja = f"{{{{{key}}}}}"
                processed_sql = processed_sql.replace(placeholder_dollar, str(value))
                processed_sql = processed_sql.replace(placeholder_jinja, str(value))
            return processed_sql

    def _execute_sql(
        self, sql: str, target_table: Optional[str], context: ExecutionContext
    ) -> Dict[str, Any]:
        """Execute SQL with proper error handling."""
        engine = self._validate_engine(context)
        processed_sql = self._process_sql_for_udfs(sql, engine)

        try:
            if target_table:
                rows_affected = self._execute_create_table_as(
                    processed_sql, target_table, engine
                )
            else:
                rows_affected = self._execute_direct_sql(processed_sql, engine)

            return {"rows_affected": rows_affected, "success": True}

        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            logger.error(f"SQL: {processed_sql}")
            raise

    def _validate_engine(self, context: ExecutionContext):
        """Validate that engine is available in context."""
        engine = context.engine
        if not engine:
            raise ValueError("No database engine available in execution context")
        return engine

    def _process_sql_for_udfs(self, sql: str, engine) -> str:
        """Process SQL for UDF references if engine supports it."""
        process_query_method = getattr(engine, "process_query_for_udfs", None)
        if process_query_method is None or not hasattr(engine, "registered_udfs"):
            return sql

        registered_udfs = getattr(engine, "registered_udfs", {})
        if not registered_udfs:
            return sql

        logger.debug(f"Processing SQL for UDFs: {list(registered_udfs.keys())}")
        processed_sql = process_query_method(sql, registered_udfs)
        logger.debug(f"SQL after UDF processing: {processed_sql}")
        return processed_sql

    def _execute_create_table_as(
        self, processed_sql: str, target_table: str, engine
    ) -> int:
        """Execute CREATE TABLE AS operation."""
        # Drop table if it exists (similar to load step replace mode)
        engine.execute_query(f"DROP TABLE IF EXISTS {target_table}")

        # This is a CREATE TABLE AS operation
        create_sql = f"CREATE TABLE {target_table} AS ({processed_sql})"
        engine.execute_query(create_sql)

        # Count rows in the created table
        return self._count_table_rows(target_table, engine)

    def _execute_direct_sql(self, processed_sql: str, engine) -> int:
        """Execute SQL directly and return affected rows count."""
        result = engine.execute_query(processed_sql)
        return self._get_rows_affected_from_result(result)

    def _count_table_rows(self, table_name: str, engine) -> int:
        """Count rows in a table."""
        count_sql = f"SELECT COUNT(*) FROM {table_name}"
        count_result = engine.execute_query(count_sql)

        if hasattr(count_result, "fetchone"):
            return count_result.fetchone()[0]
        elif hasattr(count_result, "__len__"):
            return len(count_result)
        else:
            return 0

    def _get_rows_affected_from_result(self, result) -> int:
        """Get affected rows count from query result."""
        if hasattr(result, "rowcount"):
            return result.rowcount
        elif hasattr(result, "__len__"):
            return len(result)
        else:
            return 0

    def _observability_scope(self, context: ExecutionContext, scope_name: str):
        """Create observability scope for measurements."""
        observability = getattr(context, "observability", None)
        if observability is not None:
            return observability.measure_scope(scope_name)
        else:
            # Fallback no-op context manager
            from contextlib import nullcontext

            return nullcontext()
