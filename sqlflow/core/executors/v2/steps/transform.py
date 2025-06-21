"""Transform Step Executor following Single Responsibility Principle.

Handles SQL transformation with UDF support and variable substitution.
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
class TransformStepExecutor(BaseStepExecutor):
    """Executes transform steps - SINGLE responsibility.

    Handles only transform operations:
    - SQL execution with UDF support
    - Variable substitution in SQL
    - Target table creation from queries
    """

    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the step."""
        if hasattr(step, "type"):
            return step.type == "transform"
        elif isinstance(step, dict):
            return step.get("type") == "transform"
        return False

    def execute(self, step: Step, context: ExecutionContext) -> StepExecutionResult:
        """Execute transform step with focused implementation."""
        start_time = time.time()
        step_id = getattr(step, "id", "unknown")

        try:
            logger.info(f"Executing transform step: {step_id}")

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

    def _extract_transform_details(self, step: Step) -> tuple[str, str, list]:
        """Extract transform step details with validation."""
        if hasattr(step, "sql"):
            sql = step.sql
            target_table = getattr(step, "target_table", None)
            udf_dependencies = getattr(step, "udf_dependencies", [])
        elif isinstance(step, dict):
            sql = step.get("sql", "")
            target_table = step.get("target_table")
            udf_dependencies = step.get("udf_dependencies", [])
        else:
            raise ValueError(f"Invalid step format: {type(step)}")

        # Validation
        if not sql or not sql.strip():
            raise ValueError("Transform step SQL cannot be empty")

        return sql, target_table, udf_dependencies

    def _register_udfs(self, udf_dependencies: list, context: ExecutionContext):
        """Register required UDFs for this transformation."""
        if not udf_dependencies:
            return

        # Get UDF registry from context
        if hasattr(context, "udf_registry"):
            udf_registry = context.udf_registry
            session = context.session

            for udf_name in udf_dependencies:
                if udf_registry.has_udf(udf_name):
                    udf = udf_registry.get_udf(udf_name)
                    session.register_udf(udf_name, udf)
                    logger.debug(f"Registered UDF: {udf_name}")
                else:
                    logger.warning(f"UDF not found in registry: {udf_name}")

    def _substitute_variables(self, sql: str, context: ExecutionContext) -> str:
        """Substitute variables in SQL using pure function approach."""
        variables = context.variables
        if not variables:
            return sql

        # Use functional variable substitution (from Week 1-2 refactoring)
        try:
            from ..variables.substitution import substitute_variables

            return substitute_variables(sql, variables)
        except ImportError:
            # Fallback to simple string substitution
            logger.warning("Using fallback variable substitution")
            processed_sql = sql
            for key, value in variables.items():
                placeholder = f"${{{key}}}"
                processed_sql = processed_sql.replace(placeholder, str(value))
            return processed_sql

    def _execute_sql(
        self, sql: str, target_table: str, context: ExecutionContext
    ) -> dict:
        """Execute SQL with proper error handling."""
        session = context.session

        try:
            if target_table:
                # This is a CREATE TABLE AS operation
                create_sql = f"CREATE TABLE {target_table} AS ({sql})"
                result = session.execute(create_sql)
            else:
                # Direct SQL execution
                result = session.execute(sql)

            # Get affected rows count
            rows_affected = getattr(result, "rowcount", 0)

            return {"rows_affected": rows_affected, "success": True}

        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            logger.error(f"SQL: {sql}")
            raise

    def _observability_scope(self, context: ExecutionContext, scope_name: str):
        """Create observability scope for measurements."""
        if hasattr(context, "observability"):
            return context.observability.measure_scope(scope_name)
        else:
            # Fallback no-op context manager
            from contextlib import nullcontext

            return nullcontext()
