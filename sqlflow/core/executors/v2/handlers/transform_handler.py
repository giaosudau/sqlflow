"""Transform Step Handler for V2 Executor.

This module implements the TransformStepHandler that handles SQL transformation
operations including CREATE TABLE AS statements, UDF integration, variable
substitution, and comprehensive observability.
"""

import time
from datetime import datetime
from typing import Any, Dict

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import TransformStep
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TransformStepHandler(StepHandler):
    """
    Handles the execution of SQL transformation operations.

    This handler is responsible for:
    - Executing SQL queries with proper CREATE TABLE AS formatting
    - Processing UDF dependencies and integration
    - Performing variable substitution in SQL
    - Managing CREATE vs CREATE OR REPLACE operations
    - Providing detailed performance metrics and schema tracking
    - Ensuring SQL error propagation and recovery

    The handler follows the Single Responsibility Principle by focusing solely
    on SQL transformation logic while delegating engine operations, UDF processing,
    and variable management to their respective services.
    """

    STEP_TYPE = "transform"

    @observed_execution("transform")
    def execute(
        self, step: TransformStep, context: ExecutionContext
    ) -> StepExecutionResult:
        """
        Execute a SQL transformation with comprehensive observability.

        Args:
            step: TransformStep containing SQL and configuration
            context: ExecutionContext with shared services

        Returns:
            StepExecutionResult with detailed execution metrics
        """
        start_time = datetime.utcnow()
        logger.debug(f"Executing TransformStep {step.id}: {step.operation_type}")

        try:
            # Step 1: Validate step configuration
            self._validate_transform_step(step)

            # Step 2: Process SQL with UDF integration
            processed_sql = self._process_sql_with_udfs(step, context)

            # Step 3: Perform variable substitution
            substituted_sql = self._substitute_variables(processed_sql, context)

            # Step 4: Format SQL for execution (CREATE TABLE AS, etc.)
            final_sql = self._format_sql_for_execution(step, substituted_sql)

            # Step 5: Execute SQL and measure performance
            execution_metrics = self._execute_sql_with_metrics(final_sql, context)

            # Step 6: Collect schema information for observability
            schema_info = self._collect_schema_information(step, context)

            # Step 7: Provide user feedback
            self._provide_user_feedback(step)

            end_time = datetime.utcnow()

            return StepExecutionResult.success(
                step_id=step.id,
                step_type="transform",
                start_time=start_time,
                end_time=end_time,
                rows_affected=execution_metrics.get("rows_affected"),
                output_schema=schema_info.get("output_schema"),
                input_schemas=schema_info.get("input_schemas", {}),
                performance_metrics=execution_metrics,
                data_lineage={
                    "target_table": step.target_table,
                    "operation_type": step.operation_type,
                    "sql_query": final_sql,
                    "udf_dependencies": step.udf_dependencies,
                },
            )

        except Exception as e:
            # Use common error handling pattern
            return self._handle_execution_error(step, start_time, e)

    def _validate_transform_step(self, step: TransformStep) -> None:
        """
        Validate TransformStep configuration before execution.

        Args:
            step: TransformStep to validate

        Raises:
            ValueError: If step configuration is invalid
        """
        if not step.sql or not step.sql.strip():
            raise ValueError(f"TransformStep {step.id}: SQL cannot be empty")

        # Operations that require target_table
        operations_requiring_target = {"create_table", "insert", "create_view"}
        if step.operation_type in operations_requiring_target and not step.target_table:
            raise ValueError(
                f"TransformStep {step.id}: operation_type '{step.operation_type}' "
                f"requires target_table"
            )

    def _process_sql_with_udfs(
        self, step: TransformStep, context: ExecutionContext
    ) -> str:
        """
        Process SQL to integrate UDF dependencies.

        Args:
            step: TransformStep with potential UDF dependencies
            context: ExecutionContext with UDF manager

        Returns:
            SQL with UDF calls properly processed
        """
        if not step.udf_dependencies:
            return step.sql

        # Check if UDF manager is available in context
        udf_manager = getattr(context, "udf_manager", None)
        if udf_manager:
            try:
                processed_sql = udf_manager.process_query_for_udfs(step.sql)
                if processed_sql != step.sql:
                    logger.debug(f"UDF processing: {step.sql} -> {processed_sql}")
                return processed_sql
            except Exception as e:
                logger.warning(f"UDF processing failed: {e}")
                return step.sql
        else:
            # UDF manager not available in V2 context yet
            logger.debug(
                f"UDF dependencies specified but UDF manager not available: {step.udf_dependencies}"
            )
            return step.sql

    def _substitute_variables(self, sql: str, context: ExecutionContext) -> str:
        """
        Perform variable substitution in SQL with explicit method resolution.

        Args:
            sql: SQL string potentially containing ${variable} patterns
            context: ExecutionContext with variable manager

        Returns:
            SQL with variables substituted
        """
        if not hasattr(context, "variable_manager"):
            return sql

        try:
            # Use getattr with default for cleaner method resolution
            substitute_method = getattr(
                context.variable_manager,
                "substitute_variables",
                getattr(context.variable_manager, "substitute", None),
            )
            if substitute_method is None:
                logger.warning("Variable manager has no substitute method")
                return sql

            substituted_sql = substitute_method(sql)
            if substituted_sql != sql:
                logger.debug(f"Variable substitution: {sql} -> {substituted_sql}")
            return substituted_sql

        except Exception as e:
            logger.warning(f"Variable substitution failed: {e}")
            return sql

    def _format_sql_for_execution(self, step: TransformStep, sql: str) -> str:
        """
        Format SQL for execution based on operation type.

        Args:
            step: TransformStep with operation configuration
            sql: SQL to format

        Returns:
            Properly formatted SQL for execution
        """
        if step.operation_type == "select":
            # Direct SELECT query execution
            return sql

        elif step.operation_type == "create_table":
            # Handle materialization type
            if step.materialization == "view":
                create_clause = (
                    "CREATE OR REPLACE VIEW"
                    if step.metadata.get("is_replace")
                    else "CREATE VIEW"
                )
                if (
                    sql.strip()
                    .upper()
                    .startswith(("CREATE", "INSERT", "UPDATE", "DELETE"))
                ):
                    return sql
                else:
                    return f"{create_clause} {step.target_table} AS {sql}"
            else:
                # CREATE TABLE AS operation
                create_clause = (
                    "CREATE OR REPLACE TABLE"
                    if step.metadata.get("is_replace")
                    else "CREATE TABLE"
                )
                if (
                    sql.strip()
                    .upper()
                    .startswith(("CREATE", "INSERT", "UPDATE", "DELETE"))
                ):
                    # SQL already has DDL/DML keywords
                    return sql
                else:
                    # Wrap in CREATE TABLE AS
                    return f"{create_clause} {step.target_table} AS {sql}"

        elif step.operation_type == "insert":
            # INSERT INTO operation
            return f"INSERT INTO {step.target_table} {sql}"

        elif step.operation_type == "create_view":
            # CREATE VIEW operation (deprecated - use materialization instead)
            create_clause = (
                "CREATE OR REPLACE VIEW"
                if step.metadata.get("is_replace")
                else "CREATE VIEW"
            )
            return f"{create_clause} {step.target_table} AS {sql}"

        else:
            # Default: assume it's a complete SQL statement
            return sql

    def _execute_sql_with_metrics(
        self, sql: str, context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Execute SQL and collect performance metrics.

        Args:
            sql: SQL to execute
            context: ExecutionContext with SQL engine

        Returns:
            Dictionary containing execution metrics

        Raises:
            Exception: If SQL execution fails
        """
        sql_start_time = time.monotonic()

        try:
            # Execute the SQL
            result = context.sql_engine.execute_query(sql)

            sql_execution_time = (time.monotonic() - sql_start_time) * 1000

            # Collect metrics
            metrics = {
                "sql_execution_time_ms": sql_execution_time,
                "sql_query": sql,
                "sql_length": len(sql),
            }

            # Try to get row count if available
            if hasattr(result, "rowcount") and result.rowcount >= 0:
                metrics["rows_affected"] = result.rowcount
            else:
                metrics["rows_affected"] = None

            logger.debug(f"SQL executed successfully in {sql_execution_time:.2f}ms")
            return metrics

        except Exception as e:
            sql_execution_time = (time.monotonic() - sql_start_time) * 1000
            logger.error(f"SQL execution failed after {sql_execution_time:.2f}ms: {e}")
            raise

    def _collect_schema_information(
        self, step: TransformStep, context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Collect schema information for observability.

        Args:
            step: TransformStep with target table
            context: ExecutionContext with SQL engine

        Returns:
            Dictionary containing schema information
        """
        schema_info = {"input_schemas": {}, "output_schema": None}

        if not step.target_table:
            return schema_info

        try:
            # Get output schema
            if hasattr(context.sql_engine, "get_table_schema"):
                output_schema = context.sql_engine.get_table_schema(step.target_table)
                schema_info["output_schema"] = output_schema
                logger.debug(f"Collected output schema for {step.target_table}")

        except Exception as e:
            logger.warning(f"Failed to collect schema information: {e}")

        return schema_info

    def _provide_user_feedback(self, step: TransformStep) -> None:
        """
        Provide user-friendly feedback about the transformation.

        Args:
            step: TransformStep that was executed
        """
        if step.target_table:
            print(f"🔄 Created {step.target_table}")
        else:
            print(f"🔄 Executed transform {step.id}")

        logger.debug(f"Transform step {step.id} completed successfully")
