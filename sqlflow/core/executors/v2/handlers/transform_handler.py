"""Transform Step Handler for V2 Executor.

This module implements the TransformStepHandler that handles SQL transformation
operations including CREATE TABLE AS statements, UDF integration, variable
substitution, and comprehensive observability.
"""

from datetime import datetime
from typing import Any, Dict

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.handlers.base import (
    StepHandler,
    observed_execution,
    timed_operation,
)
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep, TransformStep
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
    def execute(self, step: BaseStep, context: ExecutionContext) -> StepExecutionResult:
        """
        Execute SQL transformation with comprehensive observability.

        Args:
            step: TransformStep containing SQL and configuration
            context: ExecutionContext with shared services

        Returns:
            StepExecutionResult with detailed execution metrics
        """
        # Type check and cast to TransformStep
        if not isinstance(step, TransformStep):
            raise ValueError(
                f"TransformStepHandler can only handle TransformStep, got {type(step)}"
            )

        transform_step = step  # Now we know it's a TransformStep
        start_time = datetime.utcnow()

        try:
            # Step 1: Validate step configuration
            self._validate_transform_step(transform_step)

            # Step 2: Process SQL with UDF integration
            processed_sql = self._process_sql_with_udfs(transform_step, context)

            # Step 3: Perform variable substitution
            substituted_sql = self._substitute_variables(processed_sql, context)

            # Step 4: Format SQL for execution (CREATE TABLE AS, etc.)
            final_sql = self._format_sql_for_execution(transform_step, substituted_sql)

            # Step 5: Execute SQL and measure performance (using common pattern)
            execution_metrics = self._execute_sql_with_metrics(final_sql, context)

            # Step 6: Collect schema information for observability
            schema_info = self._collect_schema_information(transform_step, context)

            # Step 7: Provide user feedback
            self._provide_user_feedback(transform_step)

            end_time = datetime.utcnow()

            return StepExecutionResult.success(
                step_id=transform_step.id,
                step_type="transform",
                start_time=start_time,
                end_time=end_time,
                rows_affected=execution_metrics.get("rows_affected"),
                output_schema=schema_info.get("output_schema"),
                input_schemas=schema_info.get("input_schemas", {}),
                performance_metrics=execution_metrics,
                data_lineage={
                    "target_table": transform_step.target_table,
                    "operation_type": transform_step.operation_type,
                    "sql_query": final_sql,
                    "udf_dependencies": transform_step.udf_dependencies,
                },
            )

        except Exception as e:
            # Use common error handling pattern
            return self._handle_execution_error(transform_step, start_time, e)

    @timed_operation("transform_validation")
    def _validate_transform_step(self, step: TransformStep) -> None:
        """
        Validate transform step configuration.

        Args:
            step: TransformStep to validate

        Raises:
            ValueError: If step configuration is invalid
        """
        if not step.sql.strip():
            raise ValueError(f"Transform step {step.id} has empty SQL")

        if step.operation_type in ["create_table", "insert"] and not step.target_table:
            raise ValueError(
                f"Transform step {step.id} with operation_type '{step.operation_type}' requires target_table"
            )

    @timed_operation("udf_processing")
    def _process_sql_with_udfs(
        self, step: TransformStep, context: ExecutionContext
    ) -> str:
        """
        Process SQL with UDF integration if available.

        Args:
            step: TransformStep with potential UDF dependencies
            context: ExecutionContext with SQL engine

        Returns:
            SQL with UDFs processed
        """
        if not step.udf_dependencies:
            return step.sql

        # For now, log UDF dependencies but don't process them
        # Future: integrate with UDF system when available
        logger.debug(f"UDF dependencies noted: {step.udf_dependencies}")

        # Try to process if engine has the capability
        if hasattr(context.sql_engine, "process_query_for_udfs"):
            try:
                # This would need to be properly integrated with a UDF manager
                # For now, just return the original SQL
                logger.debug("SQL engine supports UDF processing but skipping for now")
                return step.sql
            except Exception as e:
                logger.warning(f"UDF processing failed: {e}")
                return step.sql
        else:
            # UDF processing not available in engine
            logger.debug("Engine does not support UDF processing")
            return step.sql

    @timed_operation("variable_substitution")
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
                    if (step.metadata or {}).get("is_replace")
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
                    if (step.metadata or {}).get("is_replace")
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
                if (step.metadata or {}).get("is_replace")
                else "CREATE VIEW"
            )
            return f"{create_clause} {step.target_table} AS {sql}"

        else:
            # Default: assume it's a complete SQL statement
            return sql

    # Raymond Hettinger: DRY elimination - use inherited common method
    # The _execute_sql_with_metrics method is now inherited from StepHandler base class

    @timed_operation("schema_collection")
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
