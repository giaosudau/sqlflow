"""Data-related exceptions for SQLFlow V2 Executor.

These exceptions handle errors during data processing, validation, and transformation.
"""

from typing import Any, Dict, List, Optional

from .base import SQLFlowError


class DataValidationError(SQLFlowError):
    """Error during data validation."""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        validation_rule: Optional[str] = None,
        failed_records: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Review data quality at the source",
            "Check validation rules for correctness",
            "Consider data cleaning steps before validation",
        ]

        if validation_rule:
            actions.append(f"Review validation rule: {validation_rule}")

        if failed_records:
            actions.append(f"Examine the {failed_records} failed records for patterns")

        validation_context = {}
        if table_name:
            validation_context["table_name"] = table_name
        if column_name:
            validation_context["column_name"] = column_name
        if validation_rule:
            validation_context["validation_rule"] = validation_rule
        if failed_records:
            validation_context["failed_records"] = failed_records

        if context:
            validation_context.update(context)

        super().__init__(
            message=message,
            context=validation_context,
            suggested_actions=actions,
            recoverable=True,
        )


class DataTransformationError(SQLFlowError):
    """Error during data transformation."""

    def __init__(
        self,
        message: str,
        transformation_type: Optional[str] = None,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        sql_query: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Check transformation logic and SQL syntax",
            "Verify source data schema and content",
            "Ensure target table structure is compatible",
        ]

        if sql_query:
            actions.append("Review SQL query for errors")

        transform_context = {}
        if transformation_type:
            transform_context["transformation_type"] = transformation_type
        if source_table:
            transform_context["source_table"] = source_table
        if target_table:
            transform_context["target_table"] = target_table
        if sql_query:
            # Truncate long SQL queries for readability
            display_query = (
                sql_query[:200] + "..." if len(sql_query) > 200 else sql_query
            )
            transform_context["sql_query"] = display_query

        if context:
            transform_context.update(context)

        super().__init__(
            message=message,
            context=transform_context,
            suggested_actions=actions,
            recoverable=True,
        )


class DataConnectorError(SQLFlowError):
    """Error in data connector operations."""

    def __init__(
        self,
        connector_type: str,
        operation: str,
        message: str,
        source_config: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            f"Check {connector_type} connector configuration",
            "Verify connection credentials and permissions",
            "Test connectivity to data source",
        ]

        if operation == "read":
            actions.extend(
                [
                    "Ensure source data exists and is accessible",
                    "Check file format and encoding",
                ]
            )
        elif operation == "write":
            actions.extend(
                [
                    "Verify write permissions to destination",
                    "Check available disk space",
                ]
            )

        connector_context = {
            "connector_type": connector_type,
            "operation": operation,
        }

        if source_config:
            # Remove sensitive information from context
            safe_config = {
                k: v
                for k, v in source_config.items()
                if k.lower() not in ["password", "token", "secret", "key"]
            }
            connector_context["source_config"] = safe_config

        if context:
            connector_context.update(context)

        super().__init__(
            message=f"{connector_type} connector {operation} failed: {message}",
            context=connector_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.connector_type = connector_type
        self.operation = operation


class SchemaValidationError(SQLFlowError):
    """Error during schema validation."""

    def __init__(
        self,
        message: str,
        expected_schema: Optional[Dict[str, Any]] = None,
        actual_schema: Optional[Dict[str, Any]] = None,
        missing_columns: Optional[List[str]] = None,
        extra_columns: Optional[List[str]] = None,
        type_mismatches: Optional[Dict[str, str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Review expected vs actual schema",
            "Check data source for schema changes",
            "Consider schema evolution strategies",
        ]

        if missing_columns:
            actions.append(f"Add missing columns: {', '.join(missing_columns)}")

        if extra_columns:
            actions.append(f"Handle extra columns: {', '.join(extra_columns)}")

        if type_mismatches:
            actions.append("Resolve data type mismatches")

        schema_context = {}
        if expected_schema:
            schema_context["expected_schema"] = expected_schema
        if actual_schema:
            schema_context["actual_schema"] = actual_schema
        if missing_columns:
            schema_context["missing_columns"] = missing_columns
        if extra_columns:
            schema_context["extra_columns"] = extra_columns
        if type_mismatches:
            schema_context["type_mismatches"] = type_mismatches

        if context:
            schema_context.update(context)

        super().__init__(
            message=message,
            context=schema_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.missing_columns = missing_columns or []
        self.extra_columns = extra_columns or []
        self.type_mismatches = type_mismatches or {}
