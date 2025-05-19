"""SQL Generator for SQLFlow.

This module provides classes for generating SQL from operation definitions.
It handles the conversion of operations to executable SQL statements with proper
template substitution and SQL dialect adaptations.
"""

import re
from datetime import datetime
from typing import Any, Dict

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class SQLGenerator:
    """Generates executable SQL for operations.

    This class is responsible for converting operation definitions
    into executable SQL statements, with proper template substitution
    and SQL dialect adaptations.

    Args:
        dialect: SQL dialect to use.
    """

    def __init__(self, dialect: str = "duckdb"):
        """Initialize the SQL generator.

        Args:
            dialect: SQL dialect to use.
        """
        self.dialect = dialect
        logger.debug(f"SQL Generator initialized with dialect: {dialect}")

    def generate_operation_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for an operation.

        Args:
            operation: Operation definition.
            context: Execution context with variables.

        Returns:
            Executable SQL string.
        """
        op_type = operation.get("type", "unknown")
        op_id = operation.get("id", "unknown")
        depends_on = operation.get("depends_on", [])

        logger.debug(f"Generating SQL for operation {op_id} of type {op_type}")

        # Generate header comments
        header = [
            f"-- Operation: {op_id}",
            f"-- Generated at: {datetime.now().isoformat()}",
            f"-- Dependencies: {', '.join(depends_on)}",
        ]

        if op_type == "source_definition":
            sql = self._generate_source_sql(operation, context)
        elif op_type == "transform":
            sql = self._generate_transform_sql(operation, context)
        elif op_type == "load":
            sql = self._generate_load_sql(operation, context)
        elif op_type == "export":
            sql = self._generate_export_sql(operation, context)
        else:
            logger.warning(f"Unknown operation type: {op_type}, using raw query")
            sql = operation.get("query", "")
            if isinstance(sql, dict):
                sql = sql.get("query", "")

        # Apply variable substitution
        original_sql_length = len(sql) if sql else 0
        sql = self._substitute_variables(sql, context.get("variables", {}))

        if sql and len(sql) != original_sql_length:
            logger.debug(f"Variable substitution applied to SQL for operation {op_id}")

        # Complete the SQL with header
        result = "\n".join(header) + "\n\n" + sql

        # Log a truncated version of the SQL to avoid very long logs
        if sql:
            preview = sql[:100] + "..." if len(sql) > 100 else sql
            logger.info(f"Generated SQL for {op_type} operation {op_id}: {preview}")
        else:
            logger.warning(f"Empty SQL generated for operation {op_id}")

        return result

    def _generate_source_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for a source operation.

        Args:
            operation: Source operation definition.
            context: Execution context.

        Returns:
            SQL for the source.
        """
        source_type = operation.get("source_connector_type", "").upper()
        query = operation.get("query", {})
        name = operation.get("name", "unnamed_source")

        logger.debug(f"Generating source SQL for {name}, type: {source_type}")

        if source_type == "CSV":
            path = query.get("path", "")
            has_header = query.get("has_header", True)
            logger.debug(f"CSV source: path={path}, has_header={has_header}")
            return f"""-- Source type: CSV
CREATE OR REPLACE TABLE {name} AS
SELECT * FROM read_csv_auto('{path}', 
                           header={str(has_header).lower()});"""

        elif source_type == "POSTGRESQL":
            pg_query = query.get("query", "")
            logger.debug(f"PostgreSQL source: query length={len(pg_query)}")
            return f"""-- Source type: PostgreSQL
CREATE OR REPLACE TABLE {name} AS
SELECT * FROM {pg_query};"""

        else:
            logger.warning(f"Unknown source type: {source_type}, using raw query")
            return f"-- Unknown source type: {source_type}\n{query.get('query', '')}"

    def _generate_transform_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for a transformation.

        Args:
            operation: Transform operation definition.
            context: Execution context.

        Returns:
            SQL for the transformation.
        """
        materialized = operation.get("materialized", "table").upper()
        name = operation.get("name", "unnamed")
        query = operation.get("query", "")

        logger.debug(
            f"Generating transform SQL for {name}, materialization: {materialized}"
        )

        if materialized == "TABLE":
            return f"""-- Materialization: TABLE
CREATE OR REPLACE TABLE {name} AS
{query};

-- Statistics collection
ANALYZE {name};"""

        elif materialized == "VIEW":
            return f"""-- Materialization: VIEW
CREATE OR REPLACE VIEW {name} AS
{query};"""

        else:
            logger.warning(
                f"Unknown materialization type: {materialized}, using raw query"
            )
            return f"""-- Materialization: {materialized}
{query};"""

    def _generate_load_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for a load operation.

        Args:
            operation: Load operation definition.
            context: Execution context.

        Returns:
            SQL for the load operation.
        """
        query = operation.get("query", {})
        source_name = query.get("source_name", "")
        table_name = query.get("table_name", "")

        logger.debug(f"Generating load SQL: {source_name} -> {table_name}")

        return f"""-- Load operation
CREATE OR REPLACE TABLE {table_name} AS
SELECT * FROM {source_name};"""

    def _generate_export_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for an export operation.

        Args:
            operation: Export operation definition.
            context: Execution context.

        Returns:
            SQL for the export operation.
        """
        query = operation.get("query", {})
        source_query = query.get("query", "")
        destination = query.get("destination_uri", "")
        export_type = query.get("type", "CSV").upper()

        logger.debug(
            f"Generating export SQL: type={export_type}, destination={destination}"
        )

        if export_type == "CSV":
            return f"""-- Export to CSV
COPY (
{source_query}
) TO '{destination}' (FORMAT CSV, HEADER);"""

        else:
            logger.warning(
                f"Export type not explicitly supported: {export_type}, using generic format"
            )
            return f"""-- Export type: {export_type}
-- Destination: {destination}
{source_query}"""

    def _substitute_variables(self, sql: str, variables: Dict[str, Any]) -> str:
        """Substitute variables in SQL.

        Args:
            sql: SQL string with variables.
            variables: Dictionary of variables.

        Returns:
            SQL with variables substituted.
        """
        if not sql:
            return ""

        if not variables:
            logger.debug("No variables to substitute in SQL")
            return sql

        logger.debug(f"Substituting {len(variables)} variables in SQL")

        # Track variable replacements for logging
        replacements_made = 0

        # First pass: replace variables that have values
        result = sql
        for var_name, var_value in variables.items():
            pattern = (
                r"\${"
                + re.escape(var_name)
                + r"}|\${"
                + re.escape(var_name)
                + r"\|[^}]*}"
            )
            # Convert Python objects to SQL literals
            if isinstance(var_value, str):
                replacement = f"'{var_value}'"
            elif isinstance(var_value, bool):
                replacement = str(var_value).lower()
            else:
                replacement = str(var_value)

            # Count replacements for this variable
            count_before = len(re.findall(pattern, result))
            result = re.sub(pattern, replacement, result)
            count_after = len(re.findall(pattern, result))
            var_replacements = count_before - count_after

            if var_replacements > 0:
                replacements_made += var_replacements
                logger.debug(
                    f"Variable '{var_name}' replaced {var_replacements} times with {replacement}"
                )

        # Second pass: handle variables with default values
        def replace_with_default(match):
            # Parse variable and default value
            var_expr = match.group(0)
            without_delimiters = var_expr[2:-1]  # Remove ${ and }
            if "|" in without_delimiters:
                # Extract default value
                var_name, default_value = without_delimiters.split("|", 1)
                var_name = var_name.strip()
                default_value = default_value.strip()

                # Handle quoted default values
                if (default_value.startswith("'") and default_value.endswith("'")) or (
                    default_value.startswith('"') and default_value.endswith('"')
                ):
                    # Keep as is - already quoted
                    pass
                elif (
                    default_value.lower() == "true" or default_value.lower() == "false"
                ):
                    # Boolean value - lowercase in SQL
                    default_value = default_value.lower()
                elif re.match(r"^-?\d+(\.\d+)?$", default_value):
                    # Numeric value - keep as is
                    pass
                else:
                    # Quote string values
                    default_value = f"'{default_value}'"

                logger.debug(f"Using default value for '{var_name}': {default_value}")
                return default_value

            # If no default value but still not replaced, use NULL
            var_name = without_delimiters.strip()
            logger.warning(
                f"No value or default found for variable: '{var_name}', using NULL"
            )
            return "NULL"

        # Apply default values
        pattern = r"\${[^}]*\|[^}]*}"
        result_with_defaults = re.sub(pattern, replace_with_default, result)

        # Handle any remaining ${var} without defaults or values
        pattern = r"\${[^}]*}"
        final_result = re.sub(
            pattern, lambda m: replace_with_default(m), result_with_defaults
        )

        if final_result != result_with_defaults:
            logger.warning(
                f"Some variables had no values or defaults and were replaced with NULL"
            )

        if replacements_made > 0:
            logger.debug(
                f"Completed variable substitution: {replacements_made} total replacements"
            )

        return final_result
