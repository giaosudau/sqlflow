"""SQL Generator for SQLFlow.

This module provides classes for generating SQL from operation definitions.
It handles the conversion of operations to executable SQL statements with proper
template substitution and SQL dialect adaptations.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger(__name__)


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
            sql = operation.get("query", "")
            if isinstance(sql, dict):
                sql = sql.get("query", "")

        # Apply variable substitution
        sql = self._substitute_variables(sql, context.get("variables", {}))

        # Complete the SQL with header
        return "\n".join(header) + "\n\n" + sql

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

        if source_type == "CSV":
            return f"""-- Source type: CSV
CREATE OR REPLACE TABLE {operation.get('name')} AS
SELECT * FROM read_csv_auto('{query.get('path')}', 
                           header={str(query.get('has_header', True)).lower()});"""

        elif source_type == "POSTGRESQL":
            return f"""-- Source type: PostgreSQL
CREATE OR REPLACE TABLE {operation.get('name')} AS
SELECT * FROM {query.get('query', '')};"""

        else:
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

        if export_type == "CSV":
            return f"""-- Export to CSV
COPY (
{source_query}
) TO '{destination}' (FORMAT CSV, HEADER);"""

        else:
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

        # First pass: replace variables that have values
        for name, value in variables.items():
            pattern = r"\$\{" + name + r"(?:\|[^}]*)?\}"
            sql = re.sub(pattern, str(value), sql)

        # Second pass: handle default values for any remaining variables
        def replace_with_default(match):
            parts = match.group(0)[2:-1].split("|")
            if len(parts) > 1:
                return parts[1]
            return match.group(0)

        sql = re.sub(r"\$\{[^}]*\}", replace_with_default, sql)
        return sql
