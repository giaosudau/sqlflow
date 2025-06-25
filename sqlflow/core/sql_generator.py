"""SQL Generator for SQLFlow.

This module provides classes for generating SQL from operation definitions.
It handles the conversion of operations to executable SQL statements with proper
template substitution and SQL dialect adaptations.
"""

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
    ----
        dialect: SQL dialect to use.

    """

    def __init__(self, dialect: str = "duckdb"):
        """Initialize the SQL generator.

        Args:
        ----
            dialect: SQL dialect to use.

        """
        self.dialect = dialect
        # Track logged warnings to prevent duplicates
        self._logged_warnings = set()
        # Track warning counts for summary
        self._warning_counts = {}
        logger.debug(f"SQL Generator initialized with dialect: {dialect}")

    def _log_warning_once(self, key: str, message: str, level: str = "warning") -> None:
        """Log a warning once to avoid duplicates.

        Args:
        ----
            key: Unique key for the warning.
            message: Warning message.
            level: Log level (warning or debug).

        """
        if key not in self._logged_warnings:
            self._logged_warnings.add(key)
            if key not in self._warning_counts:
                self._warning_counts[key] = 0
            self._warning_counts[key] += 1

            if level == "debug":
                logger.debug(message)
            else:
                logger.warning(message)
        else:
            self._warning_counts[key] += 1

    def generate_operation_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for an operation.

        Args:
        ----
            operation: Operation definition.
            context: Execution context with variables.

        Returns:
        -------
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
            warning_key = f"unknown_op_type:{op_type}"
            self._log_warning_once(
                warning_key, f"Unknown operation type: {op_type}, using raw query"
            )

            sql = operation.get("query", "")
            if isinstance(sql, dict):
                sql = sql.get("query", "")

        # Apply variable substitution
        original_sql_length = len(sql) if sql else 0
        sql, total_replacements = self._substitute_variables(
            sql, context.get("variables", {})
        )

        if sql and len(sql) != original_sql_length:
            logger.debug(f"Variable substitution applied to SQL for operation {op_id}")

        # Complete the SQL with header
        result = "\n".join(header) + "\n\n" + sql

        # Log a truncated version of the SQL to avoid very long logs
        if sql:
            preview = sql[:100] + "..." if len(sql) > 100 else sql
            logger.debug(f"Generated SQL for {op_type} operation {op_id}: {preview}")
        else:
            warning_key = f"empty_sql:{op_id}"
            self._log_warning_once(
                warning_key, f"Empty SQL generated for operation {op_id}"
            )

        return result

    def _generate_source_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for a source operation.

        SQLFlow supports two distinct syntax patterns for SOURCE statements:

        1. Profile-based syntax (recommended for production):
           SOURCE name FROM "connector_name" OPTIONS { ... };
           Example: SOURCE sales FROM "postgres" OPTIONS { "table": "sales" };

        2. Traditional syntax:
           SOURCE name TYPE connector_type PARAMS { ... };
           Example: SOURCE sales TYPE POSTGRES PARAMS { "host": "localhost", "table": "sales" };

        The two syntax patterns CANNOT be mixed. Users must choose one pattern per SOURCE statement.

        Args:
        ----
            operation: Source operation definition.
            context: Execution context.

        Returns:
        -------
            SQL for the source.

        """
        # Get connector type and normalize case for consistent handling
        source_type = operation.get(
            "source_connector_type", operation.get("connector_type", "")
        ).lower()
        query = operation.get("query", {})
        name = operation.get("name", "unnamed_source")
        operation.get("id", "unknown")

        # Get profile connector name outside the conditional block so it's always available
        profile_connector_name = operation.get("profile_connector_name", "")

        # Handle profile-based source definition (FROM syntax)
        if operation.get("is_from_profile", False):
            # Get connector type from profile for profile-based sources
            profile = context.get("profile", {})
            profile_connectors = profile.get("connectors", {})

            if profile_connector_name and profile_connector_name in profile_connectors:
                profile_connector = profile_connectors.get(profile_connector_name, {})
                source_type = profile_connector.get("type", "").lower()
                logger.debug(f"Using connector type from profile: {source_type}")
            else:
                warning_key = f"profile_connector_not_found:{profile_connector_name}"
                self._log_warning_once(
                    warning_key,
                    f"Profile connector '{profile_connector_name}' not found in profile. "
                    f"Check that '{profile_connector_name}' is defined in your profile's 'connectors' section.",
                    level="debug",
                )

        logger.debug(f"Generating source SQL for {name}, type: {source_type}")

        if source_type == "csv":
            path = query.get("path", "")
            has_header = query.get("has_header", True)
            logger.debug(f"CSV source: path={path}, has_header={has_header}")
            return f"""-- Source type: CSV
CREATE OR REPLACE TABLE {name} AS
SELECT * FROM read_csv_auto('{path}', 
                           header={str(has_header).lower()});"""

        elif source_type in ["postgresql", "postgres"]:
            pg_query = query.get("query", "")
            logger.debug(f"PostgreSQL source: query length={len(pg_query)}")
            return f"""-- Source type: PostgreSQL
CREATE OR REPLACE TABLE {name} AS
SELECT * FROM {pg_query};"""

        else:
            # Log warning only once per source type to prevent duplicate logs
            warning_key = f"unknown_source_type:{source_type}:{name}"

            # Create a more helpful error message for the tests to check
            supported_types = ["csv", "postgres", "postgresql"]
            supported_types_str = ", ".join(supported_types)

            error_msg = (
                f"Unknown or unsupported source connector type: '{source_type}' for source '{name}'.\n"
                f"Supported connector types: {supported_types_str}\n"
            )

            # Add specific guidance based on syntax pattern
            if operation.get("is_from_profile", False):
                error_msg += (
                    f"Check that connector '{profile_connector_name}' in your profile "
                    f"has a valid 'type' setting.\n"
                )
            else:
                error_msg += (
                    "Make sure you're using the correct connector type in your SOURCE statement:\n"
                    "SOURCE name TYPE connector_type PARAMS { ... };\n"
                )

            self._log_warning_once(warning_key, error_msg, level="debug")

            return f"-- Unknown source type: {source_type}\n-- Check your connector configuration\n{query.get('query', '')}"

    def reset_warning_tracking(self) -> None:
        """Reset warning tracking data.

        Call this at the beginning of a new pipeline execution to clear warnings
        from previous runs.
        """
        if self._warning_counts:
            logger.debug(
                f"Resetting warning tracking. Previous warnings: {self._warning_counts}"
            )
        self._logged_warnings.clear()
        self._warning_counts.clear()

    def get_warning_summary(self) -> Dict[str, int]:
        """Get a summary of warnings that were logged and suppressed.

        Returns
        -------
            Dictionary mapping warning keys to count of occurrences

        """
        return self._warning_counts.copy()

    def _generate_transform_sql(
        self, operation: Dict[str, Any], context: Dict[str, Any]
    ) -> str:
        """Generate SQL for a transformation.

        Args:
        ----
            operation: Transform operation definition.
            context: Execution context.

        Returns:
        -------
            SQL for the transformation.

        """
        materialized = operation.get("materialized", "table").upper()
        name = operation.get("name", "unnamed")
        query = operation.get("query", "")
        # For backward compatibility, check is_replace but default to True for consistency
        is_replace = operation.get("is_replace", True)

        logger.debug(
            f"Generating transform SQL for {name}, materialization: {materialized}, is_replace: {is_replace}"
        )

        if materialized == "TABLE":
            # Use CREATE OR REPLACE TABLE by default for consistency with sources and loads
            # This ensures idempotency - operations can be re-run safely
            create_clause = "CREATE OR REPLACE TABLE" if is_replace else "CREATE TABLE"
            return f"""-- Materialization: TABLE
{create_clause} {name} AS
{query};

-- Statistics collection
ANALYZE {name};"""

        elif materialized == "VIEW":
            # For views, always use CREATE OR REPLACE as it's the standard practice
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
        ----
            operation: Load operation definition.
            context: Execution context.

        Returns:
        -------
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
        ----
            operation: Export operation definition.
            context: Execution context.

        Returns:
        -------
            SQL for the export operation.

        """
        query = operation.get("query", {})
        source_query = query.get("query", "")
        destination = query.get("destination_uri", "")
        export_type = query.get("type", "CSV").lower()  # Normalize to lowercase

        logger.debug(
            f"Generating export SQL: type={export_type}, destination={destination}"
        )

        if export_type == "csv":
            return f"""-- Export to CSV
COPY (
{source_query}
) TO '{destination}' (FORMAT CSV, HEADER);"""

        else:
            logger.warning(
                f"Export type not explicitly supported: {export_type}, using generic format"
            )
            return f"""-- Export type: {export_type.upper()}
-- Destination: {destination}
{source_query}"""

    def _substitute_variables(
        self, sql: str, variables: Dict[str, Any]
    ) -> tuple[str, int]:
        """Substitute variables in SQL with V2 engine.

        Args:
            sql: SQL string with variables.
            variables: Dictionary of variables.

        Returns:
            A tuple containing:
            - SQL with variables substituted
            - Total number of replacements made
        """
        if not sql:
            return "", 0

        from sqlflow.core.variables.v2 import find_variables, substitute_variables

        substituted_sql = substitute_variables(sql, variables)
        total_replacements = len(find_variables(sql))

        logger.debug(
            f"Variable substitution complete using V2 engine. Replacements: {total_replacements}"
        )
        return substituted_sql, total_replacements


# Phase 2 Cleanup: Removed legacy formatting methods - now using unified VariableSubstitutionEngine
