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
        # Track logged warnings to prevent duplicates
        self._logged_warnings = set()
        # Track warning counts for summary
        self._warning_counts = {}
        logger.debug(f"SQL Generator initialized with dialect: {dialect}")

    def _log_warning_once(self, warning_key: str, message: str) -> None:
        """Log a warning message only once per key.

        Args:
            warning_key: A unique key for this warning type
            message: The warning message to log
        """
        if warning_key not in self._logged_warnings:
            logger.warning(message)
            self._logged_warnings.add(warning_key)

            # Track warning counts for summary
            self._warning_counts[warning_key] = (
                self._warning_counts.get(warning_key, 0) + 1
            )

            # Log at debug level that we're suppressing future instances
            logger.debug(
                f"Future similar warnings with key '{warning_key}' will be suppressed"
            )
        else:
            # Increment the count even for suppressed warnings
            self._warning_counts[warning_key] = (
                self._warning_counts.get(warning_key, 0) + 1
            )

            # Log at debug level that we're suppressing this instance
            logger.debug(f"Suppressing duplicate warning with key '{warning_key}'")

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
            logger.info(f"Generated SQL for {op_type} operation {op_id}: {preview}")
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
            operation: Source operation definition.
            context: Execution context.

        Returns:
            SQL for the source.
        """
        source_type = operation.get("source_connector_type", "").upper()
        query = operation.get("query", {})
        name = operation.get("name", "unnamed_source")
        operation.get("id", "unknown")

        # Handle profile-based source definition (FROM syntax)
        if operation.get("is_from_profile", False):
            # Get connector type from profile for profile-based sources
            profile_connector_name = operation.get("profile_connector_name", "")
            profile = context.get("profile", {})
            profile_connectors = profile.get("connectors", {})

            if profile_connector_name and profile_connector_name in profile_connectors:
                profile_connector = profile_connectors.get(profile_connector_name, {})
                source_type = profile_connector.get("type", "").upper()
                logger.debug(f"Using connector type from profile: {source_type}")
            else:
                warning_key = f"profile_connector_not_found:{profile_connector_name}"
                self._log_warning_once(
                    warning_key,
                    f"Profile connector '{profile_connector_name}' not found in profile. "
                    f"Check that '{profile_connector_name}' is defined in your profile's 'connectors' section.",
                )

        logger.debug(f"Generating source SQL for {name}, type: {source_type}")

        if source_type == "CSV":
            path = query.get("path", "")
            has_header = query.get("has_header", True)
            logger.debug(f"CSV source: path={path}, has_header={has_header}")
            return f"""-- Source type: CSV
CREATE OR REPLACE TABLE {name} AS
SELECT * FROM read_csv_auto('{path}', 
                           header={str(has_header).lower()});"""

        elif source_type == "POSTGRESQL" or source_type == "POSTGRES":
            pg_query = query.get("query", "")
            logger.debug(f"PostgreSQL source: query length={len(pg_query)}")
            return f"""-- Source type: PostgreSQL
CREATE OR REPLACE TABLE {name} AS
SELECT * FROM {pg_query};"""

        else:
            # Log warning only once per source type to prevent duplicate logs
            warning_key = f"unknown_source_type:{source_type}:{name}"

            # Create a more helpful error message for the tests to check
            supported_types = ["CSV", "POSTGRES", "POSTGRESQL"]
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

            self._log_warning_once(warning_key, error_msg)

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

        Returns:
            Dictionary mapping warning keys to count of occurrences
        """
        return self._warning_counts.copy()

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

    def _substitute_variables(
        self, sql: str, variables: Dict[str, Any]
    ) -> tuple[str, int]:
        """Substitute variables in SQL.

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

        if not variables:
            logger.debug("No variables to substitute in SQL")
            return sql, 0

        logger.debug(f"Substituting {len(variables)} variables in SQL")

        # Track variable replacements for logging
        total_replacements = 0

        # First pass: replace variables that have values
        result, total_replacements = self._replace_variables_with_values(
            sql, variables, total_replacements
        )

        # Second pass: handle variables with default values and missing variables
        result_with_defaults = self._handle_variable_defaults(result)

        # Log summary of replacements
        if total_replacements > 0:
            logger.debug(
                f"Completed variable substitution: {total_replacements} total replacements"
            )

        return result_with_defaults, total_replacements

    def _replace_variables_with_values(
        self, sql: str, variables: Dict[str, Any], replacements_made: int
    ) -> tuple[str, int]:
        """Replace variables with their values in the SQL.

        Args:
            sql: SQL string with variables.
            variables: Dictionary of variables.
            replacements_made: Counter for replacements (modified in place).

        Returns:
            A tuple containing:
            - SQL with variables replaced by their values
            - The total number of replacements made
        """
        result = sql
        total_replacements = replacements_made

        for var_name, var_value in variables.items():
            # Two patterns: one for variables inside quotes, one for standalone variables
            # Pattern for variables inside quotes: '${var}' or "${var}"
            quoted_pattern = (
                r"('?\${"
                + re.escape(var_name)
                + r"}'?|'?\${"
                + re.escape(var_name)
                + r"\|[^}]*}'?)"
            )
            # Pattern for standalone variables: ${var}
            standalone_pattern = (
                r"\${"
                + re.escape(var_name)
                + r"}|\${"
                + re.escape(var_name)
                + r"\|[^}]*}"
            )

            # Convert Python objects to SQL literals based on context
            if isinstance(var_value, str):
                quoted_replacement = var_value  # Already quoted
                standalone_replacement = f"'{var_value}'"  # Add quotes for standalone
            elif isinstance(var_value, bool):
                bool_value = str(var_value).lower()
                quoted_replacement = bool_value
                standalone_replacement = bool_value
            else:
                str_value = str(var_value)
                quoted_replacement = str_value
                standalone_replacement = str_value

            # First replace variables inside quotes
            count_before = len(re.findall(quoted_pattern, result))
            # Look for '${var}' or "${var}" and replace with just the value (no quotes added)
            result = re.sub(
                r"'(\${"
                + re.escape(var_name)
                + r"}|\${"
                + re.escape(var_name)
                + r"\|[^}]*})'",
                f"'{quoted_replacement}'",
                result,
            )
            result = re.sub(
                r'"(\${'
                + re.escape(var_name)
                + r"}|\${"
                + re.escape(var_name)
                + r'\|[^}]*})"',
                f'"{quoted_replacement}"',
                result,
            )
            count_after_quoted = len(re.findall(quoted_pattern, result))
            quoted_replacements = count_before - count_after_quoted

            # Then replace standalone variables
            count_before = len(re.findall(standalone_pattern, result))
            result = re.sub(standalone_pattern, standalone_replacement, result)
            count_after_standalone = len(re.findall(standalone_pattern, result))
            standalone_replacements = count_before - count_after_standalone

            var_replacements = quoted_replacements + standalone_replacements
            if var_replacements > 0:
                total_replacements += var_replacements
                logger.debug(
                    f"Variable '{var_name}' replaced {var_replacements} times with value: {var_value}"
                )

        return result, total_replacements

    def _handle_variable_defaults(self, sql: str) -> str:
        """Handle default values for variables and missing variables.

        Args:
            sql: SQL string with variables.

        Returns:
            SQL with default values applied and missing variables replaced with NULL.
        """
        # First replace variables inside quotes
        # Look for '${var|default}' patterns
        sql = re.sub(
            r"'(\${([^}]*\|[^}]*)})(')",
            lambda m: f"'{self._extract_default_value(m.group(2))}'",
            sql,
        )
        # Look for "${var|default}" patterns
        sql = re.sub(
            r'"(\${([^}]*\|[^}]*)})(")',
            lambda m: f'"{self._extract_default_value(m.group(2))}"',
            sql,
        )

        # Then handle standalone variables with defaults
        pattern_with_defaults = r"\${[^}]*\|[^}]*}"
        result_with_defaults = re.sub(
            pattern_with_defaults, self._replace_with_default, sql
        )

        # Handle any remaining ${var} without defaults or values
        pattern_without_defaults = r"\${[^}]*}"
        final_result = re.sub(
            pattern_without_defaults,
            self._handle_missing_variable,
            result_with_defaults,
        )

        if final_result != result_with_defaults:
            logger.warning(
                "Some variables had no values or defaults and were replaced with NULL"
            )

        return final_result

    def _extract_default_value(self, var_with_default: str) -> str:
        """Extract default value from a variable|default format string.

        Args:
            var_with_default: String in format "varname|default_value"

        Returns:
            The default value
        """
        if "|" in var_with_default:
            # Extract default value
            var_name, default_value = var_with_default.split("|", 1)
            var_name = var_name.strip()
            default_value = default_value.strip()
            logger.debug(f"Using default value for '{var_name}': {default_value}")
            return default_value

        # Should not happen
        logger.warning(f"Expected default value not found for: '{var_with_default}'")
        return "NULL"

    def _replace_with_default(self, match: re.Match) -> str:
        """Replace a variable reference with its default value.

        Args:
            match: Regex match object for the variable reference.

        Returns:
            Default value for the variable or NULL if no default is provided.
        """
        # Parse variable and default value
        var_expr = match.group(0)

        # Check if this is already inside quotes
        inside_quotes = False
        if var_expr.startswith("'${") and var_expr.endswith("}'"):
            inside_quotes = True
            var_expr = var_expr[1:-1]  # Remove surrounding quotes
        elif var_expr.startswith('"${') and var_expr.endswith('}"'):
            inside_quotes = True
            var_expr = var_expr[1:-1]  # Remove surrounding quotes

        without_delimiters = var_expr[2:-1]  # Remove ${ and }
        if "|" in without_delimiters:
            # Extract default value
            var_name, default_value = without_delimiters.split("|", 1)
            var_name = var_name.strip()
            default_value = default_value.strip()

            # If we're already inside quotes, don't add more quotes
            if inside_quotes:
                return default_value

            # Handle quoted default values
            if (default_value.startswith("'") and default_value.endswith("'")) or (
                default_value.startswith('"') and default_value.endswith('"')
            ):
                # Keep as is - already quoted
                pass
            elif default_value.lower() == "true" or default_value.lower() == "false":
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

        # This should not happen as we're only matching variables with defaults
        var_name = without_delimiters.strip()
        logger.warning(f"Expected default value not found for variable: '{var_name}'")
        return "NULL"

    def _handle_missing_variable(self, match: re.Match) -> str:
        """Handle a variable reference with no value or default.

        Args:
            match: Regex match object for the variable reference.

        Returns:
            NULL as a replacement for the missing variable.
        """
        # Extract variable name for logging
        var_expr = match.group(0)
        without_delimiters = var_expr[2:-1]  # Remove ${ and }
        var_name = without_delimiters.strip()
        logger.warning(
            f"No value or default found for variable: '{var_name}', using NULL"
        )
        return "NULL"
