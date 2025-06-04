"""SQL generators for DuckDB load operations."""

from typing import Dict, List

from sqlflow.utils.sql_security import SQLSafeFormatter, validate_identifier

from ..constants import SQLTemplates


class SQLGenerator:
    """Generates SQL statements for various load operations."""

    def __init__(self, dialect: str = "duckdb"):
        """Initialize SQL generator with safe formatter."""
        self.formatter = SQLSafeFormatter(dialect)

    def generate_merge_sql(
        self,
        table_name: str,
        source_name: str,
        merge_keys: List[str],
        source_schema: Dict[str, str],
    ) -> str:
        """Generate SQL for MERGE operation.

        Args:
        ----
            table_name: Target table name
            source_name: Source table/view name
            merge_keys: List of columns to use as merge keys
            source_schema: Schema of the source table

        Returns:
        -------
            Complete MERGE SQL statement

        """
        # Validate all identifiers to prevent injection
        validate_identifier(table_name)
        validate_identifier(source_name)
        for key in merge_keys:
            validate_identifier(key)
        for col in source_schema.keys():
            validate_identifier(col)

        # Build WHERE clause for UPDATE with table name prefix
        update_where_clause = self._build_update_where_clause(table_name, merge_keys)

        # Generate SET clause for UPDATE (all columns except merge keys)
        set_clause = self._build_set_clause(source_schema, merge_keys)

        # Create WHERE clause for INSERT (records not in target)
        insert_where_clause = self._build_insert_where_clause(table_name, merge_keys)

        # Build column list for INSERT - now safely quoted
        quoted_columns = [
            self.formatter.quote_identifier(col) for col in source_schema.keys()
        ]
        columns = ", ".join(quoted_columns)

        # Safe table and source references
        quoted_table = self.formatter.quote_identifier(table_name)
        quoted_source = self.formatter.quote_identifier(source_name)

        # Combine all parts using safe formatting
        return f"""
{SQLTemplates.MERGE_CREATE_TEMP_VIEW.format(source_name=quoted_source)}

-- Update existing records
{
            SQLTemplates.MERGE_UPDATE.format(
                table_name=quoted_table,
                set_clause=set_clause,
                where_clause=update_where_clause,
            )
        }

-- Insert new records
{
            SQLTemplates.MERGE_INSERT.format(
                table_name=quoted_table, columns=columns, where_clause=insert_where_clause
            )
        }

{SQLTemplates.MERGE_DROP_TEMP_VIEW}
""".strip()

    def _build_update_where_clause(self, table_name: str, merge_keys: List[str]) -> str:
        """Build WHERE clause for UPDATE operation.

        Args:
        ----
            table_name: Target table name
            merge_keys: List of merge key columns

        Returns:
        -------
            WHERE clause string

        """
        quoted_table = self.formatter.quote_identifier(table_name)
        update_where_clauses = []
        for key in merge_keys:
            quoted_key = self.formatter.quote_identifier(key)
            update_where_clauses.append(
                f"{quoted_table}.{quoted_key} = source.{quoted_key}"
            )
        return " AND ".join(update_where_clauses)

    def _build_set_clause(
        self, source_schema: Dict[str, str], merge_keys: List[str]
    ) -> str:
        """Build SET clause for UPDATE operation.

        Args:
        ----
            source_schema: Schema of the source table
            merge_keys: List of merge key columns

        Returns:
        -------
            SET clause string

        """
        set_clauses = []
        for col in source_schema.keys():
            if col not in merge_keys:
                quoted_col = self.formatter.quote_identifier(col)
                set_clauses.append(f"{quoted_col} = source.{quoted_col}")

        # Handle case where only merge keys exist
        return ", ".join(set_clauses) if set_clauses else "1 = 1"

    def _build_insert_where_clause(self, table_name: str, merge_keys: List[str]) -> str:
        """Build WHERE clause for INSERT operation.

        Args:
        ----
            table_name: Target table name
            merge_keys: List of merge key columns

        Returns:
        -------
            WHERE clause string

        """
        quoted_table = self.formatter.quote_identifier(table_name)
        where_clauses = []
        for key in merge_keys:
            quoted_key = self.formatter.quote_identifier(key)
            where_clauses.append(
                f"source.{quoted_key} NOT IN (SELECT {quoted_key} FROM {quoted_table})"
            )
        return " AND ".join(where_clauses)
