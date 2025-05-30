"""SQL generators for DuckDB load operations."""

from typing import Dict, List

from ..constants import SQLTemplates


class SQLGenerator:
    """Generates SQL statements for various load operations."""

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
        # Build WHERE clause for UPDATE with table name prefix
        update_where_clause = self._build_update_where_clause(table_name, merge_keys)

        # Generate SET clause for UPDATE (all columns except merge keys)
        set_clause = self._build_set_clause(source_schema, merge_keys)

        # Create WHERE clause for INSERT (records not in target)
        insert_where_clause = self._build_insert_where_clause(table_name, merge_keys)

        # Build column list for INSERT
        columns = ", ".join(source_schema.keys())

        # Combine all parts
        return f"""
{SQLTemplates.MERGE_CREATE_TEMP_VIEW.format(source_name=source_name)}

-- Update existing records
{
            SQLTemplates.MERGE_UPDATE.format(
                table_name=table_name,
                set_clause=set_clause,
                where_clause=update_where_clause,
            )
        }

-- Insert new records
{
            SQLTemplates.MERGE_INSERT.format(
                table_name=table_name, columns=columns, where_clause=insert_where_clause
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
        update_where_clauses = []
        for key in merge_keys:
            update_where_clauses.append(f"{table_name}.{key} = source.{key}")
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
                set_clauses.append(f"{col} = source.{col}")

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
        where_clauses = []
        for key in merge_keys:
            where_clauses.append(
                f"source.{key} NOT IN (SELECT {key} FROM {table_name})"
            )
        return " AND ".join(where_clauses)
