"""SQL generators for DuckDB load operations."""

from typing import Dict, List

from sqlflow.logging import get_logger
from sqlflow.utils.sql_security import SQLSafeFormatter, validate_identifier

logger = get_logger(__name__)


class SQLGenerator:
    """Generates SQL statements for various load operations."""

    def __init__(self, dialect: str = "duckdb"):
        """Initialize SQL generator with safe formatter."""
        self.formatter = SQLSafeFormatter(dialect)

    def generate_upsert_sql(
        self,
        table_name: str,
        source_name: str,
        upsert_keys: List[str],
        source_schema: Dict[str, str],
    ) -> str:
        """Generate SQL for UPSERT operation.

        Uses DuckDB's capabilities for efficient UPSERT operations.
        Falls back to UPDATE/INSERT pattern for compatibility.

        Args:
        ----
            table_name: Target table name
            source_name: Source table/view name
            upsert_keys: List of columns to use as upsert keys
            source_schema: Schema of the source table

        Returns:
        -------
            Complete UPSERT SQL statement

        """
        # Validate all identifiers to prevent injection
        validate_identifier(table_name)
        validate_identifier(source_name)
        for key in upsert_keys:
            validate_identifier(key)
        for col in source_schema.keys():
            validate_identifier(col)

        logger.debug(
            f"Generating UPSERT SQL for table {table_name} with keys {upsert_keys}"
        )

        # Use DuckDB-optimized UPDATE/INSERT pattern
        # This approach is more compatible and performs well
        return self._generate_update_insert_upsert(
            table_name, source_name, upsert_keys, source_schema
        )

    def _generate_update_insert_upsert(
        self,
        table_name: str,
        source_name: str,
        upsert_keys: List[str],
        source_schema: Dict[str, str],
    ) -> str:
        """Generate UPDATE/INSERT pattern for UPSERT operation.

        This method creates an efficient UPSERT using UPDATE followed by INSERT
        with proper handling for concurrent access and data integrity.
        """
        # Build WHERE clause for UPDATE with table name prefix
        update_where_clause = self._build_update_where_clause(table_name, upsert_keys)

        # Generate SET clause for UPDATE (all columns except upsert keys)
        set_clause = self._build_set_clause(source_schema, upsert_keys)

        # Create WHERE clause for INSERT (records not in target)
        insert_where_clause = self._build_insert_where_clause(table_name, upsert_keys)

        # Build column list for INSERT - now safely quoted
        quoted_columns = [
            self.formatter.quote_identifier(col) for col in source_schema.keys()
        ]
        columns = ", ".join(quoted_columns)

        # Safe table and source references
        quoted_table = self.formatter.quote_identifier(table_name)
        quoted_source = self.formatter.quote_identifier(source_name)

        # Create transactional UPSERT with direct table references (no temp views)
        upsert_sql = f"""
-- Begin UPSERT operation for {table_name}
BEGIN TRANSACTION;

-- Update existing records
UPDATE {quoted_table} 
SET {set_clause}
FROM {quoted_source} AS source
WHERE {update_where_clause};

-- Insert new records that don't exist in target
INSERT INTO {quoted_table} ({columns})
SELECT {columns}
FROM {quoted_source} AS source
WHERE {insert_where_clause};

-- Commit the transaction
COMMIT;
""".strip()

        logger.debug("Generated UPSERT SQL with transaction safety")
        return upsert_sql

    def _build_update_where_clause(
        self, table_name: str, upsert_keys: List[str]
    ) -> str:
        """Build WHERE clause for UPDATE operation.

        Args:
        ----
            table_name: Target table name
            upsert_keys: List of upsert key columns

        Returns:
        -------
            WHERE clause string

        """
        quoted_table = self.formatter.quote_identifier(table_name)
        update_where_clauses = []
        for key in upsert_keys:
            quoted_key = self.formatter.quote_identifier(key)
            update_where_clauses.append(
                f"{quoted_table}.{quoted_key} = source.{quoted_key}"
            )
        return " AND ".join(update_where_clauses)

    def _build_set_clause(
        self, source_schema: Dict[str, str], upsert_keys: List[str]
    ) -> str:
        """Build SET clause for UPDATE operation.

        Args:
        ----
            source_schema: Schema of the source table
            upsert_keys: List of upsert key columns

        Returns:
        -------
            SET clause string

        """
        set_clauses = []
        for col in source_schema.keys():
            if col not in upsert_keys:
                quoted_col = self.formatter.quote_identifier(col)
                set_clauses.append(f"{quoted_col} = source.{quoted_col}")

        # Handle case where only upsert keys exist (no other columns to update)
        if not set_clauses:
            # Create a no-op SET clause that doesn't change any data
            # but allows the UPDATE to succeed syntactically
            first_key = upsert_keys[0] if upsert_keys else "1"
            quoted_key = self.formatter.quote_identifier(first_key)
            return f"{quoted_key} = {quoted_key}"

        return ", ".join(set_clauses)

    def _build_insert_where_clause(
        self, table_name: str, upsert_keys: List[str]
    ) -> str:
        """Build WHERE clause for INSERT operation.

        Uses NOT EXISTS for better performance and handles NULLs correctly.

        Args:
        ----
            table_name: Target table name
            upsert_keys: List of upsert key columns

        Returns:
        -------
            WHERE clause string optimized for DuckDB

        """
        quoted_table = self.formatter.quote_identifier(table_name)

        # Use NOT EXISTS for better performance and NULL handling
        key_conditions = []
        for key in upsert_keys:
            quoted_key = self.formatter.quote_identifier(key)
            key_conditions.append(f"target.{quoted_key} = source.{quoted_key}")

        exists_condition = " AND ".join(key_conditions)
        return (
            f"NOT EXISTS (SELECT 1 FROM {quoted_table} target WHERE {exists_condition})"
        )

    def supports_native_upsert(self) -> bool:
        """Check if DuckDB version supports native UPSERT/MERGE.

        Returns:
        -------
            True if native UPSERT is supported, False otherwise
        """
        # For now, we use the UPDATE/INSERT pattern which is universally supported
        # This can be enhanced later to detect DuckDB version and use native MERGE
        return False
