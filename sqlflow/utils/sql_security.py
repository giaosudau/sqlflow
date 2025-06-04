"""
SQL security utilities for preventing SQL injection attacks.

This module provides safe methods for constructing SQL queries with dynamic
table and column names, protecting against SQL injection vulnerabilities.
"""

import re
from typing import Any, List, Optional, Tuple


class SQLIdentifierValidator:
    """Validator for SQL identifiers to prevent injection."""

    # Valid SQL identifier pattern: letters, numbers, underscores, no special chars
    VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    # Common SQL reserved words that should be quoted
    RESERVED_WORDS = {
        "SELECT",
        "FROM",
        "WHERE",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TABLE",
        "INDEX",
        "VIEW",
        "DATABASE",
        "SCHEMA",
        "PROCEDURE",
        "FUNCTION",
        "TRIGGER",
        "USER",
        "ROLE",
        "GROUP",
        "GRANT",
        "REVOKE",
        "COMMIT",
        "ROLLBACK",
        "TRANSACTION",
        "BEGIN",
        "END",
        "IF",
        "ELSE",
        "CASE",
        "WHEN",
        "THEN",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "DISTINCT",
        "ALL",
        "ANY",
        "SOME",
        "EXISTS",
        "IN",
        "LIKE",
        "BETWEEN",
        "IS",
        "NULL",
        "AND",
        "OR",
        "NOT",
        "TRUE",
        "FALSE",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "CROSS",
        "ON",
        "USING",
        "AS",
        "ASC",
        "DESC",
        "LIMIT",
        "OFFSET",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "UNIQUE",
        "CHECK",
        "DEFAULT",
        "AUTO_INCREMENT",
    }

    @classmethod
    def is_valid_identifier(cls, identifier: str) -> bool:
        """
        Check if an identifier is valid and safe to use in SQL.

        Args:
            identifier: The identifier to validate

        Returns:
            True if the identifier is safe, False otherwise
        """
        if not identifier or not isinstance(identifier, str):
            return False

        # Check for obvious SQL injection patterns (punctuation-based)
        injection_patterns = [
            ";",
            "--",
            "/*",
            "*/",
            "'",
            '"',
            "\\",
            "(",
            ")",
            "EXEC",
            "EXECUTE",
            "xp_",
            "sp_",
        ]

        identifier_upper = identifier.upper()
        for pattern in injection_patterns:
            if pattern in identifier_upper:
                return False

        # Check for standalone SQL keywords that would be dangerous
        # Only flag if the identifier IS the keyword, not if it contains it
        dangerous_keywords = [
            "DROP",
            "DELETE",
            "INSERT",
            "UPDATE",
            "ALTER",
            "CREATE",
            "TRUNCATE",
            "GRANT",
            "REVOKE",
            "COMMIT",
            "ROLLBACK",
        ]

        if identifier_upper in dangerous_keywords:
            return False

        # Allow valid identifier patterns including underscores and alphanumeric
        return bool(cls.VALID_IDENTIFIER_PATTERN.match(identifier))

    @classmethod
    def needs_quoting(cls, identifier: str) -> bool:
        """
        Check if an identifier needs to be quoted.

        Args:
            identifier: The identifier to check

        Returns:
            True if the identifier needs quoting
        """
        if not cls.is_valid_identifier(identifier):
            return True

        return identifier.upper() in cls.RESERVED_WORDS


class SQLSafeFormatter:
    """Safe SQL query formatter that prevents injection attacks."""

    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize the formatter.

        Args:
            dialect: SQL dialect ("duckdb", "postgres", "mysql", "sqlite")
        """
        self.dialect = dialect.lower()
        self.validator = SQLIdentifierValidator()

    def quote_identifier(self, identifier: str) -> str:
        """
        Safely quote an SQL identifier (table name, column name, etc.).

        Args:
            identifier: The identifier to quote

        Returns:
            Properly quoted identifier

        Raises:
            ValueError: If the identifier is invalid or potentially malicious
        """
        if not self.validator.is_valid_identifier(identifier):
            raise ValueError(
                f"Invalid or potentially malicious identifier: {identifier}"
            )

        # Use dialect-specific quoting
        if self.dialect in ("postgres", "duckdb"):
            return f'"{identifier}"'
        elif self.dialect == "mysql":
            return f"`{identifier}`"
        elif self.dialect == "sqlite":
            return f'"{identifier}"'
        else:
            # Default to double quotes
            return f'"{identifier}"'

    def quote_schema_table(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> str:
        """
        Safely quote a schema.table reference.

        Args:
            table_name: The table name
            schema_name: Optional schema name

        Returns:
            Properly quoted schema.table reference
        """
        quoted_table = self.quote_identifier(table_name)

        if schema_name:
            quoted_schema = self.quote_identifier(schema_name)
            return f"{quoted_schema}.{quoted_table}"

        return quoted_table

    def format_column_list(self, columns: List[str]) -> str:
        """
        Safely format a list of column names.

        Args:
            columns: List of column names

        Returns:
            Comma-separated, quoted column list
        """
        if not columns:
            return "*"

        quoted_columns = [self.quote_identifier(col) for col in columns]
        return ", ".join(quoted_columns)

    def build_select_query(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
        where_clause: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build a safe SELECT query with proper identifier quoting.

        Args:
            table_name: Target table name
            columns: Optional list of columns (defaults to *)
            schema_name: Optional schema name
            where_clause: Optional WHERE clause (should be parameterized separately)
            order_by: Optional list of columns to order by
            limit: Optional LIMIT value

        Returns:
            Safe SELECT query string
        """
        # Build column list
        column_str = self.format_column_list(columns or [])

        # Build table reference
        table_ref = self.quote_schema_table(table_name, schema_name)

        # Build base query
        query = f"SELECT {column_str} FROM {table_ref}"

        # Add WHERE clause (note: this should use parameters for values)
        if where_clause:
            query += f" WHERE {where_clause}"

        # Add ORDER BY
        if order_by:
            order_columns = [self.quote_identifier(col) for col in order_by]
            query += f" ORDER BY {', '.join(order_columns)}"

        # Add LIMIT
        if limit is not None:
            if not isinstance(limit, int) or limit < 0:
                raise ValueError("LIMIT must be a non-negative integer")
            query += f" LIMIT {limit}"

        return query

    def build_insert_query(
        self,
        table_name: str,
        columns: List[str],
        schema_name: Optional[str] = None,
        on_conflict: Optional[str] = None,
    ) -> str:
        """
        Build a safe INSERT query template with proper identifier quoting.

        Args:
            table_name: Target table name
            columns: List of column names
            schema_name: Optional schema name
            on_conflict: Optional conflict resolution clause

        Returns:
            Safe INSERT query template (use with parameters for values)
        """
        if not columns:
            raise ValueError("Columns list cannot be empty for INSERT")

        table_ref = self.quote_schema_table(table_name, schema_name)
        column_str = self.format_column_list(columns)

        # Create placeholders for parameterized values
        placeholders = ", ".join(["?" for _ in columns])

        query = f"INSERT INTO {table_ref} ({column_str}) VALUES ({placeholders})"

        if on_conflict:
            query += f" {on_conflict}"

        return query

    def build_update_query(
        self,
        table_name: str,
        set_columns: List[str],
        schema_name: Optional[str] = None,
        where_clause: Optional[str] = None,
    ) -> str:
        """
        Build a safe UPDATE query template with proper identifier quoting.

        Args:
            table_name: Target table name
            set_columns: List of columns to update
            schema_name: Optional schema name
            where_clause: Optional WHERE clause (should use parameters for values)

        Returns:
            Safe UPDATE query template
        """
        if not set_columns:
            raise ValueError("SET columns list cannot be empty for UPDATE")

        table_ref = self.quote_schema_table(table_name, schema_name)

        # Build SET clause with placeholders
        set_clauses = [f"{self.quote_identifier(col)} = ?" for col in set_columns]
        set_clause = ", ".join(set_clauses)

        query = f"UPDATE {table_ref} SET {set_clause}"

        if where_clause:
            query += f" WHERE {where_clause}"

        return query

    def build_delete_query(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        where_clause: Optional[str] = None,
    ) -> str:
        """
        Build a safe DELETE query with proper identifier quoting.

        Args:
            table_name: Target table name
            schema_name: Optional schema name
            where_clause: Optional WHERE clause (should use parameters for values)

        Returns:
            Safe DELETE query
        """
        table_ref = self.quote_schema_table(table_name, schema_name)
        query = f"DELETE FROM {table_ref}"

        if where_clause:
            query += f" WHERE {where_clause}"

        return query

    def build_create_table_query(
        self,
        table_name: str,
        column_definitions: List[str],
        schema_name: Optional[str] = None,
        if_not_exists: bool = False,
        temporary: bool = False,
    ) -> str:
        """
        Build a safe CREATE TABLE query.

        Args:
            table_name: Target table name
            column_definitions: List of column definitions (should be pre-validated)
            schema_name: Optional schema name
            if_not_exists: Whether to use IF NOT EXISTS
            temporary: Whether to create a temporary table

        Returns:
            Safe CREATE TABLE query
        """
        if not column_definitions:
            raise ValueError("Column definitions cannot be empty")

        table_ref = self.quote_schema_table(table_name, schema_name)

        create_clause = "CREATE"
        if temporary:
            create_clause += " TEMPORARY"
        create_clause += " TABLE"
        if if_not_exists:
            create_clause += " IF NOT EXISTS"

        columns_str = ", ".join(column_definitions)
        return f"{create_clause} {table_ref} ({columns_str})"


class ParameterizedQueryBuilder:
    """Builder for parameterized queries to prevent SQL injection."""

    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize the query builder.

        Args:
            dialect: SQL dialect for parameter formatting
        """
        self.dialect = dialect.lower()
        self.formatter = SQLSafeFormatter(dialect)
        self.parameters: List[Any] = []

    def reset(self) -> None:
        """Reset the parameter list."""
        self.parameters = []

    def add_parameter(self, value: Any) -> str:
        """
        Add a parameter value and return the placeholder.

        Args:
            value: The parameter value

        Returns:
            Parameter placeholder string
        """
        self.parameters.append(value)

        # Use dialect-specific parameter placeholder
        if self.dialect == "postgres":
            return f"${len(self.parameters)}"
        else:
            return "?"

    def build_where_condition(self, column: str, operator: str, value: Any) -> str:
        """
        Build a safe WHERE condition with parameters.

        Args:
            column: Column name
            operator: SQL operator (=, >, <, LIKE, etc.)
            value: Value to compare against

        Returns:
            WHERE condition string with parameter placeholder
        """
        # Validate operator to prevent injection
        valid_operators = {
            "=",
            "!=",
            "<>",
            ">",
            "<",
            ">=",
            "<=",
            "LIKE",
            "ILIKE",
            "IN",
            "NOT IN",
            "IS",
            "IS NOT",
        }
        if operator.upper() not in valid_operators:
            raise ValueError(f"Invalid operator: {operator}")

        quoted_column = self.formatter.quote_identifier(column)
        placeholder = self.add_parameter(value)

        return f"{quoted_column} {operator} {placeholder}"

    def build_in_condition(self, column: str, values: List[Any]) -> str:
        """
        Build a safe IN condition with parameters.

        Args:
            column: Column name
            values: List of values for IN clause

        Returns:
            IN condition string with parameter placeholders
        """
        if not values:
            raise ValueError("Values list cannot be empty for IN condition")

        quoted_column = self.formatter.quote_identifier(column)
        placeholders = [self.add_parameter(value) for value in values]
        placeholders_str = ", ".join(placeholders)

        return f"{quoted_column} IN ({placeholders_str})"

    def get_query_and_parameters(self, query: str) -> Tuple[str, List[Any]]:
        """
        Get the final query and parameter list.

        Args:
            query: The query string with placeholders

        Returns:
            Tuple of (query_string, parameters_list)
        """
        return query, self.parameters.copy()


# Convenience functions for common use cases
def safe_table_name(
    table_name: str, schema_name: Optional[str] = None, dialect: str = "duckdb"
) -> str:
    """
    Safely quote a table name.

    Args:
        table_name: The table name
        schema_name: Optional schema name
        dialect: SQL dialect

    Returns:
        Safely quoted table reference
    """
    formatter = SQLSafeFormatter(dialect)
    return formatter.quote_schema_table(table_name, schema_name)


def safe_column_list(columns: List[str], dialect: str = "duckdb") -> str:
    """
    Safely format a column list.

    Args:
        columns: List of column names
        dialect: SQL dialect

    Returns:
        Safely quoted, comma-separated column list
    """
    formatter = SQLSafeFormatter(dialect)
    return formatter.format_column_list(columns)


def validate_identifier(identifier: str) -> None:
    """
    Validate an SQL identifier and raise an exception if invalid.

    Args:
        identifier: The identifier to validate

    Raises:
        ValueError: If the identifier is invalid
    """
    if not SQLIdentifierValidator.is_valid_identifier(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
