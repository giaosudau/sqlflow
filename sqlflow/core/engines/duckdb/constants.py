"""Constants for DuckDB engine."""

from typing import Dict


class DuckDBConstants:
    """Constants used throughout the DuckDB engine."""

    # Database configuration
    MEMORY_DATABASE = ":memory:"
    DEFAULT_DATABASE_PATH = "target/default.db"
    DEFAULT_MEMORY_LIMIT = "2GB"

    # Load modes
    LOAD_MODE_REPLACE = "REPLACE"
    LOAD_MODE_APPEND = "APPEND"
    LOAD_MODE_UPSERT = "UPSERT"

    # UDF types
    UDF_TYPE_SCALAR = "scalar"
    UDF_TYPE_TABLE = "table"

    # SQL commands
    SQL_SELECT_VERSION = "SELECT version()"
    SQL_SELECT_ONE = "SELECT 1"
    SQL_CHECKPOINT = "CHECKPOINT"
    SQL_PRAGMA_MEMORY_LIMIT = "PRAGMA memory_limit='{memory_limit}'"
    SQL_PRAGMA_TABLE_INFO = "PRAGMA table_info({table_name})"
    SQL_DESCRIBE_TABLE = "DESCRIBE {table_name}"

    # Type mappings
    PYTHON_TO_DUCKDB_TYPES: Dict[type, str] = {
        int: "INTEGER",
        float: "DOUBLE",
        str: "VARCHAR",
        bool: "BOOLEAN",
    }


class SQLTemplates:
    """SQL template strings for common operations."""

    # Table operations
    CREATE_TABLE_AS = "CREATE TABLE {table_name} AS SELECT * FROM {source_name}"
    CREATE_OR_REPLACE_TABLE_AS = (
        "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {source_name}"
    )
    INSERT_INTO = "INSERT INTO {table_name} SELECT * FROM {source_name}"
    DROP_TABLE_IF_EXISTS = "DROP TABLE IF EXISTS {table_name}"

    # Schema operations
    CREATE_TABLE_WITH_COLUMNS = (
        "CREATE TABLE IF NOT EXISTS {table_name} AS SELECT {columns} FROM {source_name}"
    )

    # Information schema queries
    CHECK_TABLE_EXISTS = (
        "SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'"
    )
    CHECK_TABLE_EXISTS_LIMIT = "SELECT 1 FROM {table_name} LIMIT 0"

    # UPSERT operation templates with transaction safety
    UPSERT_CREATE_TEMP_VIEW = (
        "CREATE TEMPORARY VIEW temp_source AS SELECT * FROM {source_name};"
    )
    UPSERT_UPDATE = """UPDATE {table_name} 
SET {set_clause}
FROM temp_source AS source
WHERE {where_clause};"""

    UPSERT_INSERT = """INSERT INTO {table_name} ({columns})
SELECT {columns}
FROM temp_source AS source
WHERE {where_clause};"""

    UPSERT_DROP_TEMP_VIEW = "DROP VIEW IF EXISTS temp_source;"

    # Transaction control templates
    UPSERT_BEGIN_TRANSACTION = "BEGIN TRANSACTION;"
    UPSERT_COMMIT_TRANSACTION = "COMMIT;"
    UPSERT_ROLLBACK_TRANSACTION = "ROLLBACK;"

    # Performance monitoring templates
    UPSERT_COUNT_SOURCE = "SELECT COUNT(*) FROM {source_name};"
    UPSERT_COUNT_TARGET_BEFORE = "SELECT COUNT(*) FROM {table_name};"
    UPSERT_COUNT_TARGET_AFTER = "SELECT COUNT(*) FROM {table_name};"

    # UDF patterns
    UDF_PATTERN_PYTHON_FUNC = (
        r"PYTHON_FUNC\s*\(\s*[\'\"]([a-zA-Z0-9_\.]+)[\'\"]\s*,\s*(.*?)\)"
    )


class RegexPatterns:
    """Regular expression patterns used in the engine."""

    # Variable substitution
    VARIABLE_SUBSTITUTION = r"\$\{([^}]+)\}"

    # UDF detection
    UDF_PYTHON_FUNC = r"PYTHON_FUNC\s*\(\s*[\'\"]([a-zA-Z0-9_\.]+)[\'\"]\s*,\s*(.*?)\)"

    # Table UDF detection
    TABLE_UDF_FROM_PATTERN = r"SELECT\s+\*\s+FROM\s+(\w+)\s*\(\s*([^)]*)\s*\)"
