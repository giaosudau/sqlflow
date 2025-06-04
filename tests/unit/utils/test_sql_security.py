"""
Tests for SQL security utilities.

These tests verify that the SQL security utilities correctly prevent
SQL injection attacks and properly validate and quote identifiers.
"""

import pytest

from sqlflow.utils.sql_security import (
    ParameterizedQueryBuilder,
    SQLIdentifierValidator,
    SQLSafeFormatter,
    safe_column_list,
    safe_table_name,
    validate_identifier,
)


class TestSQLIdentifierValidator:
    """Test SQL identifier validation."""

    def test_valid_identifiers(self):
        """Test that valid identifiers are accepted."""
        valid_ids = [
            "table_name",
            "column1",
            "my_table",
            "test123",
            "_private",
            "CamelCase",
        ]

        for identifier in valid_ids:
            assert SQLIdentifierValidator.is_valid_identifier(identifier)

    def test_invalid_identifiers(self):
        """Test that invalid/dangerous identifiers are rejected."""
        invalid_ids = [
            "table'; DROP TABLE users; --",
            "col--comment",
            "test/*injection*/",
            "user'data",
            'table"name',
            "col\\escape",
            "123table",  # starts with number
            "",  # empty
            None,  # None
            "DROP TABLE",
            "SELECT * FROM",
            "exec xp_cmdshell",
        ]

        for identifier in invalid_ids:
            assert not SQLIdentifierValidator.is_valid_identifier(identifier)

    def test_reserved_words(self):
        """Test reserved word detection."""
        reserved_words = ["SELECT", "FROM", "WHERE", "DROP", "TABLE"]

        for word in reserved_words:
            assert SQLIdentifierValidator.needs_quoting(word.lower())
            assert SQLIdentifierValidator.needs_quoting(word.upper())

    def test_sqlflow_common_patterns(self):
        """Test that common SQLFlow patterns are accepted."""
        common_patterns = [
            "users_updates_csv",  # CSV source names
            "table_with_underscores",
            "source_data_123",
            "target_table_v2",
            "customers_updates",
        ]

        for pattern in common_patterns:
            assert SQLIdentifierValidator.is_valid_identifier(
                pattern
            ), f"Should accept common pattern: {pattern}"


class TestSQLSafeFormatter:
    """Test safe SQL formatting."""

    def test_quote_identifier_basic(self):
        """Test basic identifier quoting."""
        formatter = SQLSafeFormatter("duckdb")

        assert formatter.quote_identifier("table_name") == '"table_name"'
        assert formatter.quote_identifier("column1") == '"column1"'

    def test_quote_identifier_postgres(self):
        """Test PostgreSQL identifier quoting."""
        formatter = SQLSafeFormatter("postgres")

        assert formatter.quote_identifier("table_name") == '"table_name"'

    def test_quote_identifier_mysql(self):
        """Test MySQL identifier quoting."""
        formatter = SQLSafeFormatter("mysql")

        assert formatter.quote_identifier("table_name") == "`table_name`"

    def test_quote_identifier_invalid(self):
        """Test that invalid identifiers raise errors."""
        formatter = SQLSafeFormatter("duckdb")

        with pytest.raises(ValueError, match="Invalid or potentially malicious"):
            formatter.quote_identifier("table'; DROP TABLE users; --")

    def test_quote_schema_table(self):
        """Test schema.table quoting."""
        formatter = SQLSafeFormatter("duckdb")

        # Table only
        assert formatter.quote_schema_table("users") == '"users"'

        # Schema and table
        assert formatter.quote_schema_table("users", "public") == '"public"."users"'

    def test_format_column_list(self):
        """Test column list formatting."""
        formatter = SQLSafeFormatter("duckdb")

        # Empty list returns *
        assert formatter.format_column_list([]) == "*"

        # Single column
        assert formatter.format_column_list(["name"]) == '"name"'

        # Multiple columns
        assert (
            formatter.format_column_list(["id", "name", "email"])
            == '"id", "name", "email"'
        )

    def test_build_select_query(self):
        """Test safe SELECT query building."""
        formatter = SQLSafeFormatter("duckdb")

        # Basic SELECT
        query = formatter.build_select_query("users")
        assert query == 'SELECT * FROM "users"'

        # With columns
        query = formatter.build_select_query("users", ["id", "name"])
        assert query == 'SELECT "id", "name" FROM "users"'

        # With schema
        query = formatter.build_select_query("users", schema_name="public")
        assert query == 'SELECT * FROM "public"."users"'

        # With ORDER BY
        query = formatter.build_select_query("users", order_by=["name", "id"])
        assert query == 'SELECT * FROM "users" ORDER BY "name", "id"'

        # With LIMIT
        query = formatter.build_select_query("users", limit=10)
        assert query == 'SELECT * FROM "users" LIMIT 10'

    def test_build_insert_query(self):
        """Test safe INSERT query building."""
        formatter = SQLSafeFormatter("duckdb")

        query = formatter.build_insert_query("users", ["id", "name", "email"])
        expected = 'INSERT INTO "users" ("id", "name", "email") VALUES (?, ?, ?)'
        assert query == expected

    def test_build_update_query(self):
        """Test safe UPDATE query building."""
        formatter = SQLSafeFormatter("duckdb")

        query = formatter.build_update_query("users", ["name", "email"])
        expected = 'UPDATE "users" SET "name" = ?, "email" = ?'
        assert query == expected

        # With WHERE clause
        query = formatter.build_update_query("users", ["name"], where_clause='"id" = ?')
        expected = 'UPDATE "users" SET "name" = ? WHERE "id" = ?'
        assert query == expected

    def test_build_delete_query(self):
        """Test safe DELETE query building."""
        formatter = SQLSafeFormatter("duckdb")

        query = formatter.build_delete_query("users")
        assert query == 'DELETE FROM "users"'

        # With WHERE clause
        query = formatter.build_delete_query("users", where_clause='"id" = ?')
        assert query == 'DELETE FROM "users" WHERE "id" = ?'

    def test_build_create_table_query(self):
        """Test safe CREATE TABLE query building."""
        formatter = SQLSafeFormatter("duckdb")

        columns = ["id INTEGER PRIMARY KEY", "name VARCHAR(100)"]
        query = formatter.build_create_table_query("users", columns)
        expected = 'CREATE TABLE "users" (id INTEGER PRIMARY KEY, name VARCHAR(100))'
        assert query == expected

        # With IF NOT EXISTS
        query = formatter.build_create_table_query("users", columns, if_not_exists=True)
        expected = 'CREATE TABLE IF NOT EXISTS "users" (id INTEGER PRIMARY KEY, name VARCHAR(100))'
        assert query == expected


class TestParameterizedQueryBuilder:
    """Test parameterized query building."""

    def test_basic_parameters(self):
        """Test basic parameter handling."""
        builder = ParameterizedQueryBuilder("duckdb")

        placeholder1 = builder.add_parameter("John")
        placeholder2 = builder.add_parameter(25)

        assert placeholder1 == "?"
        assert placeholder2 == "?"
        assert builder.parameters == ["John", 25]

    def test_postgres_parameters(self):
        """Test PostgreSQL parameter placeholders."""
        builder = ParameterizedQueryBuilder("postgres")

        placeholder1 = builder.add_parameter("John")
        placeholder2 = builder.add_parameter(25)

        assert placeholder1 == "$1"
        assert placeholder2 == "$2"

    def test_build_where_condition(self):
        """Test WHERE condition building."""
        builder = ParameterizedQueryBuilder("duckdb")

        condition = builder.build_where_condition("name", "=", "John")
        assert condition == '"name" = ?'
        assert builder.parameters == ["John"]

    def test_invalid_operator(self):
        """Test that invalid operators are rejected."""
        builder = ParameterizedQueryBuilder("duckdb")

        with pytest.raises(ValueError, match="Invalid operator"):
            builder.build_where_condition("name", "'; DROP TABLE", "value")

    def test_build_in_condition(self):
        """Test IN condition building."""
        builder = ParameterizedQueryBuilder("duckdb")

        condition = builder.build_in_condition(
            "status", ["active", "pending", "inactive"]
        )
        assert condition == '"status" IN (?, ?, ?)'
        assert builder.parameters == ["active", "pending", "inactive"]

    def test_get_query_and_parameters(self):
        """Test final query and parameters extraction."""
        builder = ParameterizedQueryBuilder("duckdb")

        builder.add_parameter("John")
        builder.add_parameter(25)

        query, params = builder.get_query_and_parameters(
            "SELECT * FROM users WHERE name = ? AND age = ?"
        )
        assert query == "SELECT * FROM users WHERE name = ? AND age = ?"
        assert params == ["John", 25]


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_safe_table_name(self):
        """Test safe table name function."""
        assert safe_table_name("users") == '"users"'
        assert safe_table_name("users", "public") == '"public"."users"'
        assert safe_table_name("users", dialect="mysql") == "`users`"

    def test_safe_column_list(self):
        """Test safe column list function."""
        assert safe_column_list(["id", "name"]) == '"id", "name"'
        assert safe_column_list(["id", "name"], dialect="mysql") == "`id`, `name`"

    def test_validate_identifier_function(self):
        """Test validate identifier function."""
        # Should not raise for valid identifier
        validate_identifier("valid_name")

        # Should raise for invalid identifier
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("invalid'; DROP TABLE")


class TestSQLInjectionPrevention:
    """Test SQL injection prevention."""

    def test_prevents_union_injection(self):
        """Test prevention of UNION-based injection."""
        formatter = SQLSafeFormatter("duckdb")

        # This should fail validation
        with pytest.raises(ValueError):
            formatter.quote_identifier("id UNION SELECT password FROM users --")

    def test_prevents_comment_injection(self):
        """Test prevention of comment-based injection."""
        formatter = SQLSafeFormatter("duckdb")

        with pytest.raises(ValueError):
            formatter.quote_identifier("id; -- DROP TABLE users")

    def test_prevents_quote_escape_injection(self):
        """Test prevention of quote escape injection."""
        formatter = SQLSafeFormatter("duckdb")

        with pytest.raises(ValueError):
            formatter.quote_identifier("name'; DROP TABLE users; --")

    def test_prevents_stored_procedure_injection(self):
        """Test prevention of stored procedure injection."""
        formatter = SQLSafeFormatter("duckdb")

        with pytest.raises(ValueError):
            formatter.quote_identifier("id; EXEC xp_cmdshell('format c:')")

    def test_safe_parameterized_query_example(self):
        """Test complete safe query building example."""
        # This demonstrates how to safely build queries
        formatter = SQLSafeFormatter("duckdb")
        builder = ParameterizedQueryBuilder("duckdb")

        # Safe table and column references
        table_ref = formatter.quote_schema_table("users", "public")

        # Safe WHERE conditions with parameters
        name_condition = builder.build_where_condition("name", "=", "John")
        age_condition = builder.build_where_condition("age", ">", 18)

        # Build final query
        query = f"SELECT * FROM {table_ref} WHERE {name_condition} AND {age_condition}"
        final_query, params = builder.get_query_and_parameters(query)

        expected_query = 'SELECT * FROM "public"."users" WHERE "name" = ? AND "age" > ?'
        assert final_query == expected_query
        assert params == ["John", 18]


class TestIntegrationWithExistingCode:
    """Test integration with existing SQLFlow code patterns."""

    def test_replaces_f_string_patterns(self):
        """Test that safe patterns replace dangerous f-string patterns."""
        # Old dangerous pattern:
        # query = f"SELECT * FROM {table_name} WHERE {column} = '{value}'"

        # New safe pattern:
        formatter = SQLSafeFormatter("duckdb")
        builder = ParameterizedQueryBuilder("duckdb")

        table_name = "users"
        column = "name"
        value = "John'; DROP TABLE users; --"  # Malicious input

        # Safe construction
        table_ref = formatter.quote_identifier(table_name)
        where_condition = builder.build_where_condition(column, "=", value)

        query = f"SELECT * FROM {table_ref} WHERE {where_condition}"
        final_query, params = builder.get_query_and_parameters(query)

        # The malicious input is now safely parameterized
        assert final_query == 'SELECT * FROM "users" WHERE "name" = ?'
        assert params == ["John'; DROP TABLE users; --"]

    def test_merge_sql_generation_safety(self):
        """Test that merge SQL generation is safe."""
        formatter = SQLSafeFormatter("duckdb")

        # Simulate what the fixed SQL generator should do
        table_name = "target_table"
        source_name = "source_table"
        merge_keys = ["id", "customer_id"]

        # All identifiers are validated and quoted
        quoted_table = formatter.quote_identifier(table_name)
        quoted_source = formatter.quote_identifier(source_name)
        quoted_keys = [formatter.quote_identifier(key) for key in merge_keys]

        # Build safe merge conditions
        join_conditions = []
        for key in quoted_keys:
            join_conditions.append(f"t.{key} = s.{key}")

        join_clause = " AND ".join(join_conditions)

        # This is now safe from injection
        merge_sql = f"""
        INSERT INTO {quoted_table}
        SELECT s.* FROM {quoted_source} s
        LEFT JOIN {quoted_table} t ON {join_clause}
        WHERE t.{quoted_keys[0]} IS NULL
        """

        # Verify no unquoted identifiers remain
        assert '"target_table"' in merge_sql
        assert '"source_table"' in merge_sql
        assert '"id"' in merge_sql
        assert '"customer_id"' in merge_sql
