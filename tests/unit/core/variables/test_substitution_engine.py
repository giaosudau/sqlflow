"""Tests for the VariableSubstitutionEngine.

Tests the unified interface for variable substitution across all contexts.
Uses minimal mocking and focuses on real functionality.

Phase 2 Architectural Cleanup: These tests verify the unified engine works
correctly for all contexts (text, SQL, AST) and preserves existing behavior.
"""

from sqlflow.core.variables.substitution_engine import (
    ASTFormatter,
    SQLFormatter,
    TextFormatter,
    VariableSubstitutionEngine,
    get_substitution_engine,
    reset_substitution_engine,
)


class TestVariableSubstitutionEngine:
    """Test the unified variable substitution engine."""

    def setup_method(self):
        """Set up test fixtures."""
        self.variables = {
            "table": "users",
            "schema": "public",
            "limit": 100,
            "active": True,
            "price": 29.99,
            "name": "John's Store",  # Test escaping
        }
        self.engine = VariableSubstitutionEngine(self.variables)

    def test_engine_initialization(self):
        """Test engine initialization."""
        engine = VariableSubstitutionEngine()
        assert engine.variables == {}
        assert len(engine.formatters) == 3
        assert "text" in engine.formatters
        assert "sql" in engine.formatters
        assert "ast" in engine.formatters

    def test_engine_with_variables(self):
        """Test engine initialization with variables."""
        variables = {"var1": "value1", "var2": 42}
        engine = VariableSubstitutionEngine(variables)

        assert engine.variables == variables
        assert engine.has_variable("var1")
        assert engine.has_variable("var2")
        assert not engine.has_variable("missing")

    def test_text_context_substitution(self):
        """Test variable substitution in text context."""
        template = "Hello ${name}, your limit is ${limit}"
        result = self.engine.substitute(template, context="text")

        expected = "Hello John's Store, your limit is 100"
        assert result == expected

    def test_sql_context_substitution(self):
        """Test variable substitution in SQL context."""
        template = (
            "SELECT * FROM ${schema}.${table} WHERE active = ${active} LIMIT ${limit}"
        )
        result = self.engine.substitute(template, context="sql")

        # SQL context should quote strings and format booleans properly
        expected = "SELECT * FROM 'public'.'users' WHERE active = true LIMIT 100"
        assert result == expected

    def test_sql_context_with_quoted_strings(self):
        """Test SQL context doesn't double-quote values already in quotes."""
        template = "INSERT INTO users (name) VALUES ('${name}')"
        result = self.engine.substitute(template, context="sql")

        # Should not double-quote when already inside quotes
        expected = "INSERT INTO users (name) VALUES ('John's Store')"
        assert result == expected

    def test_ast_context_substitution(self):
        """Test variable substitution in AST context."""
        template = "region == '${schema}' and limit > ${limit}"
        result = self.engine.substitute(template, context="ast")

        # AST context should quote strings and format properly for Python
        expected = "region == 'public' and limit > 100"
        assert result == expected

    def test_default_values(self):
        """Test substitution with default values."""
        template = "Database: ${db|default_db}, Port: ${port|5432}"
        result = self.engine.substitute(template, context="text")

        expected = "Database: default_db, Port: 5432"
        assert result == expected

    def test_missing_variables_text_context(self):
        """Test handling of missing variables in text context."""
        template = "Value: ${missing_var}"
        result = self.engine.substitute(template, context="text")

        # Text context should preserve placeholder for missing variables
        assert result == "Value: ${missing_var}"

    def test_missing_variables_sql_context(self):
        """Test handling of missing variables in SQL context."""
        template = "SELECT ${missing_column} FROM users"
        result = self.engine.substitute(template, context="sql")

        # SQL context should use NULL for missing variables
        expected = "SELECT NULL FROM users"
        assert result == expected

    def test_missing_variables_ast_context(self):
        """Test handling of missing variables in AST context."""
        template = "value == ${missing_var}"
        result = self.engine.substitute(template, context="ast")

        # AST context should use None for missing variables
        expected = "value == None"
        assert result == expected

    def test_empty_template(self):
        """Test substitution with empty template."""
        result = self.engine.substitute("", context="sql")
        assert result == ""

    def test_template_without_variables(self):
        """Test substitution with template without variables."""
        template = "SELECT * FROM users WHERE active = true"
        result = self.engine.substitute(template, context="sql")
        assert result == template

    def test_register_variable(self):
        """Test registering individual variables."""
        engine = VariableSubstitutionEngine()
        engine.register_variable("new_var", "new_value")

        assert engine.has_variable("new_var")
        assert engine.get_variable("new_var") == "new_value"

    def test_register_variables(self):
        """Test registering multiple variables."""
        engine = VariableSubstitutionEngine()
        new_vars = {"var1": "value1", "var2": "value2"}
        engine.register_variables(new_vars)

        assert engine.has_variable("var1")
        assert engine.has_variable("var2")
        assert engine.get_variable("var1") == "value1"
        assert engine.get_variable("var2") == "value2"

    def test_complex_sql_substitution(self):
        """Test complex SQL with multiple variables and contexts."""
        template = """
        SELECT ${schema}.${table}.id, 
               ${schema}.${table}.name,
               ${price} as base_price
        FROM ${schema}.${table}
        WHERE active = ${active}
          AND price >= ${price}
        ORDER BY created_at DESC
        LIMIT ${limit}
        """

        result = self.engine.substitute(template, context="sql")

        # Verify key substitutions
        assert "'public'.'users'" in result
        assert "29.99 as base_price" in result
        assert "active = true" in result
        assert "price >= 29.99" in result
        assert "LIMIT 100" in result

    def test_quote_escaping_in_sql(self):
        """Test proper quote escaping in SQL context."""
        engine = VariableSubstitutionEngine({"store_name": "John's Hardware & Co."})
        template = "INSERT INTO stores (name) VALUES (${store_name})"
        result = engine.substitute(template, context="sql")

        # Should escape single quotes properly
        expected = "INSERT INTO stores (name) VALUES ('John''s Hardware & Co.')"
        assert result == expected

    def test_quote_escaping_in_ast(self):
        """Test proper quote escaping in AST context."""
        engine = VariableSubstitutionEngine({"text": "It's a test"})
        template = "message == '${text}'"
        result = engine.substitute(template, context="ast")

        # Should escape single quotes for Python
        expected = "message == 'It\\'s a test'"
        assert result == expected

    def test_numeric_types_formatting(self):
        """Test formatting of different numeric types."""
        variables = {
            "int_val": 42,
            "float_val": 3.14159,
            "negative": -10,
            "zero": 0,
        }
        engine = VariableSubstitutionEngine(variables)

        # Test SQL context
        template = "VALUES (${int_val}, ${float_val}, ${negative}, ${zero})"
        result = engine.substitute(template, context="sql")
        expected = "VALUES (42, 3.14159, -10, 0)"
        assert result == expected

        # Test AST context
        template = "values = [${int_val}, ${float_val}, ${negative}, ${zero}]"
        result = engine.substitute(template, context="ast")
        expected = "values = [42, 3.14159, -10, 0]"
        assert result == expected

    def test_boolean_formatting(self):
        """Test formatting of boolean values."""
        variables = {"flag1": True, "flag2": False}
        engine = VariableSubstitutionEngine(variables)

        # SQL context
        template = "SELECT * WHERE flag1 = ${flag1} AND flag2 = ${flag2}"
        result = engine.substitute(template, context="sql")
        expected = "SELECT * WHERE flag1 = true AND flag2 = false"
        assert result == expected

        # AST context
        template = "result = ${flag1} and ${flag2}"
        result = engine.substitute(template, context="ast")
        expected = "result = True and False"
        assert result == expected

    def test_none_value_handling(self):
        """Test handling of None values."""
        variables = {"null_value": None}
        engine = VariableSubstitutionEngine(variables)

        # SQL context
        template = "UPDATE users SET name = ${null_value}"
        result = engine.substitute(template, context="sql")
        expected = "UPDATE users SET name = NULL"
        assert result == expected

        # AST context
        template = "value is ${null_value}"
        result = engine.substitute(template, context="ast")
        expected = "value is None"
        assert result == expected

        # Text context
        template = "Value: ${null_value}"
        result = engine.substitute(template, context="text")
        expected = "Value: "
        assert result == expected

    def test_engine_statistics(self):
        """Test engine statistics."""
        stats = self.engine.get_stats()

        assert "variable_count" in stats
        assert "parser_stats" in stats
        assert stats["variable_count"] == len(self.variables)

    def test_cache_operations(self):
        """Test cache operations."""
        # Cache should work normally
        template = "${table}"
        result1 = self.engine.substitute(template, context="sql")
        result2 = self.engine.substitute(template, context="sql")
        assert result1 == result2 == "'users'"

        # Clear cache should not affect functionality
        self.engine.clear_cache()
        result3 = self.engine.substitute(template, context="sql")
        assert result3 == "'users'"


class TestContextFormatters:
    """Test individual context formatters."""

    def test_text_formatter(self):
        """Test TextFormatter."""
        formatter = TextFormatter()

        assert formatter.format_value("hello", {}) == "hello"
        assert formatter.format_value(42, {}) == "42"
        assert formatter.format_value(True, {}) == "True"
        assert formatter.format_value(None, {}) == ""

    def test_sql_formatter(self):
        """Test SQLFormatter."""
        formatter = SQLFormatter()

        # Basic string formatting
        assert formatter.format_value("hello", {}) == "'hello'"

        # Numeric formatting
        assert formatter.format_value(42, {}) == "42"
        assert formatter.format_value(3.14, {}) == "3.14"

        # Boolean formatting
        assert formatter.format_value(True, {}) == "true"
        assert formatter.format_value(False, {}) == "false"

        # None formatting
        assert formatter.format_value(None, {}) == "NULL"

        # Quote escaping
        assert formatter.format_value("it's", {}) == "'it''s'"

        # Inside quotes context
        context = {"inside_quotes": True}
        assert formatter.format_value("hello", context) == "hello"

    def test_ast_formatter(self):
        """Test ASTFormatter."""
        formatter = ASTFormatter()

        # String formatting
        assert formatter.format_value("hello", {}) == "'hello'"

        # Numeric formatting
        assert formatter.format_value(42, {}) == "42"
        assert formatter.format_value(3.14, {}) == "3.14"

        # Boolean formatting
        assert formatter.format_value(True, {}) == "True"
        assert formatter.format_value(False, {}) == "False"

        # None formatting
        assert formatter.format_value(None, {}) == "None"

        # Quote escaping
        assert formatter.format_value("it's", {}) == "'it\\'s'"


class TestGlobalEngine:
    """Test global engine functions."""

    def teardown_method(self):
        """Clean up global state after each test."""
        reset_substitution_engine()

    def test_get_substitution_engine_singleton(self):
        """Test global engine singleton behavior."""
        engine1 = get_substitution_engine()
        engine2 = get_substitution_engine()

        assert engine1 is engine2
        assert isinstance(engine1, VariableSubstitutionEngine)

    def test_get_substitution_engine_with_variables(self):
        """Test global engine with variables."""
        variables = {"var1": "value1", "var2": "value2"}
        engine = get_substitution_engine(variables)

        assert engine.has_variable("var1")
        assert engine.has_variable("var2")

    def test_get_substitution_engine_add_variables(self):
        """Test adding variables to existing global engine."""
        # First call creates engine
        engine1 = get_substitution_engine({"var1": "value1"})

        # Second call adds more variables
        engine2 = get_substitution_engine({"var2": "value2"})

        assert engine1 is engine2
        assert engine1.has_variable("var1")
        assert engine1.has_variable("var2")

    def test_reset_substitution_engine(self):
        """Test resetting global engine."""
        # Create engine
        engine1 = get_substitution_engine({"var1": "value1"})

        # Reset
        reset_substitution_engine()

        # Get new engine
        engine2 = get_substitution_engine()

        assert engine1 is not engine2
        assert not engine2.has_variable("var1")


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_variable_name(self):
        """Test handling of empty variable names."""
        engine = VariableSubstitutionEngine()
        template = "${}"
        result = engine.substitute(template, context="sql")

        # Should handle gracefully (behavior depends on parser)
        assert isinstance(result, str)

    def test_malformed_variables(self):
        """Test handling of malformed variables."""
        engine = VariableSubstitutionEngine({"var": "value"})

        test_cases = [
            "${var",  # Missing closing brace
            "var}",  # Missing opening
            "${var|",  # Incomplete default
        ]

        for template in test_cases:
            result = engine.substitute(template, context="sql")
            # Should handle gracefully without crashing
            assert isinstance(result, str)

    def test_very_long_template(self):
        """Test performance with very long templates."""
        engine = VariableSubstitutionEngine({"var": "value"})

        # Create a long template with many variables
        parts = ["${var}"] * 1000
        template = " ".join(parts)

        result = engine.substitute(template, context="text")

        # Should complete without issues
        assert "value" in result
        assert len(result.split()) == 1000

    def test_unicode_handling(self):
        """Test handling of unicode characters."""
        variables = {"emoji": "🚀", "chinese": "你好", "accents": "café"}
        engine = VariableSubstitutionEngine(variables)

        template = "Welcome ${chinese}! Enjoy your ${accents} ${emoji}"
        result = engine.substitute(template, context="text")

        expected = "Welcome 你好! Enjoy your café 🚀"
        assert result == expected

        # Test SQL context with unicode
        template = "INSERT INTO messages (text) VALUES (${chinese})"
        result = engine.substitute(template, context="sql")
        expected = "INSERT INTO messages (text) VALUES ('你好')"
        assert result == expected

    def test_nested_quotes(self):
        """Test handling of nested quotes."""
        variables = {"json": '{"key": "value"}'}
        engine = VariableSubstitutionEngine(variables)

        template = "Data: ${json}"
        result = engine.substitute(template, context="sql")

        # Should handle nested quotes properly
        expected = """Data: '{"key": "value"}'"""
        assert result == expected

    def test_context_detection_disabled(self):
        """Test substitution with context detection disabled."""
        engine = VariableSubstitutionEngine({"name": "test"})

        template = "INSERT INTO users (name) VALUES ('${name}')"
        result = engine.substitute(template, context="sql", context_detection=False)

        # Without context detection, should always apply formatter rules
        expected = "INSERT INTO users (name) VALUES (''test'')"
        assert result == expected
