"""Tests for the Unified Variable Parser.

Tests the single source of truth for variable parsing in SQLFlow.
Uses minimal mocking and focuses on real functionality.
"""

import re
from unittest.mock import patch

import pytest

from sqlflow.core.variables.unified_parser import (
    ParseResult,
    UnifiedVariableParser,
    VariableExpression,
    get_unified_parser,
    get_unified_pattern,
)


class TestUnifiedVariableParser:
    """Test the unified variable parser with comprehensive coverage."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = UnifiedVariableParser()

    def test_get_pattern_returns_compiled_regex(self):
        """Test that get_pattern returns a compiled regex pattern."""
        pattern = UnifiedVariableParser.get_pattern()

        assert isinstance(pattern, re.Pattern)
        assert pattern.pattern == r"\$\{([^}|]+)(?:\|([^}]+))?\}"
        assert pattern.flags & re.MULTILINE
        assert pattern.flags & re.DOTALL

    def test_parse_empty_string(self):
        """Test parsing empty string returns empty result."""
        result = self.parser.parse("")

        assert isinstance(result, ParseResult)
        assert result.expressions == []
        assert result.has_variables is False
        assert result.parse_time_ms >= 0
        assert result.unique_variables == set()
        assert result.total_variable_count == 0

    def test_parse_no_variables(self):
        """Test parsing text with no variables."""
        text = "SELECT * FROM users WHERE active = true"
        result = self.parser.parse(text)

        assert result.expressions == []
        assert result.has_variables is False
        assert result.unique_variables == set()
        assert result.total_variable_count == 0

    def test_parse_simple_variable(self):
        """Test parsing simple variable without default."""
        text = "SELECT * FROM ${table}"
        result = self.parser.parse(text)

        assert result.has_variables is True
        assert len(result.expressions) == 1
        assert result.total_variable_count == 1
        assert result.unique_variables == {"table"}

        expr = result.expressions[0]
        assert expr.variable_name == "table"
        assert expr.default_value is None
        assert expr.original_match == "${table}"
        assert expr.span == (14, 22)
        assert expr.line_number == 1
        assert expr.column_number == 15

    def test_parse_variable_with_default(self):
        """Test parsing variable with default value."""
        text = "Hello ${name|Anonymous}"
        result = self.parser.parse(text)

        assert result.has_variables is True
        assert len(result.expressions) == 1

        expr = result.expressions[0]
        assert expr.variable_name == "name"
        assert expr.default_value == "Anonymous"
        assert expr.original_match == "${name|Anonymous}"

    def test_parse_variable_with_quoted_default(self):
        """Test parsing variable with quoted default value."""
        text = "Config: ${env|'production'}"
        result = self.parser.parse(text)

        expr = result.expressions[0]
        assert expr.variable_name == "env"
        assert expr.default_value == "production"  # Quotes stripped
        assert expr.original_match == "${env|'production'}"

    def test_parse_variable_with_double_quoted_default(self):
        """Test parsing variable with double-quoted default value."""
        text = 'Server: ${host|"localhost"}'
        result = self.parser.parse(text)

        expr = result.expressions[0]
        assert expr.variable_name == "host"
        assert expr.default_value == "localhost"  # Quotes stripped
        assert expr.original_match == '${host|"localhost"}'

    def test_parse_variable_with_complex_default(self):
        """Test parsing variable with complex SQL default (quotes preserved)."""
        text = "SELECT * FROM ${table|'SELECT * FROM users'}"
        result = self.parser.parse(text)

        expr = result.expressions[0]
        assert expr.variable_name == "table"
        # Complex SQL expression keeps quotes
        assert expr.default_value == "'SELECT * FROM users'"

    def test_parse_multiple_variables(self):
        """Test parsing multiple variables in same text."""
        text = "INSERT INTO ${table} (name, age) VALUES ('${name}', ${age|25})"
        result = self.parser.parse(text)

        assert result.has_variables is True
        assert len(result.expressions) == 3
        assert result.total_variable_count == 3
        assert result.unique_variables == {"table", "name", "age"}

        # Check each variable
        table_expr = result.expressions[0]
        assert table_expr.variable_name == "table"
        assert table_expr.default_value is None

        name_expr = result.expressions[1]
        assert name_expr.variable_name == "name"
        assert name_expr.default_value is None

        age_expr = result.expressions[2]
        assert age_expr.variable_name == "age"
        assert age_expr.default_value == "25"

    def test_parse_duplicate_variables(self):
        """Test parsing text with duplicate variable names."""
        text = "SELECT ${col}, ${col} FROM ${table}"
        result = self.parser.parse(text)

        assert len(result.expressions) == 3  # All instances found
        assert result.total_variable_count == 3
        assert result.unique_variables == {"col", "table"}  # Unique count
        assert len(result.unique_variables) == 2

    def test_parse_multiline_text(self):
        """Test parsing multiline text with line/column tracking."""
        text = """SELECT *
FROM ${table}
WHERE active = ${status|true}"""

        result = self.parser.parse(text)

        assert len(result.expressions) == 2

        # First variable on line 2
        table_expr = result.expressions[0]
        assert table_expr.variable_name == "table"
        assert table_expr.line_number == 2
        assert table_expr.column_number == 6

        # Second variable on line 3
        status_expr = result.expressions[1]
        assert status_expr.variable_name == "status"
        assert status_expr.default_value == "true"
        assert status_expr.line_number == 3
        assert status_expr.column_number == 16

    def test_parse_with_whitespace(self):
        """Test parsing variables with whitespace in names."""
        text = "SELECT * FROM ${ table } WHERE id = ${ user_id | 1 }"
        result = self.parser.parse(text)

        assert len(result.expressions) == 2

        # Whitespace should be stripped from variable names
        table_expr = result.expressions[0]
        assert table_expr.variable_name == "table"

        user_expr = result.expressions[1]
        assert user_expr.variable_name == "user_id"
        assert user_expr.default_value == "1"

    def test_parse_empty_variable_name(self):
        """Test parsing with empty variable name (should be ignored)."""
        text = "SELECT * FROM ${} WHERE id = ${user_id}"
        result = self.parser.parse(text)

        # Only the valid variable should be parsed
        assert len(result.expressions) == 1
        assert result.expressions[0].variable_name == "user_id"

    def test_parse_with_caching(self):
        """Test that caching works correctly."""
        text = "SELECT * FROM ${table}"

        # First parse should cache the result
        result1 = self.parser.parse(text, use_cache=True)

        # Second parse should use cached result
        result2 = self.parser.parse(text, use_cache=True)

        # Results should be identical
        assert (
            result1.expressions[0].variable_name == result2.expressions[0].variable_name
        )
        assert result1.parse_time_ms >= 0
        assert result2.parse_time_ms >= 0

        # Cache should contain the text
        stats = self.parser.get_cache_stats()
        assert stats["cache_size"] == 1
        assert text in stats["sample_keys"]  # Updated cache stats structure

    def test_parse_without_caching(self):
        """Test parsing without caching."""
        text = "SELECT * FROM ${table}"

        result = self.parser.parse(text, use_cache=False)

        assert result.has_variables is True
        assert len(result.expressions) == 1

        # Cache should be empty
        stats = self.parser.get_cache_stats()
        assert stats["cache_size"] == 0

    def test_clear_cache(self):
        """Test cache clearing functionality."""
        text = "SELECT * FROM ${table}"

        # Parse to populate cache
        self.parser.parse(text, use_cache=True)
        assert self.parser.get_cache_stats()["cache_size"] == 1

        # Clear cache
        self.parser.clear_cache()
        assert self.parser.get_cache_stats()["cache_size"] == 0

    def test_performance_metrics(self):
        """Test that performance metrics are captured."""
        text = "SELECT * FROM ${table} WHERE id = ${user_id|1}"
        result = self.parser.parse(text)

        assert result.parse_time_ms >= 0
        assert isinstance(result.parse_time_ms, float)

        # Should be reasonably fast (less than 10ms for small text)
        assert result.parse_time_ms < 10.0

    def test_complex_sql_with_many_variables(self):
        """Test parsing complex SQL with many variables."""
        text = """
        SELECT ${columns|*}
        FROM ${schema|public}.${table}
        WHERE ${filter_column|id} = ${filter_value|1}
        AND created_at >= '${start_date|2023-01-01}'
        ORDER BY ${order_by|created_at} ${direction|ASC}
        LIMIT ${limit|100}
        """

        result = self.parser.parse(text)

        assert result.has_variables is True
        assert len(result.expressions) == 9
        assert result.total_variable_count == 9

        expected_vars = {
            "columns",
            "schema",
            "table",
            "filter_column",
            "filter_value",
            "start_date",
            "order_by",
            "direction",
            "limit",
        }
        assert result.unique_variables == expected_vars

        # Check a few specific variables
        columns_expr = next(
            e for e in result.expressions if e.variable_name == "columns"
        )
        assert columns_expr.default_value == "*"

        schema_expr = next(e for e in result.expressions if e.variable_name == "schema")
        assert schema_expr.default_value == "public"

        start_date_expr = next(
            e for e in result.expressions if e.variable_name == "start_date"
        )
        assert start_date_expr.default_value == "2023-01-01"  # Quotes stripped


class TestGlobalParserFunctions:
    """Test global parser functions and singleton behavior."""

    def test_get_unified_parser_singleton(self):
        """Test that get_unified_parser returns same instance."""
        parser1 = get_unified_parser()
        parser2 = get_unified_parser()

        assert parser1 is parser2
        assert isinstance(parser1, UnifiedVariableParser)

    def test_get_unified_pattern(self):
        """Test get_unified_pattern function."""
        pattern = get_unified_pattern()

        assert isinstance(pattern, re.Pattern)
        assert pattern.pattern == r"\$\{([^}|]+)(?:\|([^}]+))?\}"

        # Should be same as class method
        class_pattern = UnifiedVariableParser.get_pattern()
        assert pattern.pattern == class_pattern.pattern
        assert pattern.flags == class_pattern.flags

    @patch("sqlflow.core.variables.unified_parser.logger")
    def test_parser_logging(self, mock_logger):
        """Test that parser logs debug information."""
        parser = UnifiedVariableParser()
        text = "SELECT * FROM ${table}"

        parser.parse(text)

        # Should log debug information about parsing
        mock_logger.debug.assert_called()
        call_args = mock_logger.debug.call_args[0][0]
        assert "variables" in call_args
        assert "ms" in call_args


class TestVariableExpressionDataClass:
    """Test the VariableExpression dataclass."""

    def test_variable_expression_creation(self):
        """Test creating VariableExpression with all fields."""
        expr = VariableExpression(
            variable_name="test_var",
            default_value="default",
            original_match="${test_var|default}",
            span=(0, 19),
            line_number=2,
            column_number=5,
        )

        assert expr.variable_name == "test_var"
        assert expr.default_value == "default"
        assert expr.original_match == "${test_var|default}"
        assert expr.span == (0, 19)
        assert expr.line_number == 2
        assert expr.column_number == 5

    def test_variable_expression_defaults(self):
        """Test VariableExpression with default values."""
        expr = VariableExpression(
            variable_name="test",
            default_value=None,
            original_match="${test}",
            span=(0, 7),
        )

        # Default values should be set
        assert expr.line_number == 1
        assert expr.column_number == 1


class TestParseResultDataClass:
    """Test the ParseResult dataclass."""

    def test_parse_result_creation(self):
        """Test creating ParseResult with all fields."""
        expressions = [
            VariableExpression("var1", None, "${var1}", (0, 6)),
            VariableExpression("var2", "default", "${var2|default}", (7, 21)),
        ]

        result = ParseResult(
            expressions=expressions,
            has_variables=True,
            parse_time_ms=1.5,
            unique_variables={"var1", "var2"},
            total_variable_count=2,
        )

        assert len(result.expressions) == 2
        assert result.has_variables is True
        assert result.parse_time_ms == 1.5
        assert result.unique_variables == {"var1", "var2"}
        assert result.total_variable_count == 2


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = UnifiedVariableParser()

    def test_parse_none_input(self):
        """Test parsing None input."""
        with pytest.raises(TypeError):
            self.parser.parse(None)

    def test_parse_malformed_variables(self):
        """Test parsing malformed variable syntax."""
        test_cases = [
            "${",  # Incomplete opening
            "${}",  # Empty variable
            "${var",  # Missing closing brace
            "var}",  # Missing opening
            "${var|",  # Incomplete default
            "${var|}",  # Empty default
        ]

        for text in test_cases:
            result = self.parser.parse(text)
            # Should handle gracefully, may or may not find variables
            # depending on the specific malformation
            assert isinstance(result, ParseResult)

    def test_parse_nested_braces(self):
        """Test parsing with nested braces (not supported, should handle gracefully)."""
        text = "SELECT * FROM ${table_${env}}"
        result = self.parser.parse(text)

        # Should find the outer variable structure
        assert isinstance(result, ParseResult)
        # Behavior may vary, but should not crash

    def test_parse_very_long_text(self):
        """Test parsing very long text for performance."""
        # Create a long text with many variables
        variables = [f"${{{i}|default_{i}}}" for i in range(100)]
        text = " ".join(variables)

        result = self.parser.parse(text)

        assert result.has_variables is True
        assert len(result.expressions) == 100
        assert result.parse_time_ms >= 0
        # Should complete in reasonable time (less than 100ms)
        assert result.parse_time_ms < 100.0

    def test_unicode_handling(self):
        """Test parsing text with unicode characters."""
        text = "SELECT * FROM ${table} WHERE name = '${name|José}'"
        result = self.parser.parse(text)

        assert len(result.expressions) == 2
        name_expr = next(e for e in result.expressions if e.variable_name == "name")
        assert name_expr.default_value == "José"

    def test_special_characters_in_defaults(self):
        """Test parsing variables with special characters in defaults."""
        text = "Config: ${path|/tmp/file.txt} Port: ${port|8080}"
        result = self.parser.parse(text)

        assert len(result.expressions) == 2

        path_expr = result.expressions[0]
        assert path_expr.variable_name == "path"
        assert path_expr.default_value == "/tmp/file.txt"

        port_expr = result.expressions[1]
        assert port_expr.variable_name == "port"
        assert port_expr.default_value == "8080"
