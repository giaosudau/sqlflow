"""Tests for Context-Specific Variable Formatters.

Tests all formatter implementations with minimal mocking and comprehensive
coverage of real functionality and edge cases.
"""

import json
from unittest.mock import patch

import pytest

from sqlflow.core.variables.formatters import (
    ASTFormatter,
    FormatterRegistry,
    JSONFormatter,
    SQLFormatter,
    TextFormatter,
    VariableFormatter,
    format_missing_variable,
    format_value,
    get_formatter,
    get_formatter_registry,
)


class TestSQLFormatter:
    """Test SQL formatter with comprehensive SQL value handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = SQLFormatter()

    def test_format_none_value(self):
        """Test formatting None values as SQL NULL."""
        result = self.formatter.format_value(None)
        assert result == "NULL"

    def test_format_boolean_values(self):
        """Test formatting boolean values as SQL booleans."""
        assert self.formatter.format_value(True) == "TRUE"
        assert self.formatter.format_value(False) == "FALSE"

    def test_format_numeric_values(self):
        """Test formatting numeric values without quotes."""
        assert self.formatter.format_value(42) == "42"
        assert self.formatter.format_value(3.14) == "3.14"
        assert self.formatter.format_value(0) == "0"
        assert self.formatter.format_value(-123) == "-123"

    def test_format_string_values(self):
        """Test formatting string values with proper SQL quoting."""
        assert self.formatter.format_value("hello") == "'hello'"
        assert self.formatter.format_value("world") == "'world'"
        assert self.formatter.format_value("") == "''"

    def test_format_string_with_quotes(self):
        """Test formatting strings that contain quotes."""
        assert self.formatter.format_value("don't") == "'don''t'"
        assert self.formatter.format_value("it's working") == "'it''s working'"
        assert self.formatter.format_value("test 'value'") == "'test ''value'''"

    def test_format_already_quoted_strings(self):
        """Test that already quoted strings are not double-quoted."""
        assert self.formatter.format_value("'already quoted'") == "'already quoted'"
        assert self.formatter.format_value('"double quoted"') == '"double quoted"'

    def test_format_sql_keywords(self):
        """Test that SQL keywords are not quoted."""
        assert self.formatter.format_value("NULL") == "NULL"
        assert self.formatter.format_value("TRUE") == "TRUE"
        assert self.formatter.format_value("FALSE") == "FALSE"
        assert self.formatter.format_value("CURRENT_DATE") == "CURRENT_DATE"
        assert self.formatter.format_value("CURRENT_TIMESTAMP") == "CURRENT_TIMESTAMP"

    def test_format_sql_functions(self):
        """Test that SQL functions are not quoted."""
        assert self.formatter.format_value("NOW()") == "NOW()"
        assert self.formatter.format_value("COUNT(*)") == "COUNT(*)"
        assert self.formatter.format_value("SUM(amount)") == "SUM(amount)"

    def test_format_numeric_strings(self):
        """Test that numeric strings are not quoted."""
        assert self.formatter.format_value("123") == "123"
        assert self.formatter.format_value("45.67") == "45.67"
        assert self.formatter.format_value("0") == "0"
        assert self.formatter.format_value("-89.1") == "-89.1"

    def test_format_boolean_strings(self):
        """Test that boolean strings are converted to uppercase."""
        assert self.formatter.format_value("true") == "TRUE"
        assert self.formatter.format_value("false") == "FALSE"
        assert self.formatter.format_value("True") == "TRUE"
        assert self.formatter.format_value("False") == "FALSE"

    def test_format_complex_types(self):
        """Test formatting complex types (lists, dicts)."""
        assert self.formatter.format_value([1, 2, 3]) == "'[1, 2, 3]'"
        assert self.formatter.format_value({"key": "value"}) == '\'{"key": "value"}\''

    @patch("sqlflow.core.variables.formatters.logger")
    def test_format_missing_variable(self, mock_logger):
        """Test handling missing variables in SQL context."""
        result = self.formatter.format_missing_variable("missing_var")

        assert result == "NULL"
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0][0]
        assert "missing_var" in call_args
        assert "SQL context" in call_args


class TestTextFormatter:
    """Test text formatter for plain text contexts."""

    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = TextFormatter()

    def test_format_none_value(self):
        """Test formatting None values as empty string."""
        result = self.formatter.format_value(None)
        assert result == ""

    def test_format_basic_types(self):
        """Test formatting basic types as strings."""
        assert self.formatter.format_value("hello") == "hello"
        assert self.formatter.format_value(42) == "42"
        assert self.formatter.format_value(3.14) == "3.14"
        assert self.formatter.format_value(True) == "True"
        assert self.formatter.format_value(False) == "False"

    def test_format_complex_types(self):
        """Test formatting complex types as strings."""
        assert self.formatter.format_value([1, 2, 3]) == "[1, 2, 3]"
        assert self.formatter.format_value({"key": "value"}) == "{'key': 'value'}"

    def test_format_special_characters(self):
        """Test formatting strings with special characters."""
        assert self.formatter.format_value("line1\nline2") == "line1\nline2"
        assert self.formatter.format_value("tab\there") == "tab\there"
        assert self.formatter.format_value("unicode: café") == "unicode: café"

    @patch("sqlflow.core.variables.formatters.logger")
    def test_format_missing_variable(self, mock_logger):
        """Test handling missing variables in text context."""
        result = self.formatter.format_missing_variable("missing_var")

        assert result == "${missing_var}"
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0][0]
        assert "missing_var" in call_args
        assert "text context" in call_args


class TestASTFormatter:
    """Test AST formatter for Python evaluation contexts."""

    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = ASTFormatter()

    def test_format_none_value(self):
        """Test formatting None values as Python None."""
        result = self.formatter.format_value(None)
        assert result == "None"

    def test_format_boolean_values(self):
        """Test formatting boolean values as Python booleans."""
        assert self.formatter.format_value(True) == "True"
        assert self.formatter.format_value(False) == "False"

    def test_format_numeric_values(self):
        """Test formatting numeric values for Python."""
        assert self.formatter.format_value(42) == "42"
        assert self.formatter.format_value(3.14) == "3.14"
        assert self.formatter.format_value(0) == "0"
        assert self.formatter.format_value(-123) == "-123"

    def test_format_string_values(self):
        """Test formatting string values for Python evaluation."""
        assert self.formatter.format_value("hello") == "'hello'"
        assert self.formatter.format_value("world") == "'world'"
        assert self.formatter.format_value("") == "''"

    def test_format_string_with_quotes(self):
        """Test formatting strings with quotes for Python."""
        assert self.formatter.format_value("don't") == '"don\'t"'
        assert self.formatter.format_value("it's working") == '"it\'s working"'

    def test_format_string_with_backslashes(self):
        """Test formatting strings with backslashes for Python."""
        assert self.formatter.format_value("path\\to\\file") == "'path\\\\to\\\\file'"
        assert self.formatter.format_value("newline\n") == "'newline\\n'"

    def test_format_python_keywords(self):
        """Test that Python keywords are preserved."""
        assert self.formatter.format_value("None") == "None"
        assert self.formatter.format_value("True") == "True"
        assert self.formatter.format_value("False") == "False"

    def test_format_numeric_strings(self):
        """Test that numeric strings are preserved."""
        assert self.formatter.format_value("123") == "123"
        assert self.formatter.format_value("45.67") == "45.67"
        assert self.formatter.format_value("0") == "0"

    def test_format_complex_types(self):
        """Test formatting complex types as JSON for Python."""
        result = self.formatter.format_value([1, 2, 3])
        assert result == "[1, 2, 3]"

        result = self.formatter.format_value({"key": "value"})
        assert '"key": "value"' in result

    @patch("sqlflow.core.variables.formatters.logger")
    def test_format_missing_variable(self, mock_logger):
        """Test handling missing variables in AST context."""
        result = self.formatter.format_missing_variable("missing_var")

        assert result == "None"
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0][0]
        assert "missing_var" in call_args
        assert "AST context" in call_args


class TestJSONFormatter:
    """Test JSON formatter for JSON contexts."""

    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = JSONFormatter()

    def test_format_none_value(self):
        """Test formatting None values as JSON null."""
        result = self.formatter.format_value(None)
        assert result == "null"

    def test_format_boolean_values(self):
        """Test formatting boolean values as JSON booleans."""
        assert self.formatter.format_value(True) == "true"
        assert self.formatter.format_value(False) == "false"

    def test_format_numeric_values(self):
        """Test formatting numeric values as JSON numbers."""
        assert self.formatter.format_value(42) == "42"
        assert self.formatter.format_value(3.14) == "3.14"
        assert self.formatter.format_value(0) == "0"

    def test_format_string_values(self):
        """Test formatting string values as JSON strings."""
        assert self.formatter.format_value("hello") == '"hello"'
        assert self.formatter.format_value("world") == '"world"'
        assert self.formatter.format_value("") == '""'

    def test_format_string_with_quotes(self):
        """Test formatting strings with quotes for JSON."""
        assert self.formatter.format_value('say "hello"') == '"say \\"hello\\""'
        assert self.formatter.format_value("it's working") == '"it\'s working"'

    def test_format_complex_types(self):
        """Test formatting complex types as JSON."""
        result = self.formatter.format_value([1, 2, 3])
        assert result == "[1, 2, 3]"

        result = self.formatter.format_value({"key": "value"})
        assert result == '{"key": "value"}'

    def test_format_unicode_strings(self):
        """Test formatting unicode strings with ensure_ascii=False."""
        result = self.formatter.format_value("café")
        assert result == '"café"'

        result = self.formatter.format_value("测试")
        assert result == '"测试"'

    def test_format_non_serializable(self):
        """Test formatting non-JSON-serializable objects."""

        class CustomObject:
            def __str__(self):
                return "custom_object"

        obj = CustomObject()
        result = self.formatter.format_value(obj)
        assert result == '"custom_object"'

    @patch("sqlflow.core.variables.formatters.logger")
    def test_format_missing_variable(self, mock_logger):
        """Test handling missing variables in JSON context."""
        result = self.formatter.format_missing_variable("missing_var")

        assert result == "null"
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0][0]
        assert "missing_var" in call_args
        assert "JSON context" in call_args


class TestFormatterRegistry:
    """Test the formatter registry functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.registry = FormatterRegistry()

    def test_get_default_formatters(self):
        """Test that default formatters are available."""
        sql_formatter = self.registry.get_formatter("sql")
        assert isinstance(sql_formatter, SQLFormatter)

        text_formatter = self.registry.get_formatter("text")
        assert isinstance(text_formatter, TextFormatter)

        ast_formatter = self.registry.get_formatter("ast")
        assert isinstance(ast_formatter, ASTFormatter)

        json_formatter = self.registry.get_formatter("json")
        assert isinstance(json_formatter, JSONFormatter)

    def test_get_unsupported_formatter(self):
        """Test that unsupported contexts raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            self.registry.get_formatter("unsupported")

        assert "Unsupported formatter context: unsupported" in str(exc_info.value)

    def test_register_custom_formatter(self):
        """Test registering a custom formatter."""

        class CustomFormatter(VariableFormatter):
            def format_value(self, value, context=None):
                return f"CUSTOM:{value}"

            def format_missing_variable(self, var_name, context=None):
                return f"MISSING:{var_name}"

        custom_formatter = CustomFormatter()
        self.registry.register_formatter("custom", custom_formatter)

        retrieved = self.registry.get_formatter("custom")
        assert retrieved is custom_formatter
        assert retrieved.format_value("test") == "CUSTOM:test"

    def test_get_available_contexts(self):
        """Test getting list of available contexts."""
        contexts = self.registry.get_available_contexts()

        assert "sql" in contexts
        assert "text" in contexts
        assert "ast" in contexts
        assert "json" in contexts
        assert len(contexts) == 4

    @patch("sqlflow.core.variables.formatters.logger")
    def test_register_formatter_logging(self, mock_logger):
        """Test that registering formatter logs debug info."""

        class TestFormatter(VariableFormatter):
            def format_value(self, value, context=None):
                return str(value)

            def format_missing_variable(self, var_name, context=None):
                return var_name

        self.registry.register_formatter("test", TestFormatter())

        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0][0]
        assert "test" in call_args


class TestGlobalFunctions:
    """Test global convenience functions."""

    def test_get_formatter_registry_singleton(self):
        """Test that get_formatter_registry returns singleton instance."""
        registry1 = get_formatter_registry()
        registry2 = get_formatter_registry()

        assert registry1 is registry2
        assert isinstance(registry1, FormatterRegistry)

    def test_get_formatter_function(self):
        """Test get_formatter convenience function."""
        sql_formatter = get_formatter("sql")
        assert isinstance(sql_formatter, SQLFormatter)

        text_formatter = get_formatter("text")
        assert isinstance(text_formatter, TextFormatter)

    def test_format_value_function(self):
        """Test format_value convenience function."""
        result = format_value("hello", "sql")
        assert result == "'hello'"

        result = format_value(42, "text")
        assert result == "42"

        result = format_value(True, "json")
        assert result == "true"

    def test_format_missing_variable_function(self):
        """Test format_missing_variable convenience function."""
        result = format_missing_variable("test_var", "sql")
        assert result == "NULL"

        result = format_missing_variable("test_var", "text")
        assert result == "${test_var}"

        result = format_missing_variable("test_var", "json")
        assert result == "null"


class TestFormatterIntegration:
    """Test formatters working together in realistic scenarios."""

    def test_same_value_different_contexts(self):
        """Test same value formatted for different contexts."""
        test_value = "user's data"

        sql_result = format_value(test_value, "sql")
        text_result = format_value(test_value, "text")
        ast_result = format_value(test_value, "ast")
        json_result = format_value(test_value, "json")

        assert sql_result == "'user''s data'"  # SQL escaping
        assert text_result == "user's data"  # Plain text
        assert ast_result == '"user\'s data"'  # Python repr() uses double quotes
        assert json_result == '"user\'s data"'  # JSON format

    def test_none_value_different_contexts(self):
        """Test None value formatted for different contexts."""
        sql_result = format_value(None, "sql")
        text_result = format_value(None, "text")
        ast_result = format_value(None, "ast")
        json_result = format_value(None, "json")

        assert sql_result == "NULL"
        assert text_result == ""
        assert ast_result == "None"
        assert json_result == "null"

    def test_boolean_value_different_contexts(self):
        """Test boolean value formatted for different contexts."""
        sql_result = format_value(True, "sql")
        text_result = format_value(True, "text")
        ast_result = format_value(True, "ast")
        json_result = format_value(True, "json")

        assert sql_result == "TRUE"
        assert text_result == "True"
        assert ast_result == "True"
        assert json_result == "true"

    def test_complex_data_formatting(self):
        """Test complex data structures in different contexts."""
        data = {"name": "John", "age": 30, "active": True}

        # SQL context converts to string and quotes
        sql_result = format_value(data, "sql")
        assert sql_result.startswith("'")
        assert "John" in sql_result

        # Text context preserves Python dict format
        text_result = format_value(data, "text")
        assert "John" in text_result
        assert "30" in text_result

        # AST context uses JSON representation
        ast_result = format_value(data, "ast")
        assert '"name": "John"' in ast_result

        # JSON context creates proper JSON
        json_result = format_value(data, "json")
        parsed = json.loads(json_result)
        assert parsed["name"] == "John"
        assert parsed["age"] == 30
        assert parsed["active"] is True


class TestEdgeCases:
    """Test edge cases and error conditions for all formatters."""

    def test_empty_strings(self):
        """Test empty string handling across formatters."""
        assert format_value("", "sql") == "''"
        assert format_value("", "text") == ""
        assert format_value("", "ast") == "''"
        assert format_value("", "json") == '""'

    def test_whitespace_strings(self):
        """Test whitespace-only strings."""
        test_value = "   "

        assert format_value(test_value, "sql") == "'   '"
        assert format_value(test_value, "text") == "   "
        assert format_value(test_value, "ast") == "'   '"
        assert format_value(test_value, "json") == '"   "'

    def test_large_numbers(self):
        """Test large number formatting."""
        large_num = 123456789012345

        assert format_value(large_num, "sql") == "123456789012345"
        assert format_value(large_num, "text") == "123456789012345"
        assert format_value(large_num, "ast") == "123456789012345"
        assert format_value(large_num, "json") == "123456789012345"

    def test_special_float_values(self):
        """Test special float values (inf, nan)."""
        import math

        # Note: Different contexts may handle these differently
        inf_sql = format_value(math.inf, "sql")
        nan_sql = format_value(math.nan, "sql")

        # Should not crash and return some string representation
        assert isinstance(inf_sql, str)
        assert isinstance(nan_sql, str)
