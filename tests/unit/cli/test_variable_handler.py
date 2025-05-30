"""Test suite for CLI variable handler functionality."""

import logging
from unittest.mock import patch

import pytest

from sqlflow.cli.variable_handler import VariableHandler


class TestVariableHandler:
    """Test cases for VariableHandler class."""

    def test_initialization_empty(self):
        """Test initialization with no variables."""
        handler = VariableHandler()

        assert handler.variables == {}
        assert handler.var_pattern is not None

    def test_initialization_with_variables(self):
        """Test initialization with variables."""
        variables = {"date": "2025-05-16", "name": "test"}
        handler = VariableHandler(variables)

        assert handler.variables == variables

    def test_substitute_variables_simple(self):
        """Test simple variable substitution."""
        handler = VariableHandler({"date": "2025-05-16", "name": "test"})

        text = "SELECT * FROM table_${date} WHERE name = '${name}'"
        result = handler.substitute_variables(text)

        expected = "SELECT * FROM table_2025-05-16 WHERE name = 'test'"
        assert result == expected

    def test_substitute_variables_with_defaults_used(self):
        """Test variable substitution where defaults are used."""
        handler = VariableHandler({"date": "2025-05-16"})  # Missing 'name'

        text = "SELECT * FROM table_${date} WHERE name = '${name|default_name}'"
        result = handler.substitute_variables(text)

        expected = "SELECT * FROM table_2025-05-16 WHERE name = 'default_name'"
        assert result == expected

    def test_substitute_variables_with_defaults_not_used(self):
        """Test variable substitution where defaults are available but not used."""
        handler = VariableHandler({"date": "2025-05-16", "name": "actual_name"})

        text = "SELECT * FROM table_${date} WHERE name = '${name|default_name}'"
        result = handler.substitute_variables(text)

        expected = "SELECT * FROM table_2025-05-16 WHERE name = 'actual_name'"
        assert result == expected

    def test_substitute_variables_missing_no_default(self):
        """Test variable substitution with missing variable and no default."""
        handler = VariableHandler({"date": "2025-05-16"})

        text = "SELECT * FROM table_${date} WHERE name = '${name}'"

        with patch("sqlflow.cli.variable_handler.logger") as mock_logger:
            result = handler.substitute_variables(text)

            # Should keep original text for missing variable
            expected = "SELECT * FROM table_2025-05-16 WHERE name = '${name}'"
            assert result == expected

            # Should log warning
            mock_logger.warning.assert_called_once_with(
                "Variable 'name' not found and no default provided"
            )

    def test_substitute_variables_numeric_values(self):
        """Test variable substitution with numeric values."""
        handler = VariableHandler({"count": 100, "rate": 0.05})

        text = "SELECT * FROM table LIMIT ${count} WHERE rate > ${rate}"
        result = handler.substitute_variables(text)

        expected = "SELECT * FROM table LIMIT 100 WHERE rate > 0.05"
        assert result == expected

    def test_substitute_variables_boolean_values(self):
        """Test variable substitution with boolean values."""
        handler = VariableHandler({"enabled": True, "debug": False})

        text = "enabled=${enabled}, debug=${debug}"
        result = handler.substitute_variables(text)

        expected = "enabled=True, debug=False"
        assert result == expected

    def test_substitute_variables_no_variables_in_text(self):
        """Test substitution with text containing no variables."""
        handler = VariableHandler({"date": "2025-05-16"})

        text = "SELECT * FROM table WHERE id = 1"
        result = handler.substitute_variables(text)

        assert result == text

    def test_parse_variable_expr_simple(self):
        """Test parsing simple variable expression."""
        handler = VariableHandler()

        var_name, default = handler._parse_variable_expr("${date}")

        assert var_name == "date"
        assert default is None

    def test_parse_variable_expr_with_default(self):
        """Test parsing variable expression with default."""
        handler = VariableHandler()

        var_name, default = handler._parse_variable_expr("${name|default_value}")

        assert var_name == "name"
        assert default == "default_value"

    def test_parse_variable_expr_complex_default(self):
        """Test parsing variable expression with complex default."""
        handler = VariableHandler()

        var_name, default = handler._parse_variable_expr(
            "${path|/default/path/file.csv}"
        )

        assert var_name == "path"
        assert default == "/default/path/file.csv"

    def test_parse_variable_expr_invalid(self):
        """Test parsing invalid variable expression."""
        handler = VariableHandler()

        var_name, default = handler._parse_variable_expr("invalid")

        assert var_name == "invalid"
        assert default is None

    def test_validate_variable_usage_all_available(self):
        """Test validation when all variables are available."""
        handler = VariableHandler({"date": "2025-05-16", "name": "test"})

        text = "SELECT * FROM table_${date} WHERE name = '${name}'"

        assert handler.validate_variable_usage(text) is True

    def test_validate_variable_usage_with_defaults(self):
        """Test validation when variables have defaults."""
        handler = VariableHandler({"date": "2025-05-16"})  # Missing 'name'

        text = "SELECT * FROM table_${date} WHERE name = '${name|default_name}'"

        assert handler.validate_variable_usage(text) is True

    def test_validate_variable_usage_missing_required(self):
        """Test validation when required variables are missing."""
        handler = VariableHandler({"date": "2025-05-16"})  # Missing 'name'

        text = "SELECT * FROM table_${date} WHERE name = '${name}'"

        with patch("sqlflow.cli.variable_handler.logger") as mock_logger:
            result = handler.validate_variable_usage(text)

            assert result is False
            mock_logger.error.assert_called_once_with(
                "Missing required variables: name"
            )

    def test_validate_variable_usage_multiple_missing(self):
        """Test validation when multiple required variables are missing."""
        handler = VariableHandler({})  # No variables

        text = "SELECT * FROM table_${date} WHERE name = '${name}' AND id = ${id}"

        with patch("sqlflow.cli.variable_handler.logger") as mock_logger:
            result = handler.validate_variable_usage(text)

            assert result is False
            # Should report all missing variables
            call_args = mock_logger.error.call_args[0][0]
            assert "Missing required variables:" in call_args
            assert "date" in call_args
            assert "name" in call_args
            assert "id" in call_args

    def test_validate_variable_usage_no_variables_in_text(self):
        """Test validation with text containing no variables."""
        handler = VariableHandler({})

        text = "SELECT * FROM table WHERE id = 1"

        assert handler.validate_variable_usage(text) is True

    def test_variable_pattern_edge_cases(self):
        """Test edge cases for variable pattern matching."""
        handler = VariableHandler({"var1": "value1", "var_name": "value2"})

        # Test various valid patterns
        test_cases = [
            ("${var1}", "value1"),
            ("${var_name}", "value2"),
            ("${var1|default}", "value1"),
            ("${missing|default_val}", "default_val"),
        ]

        for input_text, expected in test_cases:
            result = handler.substitute_variables(input_text)
            assert result == expected

    def test_complex_substitution_scenario(self):
        """Test complex variable substitution scenario."""
        handler = VariableHandler(
            {"env": "production", "date": "2025-05-16", "table_prefix": "prod"}
        )

        text = """
        SET environment = '${env}';
        SOURCE data_${env} TYPE CSV PARAMS {
            "path": "data/${env}/sample_${date}.csv",
            "table_name": "${table_prefix}_customers"
        };
        
        SELECT * FROM ${table_prefix}_customers 
        WHERE created_date = '${date}'
        AND environment = '${env|development}';
        """

        result = handler.substitute_variables(text)

        # Verify all substitutions occurred
        assert "${env}" not in result
        assert "${date}" not in result
        assert "${table_prefix}" not in result
        assert "production" in result
        assert "2025-05-16" in result
        assert "prod_customers" in result

    def test_default_with_special_characters(self):
        """Test defaults containing special characters."""
        handler = VariableHandler({})

        text = "${path|/data/files with spaces/file.csv}"
        result = handler.substitute_variables(text)

        assert result == "/data/files with spaces/file.csv"

    def test_nested_variable_like_patterns(self):
        """Test handling of nested or complex patterns."""
        handler = VariableHandler({"outer": "value"})

        # Should not try to substitute inner ${inner}
        text = "SELECT '${outer}' as col, '${missing}' as other"

        with patch("sqlflow.cli.variable_handler.logger"):
            result = handler.substitute_variables(text)

        assert "value" in result
        assert "${missing}" in result  # Should remain unchanged

    def test_empty_variable_name(self):
        """Test handling of empty variable names."""
        handler = VariableHandler({})

        text = "${}"
        result = handler.substitute_variables(text)

        # Should remain unchanged for invalid pattern
        assert result == text

    def test_logging_for_defaults(self):
        """Test that using defaults is logged at debug level."""
        handler = VariableHandler({})  # No variables

        text = "value is ${missing|default_value}"

        with patch("sqlflow.cli.variable_handler.logger") as mock_logger:
            result = handler.substitute_variables(text)

            assert result == "value is default_value"
            mock_logger.debug.assert_called_once_with(
                "Using default value 'default_value' for variable 'missing'"
            )
