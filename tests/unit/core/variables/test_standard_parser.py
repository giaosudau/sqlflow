"""Unit tests for StandardVariableParser.

This module provides comprehensive test coverage for the unified variable parser,
ensuring consistent behavior across all edge cases.
"""

from sqlflow.core.variables.parser import StandardVariableParser


class TestStandardVariableParser:
    """Test the StandardVariableParser class."""

    def test_no_variables(self):
        """Test parsing text with no variables."""
        result = StandardVariableParser.find_variables("No variables here")
        assert not result.has_variables
        assert len(result.expressions) == 0

    def test_empty_string(self):
        """Test parsing empty string."""
        result = StandardVariableParser.find_variables("")
        assert not result.has_variables
        assert len(result.expressions) == 0

    def test_none_input(self):
        """Test parsing None input."""
        result = StandardVariableParser.find_variables(None)
        assert not result.has_variables
        assert len(result.expressions) == 0

    def test_simple_variable(self):
        """Test parsing simple variable without default."""
        result = StandardVariableParser.find_variables("${simple}")
        assert result.has_variables
        assert len(result.expressions) == 1

        expr = result.expressions[0]
        assert expr.variable_name == "simple"
        assert expr.default_value is None
        assert expr.original_match == "${simple}"
        assert expr.span == (0, 9)

    def test_variable_with_default(self):
        """Test parsing variable with default value."""
        result = StandardVariableParser.find_variables("${var|default}")
        assert result.has_variables
        assert len(result.expressions) == 1

        expr = result.expressions[0]
        assert expr.variable_name == "var"
        assert expr.default_value == "default"
        assert expr.original_match == "${var|default}"

    def test_variable_with_quoted_default(self):
        """Test parsing variable with quoted default value."""
        test_cases = [
            ("${var|'quoted'}", "quoted"),
            ('${var|"double"}', "double"),
            ("${var|'single with spaces'}", "single with spaces"),
            ('${var|"double with spaces"}', "double with spaces"),
        ]

        for template, expected_default in test_cases:
            result = StandardVariableParser.find_variables(template)
            assert result.has_variables
            assert len(result.expressions) == 1

            expr = result.expressions[0]
            assert expr.variable_name == "var"
            assert expr.default_value == expected_default

    def test_multiple_variables(self):
        """Test parsing multiple variables."""
        result = StandardVariableParser.find_variables("${first} and ${second|default}")
        assert result.has_variables
        assert len(result.expressions) == 2

        # First variable
        expr1 = result.expressions[0]
        assert expr1.variable_name == "first"
        assert expr1.default_value is None
        assert expr1.original_match == "${first}"

        # Second variable
        expr2 = result.expressions[1]
        assert expr2.variable_name == "second"
        assert expr2.default_value == "default"
        assert expr2.original_match == "${second|default}"

    def test_variable_with_whitespace(self):
        """Test parsing variables with whitespace."""
        test_cases = [
            ("${ var }", "var", None),
            ("${ var | default }", "var", "default"),
            ("${var |default}", "var", "default"),
            ("${var| default}", "var", "default"),
        ]

        for template, expected_name, expected_default in test_cases:
            result = StandardVariableParser.find_variables(template)
            assert result.has_variables
            assert len(result.expressions) == 1

            expr = result.expressions[0]
            assert expr.variable_name == expected_name
            assert expr.default_value == expected_default

    def test_repeated_variables(self):
        """Test parsing repeated variables."""
        result = StandardVariableParser.find_variables("${var}, ${var}, ${var}")
        assert result.has_variables
        assert len(result.expressions) == 3

        for expr in result.expressions:
            assert expr.variable_name == "var"
            assert expr.default_value is None

    def test_nested_variables_not_supported(self):
        """Test that nested variables are treated as single unresolvable variable."""
        # This is explicitly out of scope per the technical design
        result = StandardVariableParser.find_variables("${var_${inner}}")
        assert result.has_variables
        assert len(result.expressions) == 1

        expr = result.expressions[0]
        # The parser will treat this as a single variable name
        assert expr.variable_name == "var_${inner"
        assert expr.default_value is None

    def test_special_characters_in_variable_name(self):
        """Test parsing variables with special characters in names."""
        test_cases = [
            ("${var_name}", "var_name"),
            ("${var-name}", "var-name"),
            ("${var123}", "var123"),
            ("${VAR_NAME}", "VAR_NAME"),
            ("${var.name}", "var.name"),
        ]

        for template, expected_name in test_cases:
            result = StandardVariableParser.find_variables(template)
            assert result.has_variables
            assert len(result.expressions) == 1

            expr = result.expressions[0]
            assert expr.variable_name == expected_name

    def test_special_characters_in_default(self):
        """Test parsing variables with special characters in defaults."""
        test_cases = [
            ("${var|default-value}", "default-value"),
            ("${var|default_value}", "default_value"),
            ("${var|default.value}", "default.value"),
            ("${var|123}", "123"),
            ("${var|'complex-default_value.123'}", "complex-default_value.123"),
        ]

        for template, expected_default in test_cases:
            result = StandardVariableParser.find_variables(template)
            assert result.has_variables
            assert len(result.expressions) == 1

            expr = result.expressions[0]
            assert expr.default_value == expected_default

    def test_malformed_variables(self):
        """Test parsing malformed variable expressions."""
        test_cases = [
            ("${", []),  # Incomplete opening
            ("$}", []),  # No opening brace
            ("${}", []),  # Empty variable name
            ("${ }", []),  # Only whitespace
            ("${|default}", []),  # No variable name, only default
        ]

        for template, expected_count in test_cases:
            result = StandardVariableParser.find_variables(template)
            assert len(result.expressions) == len(expected_count)

    def test_edge_case_with_pipe_in_text(self):
        """Test text with pipe characters outside variables."""
        result = StandardVariableParser.find_variables(
            "normal | text ${var|default} more | text"
        )
        assert result.has_variables
        assert len(result.expressions) == 1

        expr = result.expressions[0]
        assert expr.variable_name == "var"
        assert expr.default_value == "default"

    def test_complex_real_world_example(self):
        """Test complex real-world SQL template."""
        sql = """
        SELECT * FROM ${table_name|users} 
        WHERE ${column|id} = ${value|'default'} 
        AND region = '${region|us-east-1}'
        """

        result = StandardVariableParser.find_variables(sql)
        assert result.has_variables
        assert len(result.expressions) == 4

        # Check each variable
        expected = [
            ("table_name", "users"),
            ("column", "id"),
            ("value", "default"),
            ("region", "us-east-1"),
        ]

        for i, (expected_name, expected_default) in enumerate(expected):
            expr = result.expressions[i]
            assert expr.variable_name == expected_name
            assert expr.default_value == expected_default

    def test_performance_with_large_text(self):
        """Test parser performance with large text."""
        # Create a large string with many variables
        large_text = " ".join([f"${{{i}|default{i}}}" for i in range(1000)])

        result = StandardVariableParser.find_variables(large_text)
        assert result.has_variables
        assert len(result.expressions) == 1000

        # Verify first and last variables
        assert result.expressions[0].variable_name == "0"
        assert result.expressions[0].default_value == "default0"
        assert result.expressions[-1].variable_name == "999"
        assert result.expressions[-1].default_value == "default999"

    def test_span_positions(self):
        """Test that span positions are correctly calculated."""
        text = "prefix ${var1} middle ${var2|default} suffix"
        result = StandardVariableParser.find_variables(text)

        assert len(result.expressions) == 2

        # Check first variable span
        expr1 = result.expressions[0]
        assert text[expr1.span[0] : expr1.span[1]] == "${var1}"

        # Check second variable span
        expr2 = result.expressions[1]
        assert text[expr2.span[0] : expr2.span[1]] == "${var2|default}"

    def test_is_quoted_helper(self):
        """Test the _is_quoted helper method."""
        assert StandardVariableParser._is_quoted("'quoted'")
        assert StandardVariableParser._is_quoted('"quoted"')
        assert StandardVariableParser._is_quoted("'single'")
        assert StandardVariableParser._is_quoted('"double"')

        assert not StandardVariableParser._is_quoted("not_quoted")
        assert not StandardVariableParser._is_quoted("'incomplete")
        assert not StandardVariableParser._is_quoted("incomplete'")
        assert not StandardVariableParser._is_quoted('"incomplete')
        assert not StandardVariableParser._is_quoted('incomplete"')

        # Empty quotes are still technically quoted strings
        assert StandardVariableParser._is_quoted("''")  # Empty single quotes
        assert StandardVariableParser._is_quoted('""')  # Empty double quotes
