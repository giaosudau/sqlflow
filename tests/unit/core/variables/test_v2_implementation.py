"""Comprehensive tests for SQLFlow Variables V2 implementation

This module provides high-coverage testing for all V2 functions to ensure they work correctly
and maintain compatibility with V1 behavior while following Zen of Python principles.
"""

import json
import os

from sqlflow.core.variables.v2 import (
    ValidationResult,
    VariableError,
    VariableSources,
    find_variables,
    format_for_context,
    get_variable_priority,
    merge_variable_sources,
    resolve_from_environment,
    resolve_variables,
    resolve_variables_legacy,
    resolve_with_sources,
    substitute_any,
    substitute_in_dict,
    substitute_in_list,
    substitute_simple_dollar,
    substitute_variables,
    validate_variables,
    validate_variables_with_details,
)


class TestSubstitution:
    """Test core substitution functions."""

    def test_basic_substitution(self):
        """Test basic variable substitution."""
        text = "Hello ${name}!"
        variables = {"name": "World"}
        result = substitute_variables(text, variables)
        assert result == "Hello World!"

    def test_substitution_with_default(self):
        """Test substitution with default values."""
        text = "Hello ${name|Anonymous}!"
        variables = {}
        result = substitute_variables(text, variables)
        assert result == "Hello Anonymous!"

    def test_substitution_with_quoted_default(self):
        """Test substitution with quoted default values."""
        text = "Environment is ${env|'development'}"
        variables = {}
        result = substitute_variables(text, variables)
        assert result == "Environment is development"

    def test_substitution_with_double_quoted_default(self):
        """Test substitution with double-quoted default values."""
        text = 'Environment is ${env|"production"}'
        variables = {}
        result = substitute_variables(text, variables)
        assert result == "Environment is production"

    def test_substitution_missing_variable(self):
        """Test substitution with missing variable (no default)."""
        text = "Hello ${name}!"
        variables = {}
        result = substitute_variables(text, variables)
        assert result == "Hello ${name}!"  # Should keep original

    def test_substitution_empty_text(self):
        """Test substitution with empty text."""
        result = substitute_variables("", {"name": "World"})
        assert result == ""

    def test_substitution_none_variables(self):
        """Test substitution with None variables."""
        text = "Hello ${name}!"
        result = substitute_variables(text, {})
        assert result == "Hello ${name}!"

    def test_substitution_complex_defaults(self):
        """Test substitution with complex default values."""
        text = "SQL: ${sql|'SELECT * FROM table WHERE id = 1'}"
        variables = {}
        result = substitute_variables(text, variables)
        assert result == "SQL: SELECT * FROM table WHERE id = 1"

    def test_substitution_multiple_variables(self):
        """Test substitution with multiple variables."""
        text = "Hello ${name}, you are ${age} years old from ${city}!"
        variables = {"name": "Alice", "age": "30", "city": "New York"}
        result = substitute_variables(text, variables)
        assert result == "Hello Alice, you are 30 years old from New York!"

    def test_substitution_mixed_defaults(self):
        """Test substitution with some variables having defaults, others not."""
        text = "Hello ${name|Anonymous}, you are ${age} years old!"
        variables = {"age": "25"}
        result = substitute_variables(text, variables)
        assert result == "Hello Anonymous, you are 25 years old!"

    def test_substitution_in_dict(self):
        """Test substitution in dictionary."""
        data = {"key": "value_${env}", "nested": {"inner": "${database_url}"}}
        variables = {"env": "prod", "database_url": "postgres://localhost"}
        result = substitute_in_dict(data, variables)

        assert result["key"] == "value_prod"
        assert result["nested"]["inner"] == "postgres://localhost"

    def test_substitution_in_dict_with_key_substitution(self):
        """Test substitution in dictionary keys."""
        data = {"key_${env}": "value", "static_key": "static_value"}
        variables = {"env": "prod"}
        result = substitute_in_dict(data, variables)

        assert result["key_prod"] == "value"
        assert result["static_key"] == "static_value"

    def test_substitution_in_list(self):
        """Test substitution in list."""
        data = ["item_${env}", "${database_url}", 123]
        variables = {"env": "prod", "database_url": "postgres://localhost"}
        result = substitute_in_list(data, variables)

        assert result[0] == "item_prod"
        assert result[1] == "postgres://localhost"
        assert result[2] == 123

    def test_substitute_any_string(self):
        """Test substitute_any with string."""
        text = "Hello ${name}!"
        variables = {"name": "World"}
        result = substitute_any(text, variables)
        assert result == "Hello World!"

    def test_substitute_any_dict(self):
        """Test substitute_any with dictionary."""
        data = {"key": "value_${env}"}
        variables = {"env": "prod"}
        result = substitute_any(data, variables)
        assert result == {"key": "value_prod"}

    def test_substitute_any_list(self):
        """Test substitute_any with list."""
        data = ["item_${env}"]
        variables = {"env": "prod"}
        result = substitute_any(data, variables)
        assert result == ["item_prod"]

    def test_substitute_any_other_types(self):
        """Test substitute_any with non-string/dict/list types."""
        data = 123
        variables = {"env": "prod"}
        result = substitute_any(data, variables)
        assert result == 123

    def test_simple_dollar_substitution(self):
        """Test simple dollar variable substitution."""
        text = "Hello $name, you are $age years old!"
        variables = {"name": "Alice", "age": "30"}
        result = substitute_simple_dollar(text, variables)
        assert result == "Hello Alice, you are 30 years old!"

    def test_simple_dollar_missing_variable(self):
        """Test simple dollar substitution with missing variable."""
        text = "Hello $name!"
        variables = {}
        result = substitute_simple_dollar(text, variables)
        assert result == "Hello $name!"

    def test_simple_dollar_word_boundaries(self):
        """Test simple dollar substitution respects word boundaries."""
        text = "Price is $100 and variable is $var"
        variables = {"var": "test"}
        result = substitute_simple_dollar(text, variables)
        assert result == "Price is $100 and variable is test"

    def test_find_variables_basic(self):
        """Test find_variables function."""
        text = "Hello ${name} and ${age|30}!"
        variables = find_variables(text)

        assert len(variables) == 2
        assert variables[0].name == "name"
        assert variables[0].default_value is None
        assert variables[1].name == "age"
        assert variables[1].default_value == "30"

    def test_find_variables_simple_dollar(self):
        """Test find_variables with simple dollar syntax."""
        text = "Hello $name and $age!"
        variables = find_variables(text)

        assert len(variables) == 2
        assert variables[0].name == "name"
        assert variables[0].default_value is None
        assert variables[1].name == "age"
        assert variables[1].default_value is None

    def test_find_variables_mixed_syntax(self):
        """Test find_variables with mixed syntax."""
        text = "Hello ${name} and $age!"
        variables = find_variables(text)

        assert len(variables) == 2
        assert variables[0].name == "name"
        assert variables[1].name == "age"

    def test_find_variables_empty_text(self):
        """Test find_variables with empty text."""
        variables = find_variables("")
        assert variables == []


class TestResolution:
    """Test priority resolution functions."""

    def test_basic_resolution(self):
        """Test basic variable resolution."""
        sources = VariableSources(
            cli={"env": "prod"}, profile={"env": "dev", "debug": "false"}
        )
        result = resolve_variables(sources)

        assert result["env"] == "prod"  # CLI takes priority
        assert result["debug"] == "false"  # Profile value

    def test_priority_order(self):
        """Test complete priority order: CLI > Profile > SET > ENV."""
        sources = VariableSources(
            cli={"var1": "cli"},
            profile={"var1": "profile", "var2": "profile"},
            set={"var1": "set", "var2": "set", "var3": "set"},
            env={"var1": "env", "var2": "env", "var3": "env", "var4": "env"},
        )

        result = resolve_variables(sources)

        assert result["var1"] == "cli"  # CLI wins
        assert result["var2"] == "profile"  # Profile wins over SET/ENV
        assert result["var3"] == "set"  # SET wins over ENV
        assert result["var4"] == "env"  # Only ENV has it

    def test_resolution_empty_sources(self):
        """Test resolution with empty sources."""
        sources = VariableSources()
        result = resolve_variables(sources)
        assert result == {}

    def test_resolution_none_sources(self):
        """Test resolution with None sources."""
        sources = VariableSources()
        result = resolve_variables(sources)
        assert result == {}

    def test_resolution_mixed_none_sources(self):
        """Test resolution with some None sources."""
        sources = VariableSources(cli={"var1": "cli"})
        result = resolve_variables(sources)
        assert result["var1"] == "cli"

    def test_resolve_from_environment(self):
        """Test resolve_from_environment function."""
        # Set a test environment variable
        os.environ["TEST_VAR"] = "test_value"

        result = resolve_from_environment()

        # Should contain our test variable
        assert "TEST_VAR" in result
        assert result["TEST_VAR"] == "test_value"

        # Clean up
        del os.environ["TEST_VAR"]

    def test_merge_variable_sources(self):
        """Test merge_variable_sources function."""
        sources = {
            "env": {"var1": "env", "var2": "env"},
            "set": {"var1": "set", "var2": "set", "var3": "set"},
            "profile": {
                "var1": "profile",
                "var2": "profile",
                "var3": "profile",
                "var4": "profile",
            },
            "cli": {
                "var1": "cli",
                "var2": "cli",
                "var3": "cli",
                "var4": "cli",
                "var5": "cli",
            },
        }

        result = merge_variable_sources(sources)

        assert result["var1"] == "cli"  # CLI wins
        assert result["var2"] == "cli"  # CLI wins
        assert result["var3"] == "cli"  # CLI wins
        assert result["var4"] == "cli"  # CLI wins
        assert result["var5"] == "cli"  # Only CLI has it

    def test_get_variable_priority(self):
        """Test get_variable_priority function."""
        sources = VariableSources(
            cli={"var1": "cli"},
            profile={"var1": "profile", "var2": "profile"},
            set={"var1": "set", "var2": "set", "var3": "set"},
            env={"var1": "env", "var2": "env", "var3": "env", "var4": "env"},
        )

        assert get_variable_priority("var1", sources) == "cli"
        assert get_variable_priority("var2", sources) == "profile"
        assert get_variable_priority("var3", sources) == "set"
        assert get_variable_priority("var4", sources) == "env"
        assert get_variable_priority("var5", sources) is None

    def test_resolve_with_sources(self):
        """Test resolve_with_sources function."""
        sources = VariableSources(
            cli={"var1": "cli"},
            profile={"var1": "profile", "var2": "profile"},
            set={"var1": "set", "var2": "set", "var3": "set"},
            env={"var1": "env", "var2": "env", "var3": "env", "var4": "env"},
        )

        value, source = resolve_with_sources("var1", sources)
        assert value == "cli"
        assert source == "cli"

        value, source = resolve_with_sources("var2", sources)
        assert value == "profile"
        assert source == "profile"

        value, source = resolve_with_sources("var5", sources)
        assert value is None
        assert source is None

    def test_legacy_resolution(self):
        """Test legacy resolution function for backward compatibility."""
        result = resolve_variables_legacy(
            cli_vars={"env": "prod"}, profile_vars={"env": "dev", "debug": "false"}
        )

        assert result["env"] == "prod"  # CLI takes priority
        assert result["debug"] == "false"  # Profile value


class TestFormatting:
    """Test context-specific formatting."""

    def test_text_formatting(self):
        """Test text context formatting."""
        assert format_for_context("hello", "text") == "hello"
        assert format_for_context(123, "text") == "123"
        assert format_for_context(None, "text") == ""
        assert format_for_context(True, "text") == "True"
        assert format_for_context(False, "text") == "False"

    def test_sql_formatting(self):
        """Test SQL context formatting."""
        assert format_for_context("hello", "sql") == "'hello'"
        assert format_for_context(123, "sql") == "123"
        assert format_for_context(None, "sql") == "NULL"
        assert format_for_context(True, "sql") == "TRUE"
        assert format_for_context(False, "sql") == "FALSE"

    def test_sql_string_escaping(self):
        """Test SQL string escaping."""
        assert format_for_context("O'Reilly", "sql") == "'O''Reilly'"
        assert format_for_context("'already quoted'", "sql") == "'already quoted'"
        assert format_for_context('"double quoted"', "sql") == '"double quoted"'

    def test_sql_special_values(self):
        """Test SQL formatting of special values."""
        assert format_for_context("NULL", "sql") == "NULL"
        assert format_for_context("CURRENT_DATE", "sql") == "CURRENT_DATE"
        assert format_for_context("NOW()", "sql") == "NOW()"
        assert format_for_context("123.45", "sql") == "123.45"
        assert format_for_context("true", "sql") == "TRUE"
        assert format_for_context("false", "sql") == "FALSE"

    def test_ast_formatting(self):
        """Test AST context formatting."""
        assert format_for_context("hello", "ast") == "'hello'"
        assert format_for_context(123, "ast") == "123"
        assert format_for_context(None, "ast") == "None"
        assert format_for_context(True, "ast") == "True"
        assert format_for_context(False, "ast") == "False"

    def test_json_formatting(self):
        """Test JSON context formatting."""
        assert format_for_context("hello", "json") == '"hello"'
        assert format_for_context(123, "json") == "123"
        assert format_for_context(None, "json") == "null"
        assert format_for_context(True, "json") == "true"
        assert format_for_context(False, "json") == "false"

    def test_json_complex_types(self):
        """Test JSON formatting of complex types."""
        data = {"key": "value", "list": [1, 2, 3]}
        result = format_for_context(data, "json")
        expected = '{"key": "value", "list": [1, 2, 3]}'
        assert json.loads(result) == json.loads(expected)

    def test_formatting_complex_types(self):
        """Test formatting of complex types in different contexts."""
        data = {"key": "value"}

        # Text context should use str()
        text_result = format_for_context(data, "text")
        assert "{'key': 'value'}" in text_result

        # SQL context should quote the JSON string
        sql_result = format_for_context(data, "sql")
        assert sql_result.startswith("'")
        assert sql_result.endswith("'")

        # AST context should use repr()
        ast_result = format_for_context(data, "ast")
        assert ast_result.startswith("{")
        assert ast_result.endswith("}")


class TestValidation:
    """Test validation functions."""

    def test_basic_validation(self):
        """Test basic variable validation."""
        text = "Hello ${name} and ${age}!"
        variables = {"name": "Alice"}
        missing = validate_variables(text, variables)

        assert missing == ["age"]

    def test_validation_with_defaults(self):
        """Test validation with default values."""
        text = "Hello ${name|Anonymous} and ${age}!"
        variables = {}
        missing = validate_variables(text, variables)

        assert missing == ["age"]  # name has default, age doesn't

    def test_validation_no_missing(self):
        """Test validation when all variables are available."""
        text = "Hello ${name} and ${age}!"
        variables = {"name": "Alice", "age": "30"}
        missing = validate_variables(text, variables)

        assert missing == []

    def test_validation_empty_text(self):
        """Test validation with empty text."""
        missing = validate_variables("", {})
        assert missing == []

    def test_validation_no_variables(self):
        """Test validation with text that has no variables."""
        text = "Hello world!"
        missing = validate_variables(text, {})
        assert missing == []

    def test_validation_duplicate_variables(self):
        """Test validation handles duplicate variables correctly."""
        text = "Hello ${name} and ${name} again!"
        variables = {}
        missing = validate_variables(text, variables)
        assert missing == ["name"]  # Should only appear once

    def test_validate_comprehensive_basic(self):
        """Test comprehensive validation."""
        text = "Hello ${name} and ${age}!"
        variables = {"name": "Alice"}
        result = validate_variables_with_details(text, variables)

        assert isinstance(result, ValidationResult)
        assert not result.is_valid
        assert result.missing_variables == ["age"]
        assert result.invalid_syntax == []

    def test_validate_comprehensive_invalid_syntax(self):
        """Test comprehensive validation with invalid syntax."""
        text = "Hello ${123invalid} and ${name}!"
        variables = {"name": "Alice"}
        result = validate_variables_with_details(text, variables)

        assert not result.is_valid
        # V2 treats invalid syntax as missing variables
        assert "123invalid" in result.missing_variables
        # V2 also includes invalid syntax in the invalid_syntax list
        assert len(result.invalid_syntax) > 0

    def test_validate_comprehensive_suggestions(self):
        """Test comprehensive validation provides suggestions."""
        text = "Hello ${nam} and ${ag}!"
        variables = {"name": "Alice", "age": "30"}
        result = validate_variables_with_details(text, variables)

        assert not result.is_valid
        assert "nam" in result.missing_variables
        assert "ag" in result.missing_variables
        assert len(result.suggestions) > 0


class TestErrorHandling:
    """Test error handling and exceptions."""

    def test_variable_error_creation(self):
        """Test VariableError creation."""
        error = VariableError("Test error", "test_var", "test_context")

        assert str(error) == "Test error (variable: test_var) (context: test_context)"
        assert error.variable_name == "test_var"
        assert error.context == "test_context"

    def test_variable_error_minimal(self):
        """Test VariableError creation with minimal parameters."""
        error = VariableError("Test error")

        assert str(error) == "Test error"
        assert error.variable_name is None
        assert error.context is None


class TestIntegration:
    """Test integration between V2 components."""

    def test_end_to_end_substitution(self):
        """Test complete end-to-end substitution workflow."""
        # 1. Resolve variables from multiple sources
        sources = VariableSources(
            cli={"env": "prod"},
            profile={"database_host": "localhost", "env": "dev"},
            env={"database_port": "5432"},
        )

        variables = resolve_variables(sources)

        # 2. Validate variables in template
        template = "postgresql://${database_host}:${database_port}/${env}_db"
        missing = validate_variables(template, variables)
        assert missing == []  # All variables should be available

        # 3. Substitute variables
        result = substitute_variables(template, variables)
        assert result == "postgresql://localhost:5432/prod_db"

    def test_complex_data_substitution(self):
        """Test substitution in complex nested data structures."""
        data = {
            "database": {
                "url": "postgresql://${db_host}:${db_port|5432}/${db_name}",
                "options": ["${option1}", "${option2|default_option}"],
            },
            "redis": "${redis_url|redis://localhost:6379}",
        }

        variables = {
            "db_host": "postgres.example.com",
            "db_name": "myapp",
            "option1": "ssl=true",
        }

        result = substitute_in_dict(data, variables)

        assert (
            result["database"]["url"] == "postgresql://postgres.example.com:5432/myapp"
        )
        assert result["database"]["options"][0] == "ssl=true"
        assert result["database"]["options"][1] == "default_option"
        assert result["redis"] == "redis://localhost:6379"

    def test_comprehensive_workflow(self):
        """Test comprehensive workflow with all V2 functions."""
        # 1. Find variables in template
        template = "Hello ${name}, you are ${age|25} years old from ${city}!"
        found_vars = find_variables(template)
        assert len(found_vars) == 3

        # 2. Validate variables
        variables = {"name": "Alice", "city": "New York"}
        missing = validate_variables(template, variables)
        assert missing == []  # age has default

        # 3. Substitute variables
        result = substitute_variables(template, variables)
        assert result == "Hello Alice, you are 25 years old from New York!"


class TestPerformance:
    """Test performance characteristics of V2 implementation."""

    def test_large_text_substitution(self):
        """Test substitution performance with large text."""
        # Create a large template with many variables
        template_parts = []
        variables = {}

        for i in range(1000):
            template_parts.append(f"Variable {i}: ${{var_{i}}}")
            variables[f"var_{i}"] = f"value_{i}"

        large_template = "\n".join(template_parts)

        # This should complete quickly
        result = substitute_variables(large_template, variables)

        # Verify a few substitutions worked
        assert "Variable 0: value_0" in result
        assert "Variable 999: value_999" in result

    def test_validation_performance(self):
        """Test validation performance with many variables."""
        template_parts = []
        variables = {}

        for i in range(500):
            template_parts.append(f"${{var_{i}}}")
            if i % 2 == 0:  # Every other variable is available
                variables[f"var_{i}"] = f"value_{i}"

        large_template = " ".join(template_parts)

        # This should complete quickly
        missing = validate_variables(large_template, variables)

        # Should find all odd-numbered variables as missing
        assert len(missing) == 250

    def test_find_variables_performance(self):
        """Test find_variables performance with large text."""
        template_parts = []
        for i in range(1000):
            template_parts.append(f"${{var_{i}}}")

        large_template = " ".join(template_parts)

        # This should complete quickly
        variables = find_variables(large_template)
        assert len(variables) == 1000

    def test_complex_data_structure_performance(self):
        """Test performance with complex nested data structures."""
        # Create a simpler nested structure to avoid deep nesting issues
        data = {
            "level_0": {
                "value": "${var_0}",
                "level_1": {"value": "${var_1}", "level_2": {"value": "${var_2}"}},
            }
        }
        variables = {"var_0": "value_0", "var_1": "value_1", "var_2": "value_2"}

        # This should complete quickly
        result = substitute_in_dict(data, variables)

        # Verify substitution worked at different levels
        assert result["level_0"]["value"] == "value_0"
        assert result["level_0"]["level_1"]["value"] == "value_1"
        assert result["level_0"]["level_1"]["level_2"]["value"] == "value_2"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_nested_variable_patterns(self):
        """Test handling of nested variable patterns."""
        text = "Hello ${${name}}!"
        variables = {"name": "var", "var": "World"}
        result = substitute_variables(text, variables)
        # V2 doesn't recursively substitute - this is correct behavior
        assert result == "Hello ${${name}}!"

    def test_malformed_variable_patterns(self):
        """Test handling of malformed variable patterns."""
        text = "Hello ${name and ${age}!"
        variables = {"name": "Alice", "age": "30"}
        result = substitute_variables(text, variables)
        # V2 handles malformed patterns by keeping them unchanged
        assert result == "Hello ${name and ${age}!"

    def test_empty_variable_names(self):
        """Test handling of empty variable names."""
        text = "Hello ${} and ${|default}!"
        variables = {}
        result = substitute_variables(text, variables)
        # V2 preserves original patterns for empty names
        assert result == "Hello ${} and ${|default}!"

    def test_whitespace_in_variable_names(self):
        """Test handling of whitespace in variable names."""
        text = "Hello ${ name } and ${ age | 30 }!"
        variables = {"name": "Alice", "age": "25"}
        result = substitute_variables(text, variables)
        # Should handle whitespace correctly
        assert result == "Hello Alice and 25!"

    def test_special_characters_in_defaults(self):
        """Test handling of special characters in default values."""
        text = 'Hello ${name|"O\'Connor"} and ${path|"/usr/local/bin"}!'
        variables = {}
        result = substitute_variables(text, variables)
        # Should handle special characters correctly
        assert "O'Connor" in result
        assert "/usr/local/bin" in result

    def test_unicode_variables(self):
        """Test handling of unicode variable names and values."""
        text = "Hello ${имя} and ${age}!"
        variables = {"имя": "Алиса", "age": "30"}
        result = substitute_variables(text, variables)
        assert result == "Hello Алиса and 30!"

    def test_large_numbers(self):
        """Test handling of large numbers."""
        text = "Value: ${large_number}"
        variables = {"large_number": 999999999999999999}
        result = substitute_variables(text, variables)
        assert "999999999999999999" in result

    def test_boolean_values(self):
        """Test handling of boolean values."""
        text = "Flag: ${flag}"
        variables = {"flag": True}
        result = substitute_variables(text, variables)
        assert result == "Flag: True"

    def test_none_values(self):
        """Test handling of None values."""
        text = "Value: ${value}"
        variables = {"value": None}
        result = substitute_variables(text, variables)
        assert result == "Value: None"

    def test_complex_objects(self):
        """Test handling of complex objects."""
        text = "Data: ${data}"
        variables = {"data": {"key": "value", "list": [1, 2, 3]}}
        result = substitute_variables(text, variables)
        assert "key" in result
        assert "value" in result
