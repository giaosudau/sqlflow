"""Tests for SQLFlow Variables V2 implementation

This module tests the core V2 functions to ensure they work correctly
and maintain compatibility with V1 behavior.
"""

from sqlflow.core.variables.v2 import (
    format_for_context,
    resolve_variables,
    substitute_in_dict,
    substitute_in_list,
    substitute_variables,
    validate_variables,
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

    def test_substitution_missing_variable(self):
        """Test substitution with missing variable (no default)."""
        text = "Hello ${name}!"
        variables = {}
        result = substitute_variables(text, variables)
        assert result == "Hello ${name}!"  # Should keep original

    def test_substitution_in_dict(self):
        """Test substitution in dictionary."""
        data = {"key": "value_${env}", "nested": {"inner": "${database_url}"}}
        variables = {"env": "prod", "database_url": "postgres://localhost"}
        result = substitute_in_dict(data, variables)

        assert result["key"] == "value_prod"
        assert result["nested"]["inner"] == "postgres://localhost"

    def test_substitution_in_list(self):
        """Test substitution in list."""
        data = ["item_${env}", "${database_url}", 123]
        variables = {"env": "prod", "database_url": "postgres://localhost"}
        result = substitute_in_list(data, variables)

        assert result[0] == "item_prod"
        assert result[1] == "postgres://localhost"
        assert result[2] == 123


class TestResolution:
    """Test priority resolution functions."""

    def test_basic_resolution(self):
        """Test basic variable resolution."""
        cli_vars = {"env": "prod"}
        profile_vars = {"env": "dev", "debug": "false"}
        result = resolve_variables(cli_vars=cli_vars, profile_vars=profile_vars)

        assert result["env"] == "prod"  # CLI takes priority
        assert result["debug"] == "false"  # Profile value

    def test_priority_order(self):
        """Test complete priority order: CLI > Profile > SET > ENV."""
        cli_vars = {"var1": "cli"}
        profile_vars = {"var1": "profile", "var2": "profile"}
        set_vars = {"var1": "set", "var2": "set", "var3": "set"}
        env_vars = {"var1": "env", "var2": "env", "var3": "env", "var4": "env"}

        result = resolve_variables(
            cli_vars=cli_vars,
            profile_vars=profile_vars,
            set_vars=set_vars,
            env_vars=env_vars,
        )

        assert result["var1"] == "cli"  # CLI wins
        assert result["var2"] == "profile"  # Profile wins over SET/ENV
        assert result["var3"] == "set"  # SET wins over ENV
        assert result["var4"] == "env"  # Only ENV has it


class TestFormatting:
    """Test context-specific formatting."""

    def test_text_formatting(self):
        """Test text context formatting."""
        assert format_for_context("hello", "text") == "hello"
        assert format_for_context(123, "text") == "123"
        assert format_for_context(None, "text") == ""

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


class TestIntegration:
    """Test integration between V2 components."""

    def test_end_to_end_substitution(self):
        """Test complete end-to-end substitution workflow."""
        # 1. Resolve variables from multiple sources
        cli_vars = {"env": "prod"}
        profile_vars = {"database_host": "localhost", "env": "dev"}
        env_vars = {"database_port": "5432"}

        variables = resolve_variables(
            cli_vars=cli_vars, profile_vars=profile_vars, env_vars=env_vars
        )

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
