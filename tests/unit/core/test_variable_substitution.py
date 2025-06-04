"""Tests for centralized variable substitution engine."""

from unittest.mock import patch

from sqlflow.core.variable_substitution import (
    VariableSubstitutionEngine,
    substitute_variables,
    validate_variables,
)


class TestVariableSubstitutionEngine:
    """Test cases for the centralized variable substitution engine."""

    def test_substitute_string_simple(self):
        """Test simple string substitution."""
        engine = VariableSubstitutionEngine({"name": "Alice", "age": 30})

        template = "Hello, ${name}! You are ${age} years old."
        expected = "Hello, Alice! You are 30 years old."
        assert engine.substitute(template) == expected

    def test_substitute_string_with_defaults(self):
        """Test string substitution with default values."""
        engine = VariableSubstitutionEngine({"name": "Alice"})

        # Test with default values
        template = "Hello, ${name}! You are ${age|25} years old."
        expected = "Hello, Alice! You are 25 years old."
        assert engine.substitute(template) == expected

    def test_substitute_string_with_quoted_defaults(self):
        """Test string substitution with quoted default values."""
        engine = VariableSubstitutionEngine({})

        # Test with quoted defaults
        template = 'Region: ${region|"us-west"}'
        expected = "Region: us-west"
        assert engine.substitute(template) == expected

        template = "Region: ${region|'us-east'}"
        expected = "Region: us-east"
        assert engine.substitute(template) == expected

    def test_substitute_string_missing_variable(self):
        """Test behavior with missing variables."""
        engine = VariableSubstitutionEngine({})

        template = "Hello, ${missing_name}!"
        # Should keep original text for missing variables without defaults
        assert engine.substitute(template) == template

    def test_substitute_dict(self):
        """Test dictionary substitution."""
        engine = VariableSubstitutionEngine({"region": "eu-west", "env": "prod"})

        template = {
            "destination": "s3://${region}-bucket/data/${env}/",
            "options": {"format": "csv", "path": "/tmp/${env}/"},
            "number": 42,
        }

        expected = {
            "destination": "s3://eu-west-bucket/data/prod/",
            "options": {"format": "csv", "path": "/tmp/prod/"},
            "number": 42,
        }

        assert engine.substitute(template) == expected

    def test_substitute_list(self):
        """Test list substitution."""
        engine = VariableSubstitutionEngine({"a": 1, "b": 2})

        template = [
            "Value: ${a}",
            {"nested": "Item ${b}"},
            ["${a}", "${b}"],
            42,  # Non-string/dict/list item
        ]

        expected = ["Value: 1", {"nested": "Item 2"}, ["1", "2"], 42]
        assert engine.substitute(template) == expected

    def test_update_variables(self):
        """Test updating variables."""
        engine = VariableSubstitutionEngine({"x": "old"})

        assert engine.substitute("${x}") == "old"

        engine.update_variables({"x": "new", "y": "added"})
        assert engine.substitute("${x}-${y}") == "new-added"

    def test_validate_required_variables(self):
        """Test variable validation."""
        engine = VariableSubstitutionEngine({"available": "value"})

        # Text with available variable
        text1 = "Using ${available} variable"
        assert engine.validate_required_variables(text1) == []

        # Text with missing variable
        text2 = "Using ${missing} variable"
        missing = engine.validate_required_variables(text2)
        assert "missing" in missing

        # Text with variable that has default (not considered missing)
        text3 = "Using ${missing|default} variable"
        assert engine.validate_required_variables(text3) == []

    def test_empty_and_none_inputs(self):
        """Test handling of empty and special inputs."""
        engine = VariableSubstitutionEngine({"var": "value"})

        # Empty string
        assert engine.substitute("") == ""

        # Empty dict
        assert engine.substitute({}) == {}

        # Empty list
        assert engine.substitute([]) == []

        # Non-string, non-dict, non-list types should pass through unchanged
        assert engine.substitute(42) == 42
        assert engine.substitute(True) is True

    def test_logging_behavior(self):
        """Test that appropriate logging occurs."""
        engine = VariableSubstitutionEngine({"known": "value"})

        with patch("sqlflow.core.variable_substitution.logger") as mock_logger:
            # Should log debug for known variable
            engine.substitute("${known}")
            mock_logger.debug.assert_called()

            # Should log warning for unknown variable
            engine.substitute("${unknown}")
            mock_logger.warning.assert_called()


class TestConvenienceFunctions:
    """Test convenience functions for backward compatibility."""

    def test_substitute_variables_function(self):
        """Test the substitute_variables convenience function."""
        variables = {"name": "Bob", "city": "Paris"}
        template = "I am ${name} from ${city}"
        expected = "I am Bob from Paris"

        result = substitute_variables(template, variables)
        assert result == expected

    def test_validate_variables_function(self):
        """Test the validate_variables convenience function."""
        variables = {"existing": "value"}
        text = "Using ${existing} and ${missing} variables"

        missing = validate_variables(text, variables)
        assert "missing" in missing
        assert "existing" not in missing

    def test_nested_structures(self):
        """Test substitution in deeply nested structures."""
        variables = {"db": "production", "table": "users"}
        template = {
            "source": {
                "database": "${db}",
                "tables": ["${table}", "logs"],
                "connection": {"host": "${db}-server.com", "port": 5432},
            }
        }

        expected = {
            "source": {
                "database": "production",
                "tables": ["users", "logs"],
                "connection": {"host": "production-server.com", "port": 5432},
            }
        }

        result = substitute_variables(template, variables)
        assert result == expected


class TestEdgeCases:
    """Test edge cases and error scenarios."""

    def test_malformed_variable_expressions(self):
        """Test handling of malformed variable expressions."""
        engine = VariableSubstitutionEngine({"valid": "value"})

        # Incomplete expressions should be left as-is
        assert engine.substitute("${incomplete") == "${incomplete"
        assert engine.substitute("$incomplete}") == "$incomplete}"
        assert engine.substitute("${}") == "${}"

    def test_nested_variable_like_patterns(self):
        """Test that nested patterns are handled correctly."""
        engine = VariableSubstitutionEngine({"outer": "value"})

        # Should not try to substitute inner ${inner}
        text = "SELECT '${outer}' as col, '${missing}' as other"
        result = engine.substitute(text)

        assert "value" in result
        assert "${missing}" in result  # Should remain unchanged

    def test_special_characters_in_defaults(self):
        """Test defaults containing special characters."""
        engine = VariableSubstitutionEngine({})

        text = "${path|/data/files with spaces/file.csv}"
        result = engine.substitute(text)
        assert result == "/data/files with spaces/file.csv"

    def test_regex_escape_handling(self):
        """Test that variable names with regex special characters work."""
        engine = VariableSubstitutionEngine({"var.name": "value", "var+plus": "plus"})

        # These should work correctly despite special regex characters
        assert engine.substitute("${var.name}") == "value"
        assert engine.substitute("${var+plus}") == "plus"


class TestPriorityBasedResolution:
    """Test priority-based variable resolution functionality."""

    def test_priority_order_basic(self):
        """Test basic priority order: CLI > Profile > SET > Environment."""
        # Set up variables with same name in different sources
        cli_vars = {"var": "cli_value"}
        profile_vars = {"var": "profile_value"}
        set_vars = {"var": "set_value"}

        engine = VariableSubstitutionEngine(
            cli_variables=cli_vars,
            profile_variables=profile_vars,
            set_variables=set_vars,
        )

        # CLI should win
        assert engine.substitute("${var}") == "cli_value"

    def test_priority_order_without_cli(self):
        """Test priority order when CLI variable is not present."""
        profile_vars = {"var": "profile_value"}
        set_vars = {"var": "set_value"}

        engine = VariableSubstitutionEngine(
            profile_variables=profile_vars, set_variables=set_vars
        )

        # Profile should win
        assert engine.substitute("${var}") == "profile_value"

    def test_priority_order_only_set(self):
        """Test priority order when only SET variable is present."""
        set_vars = {"var": "set_value"}

        engine = VariableSubstitutionEngine(set_variables=set_vars)

        # SET should be used
        assert engine.substitute("${var}") == "set_value"

    def test_priority_order_with_environment(self):
        """Test that environment variables are used as fallback."""
        import os

        # Set environment variable
        os.environ["TEST_ENV_VAR"] = "env_value"

        try:
            # No explicit variables provided
            engine = VariableSubstitutionEngine(
                cli_variables={}, profile_variables={}, set_variables={}
            )

            # Should use environment variable
            assert engine.substitute("${TEST_ENV_VAR}") == "env_value"

            # Explicit variables should override environment
            engine = VariableSubstitutionEngine(
                cli_variables={"TEST_ENV_VAR": "cli_overrides_env"}
            )
            assert engine.substitute("${TEST_ENV_VAR}") == "cli_overrides_env"

        finally:
            # Clean up
            del os.environ["TEST_ENV_VAR"]

    def test_backward_compatibility_mode(self):
        """Test that backward compatibility mode still works."""
        # Using old-style initialization should work exactly as before
        engine = VariableSubstitutionEngine({"var": "old_style"})

        assert engine.substitute("${var}") == "old_style"
        assert not engine._use_priority_mode

    def test_mixed_priority_sources(self):
        """Test complex scenario with variables in different sources."""
        cli_vars = {"from_cli": "cli_value", "shared": "cli_wins"}
        profile_vars = {"from_profile": "profile_value", "shared": "profile_loses"}
        set_vars = {"from_set": "set_value", "shared": "set_loses"}

        engine = VariableSubstitutionEngine(
            cli_variables=cli_vars,
            profile_variables=profile_vars,
            set_variables=set_vars,
        )

        template = "CLI: ${from_cli}, Profile: ${from_profile}, SET: ${from_set}, Shared: ${shared}"
        expected = (
            "CLI: cli_value, Profile: profile_value, SET: set_value, Shared: cli_wins"
        )

        assert engine.substitute(template) == expected

    def test_update_variables_in_priority_mode(self):
        """Test that update_variables works correctly in priority mode."""
        engine = VariableSubstitutionEngine(cli_variables={"initial": "value"})

        # Should update CLI variables (highest priority)
        engine.update_variables({"new_var": "new_value"})

        assert engine.substitute("${initial}") == "value"
        assert engine.substitute("${new_var}") == "new_value"

        # Verify it went to CLI variables
        assert "new_var" in engine.cli_variables

    def test_validate_required_variables_with_priority(self):
        """Test variable validation works with priority mode."""
        engine = VariableSubstitutionEngine(
            cli_variables={"cli_var": "value"},
            profile_variables={"profile_var": "value"},
        )

        text = "Using ${cli_var}, ${profile_var}, and ${missing_var}"
        missing = engine.validate_required_variables(text)

        assert "missing_var" in missing
        assert "cli_var" not in missing
        assert "profile_var" not in missing

    def test_priority_mode_detection(self):
        """Test that priority mode is correctly detected."""
        # Should NOT use priority mode (backward compatibility)
        engine1 = VariableSubstitutionEngine({"var": "value"})
        assert not engine1._use_priority_mode

        # Should use priority mode
        engine2 = VariableSubstitutionEngine(cli_variables={"var": "value"})
        assert engine2._use_priority_mode

        engine3 = VariableSubstitutionEngine(
            variables={"old": "style"}, cli_variables={"new": "style"}
        )
        assert (
            engine3._use_priority_mode
        )  # Priority mode because cli_variables provided

    def test_empty_priority_sources(self):
        """Test behavior with empty priority sources."""
        engine = VariableSubstitutionEngine(
            cli_variables={}, profile_variables={}, set_variables={}
        )

        # Should handle missing variables gracefully
        template = "Missing: ${missing_var}"
        assert engine.substitute(template) == template  # Should remain unchanged

        # Should detect as missing
        missing = engine.validate_required_variables(template)
        assert "missing_var" in missing
