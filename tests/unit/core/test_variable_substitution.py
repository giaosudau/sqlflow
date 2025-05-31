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
    """Test the convenience functions for backward compatibility."""

    def test_substitute_variables_function(self):
        """Test the standalone substitute_variables function."""
        variables = {"name": "Bob", "count": 5}

        # String substitution
        result = substitute_variables("Hello ${name}, count: ${count}", variables)
        assert result == "Hello Bob, count: 5"

        # Dict substitution
        template = {"greeting": "Hello ${name}", "items": ["${count} items"]}
        expected = {"greeting": "Hello Bob", "items": ["5 items"]}
        result = substitute_variables(template, variables)
        assert result == expected

    def test_validate_variables_function(self):
        """Test the standalone validate_variables function."""
        variables = {"available": "value"}

        # No missing variables
        missing = validate_variables("${available}", variables)
        assert missing == []

        # Has missing variables
        missing = validate_variables("${missing}", variables)
        assert "missing" in missing

    def test_backward_compatibility(self):
        """Ensure the convenience functions maintain backward compatibility."""
        # Test that the functions work as drop-in replacements
        variables = {"region": "us-west", "env": "dev"}

        # Complex nested structure
        template = {
            "config": {
                "database": "${env}_db",
                "regions": ["${region}", "us-east"],
            },
            "settings": ["debug=${debug|false}"],
        }

        result = substitute_variables(template, variables)
        expected = {
            "config": {
                "database": "dev_db",
                "regions": ["us-west", "us-east"],
            },
            "settings": ["debug=false"],
        }
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
