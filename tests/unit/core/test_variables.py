"""Tests for the variable resolution and substitution module."""

import pytest

from sqlflow.core.variables import VariableContext, VariableSubstitutor


class TestVariableContext:
    """Tests for the VariableContext class."""

    def test_empty_context(self):
        """Test creating an empty context."""
        context = VariableContext()
        assert context.get_all_variables() == {}
        assert not context.has_variable("any_var")
        assert context.get_variable("any_var") is None
        assert context.get_variable("any_var", "default") == "default"

    def test_priority_order(self):
        """Test variable priority order."""
        # SET variables (lowest priority)
        set_vars = {"region": "us-west", "env": "dev", "common": "set"}

        # Profile variables (medium priority)
        profile_vars = {"region": "eu-west", "stage": "prod", "common": "profile"}

        # CLI variables (highest priority)
        cli_vars = {"stage": "test", "output": "logs", "common": "cli"}

        # Create context with all variable sources
        context = VariableContext(
            cli_variables=cli_vars,
            profile_variables=profile_vars,
            set_variables=set_vars,
        )

        # Check effective variables with priority applied
        effective_vars = context.get_all_variables()

        # CLI variables should override others
        assert effective_vars["stage"] == "test"  # CLI overrides profile
        assert effective_vars["output"] == "logs"  # Only in CLI

        # Profile variables should override SET variables
        assert effective_vars["region"] == "eu-west"  # Profile overrides SET

        # Common variable in all sources should use CLI value (highest priority)
        assert effective_vars["common"] == "cli"

        # SET-only variables should be preserved
        assert effective_vars["env"] == "dev"  # Only in SET

    def test_contains_and_getitem(self):
        """Test __contains__ and __getitem__ methods."""
        context = VariableContext(cli_variables={"foo": "bar"})

        # Test contains
        assert "foo" in context
        assert "missing" not in context

        # Test getitem
        assert context["foo"] == "bar"

        # Test getitem with missing key
        with pytest.raises(KeyError):
            _ = context["missing"]

    def test_merge(self):
        """Test merging contexts."""
        context1 = VariableContext(
            cli_variables={"a": 1}, profile_variables={"b": 2}, set_variables={"c": 3}
        )

        context2 = VariableContext(
            cli_variables={"a": 10, "d": 40},
            profile_variables={"b": 20, "e": 50},
            set_variables={"c": 30, "f": 60},
        )

        # Merge contexts (context2 takes precedence)
        merged = context1.merge(context2)

        # Check merged variables
        assert merged.get_variable("a") == 10  # Overridden by context2
        assert merged.get_variable("b") == 20  # Overridden by context2
        assert merged.get_variable("c") == 30  # Overridden by context2
        assert merged.get_variable("d") == 40  # Only in context2
        assert merged.get_variable("e") == 50  # Only in context2
        assert merged.get_variable("f") == 60  # Only in context2


class TestVariableSubstitutor:
    """Tests for the VariableSubstitutor class."""

    def test_substitute_string_simple(self):
        """Test simple string substitution."""
        context = VariableContext(cli_variables={"name": "Alice", "age": 30})
        substitutor = VariableSubstitutor(context)

        # Test simple substitution
        template = "Hello, ${name}! You are ${age} years old."
        expected = "Hello, Alice! You are 30 years old."
        assert substitutor.substitute_string(template) == expected

    def test_substitute_string_with_defaults(self):
        """Test string substitution with default values."""
        context = VariableContext(cli_variables={"name": "Alice"})
        substitutor = VariableSubstitutor(context)

        # Test with default values
        template = "Hello, ${name}! You are ${age|25} years old in ${region|'us-east'}."
        expected = "Hello, Alice! You are 25 years old in us-east."
        assert substitutor.substitute_string(template) == expected

        # Default with quotes
        template = 'Region: ${region|"us-west"}'
        expected = "Region: us-west"
        assert substitutor.substitute_string(template) == expected

    def test_substitute_string_unresolved(self):
        """Test string substitution with unresolved variables."""
        context = VariableContext()
        substitutor = VariableSubstitutor(context)

        # Test with unresolved variables
        template = "Hello, ${name}!"
        assert substitutor.substitute_string(template) == template  # Keeps original
        assert "name" in context.get_unresolved_variables()
        assert context.has_unresolved_variables()

    def test_substitute_dict(self):
        """Test dictionary substitution."""
        context = VariableContext(cli_variables={"region": "eu-west", "env": "prod"})
        substitutor = VariableSubstitutor(context)

        # Test with nested dictionary
        template = {
            "destination": "s3://${region}-bucket/data/${env}/",
            "options": {"format": "csv", "path": "/tmp/${env}/"},
        }

        expected = {
            "destination": "s3://eu-west-bucket/data/prod/",
            "options": {"format": "csv", "path": "/tmp/prod/"},
        }

        assert substitutor.substitute_dict(template) == expected

    def test_substitute_list(self):
        """Test list substitution."""
        context = VariableContext(cli_variables={"a": 1, "b": 2})
        substitutor = VariableSubstitutor(context)

        # Test with mixed list
        template = [
            "Value: ${a}",
            {"nested": "Item ${b}"},
            ["${a}", "${b}"],
            42,  # Non-string/dict/list item
        ]

        expected = ["Value: 1", {"nested": "Item 2"}, ["1", "2"], 42]

        assert substitutor.substitute_list(template) == expected

    def test_substitute_any(self):
        """Test the substitute_any method."""
        context = VariableContext(cli_variables={"x": "foo"})
        substitutor = VariableSubstitutor(context)

        # Test with different data types
        assert substitutor.substitute_any("${x}") == "foo"  # String
        assert substitutor.substitute_any({"key": "${x}"}) == {"key": "foo"}  # Dict
        assert substitutor.substitute_any(["${x}"]) == ["foo"]  # List
        assert substitutor.substitute_any(42) == 42  # Other type (unchanged)

    def test_real_world_export_step(self):
        """Test a real-world export step scenario."""
        # Variables from different sources
        set_vars = {"output_dir": "default_output", "format": "csv"}
        profile_vars = {"output_dir": "profile_output", "region": "us-east"}
        cli_vars = {"region": "eu-west"}

        # Create context and substitutor
        context = VariableContext(
            cli_variables=cli_vars,
            profile_variables=profile_vars,
            set_variables=set_vars,
        )
        substitutor = VariableSubstitutor(context)

        # Mock export step with variables
        export_step = {
            "id": "export_csv_customers",
            "type": "export",
            "source_table": "customers",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": "${output_dir}/${region}/customers.${format}",
                "options": {
                    "delimiter": ",",
                    "header": True,
                    "region": "${region}",
                    "compression": "${compression|none}",
                },
            },
        }

        # Expected result after substitution
        expected_step = {
            "id": "export_csv_customers",
            "type": "export",
            "source_table": "customers",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": "profile_output/eu-west/customers.csv",
                "options": {
                    "delimiter": ",",
                    "header": True,
                    "region": "eu-west",
                    "compression": "none",
                },
            },
        }

        # Substitute variables
        result = substitutor.substitute_any(export_step)
        assert result == expected_step
