"""Comprehensive tests for the new VariableManager.

Following the refactoring plan, these tests prove that the new VariableManager
produces IDENTICAL results to all existing variable substitution systems.
This ensures backward compatibility and safe migration.

The tests follow Zen of Python principles:
- Simple is better than complex
- Readability counts
- Explicit is better than implicit
"""

import os
from unittest.mock import patch

from sqlflow.core.variables.manager import (
    ValidationResult,
    VariableConfig,
    VariableManager,
)


class TestVariableManager:
    """Test the unified VariableManager functionality."""

    def test_substitute_simple_string(self):
        """Test basic string substitution works correctly."""
        config = VariableConfig(cli_variables={"name": "Alice"})
        manager = VariableManager(config)
        result = manager.substitute("Hello ${name}")
        assert result == "Hello Alice"

    def test_substitute_multiple_variables(self):
        """Test substitution with multiple variables."""
        config = VariableConfig(cli_variables={"name": "Alice", "age": 30})
        manager = VariableManager(config)
        result = manager.substitute("Hello ${name}, you are ${age} years old")
        assert result == "Hello Alice, you are 30 years old"

    def test_substitute_with_defaults(self):
        """Test substitution with default values."""
        config = VariableConfig(cli_variables={"name": "Alice"})
        manager = VariableManager(config)
        result = manager.substitute("Hello ${name}, you are ${age|25} years old")
        assert result == "Hello Alice, you are 25 years old"

    def test_substitute_dict_structure(self):
        """Test substitution in dictionary structures."""
        config = VariableConfig(cli_variables={"env": "prod", "region": "us-west"})
        manager = VariableManager(config)

        data = {
            "environment": "${env}",
            "settings": {"region": "${region}", "debug": False},
        }

        result = manager.substitute(data)

        expected = {
            "environment": "prod",
            "settings": {"region": "us-west", "debug": False},
        }

        assert result == expected

    def test_substitute_list_structure(self):
        """Test substitution in list structures."""
        config = VariableConfig(cli_variables={"env": "prod"})
        manager = VariableManager(config)

        data = ["${env}", "development", {"env": "${env}"}]
        result = manager.substitute(data)

        expected = ["prod", "development", {"env": "prod"}]
        assert result == expected

    def test_priority_order_cli_over_profile(self):
        """Test that CLI variables override profile variables."""
        config = VariableConfig(
            cli_variables={"env": "dev"}, profile_variables={"env": "prod"}
        )
        manager = VariableManager(config)
        assert manager.substitute("${env}") == "dev"

    def test_priority_order_profile_over_set(self):
        """Test that profile variables override SET variables."""
        config = VariableConfig(
            profile_variables={"env": "dev"}, set_variables={"env": "prod"}
        )
        manager = VariableManager(config)
        assert manager.substitute("${env}") == "dev"

    def test_priority_order_complete_chain(self):
        """Test complete priority chain: CLI > Profile > SET > ENV."""
        config = VariableConfig(
            cli_variables={"priority": "cli"},
            profile_variables={"priority": "profile"},
            set_variables={"priority": "set"},
            env_variables={"priority": "env"},
        )
        manager = VariableManager(config)
        assert manager.substitute("${priority}") == "cli"

    def test_validation_all_present(self):
        """Test validation when all variables are present."""
        config = VariableConfig(cli_variables={"name": "Alice", "age": 30})
        manager = VariableManager(config)
        result = manager.validate("Hello ${name}, you are ${age}")
        assert result.is_valid
        assert len(result.missing_variables) == 0

    def test_validation_missing_required(self):
        """Test validation when required variables are missing."""
        config = VariableConfig()
        manager = VariableManager(config)
        result = manager.validate("Hello ${missing}")
        assert not result.is_valid
        assert "missing" in result.missing_variables

    def test_validation_with_defaults_passes(self):
        """Test validation passes when variables have defaults."""
        config = VariableConfig()
        manager = VariableManager(config)
        result = manager.validate("Hello ${name|Anonymous}")
        assert result.is_valid  # Should pass because default is provided

    def test_none_input_handling(self):
        """Test graceful handling of None inputs."""
        config = VariableConfig()
        manager = VariableManager(config)

        assert manager.substitute(None) is None
        result = manager.validate("")
        assert result.is_valid

    def test_empty_config_initialization(self):
        """Test initialization with empty or None config."""
        manager = VariableManager()
        assert manager.substitute("Hello World") == "Hello World"

        manager = VariableManager(None)
        assert manager.substitute("Hello World") == "Hello World"

    def test_utility_methods(self):
        """Test utility methods for introspection."""
        config = VariableConfig(
            cli_variables={"cli_var": "value1"},
            profile_variables={"profile_var": "value2"},
        )
        manager = VariableManager(config)

        # Test get_configuration
        assert manager.get_configuration() == config

        # Test get_resolved_variables
        resolved = manager.get_resolved_variables()
        assert resolved["cli_var"] == "value1"
        assert resolved["profile_var"] == "value2"

        # Test has_variable
        assert manager.has_variable("cli_var")
        assert manager.has_variable("profile_var")
        assert not manager.has_variable("missing_var")


class TestBackwardCompatibilityWithExistingSystems:
    """CRITICAL: Tests proving new VariableManager is identical to existing systems.

    These tests ensure the new system produces exactly the same results as
    the existing implementations, enabling safe migration.
    """

    def test_identical_to_variable_substitution_engine(self):
        """Test that new VariableManager produces identical results to VariableSubstitutionEngine."""
        from sqlflow.core.variable_substitution import VariableSubstitutionEngine

        # Test cases covering various scenarios
        test_cases = [
            # (variables, template, description)
            ({"name": "Alice"}, "Hello ${name}", "simple substitution"),
            ({"a": 1, "b": 2}, "${a} + ${b}", "multiple variables"),
            (
                {"name": "Alice"},
                "Hello ${name|Anonymous}",
                "variable with default (present)",
            ),
            ({}, "Hello ${name|Anonymous}", "variable with default (missing)"),
            (
                {"x": "value"},
                {"key": "${x}", "nested": {"value": "${x}"}},
                "dict substitution",
            ),
            ({"x": "value"}, ["${x}", "static", "${x}"], "list substitution"),
            ({"region": "us-west"}, 'Region: ${region|"us-east"}', "quoted default"),
        ]

        for variables, template, description in test_cases:
            # Test with old VariableSubstitutionEngine
            old_engine = VariableSubstitutionEngine(variables)
            old_result = old_engine.substitute(template)

            # Test with new VariableManager
            config = VariableConfig(cli_variables=variables)
            new_manager = VariableManager(config)
            new_result = new_manager.substitute(template)

            assert (
                old_result == new_result
            ), f"Results differ for {description}: {template}"

    def test_identical_to_variable_substitutor(self):
        """Test that new VariableManager produces identical results to VariableSubstitutor."""
        from sqlflow.core.variables import VariableContext, VariableSubstitutor

        test_cases = [
            ({"name": "Alice"}, "Hello ${name}"),
            ({"age": 30}, "Age: ${age}"),
            ({"name": "Alice"}, "Hello ${name|Anonymous}"),
            ({}, "Hello ${name|Anonymous}"),
        ]

        for variables, template in test_cases:
            # Test with old VariableSubstitutor
            old_context = VariableContext(cli_variables=variables)
            old_substitutor = VariableSubstitutor(old_context)
            old_result = old_substitutor.substitute_string(template)

            # Test with new VariableManager
            config = VariableConfig(cli_variables=variables)
            new_manager = VariableManager(config)
            new_result = new_manager.substitute(template)

            assert old_result == new_result, f"Results differ for template: {template}"

    def test_identical_validation_behavior(self):
        """Test that validation behavior matches existing systems."""
        from sqlflow.core.variable_substitution import VariableSubstitutionEngine

        test_cases = [
            ({"name": "Alice"}, "Hello ${name}", []),  # No missing variables
            ({}, "Hello ${missing}", ["missing"]),  # Missing variable
            ({}, "Hello ${name|default}", []),  # Default provided
        ]

        for variables, template, expected_missing in test_cases:
            # Test with old engine
            old_engine = VariableSubstitutionEngine(variables)
            old_missing = old_engine.validate_required_variables(template)

            # Test with new manager
            config = VariableConfig(cli_variables=variables)
            new_manager = VariableManager(config)
            new_result = new_manager.validate(template)

            assert set(old_missing) == set(new_result.missing_variables)
            assert set(expected_missing) == set(new_result.missing_variables)

    def test_priority_resolution_matches_existing(self):
        """Test that priority resolution matches existing VariableSubstitutionEngine behavior."""
        from sqlflow.core.variable_substitution import VariableSubstitutionEngine

        cli_vars = {"env": "cli_value", "name": "cli_name"}
        profile_vars = {"env": "profile_value", "region": "profile_region"}
        set_vars = {"env": "set_value", "debug": "set_debug"}

        # Test with old engine (priority mode)
        old_engine = VariableSubstitutionEngine(
            cli_variables=cli_vars,
            profile_variables=profile_vars,
            set_variables=set_vars,
        )

        # Test with new manager
        config = VariableConfig(
            cli_variables=cli_vars,
            profile_variables=profile_vars,
            set_variables=set_vars,
        )
        new_manager = VariableManager(config)

        # Test priority resolution
        test_cases = [
            "${env}",  # Should be cli_value (CLI overrides others)
            "${name}",  # Should be cli_name (only in CLI)
            "${region}",  # Should be profile_region (only in profile)
            "${debug}",  # Should be set_debug (only in SET)
        ]

        for template in test_cases:
            old_result = old_engine.substitute(template)
            new_result = new_manager.substitute(template)
            assert (
                old_result == new_result
            ), f"Priority resolution differs for: {template}"


class TestVariableConfig:
    """Test the VariableConfig dataclass."""

    def test_initialization_with_none(self):
        """Test initialization with None values."""
        config = VariableConfig()
        assert config.cli_variables == {}
        assert config.profile_variables == {}
        assert config.set_variables == {}
        assert config.env_variables == {}

    def test_initialization_with_values(self):
        """Test initialization with actual values."""
        config = VariableConfig(
            cli_variables={"cli": "value"}, profile_variables={"profile": "value"}
        )
        assert config.cli_variables == {"cli": "value"}
        assert config.profile_variables == {"profile": "value"}
        assert config.set_variables == {}
        assert config.env_variables == {}

    def test_resolve_priority_order(self):
        """Test that resolve_priority follows correct priority order."""
        config = VariableConfig(
            cli_variables={"var": "cli", "cli_only": "cli_value"},
            profile_variables={"var": "profile", "profile_only": "profile_value"},
            set_variables={"var": "set", "set_only": "set_value"},
            env_variables={"var": "env", "env_only": "env_value"},
        )

        resolved = config.resolve_priority()

        # CLI should win for conflicting variable
        assert resolved["var"] == "cli"

        # Each unique variable should be present
        assert resolved["cli_only"] == "cli_value"
        assert resolved["profile_only"] == "profile_value"
        assert resolved["set_only"] == "set_value"
        assert resolved["env_only"] == "env_value"


class TestValidationResult:
    """Test the ValidationResult dataclass."""

    def test_validation_result_structure(self):
        """Test ValidationResult structure and properties."""
        result = ValidationResult(
            is_valid=False,
            missing_variables=["var1", "var2"],
            invalid_defaults=["${var3|invalid}"],
            warnings=["Warning message"],
            context_locations={"var1": ["line 10", "line 20"]},
        )

        assert not result.is_valid
        assert result.missing_variables == ["var1", "var2"]
        assert result.invalid_defaults == ["${var3|invalid}"]
        assert result.warnings == ["Warning message"]
        assert result.context_locations == {"var1": ["line 10", "line 20"]}


class TestEnvironmentVariableIntegration:
    """Test integration with environment variables."""

    def test_environment_variables_are_used(self):
        """Test that environment variables are properly used."""
        with patch.dict(os.environ, {"TEST_ENV_VAR": "env_value"}):
            config = VariableConfig()
            manager = VariableManager(config)

            # The VariableSubstitutionEngine should automatically pick up env vars
            result = manager.substitute("Value: ${TEST_ENV_VAR}")
            assert result == "Value: env_value"

    def test_cli_overrides_environment(self):
        """Test that CLI variables override environment variables."""
        with patch.dict(os.environ, {"OVERRIDE_VAR": "env_value"}):
            config = VariableConfig(cli_variables={"OVERRIDE_VAR": "cli_value"})
            manager = VariableManager(config)

            result = manager.substitute("Value: ${OVERRIDE_VAR}")
            assert result == "Value: cli_value"


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_invalid_input_types(self):
        """Test handling of invalid input types."""
        config = VariableConfig()
        manager = VariableManager(config)

        # Should handle non-string types gracefully
        assert manager.substitute(123) == 123
        assert manager.substitute(True) is True
        assert manager.substitute([]) == []

    def test_circular_reference_protection(self):
        """Test protection against circular variable references."""
        # This depends on VariableSubstitutionEngine's behavior
        config = VariableConfig(cli_variables={"a": "${b}", "b": "${a}"})
        manager = VariableManager(config)

        # Should not crash, behavior depends on underlying engine
        result = manager.substitute("${a}")
        # The exact result depends on VariableSubstitutionEngine implementation
        assert isinstance(result, str)


# Performance and stress tests
class TestPerformance:
    """Test performance characteristics of VariableManager."""

    def test_large_variable_set_performance(self):
        """Test performance with large variable sets."""
        large_vars = {f"var_{i}": f"value_{i}" for i in range(1000)}
        config = VariableConfig(cli_variables=large_vars)
        manager = VariableManager(config)

        # Should handle large variable sets without issues
        result = manager.substitute("${var_1} and ${var_999}")
        # The VariableSubstitutionEngine adds quotes around values
        assert result == "'value_1' and 'value_999'"

    def test_complex_nested_structure_performance(self):
        """Test performance with deeply nested data structures."""
        config = VariableConfig(cli_variables={"value": "test"})
        manager = VariableManager(config)

        # Create deeply nested structure
        nested_data = {"level_0": {"level_1": {"level_2": {"value": "${value}"}}}}
        result = manager.substitute(nested_data)

        assert result["level_0"]["level_1"]["level_2"]["value"] == "test"
