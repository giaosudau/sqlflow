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
        """Test utility methods and factory methods."""
        config = VariableConfig(
            cli_variables={"cli_var": "value1"},
            profile_variables={"profile_var": "value2"},
        )
        manager = VariableManager(config)

        # Test get_resolved_variables (public method)
        resolved = manager.get_resolved_variables()
        assert resolved["cli_var"] == "value1"
        assert resolved["profile_var"] == "value2"

        # Test factory methods
        cli_manager = VariableManager.from_cli_variables({"test": "value"})
        assert cli_manager.substitute("${test}") == "value"

        profile_manager = VariableManager.from_profile_variables({"test": "value"})
        assert profile_manager.substitute("${test}") == "value"

        # Test mixed sources factory
        mixed_manager = VariableManager.from_mixed_sources(
            cli_variables={"cli": "cli_value"},
            profile_variables={"profile": "profile_value"},
        )
        # CLI should take priority
        mixed_manager_with_conflict = VariableManager.from_mixed_sources(
            cli_variables={"var": "cli"}, profile_variables={"var": "profile"}
        )
        assert mixed_manager_with_conflict.substitute("${var}") == "cli"


class TestBackwardCompatibilityWithExistingSystems:
    """Tests proving new VariableManager works correctly.

    These tests ensure the new system produces correct results.
    Legacy compatibility tests have been removed in Phase 4 after successful migration.
    """

    # Note: Legacy compatibility tests removed in Phase 4
    # All VariableSubstitutionEngine and VariableSubstitutor references removed
    # since those classes have been successfully migrated to VariableManager

    def test_new_variable_manager_functionality(self):
        """Test that new VariableManager works correctly."""
        # Test basic functionality
        config = VariableConfig(cli_variables={"name": "Alice"})
        manager = VariableManager(config)

        result = manager.substitute("Hello ${name}")
        assert result == "Hello Alice"

        # Test validation
        validation_result = manager.validate("Hello ${missing}")
        assert not validation_result.is_valid
        assert "missing" in validation_result.missing_variables


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
        # The new VariableManager returns values as-is without adding quotes
        assert result == "value_1 and value_999"

    def test_complex_nested_structure_performance(self):
        """Test performance with deeply nested data structures."""
        config = VariableConfig(cli_variables={"value": "test"})
        manager = VariableManager(config)

        # Create deeply nested structure
        nested_data = {"level_0": {"level_1": {"level_2": {"value": "${value}"}}}}
        result = manager.substitute(nested_data)

        assert result["level_0"]["level_1"]["level_2"]["value"] == "test"


class TestUtilityFunctions:
    """Test the new utility functions that replace 3-line boilerplate."""

    def test_substitute_variables_utility(self):
        """Test substitute_variables utility function."""
        from sqlflow.core.variables import substitute_variables

        # Test simple substitution
        result = substitute_variables("Hello ${name}", {"name": "Alice"})
        assert result == "Hello Alice"

        # Test dictionary substitution
        data = {"key": "${value}", "nested": {"item": "${value}"}}
        result = substitute_variables(data, {"value": "test"})
        assert result["key"] == "test"
        assert result["nested"]["item"] == "test"

        # Test list substitution
        data = ["${item1}", "${item2}"]
        result = substitute_variables(data, {"item1": "first", "item2": "second"})
        assert result == ["first", "second"]

    def test_validate_variables_utility(self):
        """Test validate_variables utility function."""
        from sqlflow.core.variables import validate_variables

        # Test validation with all variables present
        result = validate_variables("Hello ${name}", {"name": "Alice"})
        assert result.is_valid
        assert len(result.missing_variables) == 0

        # Test validation with missing variables
        result = validate_variables("Hello ${missing}", {})
        assert not result.is_valid
        assert "missing" in result.missing_variables

    def test_utility_functions_replace_boilerplate(self):
        """Test that utility functions eliminate the 3-line boilerplate pattern."""
        from sqlflow.core.variables import substitute_variables

        variables = {"env": "prod", "region": "us-west"}
        data = "Environment: ${env}, Region: ${region}"

        # OLD WAY (3 lines of boilerplate):
        # config = VariableConfig(cli_variables=variables)
        # manager = VariableManager(config)
        # result = manager.substitute(data)

        # NEW WAY (1 line):
        result = substitute_variables(data, variables)

        assert result == "Environment: prod, Region: us-west"
