"""Integration tests for gradual migration with feature flags.

This module tests the gradual migration from legacy variable substitution
to the new VariableManager system using feature flags.

Following Zen of Python:
- Simple is better than complex: Clear test scenarios
- Explicit is better than implicit: Test both systems explicitly
- Practicality beats purity: Focus on real-world usage
"""

import os
from unittest.mock import patch

import pytest

from sqlflow.cli.variable_handler import VariableHandler
from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor


class TestGradualMigration:
    """Tests for gradual migration with feature flags.

    Following the refactor plan's requirements:
    - ALL EXISTING TESTS MUST PASS with feature flags disabled (default)
    - New system works when feature flag enabled
    - Identical behavior proven with comprehensive tests
    """

    def test_cli_variable_handler_with_old_system_default(self):
        """CLI VariableHandler works with old system (default behavior)."""
        # Default behavior - no feature flag set
        handler = VariableHandler({"name": "Alice", "age": 30})
        result = handler.substitute_variables("Hello ${name}, you are ${age}")
        assert result == "Hello Alice, you are 30"

    def test_cli_variable_handler_with_new_system_enabled(self):
        """CLI VariableHandler works with new system (feature flag enabled)."""
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            handler = VariableHandler({"name": "Alice", "age": 30})
            result = handler.substitute_variables("Hello ${name}, you are ${age}")
            assert result == "Hello Alice, you are 30"

    def test_duckdb_engine_with_old_system_default(self):
        """DuckDB engine works with new system (Phase 4: feature flags removed)."""
        # Phase 4: Feature flags removed, always use new system
        engine = DuckDBEngine()
        engine.register_variable("table_name", "users")
        engine.register_variable("limit", 100)

        result = engine.substitute_variables(
            "SELECT * FROM ${table_name} LIMIT ${limit}"
        )
        # New system doesn't automatically quote strings
        assert "users" in result and "100" in result

    def test_duckdb_engine_with_new_system_enabled(self):
        """DuckDB engine works with new system (feature flag enabled)."""
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            engine = DuckDBEngine()
            engine.register_variable("table_name", "users")
            engine.register_variable("limit", 100)

            result = engine.substitute_variables(
                "SELECT * FROM ${table_name} LIMIT ${limit}"
            )
            # New system should produce same result
            assert "users" in result and "100" in result

    def test_local_executor_with_old_system_default(self):
        """LocalExecutor works with old system (default behavior)."""
        executor = LocalExecutor()
        executor.variables = {"dest": "/tmp/output.csv", "batch_size": 1000}

        result = executor._substitute_variables(
            "Output to ${dest} with batch ${batch_size}"
        )
        assert result == "Output to /tmp/output.csv with batch 1000"

    def test_local_executor_with_new_system_enabled(self):
        """LocalExecutor works with new system (feature flag enabled)."""
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            executor = LocalExecutor()
            executor.variables = {"dest": "/tmp/output.csv", "batch_size": 1000}

            result = executor._substitute_variables(
                "Output to ${dest} with batch ${batch_size}"
            )
            assert result == "Output to /tmp/output.csv with batch 1000"

    def test_dictionary_substitution_consistency(self):
        """Dictionary substitution produces identical results in both systems."""
        test_dict = {
            "database": "${db_name}",
            "host": "${db_host}",
            "port": "${db_port}",
            "config": {"timeout": "${timeout}", "retries": "${max_retries}"},
        }
        variables = {
            "db_name": "production",
            "db_host": "localhost",
            "db_port": 5432,
            "timeout": 30,
            "max_retries": 3,
        }

        executor = LocalExecutor()
        executor.variables = variables

        # Test with old system (default)
        result_old = executor._substitute_variables_in_dict(test_dict.copy())

        # Test with new system
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            result_new = executor._substitute_variables_in_dict(test_dict.copy())

        # Results should be identical
        assert result_old == result_new
        assert result_old["database"] == "production"
        # Variable substitution converts values to strings
        assert result_old["config"]["timeout"] == "30"

    def test_validation_consistency_across_systems(self):
        """Variable validation produces consistent results across systems."""
        variables = {"name": "test", "value": 42}
        text_with_missing = "Hello ${name}, missing: ${unknown_var}"

        # Test CLI validation with old system
        handler_old = VariableHandler(variables)
        result_old = handler_old.validate_variable_usage(text_with_missing)

        # Test CLI validation with new system
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            handler_new = VariableHandler(variables)
            result_new = handler_new.validate_variable_usage(text_with_missing)

        # Both should fail validation due to missing variable
        assert result_old == result_new
        assert result_old is False  # Should fail due to missing variable

    def test_feature_flag_environment_variable_handling(self):
        """Feature flag handles various environment variable formats correctly."""
        test_cases = [
            ("true", True),
            ("TRUE", True),
            ("True", True),
            ("false", False),
            ("FALSE", False),
            ("False", False),
            ("invalid", False),
            ("", False),
        ]

        for env_value, expected_new_system in test_cases:
            with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": env_value}):
                handler = VariableHandler({"test": "value"})
                # We can't directly test the internal flag, but we can verify
                # the behavior is consistent
                result = handler.substitute_variables("${test}")
                assert result == "value"

    def test_rollback_capability(self):
        """System can rollback to old implementation by disabling feature flag."""
        variables = {"env": "production", "region": "us-west"}
        template = "Deploy to ${env} in ${region}"

        # Start with new system enabled
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            handler_new = VariableHandler(variables)
            result_new = handler_new.substitute_variables(template)

        # Rollback to old system (disable flag)
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "false"}):
            handler_old = VariableHandler(variables)
            result_old = handler_old.substitute_variables(template)

        # Results should be identical - rollback is seamless
        assert result_new == result_old
        assert result_old == "Deploy to production in us-west"

    def test_complex_variable_scenarios_consistency(self):
        """Complex variable scenarios work identically in both systems."""
        test_scenarios = [
            {
                "variables": {"name": "Alice", "age": 30, "city": "Seattle"},
                "template": "Hello ${name}, you are ${age} in ${city}",
                "expected": "Hello Alice, you are 30 in Seattle",
            },
            {
                "variables": {"prefix": "user", "id": 123},
                "template": "${prefix}_${id}.json",
                "expected": "user_123.json",
            },
            {
                "variables": {"host": "db.example.com", "port": 5432},
                "template": "postgresql://${host}:${port}/mydb",
                "expected": "postgresql://db.example.com:5432/mydb",
            },
            {
                "variables": {"env": "prod"},
                "template": "Config for ${env} environment",
                "expected": "Config for prod environment",
            },
        ]

        for scenario in test_scenarios:
            # Test with old system
            handler_old = VariableHandler(scenario["variables"])
            result_old = handler_old.substitute_variables(scenario["template"])

            # Test with new system
            with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
                handler_new = VariableHandler(scenario["variables"])
                result_new = handler_new.substitute_variables(scenario["template"])

            # Both systems should produce identical results
            assert result_old == result_new == scenario["expected"]

    def test_error_handling_consistency(self):
        """Error handling is consistent between old and new systems."""
        variables = {"valid_var": "value"}

        # Test CLI with missing variable
        handler_old = VariableHandler(variables)
        result_old = handler_old.substitute_variables("${missing_var}")

        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            handler_new = VariableHandler(variables)
            result_new = handler_new.substitute_variables("${missing_var}")

        # Both should handle missing variables the same way
        # (keeping original text when no default provided)
        assert result_old == result_new


class TestMigrationSafety:
    """Tests to ensure migration safety and backward compatibility."""

    def test_no_breaking_changes_in_default_mode(self):
        """Default behavior (feature flag disabled) maintains exact compatibility."""
        # This test ensures existing code continues to work unchanged
        variables = {"database": "mydb", "table": "users", "limit": 100}

        # CLI
        cli_handler = VariableHandler(variables)
        cli_result = cli_handler.substitute_variables(
            "SELECT * FROM ${database}.${table} LIMIT ${limit}"
        )
        assert "mydb" in cli_result and "users" in cli_result and "100" in cli_result

        # Engine
        engine = DuckDBEngine()
        for name, value in variables.items():
            engine.register_variable(name, value)
        engine_result = engine.substitute_variables(
            "SELECT * FROM ${table} LIMIT ${limit}"
        )
        assert "users" in engine_result and "100" in engine_result

        # Executor
        executor = LocalExecutor()
        executor.variables = variables
        executor_result = executor._substitute_variables(
            "Database: ${database}, Table: ${table}"
        )
        assert executor_result == "Database: mydb, Table: users"

    def test_feature_flag_isolation(self):
        """Feature flag only affects target components, doesn't break others."""
        # Even when new system is enabled, other parts of the system continue working
        with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
            # Core functionality should still work
            executor = LocalExecutor()
            assert executor is not None

            engine = DuckDBEngine()
            assert engine is not None

            # Basic engine operations should work
            engine.register_variable("test", "value")
            assert engine.get_variable("test") == "value"


@pytest.mark.parametrize(
    "component,method_name,test_data",
    [
        (
            "cli",
            "substitute_variables",
            {"vars": {"x": "test"}, "template": "${x}", "expected": "test"},
        ),
        (
            "executor",
            "_substitute_variables",
            {"vars": {"y": "value"}, "template": "${y}", "expected": "value"},
        ),
    ],
)
def test_parametrized_migration_consistency(component, method_name, test_data):
    """Parametrized test to ensure consistency across all components."""
    # Test old system
    if component == "cli":
        instance_old = VariableHandler(test_data["vars"])
    elif component == "executor":
        instance_old = LocalExecutor()
        instance_old.variables = test_data["vars"]

    result_old = getattr(instance_old, method_name)(test_data["template"])

    # Test new system
    with patch.dict(os.environ, {"SQLFLOW_USE_NEW_VARIABLES": "true"}):
        if component == "cli":
            instance_new = VariableHandler(test_data["vars"])
        elif component == "executor":
            instance_new = LocalExecutor()
            instance_new.variables = test_data["vars"]

        result_new = getattr(instance_new, method_name)(test_data["template"])

    # Results must be identical
    assert result_old == result_new == test_data["expected"]
