"""Tests for variable priority resolution in the Planner.

This test file verifies that variable resolution follows the correct priority order:
1. CLI variables (highest priority)
2. Profile variables
3. SET variables from pipeline
4. Environment variables
5. Default values (lowest priority)
"""

import os

import pytest

from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser


class TestVariablePriorityResolution:
    """Test that variable resolution follows the correct priority order."""

    def test_cli_variables_override_profile_variables(self):
        """CLI variables should have highest priority over profile variables."""
        # Arrange
        sql = """
        CREATE TABLE result AS
        SELECT '${env}' as environment, 1 as value;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        cli_vars = {"env": "cli_env"}
        profile_vars = {"env": "profile_env"}

        # Act
        plan = planner.create_plan(
            pipeline, variables=cli_vars, profile_variables=profile_vars
        )

        # Assert
        assert len(plan) == 1
        sql_query = plan[0]["query"]
        assert "'cli_env'" in sql_query
        assert "'profile_env'" not in sql_query

    def test_profile_variables_override_set_variables(self):
        """Profile variables should override SET variables defined in pipeline."""
        # Arrange
        sql = """
        SET env = 'set_env';
        CREATE TABLE result AS
        SELECT '${env}' as environment, 1 as value;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        profile_vars = {"env": "profile_env"}

        # Act
        plan = planner.create_plan(pipeline, profile_variables=profile_vars)

        # Assert
        assert len(plan) == 1  # Only SQL block, SET is not an execution step
        sql_query = plan[0]["query"]
        assert "'profile_env'" in sql_query
        assert "'set_env'" not in sql_query

    def test_set_variables_used_when_no_higher_priority_exists(self):
        """SET variables should be used when no CLI or profile variables exist."""
        # Arrange
        sql = """
        SET env = 'set_env';
        CREATE TABLE result AS
        SELECT '${env}' as environment, 1 as value;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        # Act - no CLI or profile variables provided
        plan = planner.create_plan(pipeline)

        # Assert
        assert len(plan) == 1  # Only SQL block, SET is not an execution step
        sql_query = plan[0]["query"]
        assert "'set_env'" in sql_query

    def test_complex_variable_priority_scenario(self):
        """Variables from all sources should follow correct priority order."""
        # Arrange
        sql = """
        SET var1 = 'set_value1';
        SET var2 = 'set_value2';
        SET var3 = 'set_value3';
        
        CREATE TABLE result AS
        SELECT 
            '${var1}' as col1,  -- Should use CLI value
            '${var2}' as col2,  -- Should use profile value  
            '${var3}' as col3   -- Should use SET value
        ;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        cli_vars = {"var1": "cli_value1"}
        profile_vars = {"var1": "profile_value1", "var2": "profile_value2"}

        # Act
        plan = planner.create_plan(
            pipeline, variables=cli_vars, profile_variables=profile_vars
        )

        # Assert
        assert len(plan) == 1
        sql_query = plan[0]["query"]

        # Verify priority order was respected
        assert "'cli_value1'" in sql_query  # CLI beats profile and SET
        assert "'profile_value2'" in sql_query  # Profile beats SET
        assert "'set_value3'" in sql_query  # SET used when no override

        # Verify lower priority values were not used
        assert "'profile_value1'" not in sql_query  # CLI overrode this
        assert "'set_value1'" not in sql_query  # CLI overrode this
        assert "'set_value2'" not in sql_query  # Profile overrode this

    def test_environment_variables_used_as_fallback(self):
        """Environment variables should be used when no explicit variables provided."""
        # Arrange
        test_var_name = "TEST_PLANNER_VAR"
        test_var_value = "env_value"
        os.environ[test_var_name] = test_var_value

        try:
            sql = """
            CREATE TABLE result AS
            SELECT '${TEST_PLANNER_VAR}' as env_col;
            """

            parser = Parser()
            pipeline = parser.parse(sql)
            planner = Planner()

            # Act - no explicit variables provided
            plan = planner.create_plan(pipeline)

            # Assert
            assert len(plan) == 1
            sql_query = plan[0]["query"]
            assert f"'{test_var_value}'" in sql_query

        finally:
            # Clean up
            del os.environ[test_var_name]

    def test_missing_variables_cause_planning_error(self):
        """Planning should fail with clear error when required variables are missing."""
        # Arrange
        sql = """
        CREATE TABLE result AS
        SELECT '${missing_var}' as col;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        # Act & Assert
        with pytest.raises(Exception) as excinfo:
            planner.create_plan(pipeline)

        error_msg = str(excinfo.value)
        assert "undefined variables" in error_msg or "missing_var" in error_msg

    def test_default_values_used_when_variable_undefined(self):
        """Default values should be used when variable is not defined anywhere."""
        # Arrange
        sql = """
        CREATE TABLE result AS
        SELECT '${region|us-east}' as region_col;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        # Act - no variables provided, should use default
        plan = planner.create_plan(pipeline)

        # Assert
        assert len(plan) == 1
        sql_query = plan[0]["query"]
        assert "'us-east'" in sql_query

    def test_backward_compatibility_with_legacy_api(self):
        """Legacy single-variables parameter should still work correctly."""
        # Arrange - this mimics how existing code uses the planner
        sql = """
        CREATE TABLE result AS
        SELECT '${environment}' as env;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        # Act - old-style usage with just variables parameter
        variables = {"environment": "production"}
        plan = planner.create_plan(pipeline, variables=variables)

        # Assert
        assert len(plan) == 1
        sql_query = plan[0]["query"]
        assert "'production'" in sql_query


class TestVariableValidationBehavior:
    """Test variable validation behavior during planning."""

    def test_empty_variable_values_are_allowed(self):
        """Empty string variable values should be allowed and substituted."""
        # Arrange
        sql = """
        CREATE TABLE result AS
        SELECT '${empty_var}' as col;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        variables = {"empty_var": ""}

        # Act
        plan = planner.create_plan(pipeline, variables=variables)

        # Assert - should not raise error
        assert len(plan) == 1
        sql_query = plan[0]["query"]
        assert "''" in sql_query  # Empty string should be substituted

    def test_variable_with_spaces_in_default_value(self):
        """Variables with spaces in default values should work correctly."""
        # Arrange
        sql = """
        CREATE TABLE result AS
        SELECT '${region|"us west 2"}' as region_col;
        """

        parser = Parser()
        pipeline = parser.parse(sql)
        planner = Planner()

        # Act - no variables provided, should use quoted default
        plan = planner.create_plan(pipeline)

        # Assert
        assert len(plan) == 1
        sql_query = plan[0]["query"]
        assert "'us west 2'" in sql_query
