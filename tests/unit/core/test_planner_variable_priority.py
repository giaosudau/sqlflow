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

from sqlflow.core.planner_main import Planner
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    LoadStep,
    Pipeline,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)
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

    def test_planner_creates_plan_with_modern_variable_substitution_engine(self):
        """Test that planner correctly uses modern VariableSubstitutionEngine instead of legacy system."""
        # Arrange
        pipeline = Pipeline()

        # Add a source and load step that uses variables
        source_step = SourceDefinitionStep(
            name="test_source",
            connector_type="CSV",
            params={"path": "${data_path}/test.csv"},
            line_number=1,
        )
        pipeline.add_step(source_step)

        load_step = LoadStep(
            table_name="test_table", source_name="test_source", line_number=2
        )
        pipeline.add_step(load_step)

        # Act - create plan with CLI and profile variables
        cli_variables = {"data_path": "/cli/path"}
        profile_variables = {"data_path": "/profile/path"}

        planner = Planner()
        execution_plan = planner.create_plan(
            pipeline, variables=cli_variables, profile_variables=profile_variables
        )

        # Assert - CLI variables should take priority
        # Find the source definition step in the plan
        source_ops = [
            op for op in execution_plan if op.get("type") == "source_definition"
        ]
        assert len(source_ops) == 1

        # The path should use CLI variable (highest priority)
        source_query = source_ops[0]["query"]
        assert source_query["path"] == "/cli/path/test.csv"

    def test_conditional_evaluation_with_priority_based_variables(self):
        """Test that conditional evaluation properly uses priority-based variable resolution."""
        # Arrange
        pipeline = Pipeline()

        # Add SET step with default
        set_step = SetStep(
            variable_name="deploy_env",
            variable_value="${deploy_env|development}",
            line_number=1,
        )
        pipeline.add_step(set_step)

        # Create conditional based on deploy_env
        prod_step = SQLBlockStep(
            table_name="prod_data",
            sql_query="SELECT * FROM production_table",
            line_number=3,
        )

        dev_step = SQLBlockStep(
            table_name="dev_data",
            sql_query="SELECT * FROM development_table",
            line_number=6,
        )

        if_branch = ConditionalBranchStep(
            condition="${deploy_env} == 'production'", steps=[prod_step], line_number=2
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[dev_step], line_number=2
        )
        pipeline.add_step(conditional_block)

        # Act & Assert - test CLI variable override
        cli_variables = {"deploy_env": "production"}
        profile_variables = {"deploy_env": "staging"}  # Should be ignored

        planner = Planner()
        execution_plan = planner.create_plan(
            pipeline, variables=cli_variables, profile_variables=profile_variables
        )

        # Should use CLI variable and take production branch
        transform_ops = [op for op in execution_plan if op.get("type") == "transform"]
        assert len(transform_ops) == 1
        assert transform_ops[0]["query"] == "SELECT * FROM production_table"

    def test_string_variable_quoting_in_conditional_expressions(self):
        """Test that string variables are properly quoted in conditional expressions to avoid syntax errors."""
        # Arrange - this tests the specific bug that was fixed
        pipeline = Pipeline()

        # Create conditional with string variable that needs quoting
        us_step = SQLBlockStep(
            table_name="us_data", sql_query="SELECT * FROM us_sales", line_number=3
        )

        global_step = SQLBlockStep(
            table_name="global_data",
            sql_query="SELECT * FROM global_sales",
            line_number=6,
        )

        # This condition was causing the original error: ${target_region|global} == 'us-east'
        # The variable substitution was producing: global == 'us-east' (unquoted)
        # instead of: 'global' == 'us-east' (quoted)
        if_branch = ConditionalBranchStep(
            condition="${target_region|global} == 'us-east'",
            steps=[us_step],
            line_number=2,
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[global_step], line_number=2
        )
        pipeline.add_step(conditional_block)

        # Act - this should not raise a syntax error anymore
        planner = Planner()
        execution_plan = planner.create_plan(pipeline, variables={})

        # Assert - should use the default value "global" and take else branch
        transform_ops = [op for op in execution_plan if op.get("type") == "transform"]
        assert len(transform_ops) == 1
        assert transform_ops[0]["query"] == "SELECT * FROM global_sales"

    def test_hyphenated_string_variable_handling_in_conditionals(self):
        """Test that hyphenated string variables are properly handled in conditionals."""
        # Arrange - test variables with hyphens that could be parsed as subtraction
        pipeline = Pipeline()

        east_step = SQLBlockStep(
            table_name="east_data", sql_query="SELECT * FROM east_region", line_number=3
        )

        other_step = SQLBlockStep(
            table_name="other_data",
            sql_query="SELECT * FROM other_region",
            line_number=6,
        )

        # Variable with hyphen that could be misinterpreted as "us - east"
        if_branch = ConditionalBranchStep(
            condition="${region} == 'us-east'", steps=[east_step], line_number=2
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[other_step], line_number=2
        )
        pipeline.add_step(conditional_block)

        # Act - test with hyphenated variable value
        variables = {"region": "us-east"}

        planner = Planner()
        execution_plan = planner.create_plan(pipeline, variables=variables)

        # Assert - should properly match the hyphenated value
        transform_ops = [op for op in execution_plan if op.get("type") == "transform"]
        assert len(transform_ops) == 1
        assert transform_ops[0]["query"] == "SELECT * FROM east_region"

    def test_environment_variable_integration_in_conditional_planning(self):
        """Test that environment variables are properly integrated in conditional planning."""
        # Arrange
        import os

        original_env = os.environ.get("SQLFLOW_TEST_ENV")
        os.environ["SQLFLOW_TEST_ENV"] = "test_environment"

        try:
            pipeline = Pipeline()

            test_step = SQLBlockStep(
                table_name="test_data",
                sql_query="SELECT * FROM test_table",
                line_number=3,
            )

            prod_step = SQLBlockStep(
                table_name="prod_data",
                sql_query="SELECT * FROM prod_table",
                line_number=6,
            )

            # Condition that uses environment variable
            if_branch = ConditionalBranchStep(
                condition="${SQLFLOW_TEST_ENV} == 'test_environment'",
                steps=[test_step],
                line_number=2,
            )

            conditional_block = ConditionalBlockStep(
                branches=[if_branch], else_branch=[prod_step], line_number=2
            )
            pipeline.add_step(conditional_block)

            # Act - no CLI variables provided, should use environment variable
            planner = Planner()
            execution_plan = planner.create_plan(pipeline, variables={})

            # Assert - should use environment variable and take test branch
            transform_ops = [
                op for op in execution_plan if op.get("type") == "transform"
            ]
            assert len(transform_ops) == 1
            assert transform_ops[0]["query"] == "SELECT * FROM test_table"

        finally:
            # Cleanup
            if original_env is not None:
                os.environ["SQLFLOW_TEST_ENV"] = original_env
            else:
                os.environ.pop("SQLFLOW_TEST_ENV", None)
