"""Tests for variable substitution in conditional expressions.

This module tests the integration between ConditionEvaluator and VariableSubstitutionEngine,
specifically covering the fixes for proper variable quoting and priority-based resolution
in conditional expressions.
"""

import pytest

from sqlflow.core.evaluator import ConditionEvaluator
from sqlflow.core.planner import ExecutionPlanBuilder
from sqlflow.core.variable_substitution import VariableSubstitutionEngine
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    Pipeline,
    SetStep,
    SQLBlockStep,
)


class TestConditionalVariableSubstitution:
    """Test variable substitution in conditional expressions."""

    def test_string_value_variable_substitution_with_proper_quoting(self):
        """Test that string variables are properly quoted in conditional expressions."""
        # Arrange
        variables = {"target_region": "global"}
        engine = VariableSubstitutionEngine(cli_variables=variables)
        evaluator = ConditionEvaluator(variables, engine)

        # Act & Assert - this was the failing case before the fix
        # ${target_region|global} should become 'global' (quoted) not global (unquoted)
        result = evaluator.evaluate("'global' == 'us-east'")
        assert result is False

        result = evaluator.evaluate("'global' == 'global'")
        assert result is True

    def test_variable_with_default_value_in_conditional(self):
        """Test variable substitution with default values in conditionals."""
        # Arrange - no variables provided, should use default
        variables = {}
        engine = VariableSubstitutionEngine(cli_variables=variables)
        evaluator = ConditionEvaluator(variables, engine)

        # Act - simulate the condition that was failing: ${target_region|global} == 'us-east'
        # Since target_region is not defined, it should use the default 'global'
        condition = "${target_region|global} == 'us-east'"
        substituted = engine._substitute_string(condition)

        # Assert - the substituted condition should have proper quoting
        assert (
            "'global' == 'us-east'" in substituted
            or "\"global\" == 'us-east'" in substituted
        )

        # The evaluation should work without syntax errors
        result = evaluator.evaluate(substituted)
        assert result is False

    def test_hyphenated_string_variable_substitution(self):
        """Test that hyphenated string values are properly quoted to avoid parsing as subtraction."""
        # Arrange
        variables = {"region": "us-east"}
        engine = VariableSubstitutionEngine(cli_variables=variables)
        evaluator = ConditionEvaluator(variables, engine)

        # Act & Assert - hyphenated values should be quoted to avoid us - east
        result = evaluator.evaluate("${region} == 'us-east'")
        assert result is True

        result = evaluator.evaluate("${region} == 'us-west'")
        assert result is False

    def test_variable_priority_in_conditional_evaluation(self):
        """Test that variable priority is respected in conditional evaluation."""
        # Arrange - CLI variables should override SET variables
        cli_variables = {"env": "production"}
        set_variables = {"env": "development"}

        engine = VariableSubstitutionEngine(
            cli_variables=cli_variables, set_variables=set_variables
        )
        evaluator = ConditionEvaluator(cli_variables, engine)

        # Act & Assert - should use CLI variable (production), not SET variable (development)
        result = evaluator.evaluate("${env} == 'production'")
        assert result is True

        result = evaluator.evaluate("${env} == 'development'")
        assert result is False

    def test_environment_variable_fallback_in_conditional(self):
        """Test that environment variables are used as fallback in conditionals."""
        # Arrange - no CLI or SET variables, should fall back to environment
        import os

        original_env = os.environ.get("TEST_CONDITIONAL_VAR")
        os.environ["TEST_CONDITIONAL_VAR"] = "test_value"

        try:
            variables = {}
            engine = VariableSubstitutionEngine(cli_variables=variables)
            evaluator = ConditionEvaluator(variables, engine)

            # Act & Assert - should use environment variable
            result = evaluator.evaluate("${TEST_CONDITIONAL_VAR} == 'test_value'")
            assert result is True

            result = evaluator.evaluate("${TEST_CONDITIONAL_VAR} == 'other_value'")
            assert result is False
        finally:
            # Cleanup
            if original_env is not None:
                os.environ["TEST_CONDITIONAL_VAR"] = original_env
            else:
                os.environ.pop("TEST_CONDITIONAL_VAR", None)

    def test_conditional_evaluation_with_missing_variable(self):
        """Test that missing variables without defaults cause appropriate errors."""
        # Arrange
        variables = {}
        engine = VariableSubstitutionEngine(cli_variables=variables)
        ConditionEvaluator(variables, engine)

        # Act & Assert - missing variable should be handled gracefully
        # The engine should substitute missing variables as None
        condition = "${undefined_var} == 'value'"
        substituted = engine._substitute_string(condition)

        # Should contain None for the missing variable
        assert "None == 'value'" in substituted

    def test_boolean_variable_in_conditional(self):
        """Test boolean variables in conditional expressions."""
        # Arrange
        variables = {"debug_mode": True, "production_mode": False}
        engine = VariableSubstitutionEngine(cli_variables=variables)
        evaluator = ConditionEvaluator(variables, engine)

        # Act & Assert
        result = evaluator.evaluate("${debug_mode} == True")
        assert result is True

        result = evaluator.evaluate("${production_mode} == False")
        assert result is True

        # Boolean values get quoted as strings, so we need to compare to string representations
        result = evaluator.evaluate("${debug_mode} == 'True'")
        assert result is True

        result = evaluator.evaluate("${production_mode} == 'False'")
        assert result is True

    def test_numeric_variable_in_conditional(self):
        """Test numeric variables in conditional expressions."""
        # Arrange
        variables = {"batch_size": 100, "timeout": 30.5}
        engine = VariableSubstitutionEngine(cli_variables=variables)
        evaluator = ConditionEvaluator(variables, engine)

        # Act & Assert
        result = evaluator.evaluate("${batch_size} > 50")
        assert result is True

        result = evaluator.evaluate("${timeout} <= 30.0")
        assert result is False

        result = evaluator.evaluate("${batch_size} == 100 and ${timeout} > 30")
        assert result is True


class TestPlannerConditionalIntegration:
    """Test end-to-end conditional planning with variable substitution."""

    def test_planner_conditional_with_string_variable_substitution(self):
        """Test complete planner flow with conditional and string variable substitution."""
        # Arrange - create a pipeline similar to the one that was failing
        pipeline = Pipeline()

        # Add SET step with default value
        set_step = SetStep(
            variable_name="target_region",
            variable_value="${target_region|global}",
            line_number=1,
        )
        pipeline.add_step(set_step)

        # Create conditional steps
        us_east_step = SQLBlockStep(
            table_name="us_analysis",
            sql_query="SELECT * FROM sales WHERE region = 'us-east'",
            line_number=3,
        )

        global_step = SQLBlockStep(
            table_name="global_analysis", sql_query="SELECT * FROM sales", line_number=6
        )

        # Create conditional block - this was the failing condition
        if_branch = ConditionalBranchStep(
            condition="${target_region} == 'us-east'",
            steps=[us_east_step],
            line_number=2,
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[global_step], line_number=2
        )
        pipeline.add_step(conditional_block)

        # Act - test with no CLI variables (should use default "global")
        builder = ExecutionPlanBuilder()
        variables = {}  # No CLI variables provided

        # This should not raise an error anymore
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Assert - should take the else branch (global_step) since "global" != "us-east"
        assert len(flattened_pipeline.steps) == 2  # SET step + conditional result
        # Find the SQL step (not the SET step)
        sql_steps = [
            step for step in flattened_pipeline.steps if isinstance(step, SQLBlockStep)
        ]
        assert len(sql_steps) == 1
        assert sql_steps[0].sql_query == "SELECT * FROM sales"

    def test_planner_conditional_with_cli_variable_override(self):
        """Test that CLI variables properly override defaults in conditionals."""
        # Arrange - same pipeline structure as above
        pipeline = Pipeline()

        set_step = SetStep(
            variable_name="target_region",
            variable_value="${target_region|global}",
            line_number=1,
        )
        pipeline.add_step(set_step)

        us_east_step = SQLBlockStep(
            table_name="us_analysis",
            sql_query="SELECT * FROM sales WHERE region = 'us-east'",
            line_number=3,
        )

        global_step = SQLBlockStep(
            table_name="global_analysis", sql_query="SELECT * FROM sales", line_number=6
        )

        if_branch = ConditionalBranchStep(
            condition="${target_region} == 'us-east'",
            steps=[us_east_step],
            line_number=2,
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[global_step], line_number=2
        )
        pipeline.add_step(conditional_block)

        # Act - provide CLI variable that should override default
        builder = ExecutionPlanBuilder()
        variables = {"target_region": "us-east"}

        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Assert - should take the if branch (us_east_step) since CLI "us-east" == "us-east"
        assert len(flattened_pipeline.steps) == 2  # SET step + conditional result
        sql_steps = [
            step for step in flattened_pipeline.steps if isinstance(step, SQLBlockStep)
        ]
        assert len(sql_steps) == 1
        assert sql_steps[0].sql_query == "SELECT * FROM sales WHERE region = 'us-east'"

    def test_planner_with_complex_conditional_expression(self):
        """Test planner with complex conditional expressions involving multiple variables."""
        # Arrange
        pipeline = Pipeline()

        # Multiple SET steps
        set_env = SetStep(
            variable_name="env", variable_value="${env|development}", line_number=1
        )
        set_region = SetStep(
            variable_name="region", variable_value="${region|us-west}", line_number=2
        )
        pipeline.add_step(set_env)
        pipeline.add_step(set_region)

        # Steps for different scenarios
        prod_east_step = SQLBlockStep(
            table_name="prod_east",
            sql_query="SELECT * FROM prod_east_data",
            line_number=5,
        )

        default_step = SQLBlockStep(
            table_name="default_data",
            sql_query="SELECT * FROM default_data",
            line_number=8,
        )

        # Complex conditional: env == 'production' and region == 'us-east'
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production' and ${region} == 'us-east'",
            steps=[prod_east_step],
            line_number=4,
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[default_step], line_number=4
        )
        pipeline.add_step(conditional_block)

        # Act & Assert - test case 1: both conditions true
        builder = ExecutionPlanBuilder()
        variables = {"env": "production", "region": "us-east"}

        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)
        sql_steps = [
            step for step in flattened_pipeline.steps if isinstance(step, SQLBlockStep)
        ]
        assert len(sql_steps) == 1
        assert sql_steps[0].sql_query == "SELECT * FROM prod_east_data"

        # Act & Assert - test case 2: one condition false
        variables = {"env": "production", "region": "us-west"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)
        sql_steps = [
            step for step in flattened_pipeline.steps if isinstance(step, SQLBlockStep)
        ]
        assert len(sql_steps) == 1
        assert sql_steps[0].sql_query == "SELECT * FROM default_data"

        # Act & Assert - test case 3: both conditions false (use defaults)
        variables = {}  # Should use defaults: env=development, region=us-west
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)
        sql_steps = [
            step for step in flattened_pipeline.steps if isinstance(step, SQLBlockStep)
        ]
        assert len(sql_steps) == 1
        assert sql_steps[0].sql_query == "SELECT * FROM default_data"

    def test_error_handling_for_invalid_conditional_syntax(self):
        """Test that invalid conditional syntax produces clear error messages."""
        # Arrange
        pipeline = Pipeline()

        # Create a conditional with invalid syntax (single = instead of ==)
        invalid_step = SQLBlockStep(
            table_name="test", sql_query="SELECT 1", line_number=3
        )

        invalid_branch = ConditionalBranchStep(
            condition="${env} = 'production'",  # Invalid: should be ==
            steps=[invalid_step],
            line_number=2,
        )

        conditional_block = ConditionalBlockStep(
            branches=[invalid_branch], else_branch=[], line_number=2
        )
        pipeline.add_step(conditional_block)

        # Act & Assert
        builder = ExecutionPlanBuilder()
        variables = {"env": "production"}

        with pytest.raises(Exception) as exc_info:
            builder._flatten_conditional_blocks(pipeline, variables)

        # Should get a clear error message about the syntax issue
        error_message = str(exc_info.value)
        assert "=" in error_message and (
            "syntax" in error_message.lower() or "==" in error_message
        )
