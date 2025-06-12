"""Tests for conditional execution in the planner."""

from sqlflow.core.planner_main import ExecutionPlanBuilder
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    Pipeline,
    SQLBlockStep,
)


class TestConditionalPlanner:
    """Test cases for conditional execution in the planner."""

    def test_simple_if_condition_true(self):
        """Test that if condition is evaluated correctly when true."""
        # Create a simple if-then block
        pipeline = Pipeline()

        # Create SQL steps
        prod_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_users", line_number=2
        )

        dev_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM dev_users", line_number=5
        )

        # Create if branch
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[prod_step], line_number=1
        )

        # Create conditional block
        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[dev_step], line_number=1
        )

        pipeline.add_step(conditional_block)

        # Execute planner with variables
        variables = {"env": "production"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Verify only the prod step is included
        assert len(flattened_pipeline.steps) == 1
        assert isinstance(flattened_pipeline.steps[0], SQLBlockStep)
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM prod_users"

    def test_simple_if_condition_false(self):
        """Test that if condition is evaluated correctly when false."""
        # Create a simple if-then-else block
        pipeline = Pipeline()

        # Create SQL steps
        prod_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_users", line_number=2
        )

        dev_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM dev_users", line_number=5
        )

        # Create if branch
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[prod_step], line_number=1
        )

        # Create conditional block
        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[dev_step], line_number=1
        )

        pipeline.add_step(conditional_block)

        # Execute planner with variables
        variables = {"env": "development"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Verify only the dev step is included
        assert len(flattened_pipeline.steps) == 1
        assert isinstance(flattened_pipeline.steps[0], SQLBlockStep)
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM dev_users"

    def test_if_elseif_else_condition(self):
        """Test if-elseif-else condition evaluation with various variables."""
        # Create an if-elseif-else block
        pipeline = Pipeline()

        # Create SQL steps
        prod_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_users", line_number=2
        )

        staging_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM staging_users", line_number=5
        )

        dev_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM dev_users", line_number=8
        )

        # Create branches
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[prod_step], line_number=1
        )

        elseif_branch = ConditionalBranchStep(
            condition="${env} == 'staging'", steps=[staging_step], line_number=4
        )

        # Create conditional block
        conditional_block = ConditionalBlockStep(
            branches=[if_branch, elseif_branch], else_branch=[dev_step], line_number=1
        )

        pipeline.add_step(conditional_block)

        # Test with production environment
        variables = {"env": "production"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        assert len(flattened_pipeline.steps) == 1
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM prod_users"

        # Test with staging environment
        variables = {"env": "staging"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        assert len(flattened_pipeline.steps) == 1
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM staging_users"

        # Test with development environment
        variables = {"env": "development"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        assert len(flattened_pipeline.steps) == 1
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM dev_users"

    def test_nested_conditionals(self):
        """Test nested conditional blocks."""
        # Create a nested conditional structure
        pipeline = Pipeline()

        # Create SQL steps for the nested conditionals
        prod_east_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_east_users", line_number=3
        )

        prod_west_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_west_users", line_number=6
        )

        dev_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM dev_users", line_number=10
        )

        # Create inner conditional (region check)
        inner_if_branch = ConditionalBranchStep(
            condition="${region} == 'east'", steps=[prod_east_step], line_number=2
        )

        inner_conditional = ConditionalBlockStep(
            branches=[inner_if_branch], else_branch=[prod_west_step], line_number=2
        )

        # Create outer conditional (env check)
        outer_if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[inner_conditional], line_number=1
        )

        outer_conditional = ConditionalBlockStep(
            branches=[outer_if_branch], else_branch=[dev_step], line_number=1
        )

        pipeline.add_step(outer_conditional)

        # Test with production + east region
        variables = {"env": "production", "region": "east"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        assert len(flattened_pipeline.steps) == 1
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM prod_east_users"

        # Test with production + west region
        variables = {"env": "production", "region": "west"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        assert len(flattened_pipeline.steps) == 1
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM prod_west_users"

        # Test with development (region should be ignored)
        variables = {"env": "development", "region": "east"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        assert len(flattened_pipeline.steps) == 1
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM dev_users"

    def test_multiple_steps_in_branch(self):
        """Test branches with multiple steps."""
        # Create a conditional with multiple steps in each branch
        pipeline = Pipeline()

        # Create multiple SQL steps for each branch
        prod_step1 = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_users", line_number=2
        )

        prod_step2 = SQLBlockStep(
            table_name="orders", sql_query="SELECT * FROM prod_orders", line_number=3
        )

        dev_step1 = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM dev_users", line_number=6
        )

        dev_step2 = SQLBlockStep(
            table_name="orders", sql_query="SELECT * FROM dev_orders", line_number=7
        )

        # Create if branch with multiple steps
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'",
            steps=[prod_step1, prod_step2],
            line_number=1,
        )

        # Create conditional block
        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[dev_step1, dev_step2], line_number=1
        )

        pipeline.add_step(conditional_block)

        # Test with production environment
        variables = {"env": "production"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Verify both prod steps are included in order
        assert len(flattened_pipeline.steps) == 2
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM prod_users"
        assert flattened_pipeline.steps[1].sql_query == "SELECT * FROM prod_orders"

        # Test with development environment
        variables = {"env": "development"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Verify both dev steps are included in order
        assert len(flattened_pipeline.steps) == 2
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM dev_users"
        assert flattened_pipeline.steps[1].sql_query == "SELECT * FROM dev_orders"

    def test_mixed_conditional_and_regular_steps(self):
        """Test pipeline with a mix of conditional and regular steps."""
        # Create a pipeline with both conditional and regular steps
        pipeline = Pipeline()

        # Create regular steps
        regular_step1 = SQLBlockStep(
            table_name="common", sql_query="SELECT * FROM common_table", line_number=1
        )

        regular_step2 = SQLBlockStep(
            table_name="final", sql_query="SELECT * FROM temp_table", line_number=10
        )

        # Create conditional steps
        prod_step = SQLBlockStep(
            table_name="temp", sql_query="SELECT * FROM prod_data", line_number=4
        )

        dev_step = SQLBlockStep(
            table_name="temp", sql_query="SELECT * FROM dev_data", line_number=7
        )

        # Create conditional block
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[prod_step], line_number=3
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[dev_step], line_number=3
        )

        # Add steps to pipeline in order
        pipeline.add_step(regular_step1)
        pipeline.add_step(conditional_block)
        pipeline.add_step(regular_step2)

        # Test with production environment
        variables = {"env": "production"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Verify correct steps are included
        assert len(flattened_pipeline.steps) == 3
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM common_table"
        assert flattened_pipeline.steps[1].sql_query == "SELECT * FROM prod_data"
        assert flattened_pipeline.steps[2].sql_query == "SELECT * FROM temp_table"

        # Test with development environment
        variables = {"env": "development"}
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Verify correct steps are included
        assert len(flattened_pipeline.steps) == 3
        assert flattened_pipeline.steps[0].sql_query == "SELECT * FROM common_table"
        assert flattened_pipeline.steps[1].sql_query == "SELECT * FROM dev_data"
        assert flattened_pipeline.steps[2].sql_query == "SELECT * FROM temp_table"
