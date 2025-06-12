"""Tests for DAG visualization with conditional execution."""

from sqlflow.core.planner_main import ExecutionPlanBuilder
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    Pipeline,
    SQLBlockStep,
)
from sqlflow.visualizer.dag_builder_ast import ASTDAGBuilder


class TestConditionalDAG:
    """Test cases for DAG visualization with conditional execution."""

    def test_dag_with_flattened_pipeline(self):
        """Test that DAG builder works with flattened pipelines."""
        # Create a pipeline with conditional block
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

        # Flatten the pipeline based on variables
        variables = {"env": "production"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Build DAG from flattened pipeline
        dag_builder = ASTDAGBuilder()
        dag = dag_builder.build_dag_from_ast(flattened_pipeline)

        # Verify DAG has only one node for the prod_step
        nodes = dag.get_all_nodes()
        assert len(nodes) == 1

        # Verify node attributes
        attrs = dag.get_node_attributes(nodes[0])
        assert attrs["type"] == "SQL_BLOCK"
        assert attrs["label"] == "SQL_BLOCK: users"

    def test_dag_with_mixed_conditional_and_regular_steps(self):
        """Test DAG with a mix of conditional and regular steps."""
        # Create a pipeline with both conditional and regular steps
        pipeline = Pipeline()

        # Create regular steps
        regular_step1 = SQLBlockStep(
            table_name="common_data",
            sql_query="SELECT * FROM common_table",
            line_number=1,
        )

        # Create conditional steps - using customer_data as the table name
        prod_step = SQLBlockStep(
            table_name="customer_data",
            sql_query="SELECT * FROM prod_customer",
            line_number=4,
        )

        dev_step = SQLBlockStep(
            table_name="customer_data",
            sql_query="SELECT * FROM dev_customer",
            line_number=7,
        )

        # Create conditional block
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[prod_step], line_number=3
        )

        conditional_block = ConditionalBlockStep(
            branches=[if_branch], else_branch=[dev_step], line_number=3
        )

        # Final step using the customer_data table
        regular_step2 = SQLBlockStep(
            table_name="final_report",
            sql_query="SELECT * FROM customer_data",  # Depends on the conditional step
            line_number=10,
        )

        # Add steps to pipeline in order
        pipeline.add_step(regular_step1)
        pipeline.add_step(conditional_block)
        pipeline.add_step(regular_step2)

        # Flatten the pipeline with production environment
        variables = {"env": "production"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Build DAG from flattened pipeline
        dag_builder = ASTDAGBuilder()
        dag = dag_builder.build_dag_from_ast(flattened_pipeline)

        # Verify DAG has three nodes
        nodes = dag.get_all_nodes()
        assert len(nodes) == 3

        # Verify dependencies
        # Find the node IDs
        final_node = None
        customer_node = None
        for node in nodes:
            attrs = dag.get_node_attributes(node)
            if attrs["label"] == "SQL_BLOCK: final_report":
                final_node = node
            elif attrs["label"] == "SQL_BLOCK: customer_data":
                customer_node = node

        # Verify final_report step depends on customer_data step
        assert customer_node in dag.get_predecessors(final_node)

        # There should be no cycles in the DAG
        assert not dag.has_cycles()

    def test_dag_with_nested_conditionals(self):
        """Test DAG with nested conditional blocks."""
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

        # Final step that depends on users table
        final_step = SQLBlockStep(
            table_name="reports", sql_query="SELECT * FROM users", line_number=12
        )

        pipeline.add_step(final_step)

        # Flatten with production + east region
        variables = {"env": "production", "region": "east"}
        builder = ExecutionPlanBuilder()
        flattened_pipeline = builder._flatten_conditional_blocks(pipeline, variables)

        # Build DAG from flattened pipeline
        dag_builder = ASTDAGBuilder()
        dag = dag_builder.build_dag_from_ast(flattened_pipeline)

        # Verify DAG has exactly two nodes
        nodes = dag.get_all_nodes()
        assert len(nodes) == 2

        # Verify node types
        node_types = [dag.get_node_attributes(node)["label"] for node in nodes]
        assert "SQL_BLOCK: users" in node_types
        assert "SQL_BLOCK: reports" in node_types

        # Verify dependency - reports should depend on users
        reports_node = None
        users_node = None

        for node in nodes:
            attrs = dag.get_node_attributes(node)
            if attrs["label"] == "SQL_BLOCK: reports":
                reports_node = node
            elif attrs["label"] == "SQL_BLOCK: users":
                users_node = node

        assert users_node in dag.get_predecessors(reports_node)
