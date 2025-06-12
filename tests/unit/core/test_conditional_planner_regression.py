"""Regression tests for conditional planner bugs.

This module tests specific bugs found in the conditional planner where
certain operations are missing from the execution plan.
"""

from sqlflow.core.planner_main import OperationPlanner
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    LoadStep,
    Pipeline,
    SourceDefinitionStep,
    SQLBlockStep,
)
from sqlflow.parser.parser import Parser


class TestConditionalPlannerRegression:
    """Test cases for conditional planner regression bugs."""

    def test_conditional_load_operations_included_in_execution_plan_real_parser(self):
        """Test that LOAD operations within conditional blocks are included in execution plan.

        This test uses the real parser to reproduce the exact bug scenario where
        LOAD operations in conditional blocks are missing from the execution plan.

        Bug reproduction: Parse the exact same pipeline structure that causes the bug
        and verify that all LOAD operations are included in the execution plan.
        """
        # Arrange - Use the exact pipeline structure that reproduces the bug
        pipeline_content = """
-- Define data sources
SOURCE customers_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

SOURCE sales_source TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

-- Conditional loading based on environment
IF ${env} == 'production' THEN
    LOAD customers FROM customers_source;
    LOAD sales FROM sales_source;
    
    CREATE TABLE processed_customers AS
    SELECT * FROM customers WHERE account_type = 'premium';
    
ELSE
    -- In development, load raw tables first
    LOAD customers_raw FROM customers_source;
    LOAD sales_raw FROM sales_source;
    
    -- Then create processed tables from raw
    CREATE TABLE customers AS
    SELECT * FROM customers_raw LIMIT 5;
    
    CREATE TABLE sales AS
    SELECT * FROM sales_raw LIMIT 10;  -- This references sales_raw
    
END IF;

-- Export results
EXPORT SELECT * FROM customers
TO "output/conditional_validation_customers.csv"
TYPE CSV
OPTIONS {"header": true};
"""

        # Parse using the real parser (this is key to reproducing the bug)
        parser = Parser(pipeline_content)
        pipeline = parser.parse()

        # Act - Plan with development environment (triggers the bug)
        planner = OperationPlanner()
        variables = {"env": "dev"}
        execution_plan = planner.plan(pipeline, variables)

        # Debug: print what we actually got
        print("\\nDEBUG: Actual execution plan operations:")
        for op in execution_plan:
            print(
                f"  {op['type']}: {op.get('id', 'N/A')} -> {op.get('name', op.get('target_table', 'N/A'))}"
            )
        print(f"Total operations: {len(execution_plan)}")
        load_ops = [op for op in execution_plan if op["type"] == "load"]
        print(f"Load operations: {len(load_ops)}")
        for op in load_ops:
            print(
                f"  {op['id']}: source={op.get('source_name')} -> target={op.get('target_table')}"
            )

        # Assert - Check that all expected operations are in the plan
        operation_types = {op["type"] for op in execution_plan}
        [op["id"] for op in execution_plan]

        # Should have source definitions
        assert "source_definition" in operation_types
        source_ops = [op for op in execution_plan if op["type"] == "source_definition"]
        assert len(source_ops) == 2

        # Should have load operations (THIS IS THE BUG - these are missing)
        assert (
            "load" in operation_types
        ), "LOAD operations are missing from execution plan"
        load_ops = [op for op in execution_plan if op["type"] == "load"]
        assert len(load_ops) == 2, f"Expected 2 load operations, got {len(load_ops)}"

        # Verify specific load operations exist
        load_customers_raw_exists = any(
            op.get("target_table") == "customers_raw" for op in load_ops
        )
        load_sales_raw_exists = any(
            op.get("target_table") == "sales_raw" for op in load_ops
        )

        assert (
            load_customers_raw_exists
        ), "load_customers_raw operation missing from plan"
        assert load_sales_raw_exists, "load_sales_raw operation missing from plan"

        # Should have transform operations
        assert "transform" in operation_types
        transform_ops = [op for op in execution_plan if op["type"] == "transform"]
        assert len(transform_ops) == 2

        # Verify transform operations have correct dependencies
        sales_transform = next(
            (op for op in transform_ops if op["name"] == "sales"), None
        )
        assert sales_transform is not None, "sales transform operation missing"

        # The sales transform should depend on the sales_raw load operation
        load_sales_raw_id = next(
            (op["id"] for op in load_ops if op.get("target_table") == "sales_raw"), None
        )
        assert load_sales_raw_id is not None, "load_sales_raw operation ID not found"

        assert (
            load_sales_raw_id in sales_transform["depends_on"]
        ), f"sales transform should depend on {load_sales_raw_id}, but depends_on: {sales_transform['depends_on']}"

    def test_conditional_load_operations_included_in_execution_plan(self):
        """Test that LOAD operations within conditional blocks are included in execution plan.

        Regression test for bug where LOAD operations in conditional blocks
        were missing from the execution plan, causing runtime failures when
        transform operations tried to reference the missing tables.

        Bug scenario:
        - Pipeline has conditional block with LOAD operations
        - Planner generates execution plan
        - LOAD operations are missing from plan
        - Transform operations reference non-existent tables
        """
        # Arrange - Create pipeline matching the conditional_table_validation.sf structure
        pipeline = Pipeline()

        # Add source definitions
        customers_source = SourceDefinitionStep(
            name="customers_source",
            connector_type="CSV",
            params={"path": "data/customers.csv", "has_header": True},
            line_number=1,
        )
        pipeline.add_step(customers_source)

        sales_source = SourceDefinitionStep(
            name="sales_source",
            connector_type="CSV",
            params={"path": "data/sales.csv", "has_header": True},
            line_number=2,
        )
        pipeline.add_step(sales_source)

        # Create conditional block that matches the bug scenario
        # Production branch
        prod_load_customers = LoadStep(
            table_name="customers",
            source_name="customers_source",
            line_number=10,
        )

        prod_load_sales = LoadStep(
            table_name="sales",
            source_name="sales_source",
            line_number=11,
        )

        prod_transform = SQLBlockStep(
            table_name="processed_customers",
            sql_query="SELECT * FROM customers WHERE account_type = 'premium'",
            line_number=12,
        )

        # Development branch (the problematic one)
        dev_load_customers = LoadStep(
            table_name="customers_raw",
            source_name="customers_source",
            line_number=20,
        )

        dev_load_sales = LoadStep(
            table_name="sales_raw",
            source_name="sales_source",
            line_number=21,
        )

        dev_transform_customers = SQLBlockStep(
            table_name="customers",
            sql_query="SELECT * FROM customers_raw LIMIT 5",
            line_number=22,
        )

        dev_transform_sales = SQLBlockStep(
            table_name="sales",
            sql_query="SELECT * FROM sales_raw LIMIT 10",  # This references sales_raw
            line_number=23,
        )

        # Create conditional branches
        prod_branch = ConditionalBranchStep(
            condition="${env} == 'production'",
            steps=[prod_load_customers, prod_load_sales, prod_transform],
            line_number=8,
        )

        dev_branch = ConditionalBranchStep(
            condition="${env} != 'production'",  # This will be the active branch
            steps=[
                dev_load_customers,
                dev_load_sales,
                dev_transform_customers,
                dev_transform_sales,
            ],
            line_number=18,
        )

        conditional_block = ConditionalBlockStep(
            branches=[prod_branch, dev_branch],
            else_branch=None,
            line_number=7,
        )
        pipeline.add_step(conditional_block)

        # Act - Plan with development environment (triggers the bug)
        planner = OperationPlanner()
        variables = {"env": "dev"}
        execution_plan = planner.plan(pipeline, variables)

        # Debug: print what we actually got
        print("\\nDEBUG: Actual execution plan operations:")
        for op in execution_plan:
            print(
                f"  {op['type']}: {op.get('id', 'N/A')} -> {op.get('name', op.get('target_table', 'N/A'))}"
            )
        print(f"Total operations: {len(execution_plan)}")
        load_ops = [op for op in execution_plan if op["type"] == "load"]
        print(f"Load operations: {len(load_ops)}")
        for op in load_ops:
            print(
                f"  {op['id']}: source={op.get('source_name')} -> target={op.get('target_table')}"
            )

        # Assert - Check that all expected operations are in the plan
        operation_types = {op["type"] for op in execution_plan}
        [op["id"] for op in execution_plan]

        # Should have source definitions
        assert "source_definition" in operation_types
        source_ops = [op for op in execution_plan if op["type"] == "source_definition"]
        assert len(source_ops) == 2

        # Should have load operations (THIS IS THE BUG - these are missing)
        assert (
            "load" in operation_types
        ), "LOAD operations are missing from execution plan"
        load_ops = [op for op in execution_plan if op["type"] == "load"]
        assert len(load_ops) == 2, f"Expected 2 load operations, got {len(load_ops)}"

        # Verify specific load operations exist
        load_customers_raw_exists = any(
            op.get("target_table") == "customers_raw" for op in load_ops
        )
        load_sales_raw_exists = any(
            op.get("target_table") == "sales_raw" for op in load_ops
        )

        assert (
            load_customers_raw_exists
        ), "load_customers_raw operation missing from plan"
        assert load_sales_raw_exists, "load_sales_raw operation missing from plan"

        # Should have transform operations
        assert "transform" in operation_types
        transform_ops = [op for op in execution_plan if op["type"] == "transform"]
        assert len(transform_ops) == 2

        # Verify transform operations have correct dependencies
        sales_transform = next(
            (op for op in transform_ops if op["name"] == "sales"), None
        )
        assert sales_transform is not None, "sales transform operation missing"

        # The sales transform should depend on the sales_raw load operation
        load_sales_raw_id = next(
            (op["id"] for op in load_ops if op.get("target_table") == "sales_raw"), None
        )
        assert load_sales_raw_id is not None, "load_sales_raw operation ID not found"

        assert (
            load_sales_raw_id in sales_transform["depends_on"]
        ), f"sales transform should depend on {load_sales_raw_id}, but depends_on: {sales_transform['depends_on']}"

    def test_conditional_planning_with_nested_conditionals(self):
        """Test that nested conditionals also include all LOAD operations.

        This tests a more complex scenario to ensure the fix works with
        nested conditional structures.
        """
        # Arrange - Create pipeline with nested conditionals
        pipeline = Pipeline()

        source_step = SourceDefinitionStep(
            name="data_source",
            connector_type="CSV",
            params={"path": "data.csv", "has_header": True},
        )
        pipeline.add_step(source_step)

        # Create nested conditional structure
        inner_load = LoadStep(
            table_name="raw_data",
            source_name="data_source",
            line_number=10,
        )

        inner_transform = SQLBlockStep(
            table_name="processed_data",
            sql_query="SELECT * FROM raw_data WHERE active = true",
            line_number=11,
        )

        # Nested inner condition
        inner_branch = ConditionalBranchStep(
            condition="${use_sample} == 'true'",
            steps=[inner_load, inner_transform],
            line_number=9,
        )

        inner_conditional = ConditionalBlockStep(
            branches=[inner_branch],
            else_branch=[],
            line_number=8,
        )

        # Outer condition
        outer_branch = ConditionalBranchStep(
            condition="${env} == 'dev'",
            steps=[inner_conditional],
            line_number=7,
        )

        outer_conditional = ConditionalBlockStep(
            branches=[outer_branch],
            else_branch=[],
            line_number=6,
        )

        pipeline.add_step(outer_conditional)

        # Act - Plan with variables that activate nested path
        planner = OperationPlanner()
        variables = {"env": "dev", "use_sample": "true"}
        execution_plan = planner.plan(pipeline, variables)

        # Assert - Verify nested LOAD operations are included
        load_ops = [op for op in execution_plan if op["type"] == "load"]
        assert len(load_ops) == 1, "Nested LOAD operation should be included in plan"
        assert load_ops[0]["target_table"] == "raw_data"

        transform_ops = [op for op in execution_plan if op["type"] == "transform"]
        assert len(transform_ops) == 1, "Nested transform operation should be included"

        # Verify dependency
        assert (
            load_ops[0]["id"] in transform_ops[0]["depends_on"]
        ), "Transform should depend on the nested load operation"
