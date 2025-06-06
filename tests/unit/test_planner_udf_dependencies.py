"""Tests for table UDF dependency detection in planner."""

from sqlflow.core.planner import ExecutionPlanBuilder
from sqlflow.parser.ast import Pipeline, SQLBlockStep


def test_extract_udf_table_dependencies():
    """Test that the planner correctly extracts table references from UDF calls."""
    planner = ExecutionPlanBuilder()

    # Test standard SQL
    standard_sql = (
        "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id"
    )
    tables = planner._extract_referenced_tables(standard_sql)
    assert sorted(tables) == ["customers", "orders"]

    # Test UDF with table reference
    udf_sql = 'SELECT * FROM PYTHON_FUNC("module.function", input_table)'
    tables = planner._extract_referenced_tables(udf_sql)
    assert "input_table" in tables

    # Test mixed SQL with UDFs
    mixed_sql = """
    SELECT c.name, o.amount, PYTHON_FUNC("utils.calculate", o.amount) 
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE o.id IN (SELECT id FROM PYTHON_FUNC("filter.high_value", transactions))
    """
    tables = planner._extract_referenced_tables(mixed_sql)
    assert sorted(
        [t for t in tables if t in ["customers", "orders", "transactions"]]
    ) == ["customers", "orders", "transactions"]


def test_table_udf_dependency_in_pipeline():
    """Test that the planner correctly builds dependencies for table UDFs in a pipeline."""
    pipeline = Pipeline()

    # Add load step
    load_step = SQLBlockStep(
        table_name="raw_data",
        sql_query="CREATE TABLE raw_data AS SELECT * FROM source_data",
    )
    pipeline.add_step(load_step)

    # Add UDF transform step that references the loaded table
    udf_step = SQLBlockStep(
        table_name="processed_data",
        sql_query='SELECT * FROM PYTHON_FUNC("module.process", raw_data)',
    )
    pipeline.add_step(udf_step)

    # Build dependency graph
    planner = ExecutionPlanBuilder()
    planner._build_table_to_step_mapping(pipeline)
    planner._build_dependency_graph(pipeline)

    # Get step IDs using the planner's step ID generation
    load_step_id = planner._generate_step_id(load_step, 0)
    udf_step_id = planner._generate_step_id(udf_step, 1)

    # Check that UDF step depends on load step in the step_dependencies dict
    udf_dependencies = planner.step_dependencies.get(udf_step_id, [])
    assert load_step_id in udf_dependencies
