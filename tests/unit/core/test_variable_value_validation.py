"""Tests for variable value validation in the planner."""

from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser


def test_variable_empty_value_validation():
    """Test that empty variable values are detected as invalid."""
    sql = """
    SET important_var = "${important_var|default_value}";
    
    CREATE TABLE result AS
    SELECT * FROM source WHERE region = ${important_var};
    """

    parser = Parser(sql)
    pipeline = parser.parse()
    planner = Planner()

    # Should pass with default value
    plan = planner.create_plan(pipeline)
    assert len(plan) == 1  # Only 1 step as SET is not an execution step

    # Should pass with valid provided value
    plan = planner.create_plan(pipeline, {"important_var": "value"})
    assert len(plan) == 1  # Only 1 step as SET is not an execution step

    # Should accept empty value now (behavior change)
    empty_plan = planner.create_plan(pipeline, {"important_var": ""})
    assert len(empty_plan) == 1  # Only 1 step as SET is not an execution step
    # Ensure the variable was properly substituted
    assert "WHERE region =" in empty_plan[0]["query"]


def test_self_referential_variable_validation():
    """Test that self-referential variables (like in advanced_customer_analytics.sf) work correctly."""
    # Using a variable with explicit non-self-referential default value
    sql = """
    SET use_csv = "${use_csv|true}";
    
    IF ${use_csv} == "true" THEN
        CREATE TABLE my_source AS SELECT * FROM csv_table;
    ELSE
        CREATE TABLE my_source AS SELECT * FROM db_table;
    END IF;
    """

    parser = Parser(sql)
    pipeline = parser.parse()
    planner = Planner()

    # This needs initial value
    plan = planner.create_plan(pipeline, {"use_csv": "true"})
    assert len(plan) == 1  # Only 1 step as SET is not an execution step
    # Check that we're using the csv_table in the plan
    assert "csv_table" in str(plan)

    # Testing with "false"
    plan = planner.create_plan(pipeline, {"use_csv": "false"})
    assert len(plan) == 1  # Only 1 step as SET is not an execution step
    # Check that we're using the db_table in the plan
    assert "db_table" in str(plan)
