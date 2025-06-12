#!/usr/bin/env python3
"""Test script for variable validation."""

import sys

sys.path.append(".")

from sqlflow.core.planner_main import Planner, PlanningError
from sqlflow.parser.parser import Parser


def test_empty_value():
    """Test that empty variable values are detected as invalid."""
    sql = """
    SET important_var = "${important_var|default_value}";
    
    CREATE TABLE result AS
    SELECT * FROM source WHERE region = ${important_var};
    """

    parser = Parser(sql)
    pipeline = parser.parse()
    planner = Planner()

    print("Testing with default value...")
    try:
        plan = planner.create_plan(pipeline)
        print("✅ Test passed with default value")
    except Exception as e:
        print(f"❌ Test failed with default value: {e}")
        return

    print("Testing with explicit value...")
    try:
        plan = planner.create_plan(pipeline, {"important_var": "value"})
        print("✅ Test passed with explicit value")
    except Exception as e:
        print(f"❌ Test failed with explicit value: {e}")
        return

    print("Testing with empty value (should fail)...")
    try:
        planner.create_plan(pipeline, {"important_var": ""})
        print("❌ Test failed: empty value was accepted")
    except PlanningError as e:
        if "Invalid variable values detected" in str(
            e
        ) and "${important_var} has an empty value" in str(e):
            print("✅ Test passed: empty value was rejected")
        else:
            print(f"❌ Test failed with unexpected error: {e}")
    except Exception as e:
        print(f"❌ Test failed with unexpected error: {e}")


def test_self_referential():
    """Test that self-referential variables work correctly."""
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

    print("Testing with default value...")
    try:
        plan = planner.create_plan(pipeline)
        print("✅ Test passed with default value")
    except Exception as e:
        print(f"❌ Test failed with default value: {e}")
        return

    print("Testing with explicit 'true'...")
    try:
        plan = planner.create_plan(pipeline, {"use_csv": "true"})
        print("✅ Test passed with explicit 'true'")
    except Exception as e:
        print(f"❌ Test failed with explicit 'true': {e}")
        return

    print("Testing with 'false'...")
    try:
        plan = planner.create_plan(pipeline, {"use_csv": "false"})
        print("✅ Test passed with 'false'")
    except Exception as e:
        print(f"❌ Test failed with 'false': {e}")
        return


if __name__ == "__main__":
    print("=== Testing Variable Empty Value Validation ===")
    test_empty_value()
    print("\n=== Testing Self-Referential Variable Validation ===")
    test_self_referential()
    print("\n✨ All tests completed!")
