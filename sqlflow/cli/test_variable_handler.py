#!/usr/bin/env python3

from sqlflow.cli.variable_handler import VariableHandler


def test_variable_handling():
    # Test with provided variables
    handler = VariableHandler({"date": "2025-05-16", "name": "test"})

    text = """
    SET date = '${date}';
    SOURCE sample TYPE CSV PARAMS {
        "path": "data/sample_${date}.csv",
        "has_header": true
    };
    SET name = '${name|default_name}';
    """

    # Test validation
    print("Testing variable validation...")
    if handler.validate_variable_usage(text):
        print("✓ Variable validation passed")
    else:
        print("✗ Variable validation failed")

    # Test substitution
    print("\nTesting variable substitution...")
    result = handler.substitute_variables(text)
    print("Results:")
    print(result)

    # Test with missing variables
    print("\nTesting with missing variables...")
    handler = VariableHandler({"date": "2025-05-16"})
    if handler.validate_variable_usage(text):
        print("✗ Should have failed validation")
    else:
        print("✓ Correctly detected missing variable")


if __name__ == "__main__":
    test_variable_handling()
