#!/usr/bin/env python3
"""Basic Variable Substitution Examples

Demonstrates core variable substitution functionality using the new unified system.
Run this file to see variable substitution in action.
"""

from sqlflow.core.variables.manager import VariableConfig, VariableManager


def basic_substitution_example():
    """Demonstrate basic variable substitution."""
    print("=== Basic Variable Substitution ===")

    # Create variables
    variables = {"table_name": "users", "schema": "public", "limit": 100}

    # Create configuration and manager
    config = VariableConfig(cli_variables=variables)
    manager = VariableManager(config)

    # Basic substitution
    sql_template = "SELECT * FROM ${schema}.${table_name} LIMIT ${limit}"
    result = manager.substitute(sql_template)

    print(f"Template: {sql_template}")
    print(f"Result:   {result}")
    print()


def default_values_example():
    """Demonstrate default value functionality."""
    print("=== Default Values ===")

    # Only provide some variables
    variables = {"table_name": "orders"}

    config = VariableConfig(cli_variables=variables)
    manager = VariableManager(config)

    # Template with defaults
    template = (
        "SELECT * FROM ${schema|public}.${table_name} WHERE status = '${status|active}'"
    )
    result = manager.substitute(template)

    print(f"Variables: {variables}")
    print(f"Template:  {template}")
    print(f"Result:    {result}")
    print("Note: 'public' and 'active' are used as defaults")
    print()


def validation_example():
    """Demonstrate variable validation."""
    print("=== Variable Validation ===")

    variables = {"table_name": "products"}
    config = VariableConfig(cli_variables=variables)
    manager = VariableManager(config)

    # Template with missing variable
    template = "SELECT * FROM ${table_name} WHERE category = '${missing_category}'"

    # Validate before substitution
    validation_result = manager.validate(template)

    print(f"Template: {template}")
    print(f"Is valid: {validation_result.is_valid}")
    print(f"Missing variables: {validation_result.missing_variables}")

    # Show substitution result
    result = manager.substitute(template)
    print(f"Result: {result}")
    print("Note: Missing variables are left as placeholders")
    print()


def complex_template_example():
    """Demonstrate complex template with multiple variable types."""
    print("=== Complex Template Example ===")

    variables = {
        "environment": "production",
        "batch_size": 5000,
        "enable_logging": True,
        "table_prefix": "analytics_",
    }

    config = VariableConfig(cli_variables=variables)
    manager = VariableManager(config)

    template = """
    -- Environment: ${environment}
    -- Batch processing with ${batch_size} records
    CREATE TABLE ${table_prefix}daily_summary AS
    SELECT 
        date,
        region,
        COUNT(*) as events
    FROM ${table_prefix}raw_events
    WHERE created_at >= '${start_date|CURRENT_DATE - 7}'
      AND region = '${target_region|US}'
    GROUP BY date, region
    LIMIT ${batch_size};
    """.strip()

    result = manager.substitute(template)

    print("Variables:")
    for key, value in variables.items():
        print(f"  {key}: {value}")
    print()
    print("Result:")
    print(result)
    print()


def environment_variables_example():
    """Demonstrate environment variable integration."""
    print("=== Environment Variables ===")

    import os

    # Set some environment variables for demonstration
    os.environ["DEMO_TABLE"] = "demo_users"
    os.environ["DEMO_SCHEMA"] = "test_schema"

    # Create manager with no explicit variables
    config = VariableConfig()
    manager = VariableManager(config)

    template = (
        "SELECT * FROM ${DEMO_SCHEMA}.${DEMO_TABLE} WHERE active = ${active|true}"
    )
    result = manager.substitute(template)

    print(f"Environment variables set:")
    print(f"  DEMO_TABLE={os.environ.get('DEMO_TABLE')}")
    print(f"  DEMO_SCHEMA={os.environ.get('DEMO_SCHEMA')}")
    print()
    print(f"Template: {template}")
    print(f"Result:   {result}")
    print("Note: Environment variables are automatically available")
    print()


def main():
    """Run all examples."""
    print("SQLFlow Variable Substitution Examples")
    print("=" * 50)
    print()

    basic_substitution_example()
    default_values_example()
    validation_example()
    complex_template_example()
    environment_variables_example()

    print("All examples completed successfully!")
    print("\nTry modifying the variables and running again to see different results.")


if __name__ == "__main__":
    main()
