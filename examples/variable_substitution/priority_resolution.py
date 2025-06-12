#!/usr/bin/env python3
"""Variable Priority Resolution Examples

Demonstrates how SQLFlow resolves variables from multiple sources with priority.
Priority order: CLI > Profile > SET > Environment
"""

from sqlflow.core.variables.manager import VariableConfig, VariableManager


def priority_resolution_example():
    """Demonstrate complete priority resolution."""
    print("=== Variable Priority Resolution ===")
    print("Priority order: CLI > Profile > SET > Environment\n")

    # Set up variables from all sources
    cli_vars = {"environment": "cli_value", "source": "cli"}
    profile_vars = {
        "environment": "profile_value",
        "schema": "profile_schema",
        "source": "profile",
    }
    set_vars = {"environment": "set_value", "table": "set_table", "source": "set"}
    env_vars = {"environment": "env_value", "region": "env_region", "source": "env"}

    config = VariableConfig(
        cli_variables=cli_vars,
        profile_variables=profile_vars,
        set_variables=set_vars,
        env_variables=env_vars,
    )

    manager = VariableManager(config)

    print("Variable sources:")
    print(f"  CLI:     {cli_vars}")
    print(f"  Profile: {profile_vars}")
    print(f"  SET:     {set_vars}")
    print(f"  ENV:     {env_vars}")
    print()

    # Test priority resolution
    template = "Environment: ${environment}, Schema: ${schema}, Table: ${table}, Region: ${region}"
    result = manager.substitute(template)

    print(f"Template: {template}")
    print(f"Result:   {result}")
    print()
    print("Explanation:")
    print("  - environment: 'cli_value' (CLI overrides all)")
    print("  - schema: 'profile_schema' (only in Profile)")
    print("  - table: 'set_table' (only in SET)")
    print("  - region: 'env_region' (only in ENV)")
    print()


def override_demonstration():
    """Show how higher priority variables override lower ones."""
    print("=== Override Demonstration ===")

    # Same variable defined in multiple sources
    sources = [
        ("Environment only", {"env_variables": {"database": "env_db"}}),
        (
            "SET overrides ENV",
            {
                "env_variables": {"database": "env_db"},
                "set_variables": {"database": "set_db"},
            },
        ),
        (
            "Profile overrides SET + ENV",
            {
                "env_variables": {"database": "env_db"},
                "set_variables": {"database": "set_db"},
                "profile_variables": {"database": "profile_db"},
            },
        ),
        (
            "CLI overrides all",
            {
                "env_variables": {"database": "env_db"},
                "set_variables": {"database": "set_db"},
                "profile_variables": {"database": "profile_db"},
                "cli_variables": {"database": "cli_db"},
            },
        ),
    ]

    template = "Database: ${database}"

    for description, var_sources in sources:
        config = VariableConfig(**var_sources)
        manager = VariableManager(config)
        result = manager.substitute(template)

        print(f"{description:25} -> {result}")

    print()


def practical_example():
    """Practical example with environment-specific configuration."""
    print("=== Practical Example ===")

    # Simulate different environments
    environments = {
        "development": {
            "cli_variables": {},  # No CLI overrides
            "profile_variables": {
                "environment": "dev",
                "database_host": "localhost",
                "debug_mode": "true",
            },
            "set_variables": {"batch_size": "100"},
            "env_variables": {"API_KEY": "dev_key_123"},
        },
        "production": {
            "cli_variables": {"debug_mode": "false"},  # Override for prod
            "profile_variables": {
                "environment": "prod",
                "database_host": "prod-db.company.com",
                "debug_mode": "true",  # Will be overridden by CLI
            },
            "set_variables": {"batch_size": "10000"},
            "env_variables": {"API_KEY": "prod_key_xyz"},
        },
    }

    template = """
Configuration:
  Environment: ${environment}
  Database: ${database_host}
  Debug Mode: ${debug_mode}
  Batch Size: ${batch_size}
  API Key: ${API_KEY}
    """.strip()

    for env_name, var_sources in environments.items():
        print(f"\n{env_name.upper()} Environment:")
        print("-" * 30)

        config = VariableConfig(**var_sources)
        manager = VariableManager(config)
        result = manager.substitute(template)

        print(result)

        if env_name == "production":
            print("\nNote: debug_mode='false' from CLI overrides profile value 'true'")


def resolution_details():
    """Show detailed resolution information."""
    print("\n=== Resolution Details ===")

    config = VariableConfig(
        cli_variables={"shared": "cli", "cli_only": "cli_value"},
        profile_variables={"shared": "profile", "profile_only": "profile_value"},
        set_variables={"shared": "set", "set_only": "set_value"},
        env_variables={"shared": "env", "env_only": "env_value"},
    )

    manager = VariableManager(config)

    # Show all resolved variables
    resolved = manager.get_resolved_variables()

    print("All resolved variables:")
    for key, value in sorted(resolved.items()):
        print(f"  {key}: {value}")

    print(f"\nTotal variables available: {len(resolved)}")

    # Test which variables are available
    test_vars = [
        "shared",
        "cli_only",
        "profile_only",
        "set_only",
        "env_only",
        "missing",
    ]
    print("\nVariable availability:")
    for var in test_vars:
        has_var = manager.has_variable(var)
        status = "✓" if has_var else "✗"
        print(f"  {status} {var}")


def main():
    """Run all priority resolution examples."""
    print("SQLFlow Variable Priority Resolution Examples")
    print("=" * 60)
    print()

    priority_resolution_example()
    override_demonstration()
    practical_example()
    resolution_details()

    print("\n" + "=" * 60)
    print("Key Takeaways:")
    print("1. CLI variables always win (highest priority)")
    print("2. Each source can contribute unique variables")
    print("3. Priority only matters when same variable exists in multiple sources")
    print("4. Use CLI for runtime overrides, profiles for environment defaults")


if __name__ == "__main__":
    main()
