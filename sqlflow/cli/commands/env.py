"""CLI commands for environment variable management."""

from typing import Dict, Optional

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.utils.env import find_project_root, get_env_var, list_env_vars

env_app = typer.Typer(
    help="Manage environment variables",
    rich_markup_mode=None,
    pretty_exceptions_enable=False,
)
console = Console()


def _format_plain_output(env_vars: Dict[str, str], show_values: bool) -> None:
    """Format environment variables as plain text for scripting."""
    for name, value in sorted(env_vars.items()):
        if show_values:
            typer.echo(f"{name}={value}")
        else:
            typer.echo(name)


def _mask_sensitive_value(name: str, value: str) -> str:
    """Mask potentially sensitive values."""
    if any(
        sensitive in name.lower()
        for sensitive in ["password", "secret", "key", "token"]
    ):
        return "*" * min(len(value), 8) if value else ""
    return value


def _format_rich_output(
    env_vars: Dict[str, str], show_values: bool, prefix: Optional[str]
) -> None:
    """Format environment variables as a rich table."""
    table = Table(
        title=f"Environment Variables{f' (prefix: {prefix})' if prefix else ''}"
    )
    table.add_column("Name", style="cyan")
    if show_values:
        table.add_column("Value", style="green")
    else:
        table.add_column("Status", style="yellow")

    for name, value in sorted(env_vars.items()):
        if show_values:
            display_value = _mask_sensitive_value(name, value)
            table.add_row(name, display_value)
        else:
            table.add_row(name, "✓ Set" if value else "✗ Empty")

    console.print(table)

    if not show_values:
        console.print(
            "\n💡 Use --show-values to see actual values (be careful with sensitive data)"
        )


@env_app.command("list")
def list_env_variables(
    prefix: Optional[str] = typer.Option(
        None, "--prefix", "-p", help="Filter variables by prefix (e.g., 'SQLFLOW_')"
    ),
    plain: bool = typer.Option(
        False, "--plain", help="Use plain text output (for scripting)"
    ),
    show_values: bool = typer.Option(
        False,
        "--show-values",
        help="Show variable values (security warning: visible in terminal)",
    ),
):
    """List environment variables available to SQLFlow.

    Shows all environment variables or those matching a specific prefix.
    Use --show-values to display actual values (be careful with sensitive data).
    """
    env_vars = list_env_vars(prefix)

    if not env_vars:
        if prefix:
            typer.echo(f"No environment variables found with prefix '{prefix}'")
        else:
            typer.echo("No environment variables found")
        return

    if plain:
        _format_plain_output(env_vars, show_values)
    else:
        _format_rich_output(env_vars, show_values, prefix)


@env_app.command("get")
def get_env_variable(
    name: str = typer.Argument(..., help="Environment variable name"),
    default: Optional[str] = typer.Option(
        None, "--default", "-d", help="Default value if variable is not set"
    ),
):
    """Get the value of a specific environment variable."""
    value = get_env_var(name, default)

    if value is None:
        typer.echo(f"Environment variable '{name}' is not set", err=True)
        raise typer.Exit(code=1)
    else:
        typer.echo(value)


@env_app.command("check")
def check_env_file():
    """Check if a .env file exists in the SQLFlow project and show its status."""
    project_root = find_project_root()

    if project_root is None:
        typer.echo("❌ No SQLFlow project found (no 'profiles' directory found)")
        typer.echo("Make sure you're in a SQLFlow project directory")
        raise typer.Exit(code=1)

    env_file = project_root / ".env"

    if env_file.exists():
        try:
            # Count non-empty, non-comment lines
            with open(env_file, "r") as f:
                lines = f.readlines()

            var_count = 0
            for line in lines:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    var_count += 1

            typer.echo(f"✅ .env file found: {env_file}")
            typer.echo(f"📊 Contains {var_count} variable(s)")
            typer.echo(f"📁 Project root: {project_root}")

            # Show sample structure if file is small
            if var_count <= 10:
                typer.echo("\n📋 Variables in .env file:")
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        name = line.split("=", 1)[0]
                        typer.echo(f"  • {name}")

        except Exception as e:
            typer.echo(f"❌ Error reading .env file: {e}", err=True)
            raise typer.Exit(code=1)
    else:
        typer.echo(f"❌ No .env file found at: {env_file}")
        typer.echo(f"📁 Project root: {project_root}")
        typer.echo(
            "\n💡 Create a .env file to set environment variables for your SQLFlow project:"
        )
        typer.echo("   echo 'DATABASE_URL=your_database_url' > .env")
        typer.echo("   echo 'API_KEY=your_api_key' >> .env")


@env_app.command("template")
def create_env_template():
    """Create a sample .env file template in the current SQLFlow project."""
    project_root = find_project_root()

    if project_root is None:
        typer.echo("❌ No SQLFlow project found (no 'profiles' directory found)")
        typer.echo("Make sure you're in a SQLFlow project directory")
        raise typer.Exit(code=1)

    env_file = project_root / ".env"

    if env_file.exists():
        if not typer.confirm(f".env file already exists at {env_file}. Overwrite?"):
            typer.echo("Operation cancelled")
            return

    template_content = """# SQLFlow Environment Variables
# This file contains environment variables for your SQLFlow project
# Variables defined here will be automatically available in pipelines using ${VARIABLE_NAME} syntax

# Database connections
# DATABASE_URL=postgresql://user:password@localhost/dbname
# POSTGRES_HOST=localhost
# POSTGRES_USER=myuser
# POSTGRES_PASSWORD=mypassword

# API keys and tokens
# API_KEY=your_api_key_here
# AUTH_TOKEN=your_auth_token_here

# S3/Cloud storage
# AWS_ACCESS_KEY_ID=your_access_key
# AWS_SECRET_ACCESS_KEY=your_secret_key
# S3_BUCKET=my-data-bucket

# Environment configuration
# ENVIRONMENT=development
# DEBUG=true
# LOG_LEVEL=info

# Data paths
# DATA_DIR=/path/to/data
# OUTPUT_DIR=/path/to/output

# Custom variables for your pipelines
# BATCH_SIZE=1000
# REGION=us-west-2
# CUSTOMER_ID=12345
"""

    try:
        with open(env_file, "w") as f:
            f.write(template_content)

        typer.echo(f"✅ Created .env template at: {env_file}")
        typer.echo("📝 Edit the file to set your environment variables")
        typer.echo(
            "💡 Variables will be automatically available in SQLFlow pipelines using ${VARIABLE_NAME}"
        )

    except Exception as e:
        typer.echo(f"❌ Error creating .env template: {e}", err=True)
        raise typer.Exit(code=1)
