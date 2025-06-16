"""Environment variable management commands with Typer integration.

This module implements Task 2.3: Complete Command Integration for environment
management using Typer framework with Rich display and type safety.
"""

from typing import Dict, Optional

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.cli.display import display_info_panel
from sqlflow.logging import get_logger
from sqlflow.utils.env import find_project_root, get_env_var, list_env_vars

logger = get_logger(__name__)
console = Console()

env_app = typer.Typer(
    name="env",
    help="Manage environment variables and configuration",
)


def _format_plain_output(env_vars: Dict[str, str], show_values: bool) -> None:
    """Format environment variables as plain text for scripting."""
    for name, value in sorted(env_vars.items()):
        if show_values:
            console.print(f"{name}={value}")
        else:
            console.print(name)


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
    title = "🌍 Environment Variables"
    if prefix:
        title += f" (prefix: {prefix})"

    table = Table(title=title, show_header=True, header_style="bold blue")
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
            "\n💡 [dim]Use --show-values to see actual values (be careful with sensitive data)[/dim]"
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
) -> None:
    """List environment variables available to SQLFlow.

    Shows all environment variables or those matching a specific prefix.
    Use --show-values to display actual values (be careful with sensitive data).

    Args:
        prefix: Filter variables by prefix (e.g., 'SQLFLOW_')
        plain: Use plain text output for scripting
        show_values: Show actual variable values (security warning)
    """
    try:
        env_vars = list_env_vars(prefix)

        if not env_vars:
            if prefix:
                console.print(
                    f"❌ [yellow]No environment variables found with prefix '{prefix}'[/yellow]"
                )
            else:
                console.print("❌ [yellow]No environment variables found[/yellow]")
            logger.debug(f"No environment variables found (prefix: {prefix})")
            return

        if plain:
            _format_plain_output(env_vars, show_values)
        else:
            _format_rich_output(env_vars, show_values, prefix)

        logger.debug(f"Listed {len(env_vars)} environment variables")

    except Exception as e:
        logger.error(f"Error listing environment variables: {e}")
        console.print(
            f"❌ [bold red]Failed to list environment variables: {str(e)}[/bold red]"
        )
        raise typer.Exit(1)


@env_app.command("get")
def get_env_variable(
    name: str = typer.Argument(..., help="Environment variable name"),
    default: Optional[str] = typer.Option(
        None, "--default", "-d", help="Default value if variable is not set"
    ),
) -> None:
    """Get the value of a specific environment variable.

    Args:
        name: Environment variable name to retrieve
        default: Default value if variable is not set
    """
    try:
        value = get_env_var(name, default)

        if value is None:
            console.print(f"❌ [red]Environment variable '{name}' is not set[/red]")
            logger.warning(f"Environment variable '{name}' not found")
            raise typer.Exit(1)
        else:
            # Mask sensitive values in output
            display_value = _mask_sensitive_value(name, value)
            if display_value != value:
                console.print(f"🔐 [dim]{name}: {display_value} (masked)[/dim]")
            else:
                console.print(value)

            logger.debug(f"Retrieved environment variable '{name}'")

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error getting environment variable: {e}")
        console.print(
            f"❌ [bold red]Failed to get environment variable: {str(e)}[/bold red]"
        )
        raise typer.Exit(1)


def _count_env_variables(env_file_path) -> int:
    """Count non-empty, non-comment variables in .env file.

    Args:
        env_file_path: Path to the .env file

    Returns:
        Number of valid variables
    """
    var_count = 0
    try:
        with open(env_file_path, "r") as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                var_count += 1
    except Exception:
        pass
    return var_count


def _show_env_variables_preview(env_file_path, var_count: int) -> None:
    """Show preview of variables if count is manageable.

    Args:
        env_file_path: Path to the .env file
        var_count: Number of variables in the file
    """
    if var_count <= 10 and var_count > 0:
        console.print("\n📋 [bold blue]Variables in .env file:[/bold blue]")
        try:
            with open(env_file_path, "r") as f:
                lines = f.readlines()

            for line in lines:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    name = line.split("=", 1)[0]
                    console.print(f"  • [cyan]{name}[/cyan]")
        except Exception:
            pass


def _show_env_file_found(env_file, project_root) -> None:
    """Show information when .env file is found.

    Args:
        env_file: Path to the .env file
        project_root: Project root directory
    """
    var_count = _count_env_variables(env_file)

    console.print("✅ [bold green].env file found[/bold green]")

    # Display info panel
    display_info_panel(
        "Environment Configuration",
        f"File: {env_file}\nVariables: {var_count}\nProject: {project_root}",
        "green",
    )

    # Show sample structure if file is small
    _show_env_variables_preview(env_file, var_count)

    logger.info(f"Environment file found with {var_count} variables")


def _show_env_file_missing(project_root) -> None:
    """Show information when .env file is missing.

    Args:
        project_root: Project root directory
    """
    console.print("❌ [yellow]No .env file found[/yellow]")
    console.print(f"📁 [dim]Project root: {project_root}[/dim]")

    console.print(
        "\n💡 [bold blue]Create a .env file to set environment variables:[/bold blue]"
    )
    console.print("  • [cyan]sqlflow env template[/cyan] - Generate template")
    console.print(
        "  • [cyan]echo 'DATABASE_URL=your_url' > .env[/cyan] - Manual creation"
    )

    logger.info("No .env file found in project")


@env_app.command("check")
def check_env_file() -> None:
    """Check if a .env file exists in the SQLFlow project and show its status.

    Displays information about the project's .env file including variable count
    and project structure.
    """
    try:
        project_root = find_project_root()

        if project_root is None:
            console.print("❌ [red]No SQLFlow project found[/red]")
            console.print(
                "💡 [dim]Make sure you're in a SQLFlow project directory[/dim]"
            )
            logger.warning("No SQLFlow project found when checking .env file")
            raise typer.Exit(1)

        env_file = project_root / ".env"

        if env_file.exists():
            try:
                _show_env_file_found(env_file, project_root)
            except Exception as e:
                console.print(f"❌ [red]Error reading .env file: {e}[/red]")
                logger.error(f"Error reading .env file: {e}")
                raise typer.Exit(1)
        else:
            _show_env_file_missing(project_root)

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error checking environment file: {e}")
        console.print(f"❌ [bold red]Failed to check environment: {str(e)}[/bold red]")
        raise typer.Exit(1)


@env_app.command("template")
def create_env_template() -> None:
    """Create a sample .env file template in the current SQLFlow project.

    Generates a comprehensive .env template with common SQLFlow configuration
    variables and helpful comments.
    """
    try:
        project_root = find_project_root()

        if project_root is None:
            console.print("❌ [red]No SQLFlow project found[/red]")
            console.print(
                "💡 [dim]Make sure you're in a SQLFlow project directory[/dim]"
            )
            logger.warning("No SQLFlow project found when creating template")
            raise typer.Exit(1)

        env_file = project_root / ".env"

        if env_file.exists():
            if not typer.confirm(f".env file already exists at {env_file}. Overwrite?"):
                console.print("⏹️  [yellow]Operation cancelled[/yellow]")
                return

        template_content = """# SQLFlow Environment Variables
# This file contains environment variables for your SQLFlow project
# Variables defined here will be automatically available in pipelines using ${VARIABLE_NAME} syntax

# Profile and execution settings
SQLFLOW_PROFILE=dev
SQLFLOW_LOG_LEVEL=INFO

# Database connections
# DATABASE_URL=postgresql://user:password@localhost/dbname
# POSTGRES_HOST=localhost
# POSTGRES_USER=myuser
# POSTGRES_PASSWORD=mypassword
# MYSQL_HOST=localhost
# MYSQL_USER=myuser
# MYSQL_PASSWORD=mypassword

# Cloud data warehouses
# SNOWFLAKE_ACCOUNT=your_account
# SNOWFLAKE_USER=your_user
# SNOWFLAKE_PASSWORD=your_password
# SNOWFLAKE_WAREHOUSE=your_warehouse
# SNOWFLAKE_DATABASE=your_database
# SNOWFLAKE_SCHEMA=your_schema

# API keys and tokens
# API_KEY=your_api_key_here
# AUTH_TOKEN=your_auth_token_here

# S3/Cloud storage
# AWS_ACCESS_KEY_ID=your_access_key
# AWS_SECRET_ACCESS_KEY=your_secret_key
# AWS_REGION=us-west-2
# S3_BUCKET=your_bucket_name

# Google Cloud
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
# GCS_BUCKET=your_gcs_bucket

# Pipeline variables
# ENVIRONMENT=development
# DEBUG=true
# DATA_SOURCE_PATH=/path/to/data
# OUTPUT_PATH=/path/to/output
"""

        try:
            with open(env_file, "w") as f:
                f.write(template_content)

            console.print(
                "✅ [bold green].env template created successfully![/bold green]"
            )

            display_info_panel(
                "Environment Template Created",
                f"Location: {env_file}\nNext: Edit the file and uncomment/set variables as needed",
                "green",
            )

            console.print("\n💡 [bold blue]Next steps:[/bold blue]")
            console.print(f"  1. [cyan]edit {env_file}[/cyan] - Customize variables")
            console.print("  2. [cyan]sqlflow env check[/cyan] - Verify configuration")
            console.print("  3. [cyan]sqlflow env list[/cyan] - View loaded variables")

            logger.info(f"Created .env template at {env_file}")

        except Exception as e:
            console.print(f"❌ [red]Error creating .env template: {e}[/red]")
            logger.error(f"Error creating .env template: {e}")
            raise typer.Exit(1)

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error creating environment template: {e}")
        console.print(f"❌ [bold red]Failed to create template: {str(e)}[/bold red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    env_app()
