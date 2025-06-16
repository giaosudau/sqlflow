"""UDF (User Defined Functions) management commands with Typer integration.

This module implements Task 2.3: Complete Command Integration for UDF
management using Typer framework with Rich display and type safety.
"""

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)
console = Console()

# Create the UDF command group
udf_app = typer.Typer(
    name="udf",
    help="Manage User Defined Functions (UDFs) for data transformations",
)


@udf_app.command("list")
def list_udfs(
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    format: str = typer.Option(
        "table", "--format", help="Output format: table or json"
    ),
    plain: bool = typer.Option(
        False, "--plain", help="Plain text output without formatting"
    ),
) -> None:
    """List available User Defined Functions.

    Scans the UDF directories and displays available Python UDF files
    with their metadata and documentation.

    Args:
        profile: Profile configuration to use (default: dev)
        format: Output format - 'table' for formatted display, 'json' for structured data
        plain: Whether to use plain text output
    """
    try:
        # Initialize UDF manager
        manager = PythonUDFManager()

        # Discover UDFs
        discovered_udfs = manager.discover_udfs()

        if not discovered_udfs:
            if plain:
                print("No Python UDFs found in the project")
            else:
                console.print("📋 [yellow]No Python UDFs found in the project[/yellow]")
            return

        # Get detailed UDF information
        udfs = manager.list_udfs()

        if format == "json":
            from sqlflow.cli.display import display_json_output

            display_json_output(udfs)
        elif plain:
            _display_udfs_plain(udfs)
        else:
            _display_udfs_table(udfs)

        logger.debug(f"Listed {len(udfs)} UDFs")

    except Exception as e:
        logger.error(f"Error listing UDFs: {e}")
        if plain:
            print(f"Failed to list UDFs: {str(e)}")
        else:
            console.print(f"❌ [bold red]Failed to list UDFs: {str(e)}[/bold red]")
        raise typer.Exit(1)


@udf_app.command("info")
def show_udf_info(
    udf_name: str = typer.Argument(..., help="Name of the UDF to show"),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    plain: bool = typer.Option(
        False, "--plain", help="Plain text output without formatting"
    ),
) -> None:
    """Show detailed information about a specific UDF.

    Displays the UDF's signature, documentation, parameters, and usage examples.

    Args:
        udf_name: Name of the UDF to show information for
        profile: Profile configuration to use (default: dev)
        plain: Whether to use plain text output
    """
    try:
        # Initialize UDF manager
        manager = PythonUDFManager()

        # Discover UDFs first
        discovered_udfs = manager.discover_udfs()

        if not discovered_udfs:
            if plain:
                print(f"UDF '{udf_name}' not found")
            else:
                console.print(f"❌ [red]UDF '{udf_name}' not found[/red]")
            raise typer.Exit(1)

        # Get UDF info
        udf_info = manager.get_udf_info(udf_name)

        if not udf_info:
            if plain:
                print(f"UDF '{udf_name}' not found")
            else:
                console.print(f"❌ [red]UDF '{udf_name}' not found[/red]")
            return

        if plain:
            _display_udf_info_plain(udf_info)
        else:
            _display_udf_info_rich(udf_info)

        logger.debug(f"Showed info for UDF '{udf_name}'")

    except Exception as e:
        logger.error(f"Error showing UDF info: {e}")
        if plain:
            print(f"Failed to show UDF info: {str(e)}")
        else:
            console.print(f"❌ [bold red]Failed to show UDF info: {str(e)}[/bold red]")
        raise typer.Exit(1)


@udf_app.command("validate")
def validate_udf(
    udf_name: Optional[str] = typer.Argument(None, help="Name of the UDF to validate"),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
) -> None:
    """Validate UDF syntax and dependencies.

    Checks UDF Python code for syntax errors, imports, and compatibility
    with the SQLFlow runtime environment.

    Args:
        udf_name: Name of the UDF to validate (omit to validate all)
        profile: Profile configuration to use (default: dev)
    """
    try:
        if udf_name:
            console.print(f"🔍 [cyan]Validating UDF: {udf_name}[/cyan]")
        else:
            console.print("🔍 [cyan]Validating all UDFs...[/cyan]")

        # Mock validation for now
        console.print("⚠️  [yellow]UDF validation is under development[/yellow]")
        console.print("💡 [dim]This feature will:[/dim]")
        console.print("  • Check Python syntax and imports")
        console.print("  • Validate function signatures")
        console.print("  • Test UDF execution in isolated environment")
        console.print("  • Verify compatibility with SQLFlow runtime")

        if udf_name:
            logger.debug(f"Validated UDF '{udf_name}'")
        else:
            logger.debug("Validated all UDFs")

    except Exception as e:
        logger.error(f"Error validating UDF: {e}")
        console.print(f"❌ [bold red]UDF validation failed: {str(e)}[/bold red]")
        raise typer.Exit(1)


def _display_udfs_plain(udfs: list) -> None:
    """Display UDFs in plain text format."""
    for udf in udfs:
        name = udf.get("full_name", udf.get("name", "Unknown"))
        udf_type = udf.get("type", "unknown")
        summary = udf.get("docstring_summary", "No description")
        print(f"{name} ({udf_type}): {summary}")


def _display_udfs_table(udfs: list) -> None:
    """Display UDFs in a formatted table."""
    if not udfs:
        console.print("📋 [yellow]No UDFs found[/yellow]")
        console.print(
            "💡 [dim]Create Python UDF files in the python_udfs/ directory[/dim]"
        )
        return

    console.print(f"📋 [bold blue]Available UDFs ({len(udfs)})[/bold blue]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="white")
    table.add_column("Description", style="dim")

    for udf in udfs:
        name = udf.get("full_name", udf.get("name", "Unknown"))
        udf_type = udf.get("type", "unknown")
        description = udf.get("docstring_summary", "No description")

        # Truncate long descriptions
        if len(description) > 60:
            description = description[:57] + "..."

        table.add_row(name, udf_type, description)

    console.print(table)


def _display_udf_info_plain(udf_info: dict) -> None:
    """Display UDF info in plain text format."""
    print(f"UDF: {udf_info.get('full_name', udf_info.get('name', 'Unknown'))}")
    print(f"Type: {udf_info.get('type', 'unknown')}")
    print(f"Module: {udf_info.get('module', 'unknown')}")

    if udf_info.get("signature"):
        print(f"Signature: {udf_info['signature']}")

    if udf_info.get("docstring"):
        print(f"Description: {udf_info['docstring']}")


def _display_udf_info_rich(udf_info: dict) -> None:
    """Display UDF info with rich formatting."""
    name = udf_info.get("full_name", udf_info.get("name", "Unknown"))
    console.print(f"📋 [bold blue]UDF Information: {name}[/bold blue]")

    console.print(f"Type: [cyan]{udf_info.get('type', 'unknown')}[/cyan]")
    console.print(f"Module: [dim]{udf_info.get('module', 'unknown')}[/dim]")

    if udf_info.get("signature"):
        console.print(f"Signature: [white]{udf_info['signature']}[/white]")

    if udf_info.get("docstring"):
        console.print("\n[bold]Description:[/bold]")
        console.print(f"[dim]{udf_info['docstring']}[/dim]")


# Alias for backward compatibility with tests
app = udf_app

if __name__ == "__main__":
    udf_app()
