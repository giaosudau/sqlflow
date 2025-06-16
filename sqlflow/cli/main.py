#!/usr/bin/env python3
"""SQLFlow CLI - Task 2.3: Complete Command Integration

This implements the main Typer CLI application with:
- Type-driven command structure with automatic validation
- Rich formatting for beautiful output and error display
- Integrated subcommands using business operations
- Python-native error handling with helpful suggestions
- Simple factory pattern for dependencies
"""

import os
from typing import Optional

import typer
from rich.console import Console

from sqlflow.cli.business_operations import init_project_operation
from sqlflow.cli.commands.connect import connect_app
from sqlflow.cli.commands.env import env_app
from sqlflow.cli.commands.migrate import migrate_app
from sqlflow.cli.commands.pipeline import pipeline_app
from sqlflow.cli.commands.profiles import profiles_app
from sqlflow.cli.commands.udf import udf_app
from sqlflow.cli.display import (
    display_generic_error,
    display_info_panel,
    display_project_not_found_error,
)
from sqlflow.cli.errors import ProjectNotFoundError
from sqlflow.logging import get_logger

logger = get_logger(__name__)
console = Console()

# Main app with professional appearance
app = typer.Typer(
    name="sqlflow",
    help="SQLFlow CLI - Transform your data with SQL",
    add_completion=True,  # Shell completion support
)

# Add subcommands - clean organization following Typer patterns
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(profiles_app, name="profiles")
app.add_typer(udf_app, name="udf")
app.add_typer(env_app, name="env")
app.add_typer(migrate_app, name="migrate")
app.add_typer(connect_app, name="connect")


@app.callback()
def main(
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output"
    ),
) -> None:
    """SQLFlow CLI - Transform your data with SQL.

    A modern CLI for data transformation using SQL pipelines with support for
    multiple data sources, profiles, and execution environments.

    Examples:
        sqlflow pipeline run my_pipeline --profile prod
        sqlflow profiles list
        sqlflow init my_project
    """
    if version:
        from sqlflow import __version__

        console.print(f"SQLFlow CLI v{__version__}")
        raise typer.Exit()

    # Setup environment with proper error handling
    try:
        _setup_environment(verbose, False)
    except Exception as e:
        if verbose:
            logger.warning(f"Environment setup issue: {e}")


@app.command()
def init(
    project_name: str = typer.Argument(..., help="Name of the project to create"),
    directory: Optional[str] = typer.Option(
        None,
        "--directory",
        "-d",
        help="Directory to create project in (default: project_name)",
    ),
    minimal: bool = typer.Option(
        False, "--minimal", help="Create minimal project structure"
    ),
) -> None:
    """Initialize a new SQLFlow project.

    Creates a new SQLFlow project with the standard directory structure,
    sample configuration files, and example pipelines.

    Args:
        project_name: Name of the project to create
        directory: Directory to create project in (defaults to project_name)
        minimal: Create minimal project structure without examples
    """
    try:
        # Use project name as directory if not specified
        project_dir = directory or project_name

        # Make path absolute
        if not os.path.isabs(project_dir):
            project_dir = os.path.join(os.getcwd(), project_dir)

        # Execute business operation
        created_path = init_project_operation(project_dir, project_name)

        # Display success with Rich formatting
        console.print("✅ [bold green]Project initialized successfully![/bold green]")

        # Show project info
        display_info_panel(
            "Project Created",
            f"Name: {project_name}\nPath: {created_path}\nStructure: {'Minimal' if minimal else 'Standard'}",
            "green",
        )

        # Show next steps
        console.print("\n💡 [bold blue]Next steps:[/bold blue]")
        console.print(f"  1. [cyan]cd {os.path.basename(created_path)}[/cyan]")
        console.print(
            "  2. [cyan]sqlflow profiles list[/cyan] - View available profiles"
        )
        console.print(
            "  3. [cyan]sqlflow pipeline list[/cyan] - View example pipelines"
        )
        console.print(
            "  4. [cyan]sqlflow pipeline run <pipeline_name>[/cyan] - Run a pipeline"
        )

        logger.info(f"Initialized SQLFlow project '{project_name}' in {created_path}")

    except OSError as e:
        display_generic_error(e, "project initialization")
        logger.error(f"Failed to initialize project: {e}")
        raise typer.Exit(1)
    except Exception as e:
        display_generic_error(e, "project initialization")
        logger.error(f"Unexpected error during project initialization: {e}")
        raise typer.Exit(1)


@app.command()
def status() -> None:
    """Show SQLFlow project status and configuration.

    Displays information about the current project, profiles, pipelines,
    and environment configuration.
    """
    try:
        current_dir = os.getcwd()

        # Check if we're in a SQLFlow project
        profiles_dir = os.path.join(current_dir, "profiles")
        pipelines_dir = os.path.join(current_dir, "pipelines")

        if not os.path.exists(profiles_dir):
            error = ProjectNotFoundError(current_dir)
            display_project_not_found_error(error)
            raise typer.Exit(1)

        console.print("📊 [bold blue]SQLFlow Project Status[/bold blue]")
        console.print(f"🗂️  Project Directory: [cyan]{current_dir}[/cyan]")

        # Count profiles
        profile_count = 0
        if os.path.exists(profiles_dir):
            profile_count = len(
                [f for f in os.listdir(profiles_dir) if f.endswith((".yml", ".yaml"))]
            )

        # Count pipelines
        pipeline_count = 0
        if os.path.exists(pipelines_dir):
            pipeline_count = len(
                [f for f in os.listdir(pipelines_dir) if f.endswith(".sf")]
            )

        console.print(f"📋 Profiles: [cyan]{profile_count}[/cyan]")
        console.print(f"🔄 Pipelines: [cyan]{pipeline_count}[/cyan]")

        # Show suggested commands
        console.print("\n💡 [bold blue]Quick actions:[/bold blue]")
        console.print(
            "  • [cyan]sqlflow profiles list[/cyan] - View available profiles"
        )
        console.print(
            "  • [cyan]sqlflow pipeline list[/cyan] - View available pipelines"
        )
        console.print("  • [cyan]sqlflow env check[/cyan] - Check environment setup")

        logger.debug(
            f"Project status: {profile_count} profiles, {pipeline_count} pipelines"
        )

    except typer.Exit:
        raise
    except Exception as e:
        display_generic_error(e, "status check")
        logger.error(f"Error checking project status: {e}")
        raise typer.Exit(1)


@app.command()
def version() -> None:
    """Show SQLFlow version information."""
    try:
        from sqlflow import __version__

        console.print("📦 [bold blue]SQLFlow Version Information[/bold blue]")
        console.print(f"Version: [cyan]{__version__}[/cyan]")
        console.print(f"Python: [cyan]{__import__('sys').version.split()[0]}[/cyan]")

        # Show additional version info if available
        try:
            import rich
            import typer

            console.print(f"Typer: [dim]{typer.__version__}[/dim]")
            console.print(f"Rich: [dim]{rich.__version__}[/dim]")
        except AttributeError:
            pass

    except Exception as e:
        console.print(f"❌ [red]Error getting version information: {e}[/red]")
        raise typer.Exit(1)


def _setup_environment(verbose: bool = False, quiet: bool = False) -> None:
    """Set up CLI environment with proper error handling.

    Args:
        verbose: Enable verbose output
        quiet: Reduce output to essentials
    """
    try:
        from sqlflow.logging import configure_logging, suppress_third_party_loggers
        from sqlflow.utils.env import setup_environment as setup_env

        # Load environment variables
        env_loaded = setup_env()

        # Configure logging
        configure_logging(verbose=verbose, quiet=quiet)
        suppress_third_party_loggers()

        if verbose and env_loaded:
            console.print("✓ [dim]Environment variables loaded from .env file[/dim]")

    except Exception as e:
        if verbose:
            logger.warning(f"Environment setup issue: {e}")


def cli() -> None:
    """Entry point for the CLI application.

    This is the main entry point called from setup.py or when running
    the module directly. It handles top-level error catching and
    provides consistent exit behavior.
    """
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n⚠️  [yellow]Operation cancelled by user[/yellow]")
        raise typer.Exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        logger.error(f"Unexpected CLI error: {e}")
        console.print(f"❌ [bold red]Unexpected error: {str(e)}[/bold red]")
        console.print("💡 [dim]Run with --verbose for more details[/dim]")
        raise typer.Exit(1)


if __name__ == "__main__":
    # Direct execution support
    cli()
