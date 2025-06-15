"""Rich display functions for SQLFlow CLI.

This module provides beautiful console output using Rich formatting.
"""

from typing import Dict, List

from rich.console import Console
from rich.table import Table

console = Console()


def display_compilation_success(
    pipeline_name: str, profile: str, operation_count: int, output_path: str
) -> None:
    """Display successful compilation with Rich formatting."""
    console.print("✅ [bold green]Pipeline compiled successfully[/bold green]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Property", style="cyan", width=12)
    table.add_column("Value", style="white")

    table.add_row("Pipeline", pipeline_name)
    table.add_row("Profile", profile)
    table.add_row("Operations", str(operation_count))
    table.add_row("Output", output_path)

    console.print(table)


def display_pipeline_not_found_error(*args, **kwargs):
    """Stub for display_pipeline_not_found_error."""


def display_validation_error(*args, **kwargs):
    """Stub for display_validation_error."""


def display_error(*args, **kwargs):
    """Stub for display_error."""


def display_success(*args, **kwargs):
    """Stub for display_success."""


def display_info(*args, **kwargs):
    """Stub for display_info."""


def display_compilation_error(*args, **kwargs):
    """Stub for display_compilation_error."""


def display_execution_error(*args, **kwargs):
    """Stub for display_execution_error."""


def display_pipeline_list(*args, **kwargs):
    """Stub for display_pipeline_list."""


def display_execution_success(*args, **kwargs):
    """Stub for display_execution_success."""


def display_variable_parsing_error(*args, **kwargs):
    """Stub for display_variable_parsing_error."""


def display_error_with_panel(
    title: str, message: str, suggestions: List[str] = None
) -> None:
    """Display error with Rich panel formatting."""
    from rich.panel import Panel

    content = f"[red]{message}[/red]"

    if suggestions:
        content += "\n\n[yellow]💡 Suggestions:[/yellow]"
        for suggestion in suggestions:
            content += f"\n  • {suggestion}"

    panel = Panel(content, title=f"❌ {title}", border_style="red")
    console.print(panel)


def display_pipelines_list(pipelines: List[Dict[str, str]]) -> None:
    """Display list of available pipelines."""
    if not pipelines:
        console.print("📋 [yellow]No pipelines found[/yellow]")
        return

    console.print(f"📋 [bold blue]Available Pipelines ({len(pipelines)})[/bold blue]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan")
    table.add_column("Size", style="dim")
    table.add_column("Modified", style="dim")

    for pipeline in pipelines:
        table.add_row(
            pipeline["name"], pipeline.get("size", ""), pipeline.get("modified", "")
        )

    console.print(table)


def display_validation_result(
    pipeline_name: str, is_valid: bool, errors: List[str]
) -> None:
    """Display pipeline validation results."""
    if is_valid:
        console.print(
            f"✅ [bold green]Pipeline '{pipeline_name}' is valid[/bold green]"
        )
    else:
        console.print(
            f"❌ [bold red]Pipeline '{pipeline_name}' validation failed[/bold red]"
        )
        if errors:
            console.print("\n📋 [yellow]Issues found:[/yellow]")
            for error in errors:
                console.print(f"  • [red]{error}[/red]")


def display_profile_not_found_error(error) -> None:
    """Display profile not found error with suggestions."""
    from sqlflow.cli.errors import ProfileNotFoundError

    if isinstance(error, ProfileNotFoundError):
        console.print(
            f"❌ [bold red]Profile '{error.profile_name}' not found[/bold red]"
        )
        if error.suggestions:
            for suggestion in error.suggestions:
                console.print(f"💡 [yellow]{suggestion}[/yellow]")
    else:
        console.print(f"❌ [bold red]Profile error: {error}[/bold red]")
