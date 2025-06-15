"""Rich display functions for SQLFlow CLI.

This module provides beautiful, consistent CLI output using Rich formatting,
following the technical design's approach for professional UX.
"""

from typing import Any, Dict, List

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from sqlflow.cli.errors import (
    PipelineCompilationError,
    PipelineExecutionError,
    PipelineNotFoundError,
    PipelineValidationError,
    ProfileNotFoundError,
    VariableParsingError,
)

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


def display_pipeline_not_found_error(error: PipelineNotFoundError) -> None:
    """Display pipeline not found error with suggestions."""
    console.print(f"❌ [bold red]Pipeline '{error.pipeline_name}' not found[/bold red]")
    console.print(f"🔍 [dim]Searched in: {', '.join(error.search_paths)}[/dim]")

    if error.available_pipelines:
        console.print("\n💡 [yellow]Available pipelines:[/yellow]")
        for pipeline in error.available_pipelines[:5]:
            console.print(f"  • [cyan]{pipeline}[/cyan]")


def display_variable_parsing_error(error: VariableParsingError) -> None:
    """Display variable parsing error with example."""
    console.print("❌ [bold red]Invalid variables parameter[/bold red]")
    console.print(f"📝 [dim]Input: {error.variable_string}[/dim]")
    console.print(f"🚫 [red]Error: {error.parse_error}[/red]")

    for suggestion in error.suggestions:
        console.print(f"💡 [yellow]{suggestion}[/yellow]")


def display_json_error() -> None:
    """Display JSON parsing error with example.

    This function provides a simple JSON error display as specified
    in the technical design document.
    """
    console.print("❌ [bold red]Invalid JSON in variables parameter[/bold red]")
    console.print(
        '💡 [yellow]Example:[/yellow] --variables \'{"env": "prod", "debug": true}\''
    )


def display_pipeline_validation_error(error: PipelineValidationError) -> None:
    """Display validation error with details."""
    console.print(
        f"❌ [bold red]Validation failed for '{error.pipeline_name}'[/bold red]"
    )

    if error.errors:
        console.print("\n📋 [yellow]Issues found:[/yellow]")
        for err in error.errors[:5]:  # Show first 5 errors
            console.print(f"  • [red]{err}[/red]")

        if len(error.errors) > 5:
            console.print(f"  ... and {len(error.errors) - 5} more issues")


def display_profile_not_found_error(error: ProfileNotFoundError) -> None:
    """Display profile not found error with suggestions."""
    console.print(f"❌ [bold red]Profile '{error.profile_name}' not found[/bold red]")

    if error.available_profiles:
        console.print("💡 [yellow]Available profiles:[/yellow]")
        for profile in error.available_profiles[:5]:
            console.print(f"  • [cyan]{profile}[/cyan]")


def display_compilation_error(error: PipelineCompilationError) -> None:
    """Display compilation error with context."""
    console.print(
        f"❌ [bold red]Compilation failed for '{error.pipeline_name}'[/bold red]"
    )
    console.print(f"🚫 [red]{error.error_message}[/red]")

    if error.context:
        console.print("\n📊 [yellow]Context:[/yellow]")
        for key, value in error.context.items():
            console.print(f"  • [cyan]{key}[/cyan]: {value}")


def display_execution_error(error: PipelineExecutionError) -> None:
    """Display execution error with step information."""
    console.print(
        f"❌ [bold red]Execution failed for '{error.pipeline_name}'[/bold red]"
    )
    console.print(f"🚫 [red]{error.error_message}[/red]")

    if error.failed_step:
        console.print(f"📍 [yellow]Failed step: {error.failed_step}[/yellow]")


def display_pipeline_list(pipelines: List[Dict[str, Any]], profile: str) -> None:
    """Display list of pipelines in a beautiful table."""
    if not pipelines:
        console.print(f"📭 [yellow]No pipelines found for profile '{profile}'[/yellow]")
        return

    console.print(f"📋 [bold blue]Pipelines for profile '{profile}'[/bold blue]\n")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan", width=20)
    table.add_column("Description", style="white", width=40)
    table.add_column("Status", style="green", width=10)

    for pipeline in pipelines:
        name = pipeline.get("name", "Unknown")
        description = pipeline.get("description", "No description")
        status = "✅ Valid" if pipeline.get("valid", True) else "❌ Invalid"

        table.add_row(name, description, status)

    console.print(table)


def display_execution_progress(
    pipeline_name: str, current_step: int, total_steps: int, step_name: str
) -> None:
    """Display execution progress."""
    progress = f"[{current_step}/{total_steps}]"
    console.print(f"🔄 {progress} [cyan]{pipeline_name}[/cyan] - {step_name}")


def display_execution_success(
    pipeline_name: str, profile: str, execution_time: float, operations_count: int
) -> None:
    """Display successful execution with summary."""
    console.print("✅ [bold green]Pipeline executed successfully[/bold green]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Property", style="cyan", width=15)
    table.add_column("Value", style="white")

    table.add_row("Pipeline", pipeline_name)
    table.add_row("Profile", profile)
    table.add_row("Operations", str(operations_count))
    table.add_row("Duration", f"{execution_time:.2f}s")

    console.print(table)


def display_warning(message: str) -> None:
    """Display a warning message."""
    console.print(f"⚠️  [yellow]{message}[/yellow]")


def display_info(message: str) -> None:
    """Display an info message."""
    console.print(f"ℹ️  [cyan]{message}[/cyan]")


def display_success(message: str) -> None:
    """Display a success message."""
    console.print(f"✅ [green]{message}[/green]")


def display_error(message: str) -> None:
    """Display a generic error message."""
    console.print(f"❌ [red]{message}[/red]")


def display_error_with_panel(
    title: str, message: str, suggestions: List[str] = None
) -> None:
    """Display error in a Rich panel with optional suggestions.

    This provides a more structured error display format for complex errors.
    """
    content = f"[red]{message}[/red]"

    if suggestions:
        content += "\n\n[yellow]💡 Suggestions:[/yellow]"
        for suggestion in suggestions:
            content += f"\n  • {suggestion}"

    panel = Panel(content, title=f"❌ {title}", border_style="red", expand=False)
    console.print(panel)
