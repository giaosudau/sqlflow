"""Rich display functions for SQLFlow CLI.

This module implements beautiful, consistent CLI output and error display
using Rich formatting as specified in Task 2.3 of the technical design.
"""

import json
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from sqlflow.cli.errors import (
    PipelineCompilationError,
    PipelineExecutionError,
    PipelineNotFoundError,
    PipelineValidationError,
    ProfileNotFoundError,
    ProfileValidationError,
    ProjectNotFoundError,
    VariableParsingError,
)

console = Console()


# Success Display Functions
def display_compilation_success(
    pipeline_name: str,
    profile: str,
    operation_count: int,
    output_path: str,
    operations: Optional[List[Dict[str, Any]]] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> None:
    """Display successful compilation with Rich formatting.

    Args:
        pipeline_name: Name of the compiled pipeline
        profile: Profile used for compilation
        operation_count: Number of operations in the plan
        output_path: Path to the compilation output
        operations: List of operations for detailed display
        variables: Variables used for compilation
    """
    console.print("✅ [bold green]Pipeline compiled successfully[/bold green]")
    # For backward compatibility with integration tests
    console.print("Compilation successful")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Property", style="cyan", width=12)
    table.add_column("Value", style="white")

    table.add_row("Pipeline", pipeline_name)
    table.add_row("Profile", profile)
    table.add_row("Operations", str(operation_count))
    table.add_row("Output", output_path)

    console.print(table)

    # Show operation details if available
    if operations:
        console.print("\n📋 [bold blue]Operations:[/bold blue]")
        for i, op in enumerate(operations[:5]):  # Show max 5 operations
            op_id = op.get("id", f"operation_{i}")
            op_type = op.get("type", "unknown")
            console.print(f"  {i+1}. [cyan]{op_id}[/cyan] ({op_type})")

        if len(operations) > 5:
            console.print(f"  ... and {len(operations) - 5} more operations")

    # Show variables if provided
    if variables:
        console.print("\n🔧 [bold blue]Variables:[/bold blue]")
        for key, value in list(variables.items())[:5]:  # Show max 5 variables
            console.print(f"  [cyan]{key}[/cyan]: {value}")

        if len(variables) > 5:
            console.print(f"  ... and {len(variables) - 5} more variables")


def display_execution_success(
    pipeline_name: str,
    profile: str,
    duration: float,
    operation_count: int = 0,
    executed_steps: Optional[List[str]] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> None:
    """Display successful pipeline execution with performance metrics.

    Args:
        pipeline_name: Name of the executed pipeline
        profile: Profile used for execution
        duration: Execution duration in seconds
        operation_count: Number of operations executed
        executed_steps: List of executed step names
        variables: Variables used for execution
    """
    console.print("✅ [bold green]Pipeline executed successfully[/bold green]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Property", style="cyan", width=12)
    table.add_column("Value", style="white")

    table.add_row("Pipeline", pipeline_name)
    table.add_row("Profile", profile)
    table.add_row("Duration", f"{duration:.1f}s")
    table.add_row("Operations", str(operation_count))
    table.add_row("Status", "✅ Completed")

    console.print(table)

    # Show executed steps if available
    if executed_steps:
        console.print("\n🔄 [bold blue]Executed Steps:[/bold blue]")
        for i, step in enumerate(executed_steps[:5]):  # Show max 5 steps
            console.print(f"  {i+1}. [cyan]{step}[/cyan]")

        if len(executed_steps) > 5:
            console.print(f"  ... and {len(executed_steps) - 5} more steps")

    # Show variables if provided
    if variables:
        console.print("\n🔧 [bold blue]Variables:[/bold blue]")
        for key, value in list(variables.items())[:5]:  # Show max 5 variables
            console.print(f"  [cyan]{key}[/cyan]: {value}")

        if len(variables) > 5:
            console.print(f"  ... and {len(variables) - 5} more variables")


def display_pipelines_list(pipelines: List[Dict[str, str]]) -> None:
    """Display list of available pipelines.

    Args:
        pipelines: List of pipeline dictionaries with name, path, size, modified
    """
    if not pipelines:
        console.print("📋 [yellow]No pipelines found[/yellow]")
        console.print(
            "💡 [dim]Create a pipeline file with .sf extension in the "
            "pipelines/ directory[/dim]"
        )
        return

    console.print(f"📋 [bold blue]Available Pipelines ({len(pipelines)})[/bold blue]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan")
    table.add_column("Size", style="white", justify="right")
    table.add_column("Modified", style="dim")

    for pipeline in pipelines:
        # Format file size
        try:
            size_bytes = int(pipeline["size"])
            size_str = (
                f"{size_bytes:,} bytes"
                if size_bytes < 1024
                else f"{size_bytes/1024:.1f} KB"
            )
        except (ValueError, KeyError):
            size_str = "Unknown"

        # Format modification time
        try:
            import datetime

            mod_time = float(pipeline["modified"])
            mod_str = datetime.datetime.fromtimestamp(mod_time).strftime(
                "%Y-%m-%d %H:%M"
            )
        except (ValueError, KeyError):
            mod_str = "Unknown"

        table.add_row(pipeline["name"], size_str, mod_str)

    console.print(table)


def display_profiles_list(profiles: List[Dict[str, Any]]) -> None:
    """Display list of available profiles.

    Args:
        profiles: List of profile dictionaries with name, status, description
    """
    if not profiles:
        console.print("📋 [yellow]No profiles found[/yellow]")
        console.print(
            "💡 [dim]Create a profile file with .yml extension in the "
            "profiles/ directory[/dim]"
        )
        return

    console.print(f"📋 [bold blue]Available profiles ({len(profiles)})[/bold blue]")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Profile", style="cyan")
    table.add_column("Status", style="white", justify="center")
    table.add_column("Description", style="dim")

    for profile in profiles:
        status = profile.get("status")
        if status == "valid":
            status_display = "✅ Valid"
        elif "Error:" in profile.get("description", ""):
            status_display = "⚠️  Error"
        else:
            status_display = "❌ Issues"

        description = profile.get("description", "No description")[:50]
        if len(profile.get("description", "")) > 50:
            description += "..."

        table.add_row(profile["name"], status_display, description)

    console.print(table)


def display_validation_success(resource_name: str, resource_type: str) -> None:
    """Display successful validation.

    Args:
        resource_name: Name of the validated resource
        resource_type: Type of resource (pipeline, profile, etc.)
    """
    console.print(
        f"✅ [bold green]{resource_type.title()} '{resource_name}' is "
        f"valid[/bold green]"
    )
    # For backward compatibility with integration tests
    console.print("validation passed")


def display_profile_validation_success(profile_name: str) -> None:
    """Display successful profile validation with expected message format."""
    console.print(
        f"✅ [bold green]Profile '{profile_name}' validation passed[/bold green]"
    )


def display_compilation_success_simple(message: str = "Compilation successful") -> None:
    """Display simple compilation success message for tests."""
    console.print(f"✅ [bold green]{message}[/bold green]")


# Error Display Functions
def display_pipeline_not_found_error(error: PipelineNotFoundError) -> None:
    """Display pipeline not found error with suggestions.

    Args:
        error: PipelineNotFoundError with context
    """
    console.print(f"❌ [bold red]Pipeline '{error.pipeline_name}' not found[/bold red]")
    console.print(f"🔍 [dim]Searched in: {', '.join(error.search_paths)}[/dim]")

    if error.available_pipelines:
        console.print("\n💡 [yellow]Available pipelines:[/yellow]")
        for pipeline in error.available_pipelines[:5]:  # Show max 5
            console.print(f"  • [cyan]{pipeline}[/cyan]")

        if len(error.available_pipelines) > 5:
            console.print(f"  ... and {len(error.available_pipelines) - 5} more")


def display_pipeline_validation_error(error: PipelineValidationError) -> None:
    """Display pipeline validation error with details.

    Args:
        error: PipelineValidationError with validation details
    """
    console.print(
        f"❌ [bold red]Pipeline '{error.pipeline_name}' validation "
        f"failed[/bold red]"
    )

    if error.errors:
        console.print("\n📋 [yellow]Issues found:[/yellow]")
        for i, err in enumerate(error.errors[:5], 1):  # Show max 5 errors
            console.print(f"  {i}. [red]{err}[/red]")

        if len(error.errors) > 5:
            console.print(f"  ... and {len(error.errors) - 5} more issues")


def display_profile_not_found_error(error: ProfileNotFoundError) -> None:
    """Display profile not found error with available options."""
    console.print(f"❌ [bold red]Profile '{error.profile_name}' not found[/bold red]")

    if hasattr(error, "available_profiles") and error.available_profiles:
        console.print("💡 [bold yellow]Available profiles:[/bold yellow]")
        for profile in error.available_profiles:
            console.print(f"   • {profile}")


def display_profile_validation_error(error: ProfileValidationError) -> None:
    """Display profile validation error with details.

    Args:
        error: ProfileValidationError with validation details
    """
    console.print(
        f"❌ [bold red]Profile '{error.profile_name}' validation " f"failed[/bold red]"
    )

    if error.errors:
        console.print("\n📋 [yellow]Issues found:[/yellow]")
        for i, err in enumerate(error.errors[:5], 1):  # Show max 5 errors
            console.print(f"  {i}. [red]{err}[/red]")

        if len(error.errors) > 5:
            console.print(f"  ... and {len(error.errors) - 5} more issues")


def display_project_not_found_error(error: ProjectNotFoundError) -> None:
    """Display project not found error with helpful suggestions.

    Args:
        error: ProjectNotFoundError with context
    """
    console.print("❌ [bold red]No SQLFlow project found[/bold red]")
    console.print(f"🔍 [dim]Searched in: {error.directory}[/dim]")

    if error.suggestions:
        console.print("\n💡 [yellow]Get started:[/yellow]")
        for suggestion in error.suggestions:
            console.print(f"  • [cyan]{suggestion}[/cyan]")


def display_variable_parsing_error(error: "VariableParsingError") -> None:
    """Display variable parsing error with input and suggestions."""
    console.print("❌ [bold red]Invalid variables parameter[/bold red]")
    console.print(f"   Input: [yellow]{error.variable_string}[/yellow]")
    console.print(f"   Error: {error.parse_error}")

    if error.suggestions:
        console.print("💡 [bold yellow]Suggestions:[/bold yellow]")
        for suggestion in error.suggestions:
            console.print(f"   • {suggestion}")


def display_json_error() -> None:
    """Display JSON parsing error with example."""
    console.print("❌ [bold red]Invalid JSON format[/bold red]")
    console.print("💡 [bold yellow]Example:[/bold yellow]")
    console.print('   {"env": "prod", "debug": true}')


def display_generic_error(error: Exception, context: str = "") -> None:
    """Display generic error with context.

    Args:
        error: Exception that occurred
        context: Optional context about where the error occurred
    """
    context_text = f" during {context}" if context else ""
    console.print(f"❌ [bold red]Error{context_text}[/bold red]")
    console.print(f"🔍 [dim]{str(error)}[/dim]")


# Utility Display Functions
def display_info_panel(title: str, content: str, style: str = "blue") -> None:
    """Display an information panel.

    Args:
        title: Panel title
        content: Panel content
        style: Rich style for the panel border
    """
    panel = Panel(content, title=title, border_style=style)
    console.print(panel)


def display_json_output(data: Any) -> None:
    """Display JSON output with proper formatting.

    Args:
        data: Data to display as JSON
    """
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        console.print(json_str)
    except (TypeError, ValueError) as e:
        console.print(f"❌ [red]Error formatting JSON output: {e}[/red]")


def display_progress_message(message: str) -> None:
    """Display a progress message.

    Args:
        message: Progress message to display
    """
    console.print(f"🔄 [cyan]{message}[/cyan]")


def display_warning(message: str) -> None:
    """Display warning message."""
    console.print(f"⚠️  [bold yellow]{message}[/bold yellow]")


# Legacy function aliases for backward compatibility with tests
def display_compilation_error(error: "PipelineCompilationError") -> None:
    """Display compilation error with context information."""
    console.print(
        f"❌ [bold red]Compilation failed for {error.pipeline_name}[/bold red]"
    )
    console.print(f"   {error.error_message}")

    if hasattr(error, "context") and error.context:
        console.print("📊 [bold blue]Context:[/bold blue]")
        for key, value in error.context.items():
            console.print(f"   {key}: {value}")

    if error.suggestions:
        console.print("💡 [bold yellow]Suggestions:[/bold yellow]")
        for suggestion in error.suggestions:
            console.print(f"   • {suggestion}")


def display_execution_error(error: "PipelineExecutionError") -> None:
    """Display execution error with failed step information."""
    console.print(f"❌ [bold red]Execution failed for {error.pipeline_name}[/bold red]")
    console.print(f"   {error.error_message}")

    if hasattr(error, "failed_step") and error.failed_step:
        console.print(
            f"   📍 Failed at step: [bold cyan]{error.failed_step}[/bold cyan]"
        )

    if error.suggestions:
        console.print("💡 [bold yellow]Suggestions:[/bold yellow]")
        for suggestion in error.suggestions:
            console.print(f"   • {suggestion}")


def display_pipeline_list(pipelines: List[Dict[str, str]], profile: str = "") -> None:
    """Display pipeline list with profile context."""
    if not pipelines:
        console.print("📭 [bold blue]Pipeline List[/bold blue]")
        console.print("   No pipelines found")
        if profile:
            console.print(f"   Using profile: [cyan]{profile}[/cyan]")
        console.print("💡 [bold yellow]Suggestion:[/bold yellow]")
        console.print("   • Create a pipeline file in the pipelines/ directory")
    else:
        # Show profile context with pipelines
        if profile:
            console.print(
                f"📋 [bold blue]Available Pipelines[/bold blue] "
                f"(profile: [cyan]{profile}[/cyan])"
            )
        display_pipelines_list(pipelines)


def display_execution_progress(
    pipeline_name: str, current_step: int, total_steps: int, step_name: str
) -> None:
    """Display execution progress with step information."""
    progress_text = f"[{current_step}/{total_steps}]"
    console.print(f"🔄 [bold blue]Running {pipeline_name}[/bold blue] {progress_text}")
    console.print(f"   Current step: [cyan]{step_name}[/cyan]")


def display_info(message: str) -> None:
    """Display info message (legacy alias)."""
    console.print(f"ℹ️  [bold blue]{message}[/bold blue]")


def display_success(message: str) -> None:
    """Display success message (legacy alias)."""
    console.print(f"✅ [bold green]{message}[/bold green]")


def display_error(message: str) -> None:
    """Display error message (legacy alias)."""
    console.print(f"❌ [bold red]{message}[/bold red]")


def display_error_with_panel(
    title: str, error_message: str, suggestions: Optional[List[str]] = None
) -> None:
    """Display error with panel (legacy alias)."""
    content = error_message
    if suggestions:
        content += "\n\nSuggestions:\n" + "\n".join(f"• {s}" for s in suggestions)
    display_info_panel(title, content, "red")
