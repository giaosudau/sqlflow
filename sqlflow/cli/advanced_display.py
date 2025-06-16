"""Advanced Rich display features for SQLFlow CLI.

This module implements enhanced Rich features as specified in Task 3.1 of the
technical design. These features provide professional-grade CLI interactions
with real-time progress display and interactive selection capabilities.

Following technical design principles:
- Rich Progress for real-time execution display
- Interactive pipeline selection with Rich formatting
- Type-safe function signatures with comprehensive type hints
- Clean integration with existing business operations
- Optional features that enhance but don't complicate basic usage
"""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from sqlflow.cli.business_operations import list_pipelines_operation
from sqlflow.cli.errors import ProfileNotFoundError
from sqlflow.logging import get_logger

console = Console()
logger = get_logger(__name__)


def display_pipeline_execution_progress(
    operations: List[Dict[str, Any]],
    executor_callback: Callable[[Dict[str, Any]], Dict[str, Any]],
    pipeline_name: str = "Unknown Pipeline",
) -> Dict[str, Any]:
    """Display real-time pipeline execution progress with Rich formatting.

    This function provides a beautiful, real-time display of pipeline execution
    progress using Rich's Progress widget. It shows spinners, elapsed time,
    and operation status updates.

    Args:
        operations: List of operation dictionaries to execute
        executor_callback: Function that executes a single operation
        pipeline_name: Name of the pipeline being executed

    Returns:
        Execution results dictionary with status and metrics

    Raises:
        Exception: Re-raises any execution exceptions after display cleanup
    """
    logger.debug(f"Starting advanced progress display for {len(operations)} operations")

    executed_steps = []
    total_start_time = time.time()

    # Create Rich progress display with custom columns
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        transient=False,  # Keep progress visible after completion
        console=console,
    ) as progress:

        # Add main pipeline task
        pipeline_task = progress.add_task(
            f"🚀 Executing {pipeline_name}", total=len(operations)
        )

        try:
            for i, operation in enumerate(operations):
                operation_id = operation.get("id", f"operation_{i}")
                operation_type = operation.get("type", "unknown")
                operation_name = operation.get("name", operation_id)

                # Create task for this specific operation
                operation_task = progress.add_task(
                    f"⚙️  {operation_type}: {operation_name}...",
                    total=None,  # Indeterminate progress
                )

                step_start_time = time.time()

                try:
                    # Execute the operation using the provided callback
                    result = executor_callback(operation)
                    step_duration = time.time() - step_start_time

                    # Check execution status
                    status = result.get("status", "unknown")

                    if status == "success":
                        # Update operation task to show completion
                        progress.update(
                            operation_task,
                            description=f"✅ {operation_type}: {operation_name} ({step_duration:.1f}s)",
                        )
                        executed_steps.append(operation_id)

                        # Update pipeline progress
                        progress.update(pipeline_task, advance=1)

                        logger.debug(
                            f"Operation {operation_id} completed in {step_duration:.1f}s"
                        )

                    elif status == "error":
                        # Handle operation failure
                        error_msg = result.get("message", "Unknown error")
                        progress.update(
                            operation_task,
                            description=f"❌ {operation_type}: {operation_name} - {error_msg}",
                        )

                        # Calculate metrics for failure response
                        total_duration = time.time() - total_start_time

                        return {
                            "status": "failed",
                            "error": error_msg,
                            "failed_step": operation_id,
                            "failed_step_type": operation_type,
                            "executed_steps": executed_steps,
                            "total_steps": len(operations),
                            "failed_at_step": i + 1,
                            "duration": total_duration,
                        }
                    else:
                        # Handle unexpected status
                        progress.update(
                            operation_task,
                            description=f"⚠️  {operation_type}: {operation_name} - Unexpected status: {status}",
                        )

                        total_duration = time.time() - total_start_time

                        return {
                            "status": "failed",
                            "error": f"Unexpected status: {status}",
                            "failed_step": operation_id,
                            "failed_step_type": operation_type,
                            "executed_steps": executed_steps,
                            "total_steps": len(operations),
                            "failed_at_step": i + 1,
                            "duration": total_duration,
                        }

                except Exception as e:
                    # Handle execution exception
                    step_duration = time.time() - step_start_time
                    error_msg = f"Exception in {operation_type}: {str(e)}"

                    progress.update(
                        operation_task,
                        description=f"💥 {operation_type}: {operation_name} - Exception ({step_duration:.1f}s)",
                    )

                    logger.error(
                        f"Exception in operation {operation_id}: {e}", exc_info=True
                    )

                    total_duration = time.time() - total_start_time

                    return {
                        "status": "failed",
                        "error": error_msg,
                        "failed_step": operation_id,
                        "failed_step_type": operation_type,
                        "executed_steps": executed_steps,
                        "total_steps": len(operations),
                        "failed_at_step": i + 1,
                        "duration": total_duration,
                        "exception": str(e),
                    }

                finally:
                    # Remove the operation-specific task to keep display clean
                    progress.remove_task(operation_task)

            # All operations completed successfully
            total_duration = time.time() - total_start_time

            progress.update(
                pipeline_task,
                description=f"🎉 {pipeline_name} completed successfully! ({total_duration:.1f}s)",
            )

            logger.info(f"Pipeline {pipeline_name} completed in {total_duration:.1f}s")

            return {
                "status": "success",
                "executed_steps": executed_steps,
                "total_steps": len(operations),
                "duration": total_duration,
            }

        except Exception as e:
            # Handle unexpected pipeline-level exception
            total_duration = time.time() - total_start_time

            progress.update(
                pipeline_task,
                description=f"💥 {pipeline_name} failed - {str(e)}",
            )

            logger.error(f"Pipeline {pipeline_name} failed: {e}", exc_info=True)

            return {
                "status": "failed",
                "error": f"Pipeline exception: {str(e)}",
                "executed_steps": executed_steps,
                "total_steps": len(operations),
                "duration": total_duration,
                "exception": str(e),
            }


def _format_pipeline_size(pipeline: Dict[str, Any]) -> str:
    """Format file size for display in pipeline tables."""
    try:
        size_bytes = int(pipeline["size"])
        if size_bytes < 1024:
            return f"{size_bytes:,} bytes"
        return f"{size_bytes/1024:.1f} KB"
    except (ValueError, KeyError):
        return "Unknown"


def _format_modification_time(pipeline: Dict[str, Any]) -> str:
    """Format modification time for display in pipeline tables."""
    try:
        import datetime

        mod_time = float(pipeline["modified"])
        return datetime.datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M")
    except (ValueError, KeyError):
        return "Unknown"


def _create_pipeline_table(
    pipelines: List[Dict[str, Any]], show_details: bool = True
) -> Table:
    """Create a Rich table for pipeline display."""
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("#", style="cyan", width=3)
    table.add_column("Pipeline Name", style="cyan")

    if show_details:
        table.add_column("Size", style="white", justify="right")
        table.add_column("Modified", style="dim")

    # Populate table with pipeline data
    for i, pipeline in enumerate(pipelines, 1):
        if show_details:
            size_str = _format_pipeline_size(pipeline)
            mod_str = _format_modification_time(pipeline)
            table.add_row(str(i), pipeline["name"], size_str, mod_str)
        else:
            table.add_row(str(i), pipeline["name"])

    return table


def _handle_quit_command(choice: str) -> bool:
    """Check if user wants to quit and handle gracefully."""
    if choice.lower() in ("q", "quit", "exit"):
        console.print("👋 [yellow]Selection cancelled[/yellow]")
        raise typer.Exit(0)
    return False


def _handle_pipeline_selection(
    choice: str, pipelines: List[Dict[str, Any]]
) -> Optional[str]:
    """Handle pipeline selection and validation."""
    try:
        index = int(choice) - 1
        if 0 <= index < len(pipelines):
            selected_pipeline = pipelines[index]["name"]
            console.print(
                f"✅ [bold green]Selected pipeline: {selected_pipeline}[/bold green]"
            )
            logger.debug(f"User selected pipeline: {selected_pipeline}")
            return selected_pipeline
        else:
            console.print(f"❌ [red]Invalid selection: {choice}[/red]")
            console.print(
                f"💡 [yellow]Please enter a number between 1 and {len(pipelines)}[/yellow]"
            )
            return None
    except ValueError:
        console.print(f"❌ [red]Invalid input: '{choice}'[/red]")
        console.print("💡 [yellow]Please enter a number, or 'q' to quit[/yellow]")
        return None


def _handle_user_input_errors() -> None:
    """Handle keyboard interrupt and EOF errors consistently."""
    console.print("\n👋 [yellow]Selection cancelled[/yellow]")
    raise typer.Exit(0)


def _get_and_validate_pipelines(profile_name: Optional[str]) -> List[Dict[str, Any]]:
    """Get and validate pipelines list, handling no pipelines case."""
    pipelines = list_pipelines_operation(profile_name)

    if not pipelines:
        console.print("📋 [yellow]No pipelines found[/yellow]")
        console.print(
            "💡 [dim]Create a pipeline file with .sf extension in the "
            "pipelines/ directory[/dim]"
        )
        raise typer.Exit(1)

    return pipelines


def _display_pipeline_table_with_header(
    pipelines: List[Dict[str, Any]], show_details: bool = True
) -> None:
    """Display pipeline table with header."""
    console.print(f"\n📋 [bold blue]Available Pipelines ({len(pipelines)})[/bold blue]")
    table = _create_pipeline_table(pipelines, show_details)
    console.print(table)


def _run_interactive_selection_loop(pipelines: List[Dict[str, Any]]) -> str:
    """Run the interactive selection loop."""
    console.print(
        "\n💡 [dim]Enter the number of the pipeline you want to select, or 'q' to quit[/dim]"
    )

    while True:
        try:
            choice = typer.prompt("Select pipeline")

            # Handle quit command
            _handle_quit_command(choice)

            # Handle pipeline selection
            selected_pipeline = _handle_pipeline_selection(choice, pipelines)
            if selected_pipeline:
                return selected_pipeline

        except KeyboardInterrupt:
            _handle_user_input_errors()
        except EOFError:
            _handle_user_input_errors()


def display_interactive_pipeline_selection(
    profile_name: Optional[str] = None,
    show_details: bool = True,
) -> str:
    """Interactive pipeline selection with Rich formatting.

    Displays available pipelines in a beautiful table format and allows
    the user to select one interactively. Provides helpful error messages
    and suggestions for invalid selections.

    Args:
        profile_name: Profile to use for pipeline listing
        show_details: Whether to show detailed pipeline information

    Returns:
        Name of the selected pipeline

    Raises:
        typer.Exit: When no pipelines are available or user cancels
        ProfileNotFoundError: When the specified profile is not found
        PipelineNotFoundError: When no pipelines are found
    """
    logger.debug(f"Starting interactive pipeline selection for profile: {profile_name}")

    try:
        # Get and validate pipelines
        pipelines = _get_and_validate_pipelines(profile_name)

        # Display pipelines table
        _display_pipeline_table_with_header(pipelines, show_details)

        # Run interactive selection
        return _run_interactive_selection_loop(pipelines)

    except ProfileNotFoundError as e:
        console.print(f"❌ [bold red]Profile Error:[/bold red] {e.message}")
        if e.suggestions:
            for suggestion in e.suggestions:
                console.print(f"💡 [yellow]{suggestion}[/yellow]")
        raise typer.Exit(1)

    except typer.Exit:
        # Re-raise typer exits (user cancellation, no pipelines, etc.)
        raise
    except Exception as e:
        console.print(f"❌ [bold red]Unexpected error:[/bold red] {str(e)}")
        logger.error(f"Unexpected error in interactive selection: {e}", exc_info=True)
        raise typer.Exit(1)


def _create_preview_table(pipelines: List[Dict[str, Any]]) -> Table:
    """Create a Rich table for pipeline preview display."""
    pipeline_table = Table(show_header=True, header_style="bold blue")
    pipeline_table.add_column("#", style="cyan", width=3)
    pipeline_table.add_column("Pipeline Name", style="cyan")
    pipeline_table.add_column("Size", style="white", justify="right")

    for i, pipeline in enumerate(pipelines, 1):
        try:
            size_bytes = int(pipeline["size"])
            size_str = (
                f"{size_bytes/1024:.1f} KB"
                if size_bytes >= 1024
                else f"{size_bytes} bytes"
            )
        except (ValueError, KeyError):
            size_str = "Unknown"

        pipeline_table.add_row(str(i), pipeline["name"], size_str)

    return pipeline_table


def _handle_preview_command(choice: str, pipelines: List[Dict[str, Any]]) -> bool:
    """Handle preview command and display pipeline content."""
    if not choice.endswith("p") or len(choice) <= 1:
        return False

    try:
        index = int(choice[:-1]) - 1
        if 0 <= index < len(pipelines):
            pipeline_name = pipelines[index]["name"]
            pipeline_path = pipelines[index]["path"]

            # Read and display pipeline preview
            try:
                with open(pipeline_path, "r") as f:
                    content = f.read()

                # Show first 20 lines as preview
                lines = content.split("\n")
                preview_lines = lines[:20]
                if len(lines) > 20:
                    preview_lines.append(f"... ({len(lines) - 20} more lines)")

                preview_content = "\n".join(preview_lines)
                preview_panel = Panel(
                    preview_content,
                    title=f"📄 Preview: {pipeline_name}",
                    border_style="green",
                )
                console.print(preview_panel)
                return True

            except IOError as e:
                console.print(f"❌ [red]Cannot read pipeline file: {e}[/red]")
                return True
        else:
            console.print(f"❌ [red]Invalid pipeline number: {choice[:-1]}[/red]")
            return True
    except ValueError:
        console.print(f"❌ [red]Invalid preview command: {choice}[/red]")
        return True


def _handle_regular_selection(
    choice: str, pipelines: List[Dict[str, Any]]
) -> Optional[str]:
    """Handle regular pipeline selection."""
    try:
        index = int(choice) - 1
        if 0 <= index < len(pipelines):
            selected_pipeline = pipelines[index]["name"]
            console.print(
                f"✅ [bold green]Selected pipeline: {selected_pipeline}[/bold green]"
            )
            return selected_pipeline
        else:
            console.print(f"❌ [red]Invalid selection: {choice}[/red]")
            console.print(
                f"💡 [yellow]Please enter a number between 1 and {len(pipelines)}[/yellow]"
            )
            return None
    except ValueError:
        console.print(f"❌ [red]Invalid input: '{choice}'[/red]")
        console.print(
            "💡 [yellow]Enter a number, [number]p to preview, or 'q' to quit[/yellow]"
        )
        return None


def _display_pipeline_preview_panel(pipelines: List[Dict[str, Any]]) -> None:
    """Display pipeline table in a panel for preview mode."""
    pipeline_table = _create_preview_table(pipelines)
    panel = Panel(
        pipeline_table,
        title="📋 Available Pipelines",
        border_style="blue",
    )
    console.print(panel)
    console.print(
        "\n💡 [dim]Commands: [number] to select, [number]p to preview, 'q' to quit[/dim]"
    )


def _run_preview_selection_loop(
    pipelines: List[Dict[str, Any]],
) -> Tuple[str, Optional[str]]:
    """Run the preview-enabled selection loop."""
    while True:
        try:
            choice = typer.prompt("Select pipeline").strip()

            # Handle quit command
            _handle_quit_command(choice)

            # Check for preview command
            if _handle_preview_command(choice, pipelines):
                continue

            # Handle regular selection
            selected_pipeline = _handle_regular_selection(choice, pipelines)
            if selected_pipeline:
                return selected_pipeline, None

        except KeyboardInterrupt:
            _handle_user_input_errors()
        except EOFError:
            _handle_user_input_errors()


def display_pipeline_selection_with_preview(
    profile_name: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """Interactive pipeline selection with preview capability.

    Enhanced version of pipeline selection that allows users to preview
    pipeline contents before making their final selection.

    Args:
        profile_name: Profile to use for pipeline listing

    Returns:
        Tuple of (selected_pipeline_name, preview_content)

    Raises:
        typer.Exit: When no pipelines are available or user cancels
    """
    logger.debug("Starting interactive pipeline selection with preview")

    try:
        # Get and validate pipelines
        pipelines = _get_and_validate_pipelines(profile_name)

        # Display preview panel
        _display_pipeline_preview_panel(pipelines)

        # Run preview selection loop
        return _run_preview_selection_loop(pipelines)

    except typer.Exit:
        # Re-raise typer exits (user cancellation, no pipelines, etc.)
        raise
    except Exception as e:
        console.print(f"❌ [bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error in pipeline selection with preview: {e}", exc_info=True)
        raise typer.Exit(1)


def display_execution_summary_with_metrics(
    results: Dict[str, Any],
    pipeline_name: str,
    show_detailed_metrics: bool = True,
) -> None:
    """Display execution summary with detailed metrics and Rich formatting.

    Shows a comprehensive summary of pipeline execution results including
    performance metrics, executed steps, and status information.

    Args:
        results: Execution results dictionary
        pipeline_name: Name of the executed pipeline
        show_detailed_metrics: Whether to show detailed performance metrics
    """
    status = results.get("status", "unknown")

    if status == "success":
        # Success summary with metrics
        console.print(
            f"\n🎉 [bold green]Pipeline '{pipeline_name}' executed successfully![/bold green]"
        )

        # Create metrics table
        metrics_table = Table(show_header=True, header_style="bold blue")
        metrics_table.add_column("Metric", style="cyan")
        metrics_table.add_column("Value", style="white")

        # Basic metrics
        total_steps = results.get("total_steps", 0)
        executed_steps = results.get("executed_steps", [])
        duration = results.get("duration", 0)

        metrics_table.add_row("Total Steps", str(total_steps))
        metrics_table.add_row("Executed Steps", str(len(executed_steps)))
        metrics_table.add_row("Duration", f"{duration:.2f}s")

        if show_detailed_metrics and duration > 0:
            avg_step_time = duration / max(len(executed_steps), 1)
            metrics_table.add_row("Avg Step Time", f"{avg_step_time:.2f}s")
            metrics_table.add_row("Steps/Second", f"{len(executed_steps)/duration:.2f}")

        console.print(metrics_table)

        # Show executed steps if available
        if executed_steps and show_detailed_metrics:
            console.print(
                f"\n✅ [bold blue]Executed Steps ({len(executed_steps)}):[/bold blue]"
            )
            for i, step in enumerate(executed_steps[:10], 1):  # Show first 10 steps
                console.print(f"  {i}. [cyan]{step}[/cyan]")

            if len(executed_steps) > 10:
                console.print(f"  ... and {len(executed_steps) - 10} more steps")

    elif status == "failed":
        # Failure summary with error details
        console.print(
            f"\n💥 [bold red]Pipeline '{pipeline_name}' execution failed![/bold red]"
        )

        error_msg = results.get("error", "Unknown error")
        failed_step = results.get("failed_step", "unknown")
        failed_at_step = results.get("failed_at_step", 0)
        total_steps = results.get("total_steps", 0)

        # Create error details table
        error_table = Table(show_header=True, header_style="bold red")
        error_table.add_column("Property", style="red")
        error_table.add_column("Value", style="white")

        error_table.add_row("Error", error_msg)
        error_table.add_row("Failed Step", failed_step)
        error_table.add_row("Failed At", f"Step {failed_at_step} of {total_steps}")

        if "duration" in results:
            error_table.add_row(
                "Duration Before Failure", f"{results['duration']:.2f}s"
            )

        console.print(error_table)

        # Show executed steps before failure
        executed_steps = results.get("executed_steps", [])
        if executed_steps:
            console.print(
                f"\n✅ [dim]Successfully completed before failure ({len(executed_steps)} steps):[/dim]"
            )
            for i, step in enumerate(
                executed_steps[-5:], 1
            ):  # Show last 5 successful steps
                console.print(f"  {i}. [dim cyan]{step}[/dim cyan]")

    else:
        # Unknown status
        console.print(
            f"\n❓ [yellow]Pipeline '{pipeline_name}' finished with unknown status: {status}[/yellow]"
        )
        console.print(f"Results: {results}")
