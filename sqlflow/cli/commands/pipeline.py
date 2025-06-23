"""Pipeline management commands with Typer integration.

This module implements Task 2.3: Complete Command Integration for pipeline
commands using Typer framework with Rich display and type safety.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import typer
from rich.console import Console

from sqlflow.cli.advanced_display import (
    display_execution_summary_with_metrics,
    display_interactive_pipeline_selection,
    display_pipeline_execution_progress,
)
from sqlflow.cli.business_operations import (
    compile_pipeline_operation,
    list_pipelines_operation,
    validate_pipeline_operation,
)
from sqlflow.cli.display import (
    display_compilation_success,
    display_pipeline_not_found_error,
    display_pipeline_validation_error,
    display_pipelines_list,
    display_profile_not_found_error,
    display_validation_success,
    display_variable_parsing_error,
)
from sqlflow.cli.errors import (
    PipelineNotFoundError,
    PipelineValidationError,
    ProfileNotFoundError,
    VariableParsingError,
)
from sqlflow.logging import get_logger

logger = get_logger(__name__)
console = Console()

# Create the pipeline command group
pipeline_app = typer.Typer(
    name="pipeline",
    help="Pipeline management commands - compile, run, list, and validate pipelines",
)


def _get_pipeline_interactively(profile: str) -> str:
    """Get pipeline name through interactive selection.

    Args:
        profile: Profile to use for listing pipelines

    Returns:
        Selected pipeline name

    Raises:
        typer.Exit: If no pipelines found or selection cancelled
    """
    pipelines = list_pipelines_operation(profile_name=profile)
    if not pipelines:
        console.print("📋 [yellow]No pipelines found[/yellow]")
        console.print(
            "💡 [dim]Create a pipeline file with .sf extension in pipelines/[/dim]"
        )
        raise typer.Exit(1)

    return display_interactive_pipeline_selection(profile_name=profile)


def _execute_pipeline_with_progress(
    operations: List[Dict[str, Any]],
    pipeline_name: str,
    profile: str,
    variables: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Execute pipeline operations with Rich progress display.

    Args:
        operations: List of operations to execute
        pipeline_name: Name of the pipeline
        profile: Profile to use
        variables: Variables for execution

    Returns:
        Execution results dictionary
    """
    from sqlflow.cli.factories import create_executor_for_command
    from sqlflow.core.engines.duckdb.engine import DuckDBEngine
    from sqlflow.core.executors.v2.execution.context import create_test_context

    def executor_callback(operation):
        """Callback function to execute a single operation."""
        executor = create_executor_for_command(profile, variables)
        try:
            # Create proper ExecutionContext for V2
            engine = DuckDBEngine(":memory:")
            context = create_test_context(engine=engine, variables=variables or {})
            result = executor.execute([operation], context)
            return {"status": "success", "result": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    return display_pipeline_execution_progress(
        operations=operations,
        executor_callback=executor_callback,
        pipeline_name=pipeline_name,
    )


def _display_execution_results(
    results: Dict[str, Any],
    pipeline_name: str,
    summary: bool,
    variables: Optional[Dict[str, Any]],
) -> None:
    """Display execution results with optional summary.

    Args:
        results: Execution results dictionary
        pipeline_name: Name of the pipeline
        summary: Whether to show detailed summary
        variables: Variables used in execution
    """
    if summary:
        display_execution_summary_with_metrics(
            results=results, pipeline_name=pipeline_name, show_detailed_metrics=True
        )
    else:
        # Show simple success message
        if results.get("status") == "success":
            duration = results.get("duration", 0.0)
            steps_count = results.get("total_steps", 0)
            console.print(
                f"✅ [bold green]Pipeline '{pipeline_name}' completed successfully![/bold green]"
            )
            console.print(
                f"📊 [dim]Executed {steps_count} steps in {duration:.2f}s[/dim]"
            )
            # Show variables if provided
            if variables:
                console.print(f"🔧 [dim]Variables: {variables}[/dim]")


def _parse_variables_safely(variables: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse variables with fallback from JSON to key=value format.

    Args:
        variables: Variables string to parse

    Returns:
        Parsed variables dictionary or None

    Raises:
        typer.Exit: If variable parsing fails
    """
    if not variables:
        return None

    try:
        # Try JSON format first
        vars_dict = json.loads(variables)
        logger.debug(f"Parsed variables as JSON: {list(vars_dict.keys())}")
        return vars_dict
    except json.JSONDecodeError:
        # Fallback to key=value format
        try:
            vars_dict = {}
            for pair in variables.split(","):
                if "=" not in pair:
                    raise ValueError(f"Invalid format: {pair}")
                key, value = pair.split("=", 1)
                key = key.strip()
                value = value.strip()

                # Try to parse value as JSON, fallback to string
                try:
                    vars_dict[key] = json.loads(value)
                except json.JSONDecodeError:
                    vars_dict[key] = value

            logger.debug(f"Parsed variables as key=value: {list(vars_dict.keys())}")
            return vars_dict
        except Exception as e:
            error = VariableParsingError(variables, str(e))
            display_variable_parsing_error(error)
            raise typer.Exit(1)


@pipeline_app.command("compile")
def compile_pipeline(
    pipeline_name: Optional[str] = typer.Argument(
        None, help="Pipeline name to compile (shows selection if omitted)"
    ),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    variables: Optional[str] = typer.Option(
        None, "--variables", "--vars", help="Variables as JSON string"
    ),
    output_dir: Optional[str] = typer.Option(
        None, "--output-dir", help="Output directory for compilation results"
    ),
) -> None:
    """Compile a pipeline to execution plan with Rich display. Shows interactive selection if no pipeline specified.

    Parses the pipeline file, validates syntax, creates an execution plan,
    and saves the result to the target directory.

    Args:
        pipeline_name: Name of the pipeline to compile (without .sf extension). If not provided, shows interactive selection.
        profile: Profile configuration to use (default: dev)
        variables: Optional variables as JSON string for substitution
        output_dir: Directory to save compilation output (default: target)
    """
    try:
        # Auto-interactive when no pipeline name provided
        if not pipeline_name:
            pipeline_name = _get_pipeline_interactively(profile)
            console.print(f"⚙️ Compiling: [cyan]{pipeline_name}[/cyan]")

        # Parse variables with type validation
        vars_dict = _parse_variables_safely(variables)

        # Execute business operation
        operations, output_path = compile_pipeline_operation(
            pipeline_name=pipeline_name,
            profile_name=profile,
            variables=vars_dict,
            output_dir=output_dir,
        )

        # Display success with Rich formatting
        display_compilation_success(
            pipeline_name=pipeline_name,
            profile=profile,
            operation_count=len(operations),
            output_path=output_path,
            operations=operations,
            variables=vars_dict,
        )

        logger.info(f"Pipeline '{pipeline_name}' compiled successfully")

    except PipelineNotFoundError as e:
        display_pipeline_not_found_error(e)
        raise typer.Exit(1)
    except ProfileNotFoundError as e:
        display_profile_not_found_error(e)
        raise typer.Exit(1)
    except PipelineValidationError as e:
        display_pipeline_validation_error(e)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during compilation: {e}")
        console.print(f"❌ [bold red]Compilation failed: {str(e)}[/bold red]")
        raise typer.Exit(1)


@pipeline_app.command("run")
def run_pipeline(
    pipeline_name: Optional[str] = typer.Argument(
        None, help="Pipeline name to run (shows selection if omitted)"
    ),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    variables: Optional[str] = typer.Option(
        None, "--variables", "--vars", help="Variables as JSON string"
    ),
    summary: bool = typer.Option(False, "--summary", help="Show execution summary"),
) -> None:
    """Execute a pipeline with Rich progress. Shows interactive selection if no pipeline specified.

    Compiles and executes the pipeline using the specified profile and variables.
    Displays execution results with timing and status information.

    Args:
        pipeline_name: Name of the pipeline to run (without .sf extension). If not provided, shows interactive selection.
        profile: Profile configuration to use (default: dev)
        variables: Optional variables as JSON string for substitution
        summary: Show detailed execution summary after completion
    """
    try:
        # Auto-interactive when no pipeline name provided
        if not pipeline_name:
            pipeline_name = _get_pipeline_interactively(profile)
            console.print(f"🚀 Running: [cyan]{pipeline_name}[/cyan]")

        # Parse variables and compile pipeline
        vars_dict = _parse_variables_safely(variables)
        operations, _ = compile_pipeline_operation(
            pipeline_name=pipeline_name, profile_name=profile, variables=vars_dict
        )

        # Execute with Rich progress display
        results = _execute_pipeline_with_progress(
            operations, pipeline_name, profile, vars_dict
        )

        # Display results
        _display_execution_results(results, pipeline_name, summary, vars_dict)

        # Exit with error code if execution failed
        if results.get("status") != "success":
            raise typer.Exit(1)

        logger.info(f"Pipeline '{pipeline_name}' executed successfully")

    except PipelineNotFoundError as e:
        display_pipeline_not_found_error(e)
        raise typer.Exit(1)
    except ProfileNotFoundError as e:
        display_profile_not_found_error(e)
        raise typer.Exit(1)
    except PipelineValidationError as e:
        display_pipeline_validation_error(e)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        console.print(f"❌ [bold red]Execution failed: {str(e)}[/bold red]")
        raise typer.Exit(1)


@pipeline_app.command("list")
def list_pipelines(
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    format: str = typer.Option(
        "table", "--format", help="Output format: table or json"
    ),
) -> None:
    """List available pipelines.

    Scans the pipelines directory and displays available pipeline files
    with metadata like size and modification time.

    Args:
        profile: Profile configuration to use (default: dev)
        format: Output format - 'table' for formatted display, 'json' for structured data
    """
    try:
        # Execute business operation
        pipelines = list_pipelines_operation(profile_name=profile)

        # Display results based on format
        if format == "json":
            from sqlflow.cli.display import display_json_output

            display_json_output(pipelines)
        else:
            display_pipelines_list(pipelines)

        logger.debug(f"Listed {len(pipelines)} pipelines")

    except ProfileNotFoundError as e:
        display_profile_not_found_error(e)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Unexpected error listing pipelines: {e}")
        console.print(f"❌ [bold red]Failed to list pipelines: {str(e)}[/bold red]")
        raise typer.Exit(1)


def _validate_single_pipeline(
    pipeline_name: str, profile: str, verbose: bool, quiet: bool
) -> None:
    """Validate a single pipeline.

    Args:
        pipeline_name: Name of the pipeline to validate
        profile: Profile to use
        verbose: Show detailed validation information
        quiet: Suppress non-error output

    Raises:
        typer.Exit: If validation fails
    """
    is_valid, errors = validate_pipeline_operation(
        pipeline_name=pipeline_name,
        profile_name=profile,
    )

    if is_valid:
        if not quiet:
            display_validation_success(pipeline_name, "pipeline")
        logger.info(f"Pipeline '{pipeline_name}' validation passed")
    else:
        # Create validation error for display
        error = PipelineValidationError(pipeline_name, errors)
        display_pipeline_validation_error(error)
        if verbose:
            # Show detailed error information
            for error_msg in errors:
                console.print(f"  [yellow]Detail:[/yellow] {error_msg}")
        logger.warning(f"Pipeline '{pipeline_name}' validation failed")
        raise typer.Exit(1)


def _validate_all_pipelines(profile: str, verbose: bool, quiet: bool) -> None:
    """Validate all pipelines.

    Args:
        profile: Profile to use
        verbose: Show detailed validation information
        quiet: Suppress non-error output

    Raises:
        typer.Exit: If any validation fails
    """
    from sqlflow.cli.business_operations import list_pipelines_operation

    pipelines = list_pipelines_operation(profile_name=profile)
    all_valid = True
    failed_pipelines = []

    for pipeline_info in pipelines:
        pipeline_name = pipeline_info["name"]
        try:
            is_valid, errors = validate_pipeline_operation(
                pipeline_name=pipeline_name,
                profile_name=profile,
            )

            if is_valid:
                if not quiet:
                    logger.info(f"Pipeline '{pipeline_name}' validation passed")
            else:
                all_valid = False
                failed_pipelines.append((pipeline_name, errors))
                logger.warning(f"Pipeline '{pipeline_name}' validation failed")

        except Exception as e:
            all_valid = False
            failed_pipelines.append((pipeline_name, [str(e)]))
            logger.error(f"Pipeline '{pipeline_name}' validation error: {e}")

    _display_bulk_validation_results(
        pipelines, failed_pipelines, all_valid, verbose, quiet
    )


def _display_bulk_validation_results(
    pipelines: List[Dict],
    failed_pipelines: List[Tuple[str, List[str]]],
    all_valid: bool,
    verbose: bool,
    quiet: bool,
) -> None:
    """Display results of bulk pipeline validation.

    Args:
        pipelines: List of all pipelines
        failed_pipelines: List of failed pipelines with errors
        all_valid: Whether all pipelines passed validation
        verbose: Show detailed error information
        quiet: Suppress non-error output

    Raises:
        typer.Exit: If any validation failed
    """
    if all_valid:
        if not quiet:
            console.print(
                f"✅ [bold green]All {len(pipelines)} pipelines validated successfully[/bold green]"
            )
    else:
        console.print(
            f"❌ [bold red]Validation failed for {len(failed_pipelines)} pipeline(s)[/bold red]"
        )
        for pipeline_name, errors in failed_pipelines:
            if verbose:
                console.print(f"  • {pipeline_name}:")
                for error in errors:
                    console.print(f"    - {error}")
            else:
                console.print(f"  • {pipeline_name}: {'; '.join(errors)}")
        raise typer.Exit(1)


@pipeline_app.command("validate")
def validate_pipeline(
    pipeline_name: Optional[str] = typer.Argument(
        None,
        help="Pipeline name to validate (if not provided, validates all pipelines)",
    ),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed validation information"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress non-error output"
    ),
) -> None:
    """Validate a pipeline or all pipelines without executing.

    Checks pipeline syntax, dependencies, and configuration without running
    the actual data transformations.

    Args:
        pipeline_name: Name of the pipeline to validate (without .sf extension). If not provided, validates all pipelines.
        profile: Profile configuration to use (default: dev)
        verbose: Show detailed validation information including suggestions
        quiet: Suppress non-error output
    """
    try:
        if pipeline_name:
            _validate_single_pipeline(pipeline_name, profile, verbose, quiet)
        else:
            _validate_all_pipelines(profile, verbose, quiet)

    except PipelineNotFoundError as e:
        display_pipeline_not_found_error(e)
        raise typer.Exit(1)
    except ProfileNotFoundError as e:
        display_profile_not_found_error(e)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during validation: {e}")
        console.print(f"❌ [bold red]Validation failed: {str(e)}[/bold red]")
        raise typer.Exit(1)


# Short aliases for common commands
@pipeline_app.command("ls")
def list_pipelines_short(
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
) -> None:
    """List pipelines (short alias for 'list')."""
    list_pipelines(profile=profile, format="table")


if __name__ == "__main__":
    pipeline_app()
