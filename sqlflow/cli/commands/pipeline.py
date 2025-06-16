"""Pipeline management commands with Typer integration.

This module implements Task 2.3: Complete Command Integration for pipeline
commands using Typer framework with Rich display and type safety.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import typer
from rich.console import Console

from sqlflow.cli.business_operations import (
    compile_pipeline_operation,
    list_pipelines_operation,
    run_pipeline_operation,
    validate_pipeline_operation,
)
from sqlflow.cli.display import (
    display_compilation_success,
    display_execution_success,
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
    pipeline_name: str = typer.Argument(..., help="Pipeline name to compile"),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    variables: Optional[str] = typer.Option(
        None, "--variables", "--vars", help="Variables as JSON string"
    ),
    output_dir: Optional[str] = typer.Option(
        None, "--output-dir", help="Output directory for compilation results"
    ),
) -> None:
    """Compile a pipeline to execution plan.

    Parses the pipeline file, validates syntax, creates an execution plan,
    and saves the result to the target directory.

    Args:
        pipeline_name: Name of the pipeline to compile (without .sf extension)
        profile: Profile configuration to use (default: dev)
        variables: Optional variables as JSON string for substitution
        output_dir: Directory to save compilation output (default: target)
    """
    try:
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
    pipeline_name: str = typer.Argument(..., help="Pipeline name to run"),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    variables: Optional[str] = typer.Option(
        None, "--variables", "--vars", help="Variables as JSON string"
    ),
) -> None:
    """Execute a pipeline.

    Compiles and executes the pipeline using the specified profile and variables.
    Displays execution results with timing and status information.

    Args:
        pipeline_name: Name of the pipeline to run (without .sf extension)
        profile: Profile configuration to use (default: dev)
        variables: Optional variables as JSON string for substitution
    """
    try:
        # Parse variables with type validation
        vars_dict = _parse_variables_safely(variables)

        # Execute business operation
        results = run_pipeline_operation(
            pipeline_name=pipeline_name,
            profile_name=profile,
            variables=vars_dict,
        )

        # Display success with Rich formatting
        duration = results.get("execution_time", 0.0)
        operation_count = results.get(
            "operations_count", len(results.get("executed_steps", []))
        )
        executed_steps = results.get("executed_steps", [])
        display_execution_success(
            pipeline_name=pipeline_name,
            profile=profile,
            duration=duration,
            operation_count=operation_count,
            executed_steps=executed_steps,
            variables=vars_dict,
        )

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
