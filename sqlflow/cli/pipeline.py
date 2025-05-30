"""Pipeline commands for the SQLFlow CLI."""

import datetime
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import typer

from sqlflow.cli.utils import parse_vars, resolve_pipeline_name
from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.core.storage.artifact_manager import ArtifactManager
from sqlflow.logging import configure_logging, get_logger
from sqlflow.parser.parser import Parser
from sqlflow.project import Project

logger = get_logger(__name__)

pipeline_app = typer.Typer(
    help="Pipeline management commands",
    no_args_is_help=True,
)


def _get_pipeline_info(
    pipeline_name: str, profile: str, variables: Optional[Dict[str, Any]] = None
) -> Tuple[Project, str, str]:
    """Get project, pipeline path, and target path for a pipeline.

    Args:
    ----
        pipeline_name: Name of the pipeline
        profile: Profile to use
        variables: Variables for the pipeline

    Returns:
    -------
        Tuple of (project, pipeline_path, target_path)

    """
    project = Project(os.getcwd(), profile_name=profile)
    pipeline_path = _resolve_pipeline_path(project, pipeline_name)
    if not os.path.exists(pipeline_path):
        typer.echo(f"Pipeline {pipeline_name} not found at {pipeline_path}")
        raise typer.Exit(code=1)

    target_dir = os.path.join(project.project_dir, "target", "compiled")
    os.makedirs(target_dir, exist_ok=True)
    target_path = os.path.join(target_dir, f"{pipeline_name}.json")

    return project, pipeline_path, target_path


def _read_pipeline_file(pipeline_path: str) -> str:
    """Read and return the contents of a pipeline file.

    Args:
    ----
        pipeline_path: Path to the pipeline file

    Returns:
    -------
        Contents of the pipeline file

    """
    with open(pipeline_path, "r") as f:
        return f.read()


def _apply_variable_substitution(pipeline_text: str, variables: Dict[str, Any]) -> str:
    """Apply variable substitution to pipeline text.

    Args:
    ----
        pipeline_text: Pipeline text with variables
        variables: Dictionary of variable values

    Returns:
    -------
        Pipeline text with variables substituted

    """
    from sqlflow.core.variables import VariableContext, VariableSubstitutor

    # Create a variable context and substitutor
    var_context = VariableContext(cli_variables=variables)
    substitutor = VariableSubstitutor(var_context)

    # Substitute variables in the pipeline text
    result = substitutor.substitute_string(pipeline_text)

    # Log any unresolved variables
    if var_context.has_unresolved_variables():
        unresolved = var_context.get_unresolved_variables()
        logger.warning(
            f"Pipeline contains unresolved variables: {', '.join(unresolved)}"
        )

    return result


def _is_test_pipeline(pipeline_path: str, pipeline_text: str) -> bool:
    """Check if this is a test pipeline.

    Args:
    ----
        pipeline_path: Path to the pipeline file
        pipeline_text: Contents of the pipeline file

    Returns:
    -------
        True if this is a test pipeline, False otherwise

    """
    # TODO: Implement test pipeline detection logic
    return False


def _get_test_plan() -> List[Dict[str, Any]]:
    """Get the execution plan for a test pipeline.

    Returns
    -------
        List of operations for the test pipeline

    """
    # TODO: Implement test plan generation
    return []


def _handle_source_error(error: Exception) -> None:
    """Handle and format SOURCE directive errors.

    Args:
    ----
        error: The exception to handle

    """
    if hasattr(error, "message") and "SOURCE" in str(error):
        error_lines = str(error).split("\n")
        unique_errors = set()
        format_examples = []

        for line in error_lines:
            if "SOURCE" in line and "{" in line:
                format_examples.append(line.strip())
            elif "at line" in line:
                base_error = line.split(" at line")[0].strip()
                if base_error:
                    unique_errors.add(base_error)

        formatted_errors = "\n".join(unique_errors)
        formatted_examples = "\n".join(format_examples)

        if formatted_examples:
            typer.echo(
                f"Error: {formatted_errors}\n\nCorrect formats:\n{formatted_examples}"
            )
        else:
            typer.echo(f"Error: {formatted_errors}")
    else:
        error_msg = str(error).strip()
        if " at line" in error_msg:
            error_msg = error_msg.split(" at line")[0].strip()
        typer.echo(f"Error: {error_msg}")


def _compile_pipeline_to_plan(
    pipeline_path: str,
    target_path: str,
    variables: Optional[Dict[str, Any]] = None,
    save_plan: bool = True,
) -> List[Dict[str, Any]]:
    """Compile a pipeline file to an execution plan.

    Args:
    ----
        pipeline_path: Path to the pipeline file
        target_path: Path to save the execution plan
        variables: Variables to substitute in the pipeline
        save_plan: Whether to save the plan to disk

    Returns:
    -------
        The execution plan as a list of operations

    """
    from sqlflow.core.planner import Planner
    from sqlflow.project import Project

    try:
        # Read the pipeline file
        pipeline_text = _read_pipeline_file(pipeline_path)

        # Get project and profile to access profile variables
        project_dir = os.getcwd()
        project = Project(project_dir)
        profile_dict = project.get_profile()
        profile_variables = (
            profile_dict.get("variables", {}) if isinstance(profile_dict, dict) else {}
        )

        # Parse the pipeline
        parser = Parser()
        pipeline = parser.parse(pipeline_text)

        # Create a plan with variable substitution
        planner = Planner()
        operations = planner.create_plan(
            pipeline, variables=variables, profile_variables=profile_variables
        )

        # Save the plan if requested
        if save_plan:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, "w") as f:
                json.dump(operations, f, indent=2)
                logger.debug(f"Saved execution plan to {target_path}")

        return operations
    except Exception as e:
        logger.error(f"Error compiling pipeline: {str(e)}")
        _handle_source_error(e)
        raise typer.Exit(code=1)


def _build_execution_graph(operations: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Build a simple execution graph from operations.

    Args:
    ----
        operations: List of operations

    Returns:
    -------
        Dict mapping step IDs to lists of dependent step IDs

    """
    graph = {}
    for op in operations:
        op_id = op.get("id", "unknown")
        depends_on = op.get("depends_on", [])
        graph[op_id] = depends_on
    return graph


def _prepare_compile_environment(
    vars_arg: Optional[str], profile_arg: str
) -> Tuple[Optional[Dict[str, Any]], Project, str, str]:
    """Parses variables, sets up project, and resolves common paths for compilation."""
    try:
        variables = parse_vars(vars_arg)
    except ValueError as e:
        typer.echo(f"Error parsing variables: {str(e)}")
        raise typer.Exit(code=1)

    project = Project(os.getcwd(), profile_name=profile_arg)
    profile_dict = project.get_profile()
    pipelines_dir = os.path.join(
        project.project_dir,
        profile_dict.get("paths", {}).get("pipelines", "pipelines"),
    )
    target_dir = os.path.join(project.project_dir, "target", "compiled")
    os.makedirs(target_dir, exist_ok=True)
    return variables, project, pipelines_dir, target_dir


def _do_compile_single_pipeline(
    pipeline_name: str,
    output_override: Optional[str],
    variables: Optional[Dict[str, Any]],
    pipelines_dir: str,
    target_dir: str,
):
    """Compiles a single specified pipeline."""
    try:
        pipeline_path = resolve_pipeline_name(pipeline_name, pipelines_dir)
        # Use pipeline_name for the .json file, not a potentially longer pipeline_path
        name_without_ext = (
            pipeline_name if not pipeline_name.endswith(".sf") else pipeline_name[:-3]
        )
        auto_output_path = os.path.join(target_dir, f"{name_without_ext}.json")
        final_output_path = output_override or auto_output_path

        # Display compilation start with consistent formatting
        typer.echo(f"📝 Compiling {pipeline_name}")
        typer.echo(f"Pipeline: {pipeline_path}")

        if variables:
            typer.echo(f"With variables: {json.dumps(variables, indent=2)}")

        operations = _compile_pipeline_to_plan(
            pipeline_path, final_output_path, variables
        )

        # Display compilation summary with better formatting
        _print_compilation_summary(operations, final_output_path)

        if variables:
            typer.echo(f"\n📋 Applied variables: {json.dumps(variables, indent=2)}")

    except FileNotFoundError as e:
        # Keep simple error for file not found
        typer.echo(str(e))
        raise typer.Exit(code=1)
    except typer.Exit:
        # Re-raise typer.Exit to avoid adding additional error messages
        # This exception would have been raised by _compile_pipeline_to_plan
        # which already printed an appropriate error message
        raise
    except Exception as e:
        # Log details at debug level only to avoid duplicating errors
        logger.debug(
            f"Unexpected error compiling {pipeline_name}: {str(e)}", exc_info=True
        )
        # Print simple error message for the user
        error_msg = str(e)
        if " at line" in error_msg:
            error_msg = error_msg.split(" at line")[0].strip()
        typer.echo(f"❌ Error compiling pipeline {pipeline_name}: {error_msg}")
        raise typer.Exit(code=1)


def _print_compilation_summary(
    operations: List[Dict[str, Any]], output_path: str
) -> None:
    """Print a user-friendly compilation summary.

    Args:
    ----
        operations: List of operations in the compiled plan
        output_path: Path where the execution plan was saved

    """
    # Count operations by type
    operation_counts = {}
    for op in operations:
        op_type = op.get("type", "unknown")
        operation_counts[op_type] = operation_counts.get(op_type, 0) + 1

    # Print summary header
    typer.echo("\n✅ Compilation successful!")
    typer.echo(f"📄 Execution plan: {output_path}")
    typer.echo(f"🔢 Total operations: {len(operations)}")

    # Print operations by type with counts
    typer.echo("\n📋 Operations by type:")
    for op_type, count in sorted(operation_counts.items()):
        typer.echo(f"  • {op_type}: {count}")

    # Print operation list in a more readable format
    typer.echo("\n🔗 Execution order:")
    for i, op in enumerate(operations, 1):
        op_id = op.get("id", "unknown")
        op_type = op.get("type", "unknown")
        typer.echo(f"  {i:2d}. {op_id} ({op_type})")

    typer.echo(f"\n💾 Plan saved to: {output_path}")


def _do_compile_all_pipelines(
    pipelines_dir: str, target_dir: str, variables: Optional[Dict[str, Any]]
):
    """Compiles all .sf pipelines in the specified directory."""
    if not os.path.exists(pipelines_dir):
        typer.echo(f"❌ Pipelines directory '{pipelines_dir}' not found.")
        raise typer.Exit(code=1)

    pipeline_files = [f for f in os.listdir(pipelines_dir) if f.endswith(".sf")]

    if not pipeline_files:
        typer.echo(f"❌ No pipeline files found in '{pipelines_dir}'.")
        # Consider if this should be an error or just a silent return.
        # For now, exiting as it implies a misconfiguration or empty project.
        raise typer.Exit(code=1)

    # Show initial summary
    typer.echo(f"📝 Compiling {len(pipeline_files)} pipeline(s) from '{pipelines_dir}'")
    if variables:
        typer.echo(f"With variables: {json.dumps(variables, indent=2)}")
    typer.echo()

    compiled_count = 0
    error_count = 0

    # Track all operations for final summary
    total_operations = 0
    all_operation_types = {}

    for i, file_name in enumerate(pipeline_files, 1):
        pipeline_path = os.path.join(pipelines_dir, file_name)
        name_without_ext = file_name[:-3]
        auto_output_path = os.path.join(target_dir, f"{name_without_ext}.json")

        try:
            typer.echo(f"  📄 [{i}/{len(pipeline_files)}] {file_name}")
            operations = _compile_pipeline_to_plan(
                pipeline_path, auto_output_path, variables
            )

            # Count operations for summary
            total_operations += len(operations)
            for op in operations:
                op_type = op.get("type", "unknown")
                all_operation_types[op_type] = all_operation_types.get(op_type, 0) + 1

            typer.echo(f"      ✅ Success ({len(operations)} operations)")
            compiled_count += 1
        except typer.Exit:
            # Exit was already handled in _compile_pipeline_to_plan
            # Just count it as an error and continue with other pipelines
            typer.echo("      ❌ Failed")
            error_count += 1
        except Exception as e:
            error_count += 1
            # Format error message cleanly
            error_msg = str(e)
            if " at line" in error_msg:
                error_msg = error_msg.split(" at line")[0]
            typer.echo(f"      ❌ Failed: {error_msg}")
            # Log at debug level to avoid duplicate error messages
            logger.debug(f"Error compiling pipeline {file_name}", exc_info=True)
            # Continue to compile other pipelines

    # Print final summary
    typer.echo()
    if error_count > 0:
        typer.echo("⚠️  Compilation completed with errors:")
        typer.echo(f"   ✅ {compiled_count} succeeded")
        typer.echo(f"   ❌ {error_count} failed")
        if compiled_count > 0:
            typer.echo(
                f"   📊 {total_operations} total operations in successful pipelines"
            )
            typer.echo(
                f"   📋 Operation types: {', '.join(f'{k}: {v}' for k, v in sorted(all_operation_types.items()))}"
            )
        typer.echo(f"   💾 Plans saved to: {target_dir}")
        raise typer.Exit(code=1)  # Exit with error if any compilation failed
    else:
        typer.echo("✅ All pipelines compiled successfully!")
        typer.echo(
            f"   📊 {compiled_count} pipelines, {total_operations} total operations"
        )
        typer.echo(
            f"   📋 Operation types: {', '.join(f'{k}: {v}' for k, v in sorted(all_operation_types.items()))}"
        )
        typer.echo(f"   💾 Plans saved to: {target_dir}")

    if variables and compiled_count > 0:
        typer.echo(
            f"\n📋 Applied variables to all pipelines: {json.dumps(variables, indent=2)}"
        )


@pipeline_app.command("compile")
def compile_pipeline(
    pipeline_name: Optional[str] = typer.Argument(
        None, help="Name of the pipeline (omit .sf extension, or provide full path)"
    ),
    output: Optional[str] = typer.Option(
        None,
        help="Custom output file for the execution plan (default: target/compiled/<pipeline_name>.json). Only applies when a single pipeline_name is provided.",
    ),
    vars: Optional[str] = typer.Option(
        None, help="Pipeline variables as JSON or key=value pairs"
    ),
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="Profile to use (default: dev)"
    ),
    skip_validation: bool = typer.Option(
        False,
        "--skip-validation",
        help="Skip validation before compilation (for CI/CD performance)",
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output with technical details"
    ),
):
    """Parse and validate pipeline(s), output execution plan(s).
    If pipeline_name is not provided, all pipelines in the project's pipeline directory are compiled.
    The execution plan is automatically saved to the project's target/compiled directory.

    By default, validation is performed before compilation to catch errors early.
    Use --skip-validation to skip validation for CI/CD performance scenarios.
    """
    # Configure logging based on command-specific flags
    configure_logging(verbose=verbose, quiet=quiet)

    _variables, _project, _pipelines_dir, _target_dir = _prepare_compile_environment(
        vars, profile
    )

    # Helper function to validate a pipeline file
    def validate_pipeline_file(pipeline_path: str, pipeline_display_name: str) -> bool:
        """Validate a pipeline file and return True if valid, False otherwise."""
        if skip_validation:
            return True

        from sqlflow.cli.validation_helpers import validate_pipeline_with_caching

        try:
            errors = validate_pipeline_with_caching(pipeline_path)

            if errors:
                if not quiet:
                    typer.echo(
                        f"❌ Validation failed for {pipeline_display_name}:", err=True
                    )
                    for error in errors:
                        typer.echo(f"  {error}", err=True)
                return False
            else:
                if verbose:
                    typer.echo(f"✅ Validation passed for {pipeline_display_name}")
                return True

        except Exception as e:
            typer.echo(
                f"❌ Validation error for {pipeline_display_name}: {str(e)}", err=True
            )
            return False

    if pipeline_name:
        if os.path.isfile(pipeline_name):  # User provided a full path
            _pipeline_path = pipeline_name
            _name_for_output = os.path.splitext(os.path.basename(_pipeline_path))[0]
            _auto_output_path = os.path.join(_target_dir, f"{_name_for_output}.json")
            _final_output_path = output or _auto_output_path

            try:
                # Validate before compilation
                if not validate_pipeline_file(_pipeline_path, pipeline_name):
                    raise typer.Exit(code=1)

                # Display compilation start with consistent formatting
                typer.echo(f"📝 Compiling {pipeline_name}")
                typer.echo(f"Pipeline: {_pipeline_path}")

                if _variables:
                    typer.echo(f"With variables: {json.dumps(_variables, indent=2)}")

                operations = _compile_pipeline_to_plan(
                    _pipeline_path, _final_output_path, _variables
                )

                # Display compilation summary with better formatting
                _print_compilation_summary(operations, _final_output_path)

                if _variables:
                    typer.echo(
                        f"\n📋 Applied variables: {json.dumps(_variables, indent=2)}"
                    )
            except typer.Exit:
                # Exit was already handled in _compile_pipeline_to_plan with appropriate error message
                raise
            except Exception as e:
                # Only log unexpected errors that weren't handled previously
                logger.debug(
                    f"Unexpected compilation error for {_pipeline_path}", exc_info=True
                )
                # Format error cleanly
                error_msg = str(e)
                if " at line" in error_msg:
                    error_msg = error_msg.split(" at line")[0].strip()
                typer.echo(f"❌ Error compiling pipeline {_pipeline_path}: {error_msg}")
                raise typer.Exit(code=1)

        else:  # User provided a name to be resolved
            # Validate before compilation
            try:
                from sqlflow.cli.utils import resolve_pipeline_name

                resolved_path = resolve_pipeline_name(pipeline_name, _pipelines_dir)
                if not validate_pipeline_file(resolved_path, pipeline_name):
                    raise typer.Exit(code=1)
            except FileNotFoundError:
                # Let _do_compile_single_pipeline handle the file not found error
                pass

            _do_compile_single_pipeline(
                pipeline_name, output, _variables, _pipelines_dir, _target_dir
            )
    else:
        if output:
            typer.echo(
                "Warning: --output option is ignored when compiling all pipelines."
            )

        # For multiple pipelines, validate each before compilation if not skipped
        if not skip_validation:
            validation_failed = False
            pipeline_files = [
                f for f in os.listdir(_pipelines_dir) if f.endswith(".sf")
            ]

            for pipeline_file in pipeline_files:
                pipeline_path = os.path.join(_pipelines_dir, pipeline_file)
                if not validate_pipeline_file(pipeline_path, pipeline_file):
                    validation_failed = True

            if validation_failed:
                typer.echo(
                    "❌ Validation failed for one or more pipelines. Aborting compilation.",
                    err=True,
                )
                raise typer.Exit(code=1)

        _do_compile_all_pipelines(_pipelines_dir, _target_dir, _variables)


def _parse_pipeline(pipeline_text: str, pipeline_path: str):
    """Parse a pipeline file using the SQLFlow parser.

    Args:
    ----
        pipeline_text: Text of the pipeline file
        pipeline_path: Path to the pipeline file

    Returns:
    -------
        Parsed pipeline object

    """
    try:
        parser = Parser()
        pipeline = parser.parse(pipeline_text)
        return pipeline
    except Exception as e:
        typer.echo(f"Error parsing pipeline {pipeline_path}: {str(e)}")
        return None


def _print_plan_summary(operations: List[Dict[str, Any]], pipeline_name: str):
    """Print a summary of an execution plan.

    Args:
    ----
        operations: List of operations in the plan
        pipeline_name: Name of the pipeline

    """
    step_types = {}
    for op in operations:
        step_type = op.get("type", "unknown")
        if step_type not in step_types:
            step_types[step_type] = 0
        step_types[step_type] += 1

    typer.echo(f"Pipeline: {pipeline_name}")
    typer.echo(f"Total operations: {len(operations)}")
    typer.echo("Operations by type:")
    for op_type, count in step_types.items():
        typer.echo(f"  - {op_type}: {count}")
    typer.echo("\nDependencies:")

    for op in operations:
        depends_on = op.get("depends_on", [])
        if depends_on:
            typer.echo(f"  - {op['id']} depends on: {', '.join(depends_on)}")
        else:
            typer.echo(f"  - {op['id']}: no dependencies")


def _write_execution_plan(plan_data: Dict[str, Any], target_path: str) -> None:
    """Write the execution plan to a file.

    Args:
    ----
        plan_data: The execution plan data
        target_path: Path to save the plan to

    """
    # Add metadata to the plan
    full_plan = {
        "pipeline_metadata": {
            "name": os.path.basename(target_path).replace(".json", ""),
            "compiled_at": datetime.datetime.now().isoformat(),
            "compiler_version": "0.1.0",
        },
        "operations": plan_data.get("operations", []),
        "execution_graph": _build_execution_graph(plan_data.get("operations", [])),
    }

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    # Write the plan
    try:
        with open(target_path, "w") as f:
            json.dump(full_plan, f, indent=2)
        typer.echo(f"\nExecution plan written to {target_path}")
    except Exception as e:
        logger.error(f"Failed to write execution plan: {str(e)}")
        typer.echo(f"Error writing execution plan: {str(e)}")


def _compile_single_pipeline(
    pipeline_path: str, output: Optional[str] = None, variables: Optional[dict] = None
):
    """Compile a single pipeline and output the execution plan."""
    try:
        # Provide a default target path if output is None to satisfy type checker
        target_path = output or "/tmp/temp_plan.json"

        operations = _compile_pipeline_to_plan(
            pipeline_path=pipeline_path,
            target_path=target_path,
            variables=variables,
            save_plan=output is not None,
        )

        if not output:
            operations_json = json.dumps(operations, indent=2)
            typer.echo("\nExecution plan:")
            typer.echo(operations_json)

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Unexpected error compiling pipeline {pipeline_path}: {str(e)}")
        logger.exception("Unexpected compilation error")
        raise typer.Exit(code=1)


def _read_and_substitute_pipeline(pipeline_path: str, variables: dict) -> str:
    """Read a pipeline file and substitute variables.

    Args:
    ----
        pipeline_path: Path to the pipeline file
        variables: Dictionary of variable values

    Returns:
    -------
        Pipeline text with variables substituted

    """
    from sqlflow.core.variables import VariableContext, VariableSubstitutor

    # Read the pipeline file
    with open(pipeline_path, "r") as f:
        pipeline_text = f.read()

    # If no variables, return as is
    if not variables:
        return pipeline_text

    # Create a variable context and substitutor
    var_context = VariableContext(cli_variables=variables)
    substitutor = VariableSubstitutor(var_context)

    # Substitute variables in the pipeline text
    result = substitutor.substitute_string(pipeline_text)

    # Log any unresolved variables
    if var_context.has_unresolved_variables():
        unresolved = var_context.get_unresolved_variables()
        logger.warning(
            f"Pipeline contains unresolved variables: {', '.join(unresolved)}"
        )

    return result


def _build_dependency_resolver(plan: list) -> DependencyResolver:
    """Build and return a DependencyResolver for the plan."""
    dependency_resolver = DependencyResolver()
    for step in plan:
        step_id = step["id"]
        for dependency in step.get("depends_on", []):
            dependency_resolver.add_dependency(step_id, dependency)
    return dependency_resolver


def _find_entry_points(plan: List[Dict[str, Any]]) -> List[str]:
    """Find entry points in the plan.

    Entry points are steps that are not dependent on other steps.

    Args:
    ----
        plan: The pipeline plan

    Returns:
    -------
        List of entry point step IDs

    """
    # Find all steps that don't have any dependencies
    entry_points = []
    for step in plan:
        if not step.get("depends_on"):
            entry_points.append(step["id"])

    # If no entry points were found but there are steps in the plan,
    # use the first step as an entry point
    if not entry_points and plan:
        entry_points = [plan[0]["id"]]

    return entry_points


def _build_execution_order_from_entry_points(
    dependency_resolver: DependencyResolver, entry_points: List[str]
) -> List[str]:
    """Build the execution order from entry points."""
    execution_order = []
    for entry_point in entry_points:
        if entry_point in execution_order:
            continue
        deps = dependency_resolver.resolve_dependencies(entry_point)
        for dep in deps:
            if dep not in execution_order:
                execution_order.append(dep)

    # Store the resolved order in the resolver for future reference
    dependency_resolver.last_resolved_order = execution_order

    return execution_order


def _resolve_execution_order(
    dependency_resolver: DependencyResolver, plan: List[Dict[str, Any]]
) -> List[str]:
    """Resolve the execution order using the dependency resolver.

    Args:
    ----
        dependency_resolver: The dependency resolver to use
        plan: The execution plan

    Returns:
    -------
        List of step IDs in execution order

    """
    # Get all step IDs from the plan
    all_step_ids = [step["id"] for step in plan]

    # Find entry points (steps with no dependencies)
    entry_points = _find_entry_points(plan)

    # Build the execution order from entry points
    execution_order = _build_execution_order_from_entry_points(
        dependency_resolver, entry_points
    )

    # Ensure all steps are included in the execution order
    for step_id in all_step_ids:
        if step_id not in execution_order:
            execution_order.append(step_id)

    # Store the resolved order in the resolver for future reference
    dependency_resolver.last_resolved_order = execution_order

    return execution_order


def _print_summary(summary: dict) -> None:
    """Print the summary of pipeline execution results."""
    success_color = typer.colors.GREEN
    error_color = typer.colors.RED
    warning_color = typer.colors.YELLOW
    total_steps = summary.get("total_steps", 0)
    successful_steps = summary.get("successful_steps", 0)
    failed_steps = summary.get("failed_steps", 0)
    typer.echo(f"Total steps: {total_steps}")
    if successful_steps == total_steps:
        typer.echo(
            typer.style(
                "✅ All steps executed successfully!", fg=success_color, bold=True
            )
        )
    else:
        success_percent = (
            (successful_steps / total_steps) * 100 if total_steps > 0 else 0
        )
        typer.echo(
            typer.style(
                f"⚠️ {successful_steps}/{total_steps} steps succeeded ({success_percent:.1f}%)",
                fg=warning_color if success_percent > 0 else error_color,
                bold=True,
            )
        )
        if failed_steps > 0:
            typer.echo(
                typer.style(
                    f"❌ {failed_steps} steps failed", fg=error_color, bold=True
                )
            )


def _print_status_by_step_type(by_type: dict) -> None:
    """Print detailed status by step type."""
    success_color = typer.colors.GREEN
    error_color = typer.colors.RED
    warning_color = typer.colors.YELLOW
    info_color = typer.colors.BLUE
    typer.echo("\nStatus by step type:")
    for step_type, info in by_type.items():
        total = info.get("total", 0)
        success = info.get("success", 0)
        failed = info.get("failed", 0)
        status_color = (
            success_color
            if success == total
            else warning_color
            if success > 0
            else error_color
        )
        typer.echo(
            f"  {typer.style(step_type, fg=info_color)}: {typer.style(f'{success}/{total}', fg=status_color)} completed successfully"
        )
        if failed > 0:
            typer.echo(f"  Failed {step_type} steps:")
            for step in info.get("steps", []):
                if step.get("status") != "success":
                    step_id = step.get("id", "unknown")
                    error = step.get("error", "Unknown error")
                    typer.echo(typer.style(f"    - {step_id}: {error}", fg=error_color))


def _print_export_steps_status(plan: list, results: dict) -> None:
    """Print the status of export steps."""
    error_color = typer.colors.RED
    export_steps = [step for step in plan if step.get("type") == "export"]
    if export_steps:
        typer.echo("\nExport steps status:")
        for step in export_steps:
            status = (
                "success"
                if step["id"] in results
                and results[step["id"]].get("status") == "success"
                else "failed/not executed"
            )
            destination = step.get("query", {}).get("destination_uri", "unknown")
            typer.echo(f"  {step['id']}: {status} - Target: {destination}")
            if step["id"] in results and results[step["id"]].get("status") == "failed":
                error = results[step["id"]].get("error", "Unknown error")
                typer.echo(typer.style(f"    Error: {error}", fg=error_color))
            elif status == "failed/not executed":
                dependencies = step.get("depends_on", [])
                failed_deps = [
                    dep
                    for dep in dependencies
                    if dep in results and results[dep].get("status") == "failed"
                ]
                if failed_deps:
                    deps_str = ", ".join(failed_deps)
                    typer.echo(
                        typer.style(
                            f"    Error: Not executed because dependencies failed: {deps_str}",
                            fg=error_color,
                        )
                    )


def _report_pipeline_results(operations: List[Dict[str, Any]], results: Dict[str, Any]):
    """Report the results of pipeline execution.

    Args:
    ----
        operations: List of operations in the plan
        results: Results of execution

    """
    summary = results.get("summary", {})
    if summary:
        _print_summary(summary)
        _print_status_by_step_type(summary.get("by_type", {}))

    # Print export results if there are exports
    _print_export_steps_status(operations, results)


def _resolve_pipeline_path(project: Project, pipeline_name: str) -> str:
    """Resolve the full path to the pipeline file given its name and project."""
    if "/" in pipeline_name:
        if pipeline_name.endswith(".sf"):
            return os.path.join(project.project_dir, pipeline_name)
        else:
            return os.path.join(project.project_dir, f"{pipeline_name}.sf")
    else:
        return project.get_pipeline_path(pipeline_name)


def _parse_and_plan_pipeline(pipeline_text: str) -> list:
    """Parse the pipeline text and create an execution plan."""
    parser = Parser()
    ast = parser.parse(pipeline_text)
    planner = Planner()
    return planner.create_plan(ast)


def _load_execution_plan(plan_path: str) -> List[Dict[str, Any]]:
    """Load execution plan from a JSON file.

    Args:
    ----
        plan_path: Path to the execution plan JSON file

    Returns:
    -------
        Execution plan as a list of operations

    Raises:
    ------
        typer.Exit: If the plan cannot be loaded

    """
    try:
        with open(plan_path, "r") as f:
            return json.load(f)
    except Exception as e:
        typer.echo(f"Error loading execution plan from {plan_path}: {str(e)}")
        raise typer.Exit(code=1)


# Helper function to set up the run environment
def _setup_run_environment(
    pipeline_name_arg: str, vars_arg: Optional[str], profile_arg: str
) -> Tuple[Project, Optional[Dict[str, Any]], str, ArtifactManager, str, str]:
    """Parses variables, sets up project, artifact manager, paths, and logs profile info."""
    try:
        variables = parse_vars(vars_arg)
    except ValueError as e:
        typer.echo(f"Error parsing variables: {str(e)}")
        raise typer.Exit(code=1)

    project = Project(os.getcwd(), profile_name=profile_arg)
    artifact_manager = ArtifactManager(project.project_dir)
    artifact_manager.clean_run_dir(pipeline_name_arg)

    pipeline_path = _resolve_pipeline_path(project, pipeline_name_arg)
    if not os.path.exists(pipeline_path):
        typer.echo(f"Pipeline {pipeline_name_arg} not found at {pipeline_path}")
        raise typer.Exit(code=1)

    compiled_plan_path = artifact_manager.get_compiled_path(pipeline_name_arg)

    profile_config = project.get_profile()
    typer.echo(f"[SQLFlow] Using profile: {profile_arg}")
    duckdb_mode = (
        profile_config.get("engines", {}).get("duckdb", {}).get("mode", "memory")
    )
    duckdb_path_info = (
        profile_config.get("engines", {}).get("duckdb", {}).get("path", None)
    )
    if duckdb_mode == "memory":
        typer.echo(
            "🚨 Running in DuckDB memory mode: results will NOT be saved after process exit."
        )
    else:
        typer.echo(
            f"💾 Running in DuckDB persistent mode: results saved to {duckdb_path_info or '[not set]'}."
        )

    typer.echo(f"Running pipeline: {pipeline_path}")
    if variables:
        typer.echo(f"With variables: {json.dumps(variables, indent=2)}")

    return (
        project,
        variables,
        profile_arg,
        artifact_manager,
        pipeline_path,
        compiled_plan_path,
    )


# Helper function to get execution operations (compile or load)
def _get_execution_operations(
    from_compiled_arg: bool,
    compiled_plan_path: str,
    pipeline_path: str,
    variables: Optional[Dict[str, Any]],
    pipeline_name: str,
    profile_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Loads a compiled plan or compiles the pipeline to get operations.

    Args:
    ----
        from_compiled_arg: Whether to use a pre-compiled plan
        compiled_plan_path: Path to the compiled plan if from_compiled_arg is True
        pipeline_path: Path to the pipeline file
        variables: CLI variables for substitution
        pipeline_name: Name of the pipeline
        profile_name: Name of the profile to use

    Returns:
    -------
        List of operations for execution

    """
    if from_compiled_arg and os.path.exists(compiled_plan_path):
        logger.info(f"Using compiled plan: {compiled_plan_path}")
        try:
            return _load_execution_plan(compiled_plan_path)
        except Exception as e:
            logger.error(f"Error loading compiled plan: {str(e)}")
            raise typer.Exit(code=1)
    else:
        # If from_compiled_arg but file doesn't exist, or not from_compiled
        if from_compiled_arg:
            logger.warning(
                f"Compiled plan not found: {compiled_plan_path}, recompiling..."
            )

        # User-friendly compilation message
        pipeline_filename = os.path.basename(pipeline_path)
        print(f"📝 Compiling {pipeline_filename}")
        logger.debug(f"Compiling pipeline: {pipeline_path}")

        # Get profile variables if profile_name is provided
        profile_variables = None
        if profile_name:
            project = Project(os.getcwd(), profile_name=profile_name)
            profile = project.get_profile()
            if profile and isinstance(profile, dict):
                profile_variables = profile.get("variables", {})
                logger.debug(f"Extracted profile variables: {profile_variables}")

        try:
            # Read the pipeline file
            with open(pipeline_path, "r") as f:
                pipeline_text = f.read()

            # Parse the pipeline
            parser = Parser()
            pipeline = parser.parse(pipeline_text)

            # Create execution plan with planner
            planner = Planner()

            # Pass both CLI variables and profile variables to the planner
            operations = planner.create_plan(
                pipeline, variables=variables, profile_variables=profile_variables
            )

            return operations
        except Exception as e:
            logger.error(f"Error compiling pipeline: {str(e)}")
            raise typer.Exit(code=1)


def _extract_set_variables_from_operations(
    operations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Extract SET variables with default values from operations.

    Args:
    ----
        operations: List of operation steps

    Returns:
    -------
        Dictionary of variable names to default values

    """
    set_variables = {}
    for op in operations:
        if op.get("type") == "transform" and op.get(
            "query", ""
        ).strip().upper().startswith("SET "):
            # Simple extraction of variable name and value from SET statements
            set_match = re.match(
                r"SET\s+([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*;?",
                op.get("query", ""),
                re.IGNORECASE,
            )
            if set_match:
                var_name = set_match.group(1).strip()
                var_value = set_match.group(2).strip()

                # Check if this has a default value in ${var|default} format
                default_match = re.match(r"\$\{([^|{}]+)\|([^{}]*)\}", var_value)
                if default_match:
                    # Use the default value as lowest priority
                    default_val = default_match.group(2).strip()
                    # Remove quotes if present
                    if (default_val.startswith('"') and default_val.endswith('"')) or (
                        default_val.startswith("'") and default_val.endswith("'")
                    ):
                        default_val = default_val[1:-1]

                    logger.debug(
                        f"Extracted default value '{default_val}' for variable '{var_name}'"
                    )
                    set_variables[var_name] = default_val

    logger.debug(f"Extracted SET variables with defaults: {set_variables}")
    return set_variables


def _build_effective_variables(
    set_variables: Dict[str, Any],
    executor_profile: Any,
    cli_variables: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build effective variables by applying priority rules.

    Args:
    ----
        set_variables: Variables from SET statements (lowest priority)
        executor_profile: Executor profile for profile variables (medium priority)
        cli_variables: Variables from CLI (highest priority)

    Returns:
    -------
        Combined dictionary of variables

    """
    # 1. Start with SET variables (lowest priority)
    effective_variables = set_variables.copy() if set_variables else {}

    # 2. Add profile variables (medium priority)
    profile_vars = (
        executor_profile.get("variables", {})
        if isinstance(executor_profile, dict)
        else {}
    )
    if profile_vars:
        logger.debug(f"Adding profile variables: {profile_vars}")
        effective_variables.update(profile_vars)

    # 3. Add CLI variables (highest priority)
    if cli_variables:
        logger.debug(f"Adding CLI variables: {cli_variables}")
        effective_variables.update(cli_variables)

    logger.debug(f"Final executor variables: {effective_variables}")
    return effective_variables


def _check_duckdb_engine(executor) -> None:
    """Check if DuckDB engine is initialized and log its status.

    Args:
    ----
        executor: Executor instance with DuckDB engine

    """
    logger = get_logger(__name__)
    if hasattr(executor, "duckdb_engine") and executor.duckdb_engine:
        logger.debug(
            f"DuckDB engine initialized with path: {executor.duckdb_engine.database_path}"
        )
        logger.debug(f"DuckDB mode: {executor.duckdb_mode}")
    else:
        logger.debug("DuckDB engine not initialized!")


def _verify_duckdb_tables(executor) -> None:
    """Verify DuckDB tables after execution.

    Args:
    ----
        executor: Executor instance with DuckDB engine

    """
    logger = get_logger(__name__)
    if (
        hasattr(executor, "duckdb_engine")
        and executor.duckdb_engine
        and executor.duckdb_mode == "persistent"
    ):
        try:
            logger.debug("Checking DuckDB tables after execution...")
            tables_result = executor.duckdb_engine.execute_query(
                "SHOW TABLES"
            ).fetchdf()
            logger.debug(f"Tables after execution: {tables_result}")
        except Exception as e:
            logger.debug(f"Error checking tables: {e}")


def _execute_and_handle_result(
    executor,
    operations: List[Dict[str, Any]],
    variables: Optional[Dict[str, Any]],
    pipeline_name: str,
    artifact_manager: ArtifactManager,
    start_time: datetime.datetime,
) -> bool:
    """Execute operations and handle the execution result.

    Args:
    ----
        executor: Executor instance
        operations: List of operations to execute
        variables: Variables for execution
        pipeline_name: Name of the pipeline
        artifact_manager: Artifact manager
        start_time: Start time of execution

    Returns:
    -------
        True if execution succeeded, False otherwise

    """
    # Get logger - ensure it's available in scope
    logger = get_logger(__name__)

    try:
        # Execute pipeline with CLI variables
        logger.debug("Calling executor.execute...")
        result = executor.execute(operations, variables=variables)
        logger.debug(f"Execution result: {result.get('status', 'unknown')}")

        # Verify DuckDB tables if needed
        _verify_duckdb_tables(executor)

        # Report execution time
        end_time = datetime.datetime.now()
        execution_time = end_time - start_time
        typer.echo(
            f"⏱️  Execution completed in {execution_time.total_seconds():.2f} seconds"
        )

        # Check overall success
        if result.get("status") == "success":
            typer.echo("✅ Pipeline completed successfully")
            artifact_manager.finalize_execution(pipeline_name, True)
            return True
        else:
            error_message = result.get("error", "Unknown error")
            typer.echo(f"❌ Pipeline failed: {error_message}")
            artifact_manager.finalize_execution(pipeline_name, False, error_message)
            return False

    except Exception as e:
        # Handle unexpected errors
        end_time = datetime.datetime.now()
        execution_time = end_time - start_time
        typer.echo(
            f"⏱️  Execution failed after {execution_time.total_seconds():.2f} seconds"
        )
        typer.echo(f"❌ Error: {e}")
        logger.debug(f"Exception during execution: {e}")
        artifact_manager.finalize_execution(pipeline_name, False, str(e))
        return False


# Helper function to execute operations and report results
def _log_pipeline_execution_details(
    operations: List[Dict[str, Any]],
    pipeline_name: str,
    profile_name: str,
    variables: Optional[Dict[str, Any]],
    execution_id: str,
) -> None:
    """Log pipeline execution details for debugging.

    Args:
    ----
        operations: List of operations in the pipeline
        pipeline_name: Name of the pipeline
        profile_name: Name of the profile to use
        variables: Dictionary of variables for the pipeline
        execution_id: Execution ID for tracking

    """
    logger.debug(f"Initializing executor with profile {profile_name}")
    logger.debug(f"Executing pipeline {pipeline_name} with profile {profile_name}")
    logger.debug(f"Variables: {variables}")
    logger.debug(f"Execution ID: {execution_id}")

    # Log operations for debugging
    logger.debug("Operations to execute:")
    for i, op in enumerate(operations):
        logger.debug(
            f"Operation {i}: {op.get('type')} - {op.get('id')} - {op.get('name', '')}"
        )
        if op.get("type") == "transform":
            # Log transform SQL for debugging
            logger.debug(f"Transform SQL: {op.get('query', '')}")


def _initialize_executor(
    profile_name: str,
    execution_id: str,
    artifact_manager: ArtifactManager,
    operations: List[Dict[str, Any]],
    variables: Optional[Dict[str, Any]],
) -> LocalExecutor:
    """Initialize the executor with proper configuration.

    Args:
    ----
        profile_name: Name of the profile to use
        execution_id: Execution ID for tracking
        artifact_manager: Artifact manager for tracking
        operations: List of operations in the pipeline
        variables: Dictionary of variables for the pipeline

    Returns:
    -------
        Configured LocalExecutor instance

    """
    # Initialize executor with project directory for UDF discovery
    project_dir = os.getcwd()
    executor = LocalExecutor(profile_name=profile_name, project_dir=project_dir)
    # Note: execution_id and artifact_manager are passed as parameters where needed
    # rather than assigned as attributes since they're not part of the LocalExecutor interface

    # Extract variables from different sources and build effective set
    set_variables = _extract_set_variables_from_operations(operations)
    executor.variables = _build_effective_variables(
        set_variables, executor.profile, variables
    )

    # Check DuckDB engine initialization
    _check_duckdb_engine(executor)

    return executor


# flake8: noqa: C901
def _execute_pipeline_operations_and_report(
    operations: List[Dict[str, Any]],
    pipeline_name: str,
    profile_name: str,
    variables: Optional[Dict[str, Any]],
    artifact_manager: ArtifactManager,
    execution_id: str,
) -> bool:
    """Execute a pipeline with the given operations.

    Args:
    ----
        operations: List of operations in the pipeline
        pipeline_name: Name of the pipeline
        profile_name: Name of the profile to use
        variables: Dictionary of variables for the pipeline
        artifact_manager: Artifact manager for tracking
        execution_id: Execution ID for tracking

    Returns:
    -------
        True if execution succeeded, False otherwise

    """
    # Log execution details
    _log_pipeline_execution_details(
        operations, pipeline_name, profile_name, variables, execution_id
    )

    # Initialize executor
    executor = _initialize_executor(
        profile_name, execution_id, artifact_manager, operations, variables
    )

    # Start execution timer
    start_time = datetime.datetime.now()
    typer.echo(f"⏱️  Starting execution at {start_time.strftime('%H:%M:%S')}")

    # Execute operations and handle result
    return _execute_and_handle_result(
        executor, operations, variables, pipeline_name, artifact_manager, start_time
    )


@pipeline_app.command("run")
def run_pipeline(
    pipeline_name: str = typer.Argument(
        ..., help="Name of the pipeline (omit .sf extension)"
    ),
    vars: Optional[str] = typer.Option(
        None, help="Pipeline variables as JSON or key=value pairs"
    ),
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="Profile to use (default: dev)"
    ),
    from_compiled: bool = typer.Option(
        False,
        "--from-compiled",
        help="Use existing compilation in target/compiled/ instead of recompiling",
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output with technical details"
    ),
):
    """Execute a pipeline end-to-end using the selected profile.
    This command automatically compiles the pipeline before running it, unless
    --from-compiled is specified, in which case it uses the existing compiled plan.

    Validation is always performed before execution to catch errors early.
    """
    # Configure logging based on command-specific flags
    configure_logging(verbose=verbose, quiet=quiet)

    (
        _project,
        _variables,
        _profile_name,
        _artifact_manager,
        _pipeline_path,
        _compiled_plan_path,
    ) = _setup_run_environment(pipeline_name, vars, profile)

    # Always validate pipeline before execution
    from sqlflow.cli.validation_helpers import validate_pipeline_with_caching

    try:
        errors = validate_pipeline_with_caching(_pipeline_path)

        if errors:
            if not quiet:
                typer.echo(
                    "❌ Pipeline validation failed. Aborting execution.", err=True
                )
                for error in errors:
                    typer.echo(f"  {error}", err=True)
            raise typer.Exit(code=1)
        else:
            if not quiet:
                logger.debug("✅ Pipeline validation passed")

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"❌ Validation error: {str(e)}", err=True)
        raise typer.Exit(code=1)

    operations = _get_execution_operations(
        from_compiled,
        _compiled_plan_path,
        _pipeline_path,
        _variables,
        pipeline_name,
        _profile_name,
    )

    # Initialize execution tracking - this returns execution_id needed by the execute helper
    execution_id, _ = _artifact_manager.initialize_execution(
        pipeline_name, _variables or {}, _profile_name
    )

    _execute_pipeline_operations_and_report(
        operations,
        pipeline_name,  # Pass original pipeline_name
        _profile_name,
        _variables,
        _artifact_manager,
        execution_id,
    )


@pipeline_app.command("list")
def list_pipelines(
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="Profile to use (default: dev)"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output with technical details"
    ),
):
    """List available pipelines in the project."""
    # Configure logging based on command-specific flags
    configure_logging(verbose=verbose, quiet=quiet)

    project = Project(os.getcwd(), profile_name=profile)
    profile_dict = project.get_profile()
    pipelines_dir = os.path.join(
        project.project_dir,
        profile_dict.get("paths", {}).get("pipelines", "pipelines"),
    )

    if not os.path.exists(pipelines_dir):
        typer.echo(f"Pipelines directory '{pipelines_dir}' not found.")
        raise typer.Exit(code=1)

    pipeline_files = [f for f in os.listdir(pipelines_dir) if f.endswith(".sf")]

    if not pipeline_files:
        typer.echo(f"No pipeline files found in '{pipelines_dir}'.")
        return

    typer.echo("Available pipelines:")
    for file_name in pipeline_files:
        pipeline_name = file_name[:-3]
        typer.echo(f"  - {pipeline_name}")


def _resolve_and_build_execution_order(
    plan: List[Dict[str, Any]], pipeline_name: Optional[str] = None
) -> tuple[DependencyResolver, List[str]]:
    """Resolve dependencies and build execution order for the pipeline plan."""
    dependency_resolver = _build_dependency_resolver(plan)
    execution_order = _resolve_execution_order(dependency_resolver, plan)
    return dependency_resolver, execution_order


def _print_step_success(
    step_type: str, step_name: str, row_count: Optional[int] = None
) -> None:
    """Print a clean success message for a completed step.

    Args:
    ----
        step_type: Type of step (load, transform, export)
        step_name: Name/identifier of the step
        row_count: Optional row count for data operations

    """
    emoji_map = {
        "load": "📥",
        "transform": "🔄",
        "export": "📤",
        "source_definition": "🔗",
    }

    emoji = emoji_map.get(step_type, "✅")

    if step_type == "load":
        verb = "Loaded"
    elif step_type == "transform":
        verb = "Created"
    elif step_type == "export":
        verb = "Exported"
    else:
        verb = "Completed"

    if row_count is not None:
        typer.echo(f"{emoji} {verb} {step_name} ({row_count:,} rows)")
    else:
        typer.echo(f"{emoji} {verb} {step_name}")


@pipeline_app.command("validate")
def validate_pipeline_command(
    pipeline_name: Optional[str] = typer.Argument(
        None, help="Name of the pipeline (omit .sf extension, or provide full path)"
    ),
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="Profile to use (default: dev)"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output with technical details"
    ),
    clear_cache: bool = typer.Option(
        False, "--clear-cache", help="Clear validation cache before validating"
    ),
):
    """Validate pipeline(s) without executing them.

    Validates pipeline syntax, connector configurations, and cross-references.
    Uses smart caching for improved performance on unchanged files.
    """
    from sqlflow.cli.validation_cache import ValidationCache
    from sqlflow.cli.validation_helpers import (
        print_validation_summary,
        validate_pipeline_with_caching,
    )

    # Configure logging
    configure_logging(verbose=verbose, quiet=quiet)

    # Clear cache if requested
    if clear_cache:
        cache = ValidationCache(".")
        cache.clear_cache()
        if not quiet:
            typer.echo("🗑️  Validation cache cleared")

    if pipeline_name:
        # Validate single pipeline
        try:
            # Get project and resolve pipeline path
            project = Project(os.getcwd(), profile_name=profile)
            pipeline_path = _resolve_pipeline_path(project, pipeline_name)

            if not os.path.exists(pipeline_path):
                typer.echo(f"❌ Pipeline {pipeline_name} not found at {pipeline_path}")
                raise typer.Exit(code=1)

            # Validate with caching
            errors = validate_pipeline_with_caching(pipeline_path)

            # Print results
            print_validation_summary(errors, pipeline_name, quiet=quiet)

            # Exit with error code if validation failed
            if errors:
                raise typer.Exit(code=1)

        except typer.Exit:
            raise
        except Exception as e:
            typer.echo(f"❌ Validation failed: {str(e)}", err=True)
            raise typer.Exit(code=1)
    else:
        # Validate all pipelines in the project
        try:
            project = Project(os.getcwd(), profile_name=profile)
            pipelines_dir = os.path.join(project.project_dir, "pipelines")

            if not os.path.exists(pipelines_dir):
                typer.echo(f"❌ Pipelines directory not found: {pipelines_dir}")
                raise typer.Exit(code=1)

            # Find all pipeline files
            pipeline_files = []
            for file in os.listdir(pipelines_dir):
                if file.endswith(".sf"):
                    pipeline_files.append(file)

            if not pipeline_files:
                typer.echo(f"❌ No pipeline files found in {pipelines_dir}")
                raise typer.Exit(code=1)

            # Validate each pipeline
            total_errors = 0
            failed_pipelines = []

            for pipeline_file in sorted(pipeline_files):
                pipeline_path = os.path.join(pipelines_dir, pipeline_file)
                current_pipeline_name = os.path.splitext(pipeline_file)[0]

                try:
                    errors = validate_pipeline_with_caching(pipeline_path)

                    if errors:
                        total_errors += len(errors)
                        failed_pipelines.append(current_pipeline_name)
                        if not quiet:
                            typer.echo(f"\n📋 Pipeline: {current_pipeline_name}")
                            print_validation_summary(
                                errors, current_pipeline_name, quiet=True
                            )
                    else:
                        if not quiet:
                            typer.echo(f"✅ {current_pipeline_name}")

                except Exception as e:
                    total_errors += 1
                    failed_pipelines.append(current_pipeline_name)
                    typer.echo(f"❌ {current_pipeline_name}: {str(e)}", err=True)

            # Print summary
            if not quiet:
                typer.echo("\n📊 Validation Summary:")
                typer.echo(f"  Total pipelines: {len(pipeline_files)}")
                typer.echo(f"  Passed: {len(pipeline_files) - len(failed_pipelines)}")
                typer.echo(f"  Failed: {len(failed_pipelines)}")
                typer.echo(f"  Total errors: {total_errors}")

            if failed_pipelines:
                if not quiet:
                    typer.echo(f"\n❌ Failed pipelines: {', '.join(failed_pipelines)}")
                raise typer.Exit(code=1)
            else:
                if not quiet:
                    typer.echo("\n✅ All pipelines passed validation!")

        except typer.Exit:
            raise
        except Exception as e:
            typer.echo(f"❌ Validation failed: {str(e)}", err=True)
            raise typer.Exit(code=1)
