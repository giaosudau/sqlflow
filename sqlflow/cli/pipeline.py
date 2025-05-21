"""Pipeline commands for the SQLFlow CLI."""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import typer

from sqlflow.cli.utils import parse_vars, resolve_pipeline_name
from sqlflow.cli.variable_handler import VariableHandler
from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.core.sql_generator import SQLGenerator
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
        pipeline_name: Name of the pipeline
        profile: Profile to use
        variables: Variables for the pipeline

    Returns:
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
        pipeline_path: Path to the pipeline file

    Returns:
        Contents of the pipeline file
    """
    with open(pipeline_path, "r") as f:
        return f.read()


def _apply_variable_substitution(pipeline_text: str, variables: Dict[str, Any]) -> str:
    """Apply variable substitution to pipeline text.

    Args:
        pipeline_text: Original pipeline text
        variables: Variables for substitution

    Returns:
        Pipeline text with variables substituted
    """
    # TODO: Implement variable substitution logic
    return pipeline_text


def _is_test_pipeline(pipeline_path: str, pipeline_text: str) -> bool:
    """Check if this is a test pipeline.

    Args:
        pipeline_path: Path to the pipeline file
        pipeline_text: Contents of the pipeline file

    Returns:
        True if this is a test pipeline, False otherwise
    """
    # TODO: Implement test pipeline detection logic
    return False


def _get_test_plan() -> List[Dict[str, Any]]:
    """Get the execution plan for a test pipeline.

    Returns:
        List of operations for the test pipeline
    """
    # TODO: Implement test plan generation
    return []


def _handle_source_error(error: Exception) -> None:
    """Handle and format SOURCE directive errors.

    Args:
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
        pipeline_path: Path to the pipeline file
        target_path: Path to save the compilation output
        variables: Variables for variable substitution
        save_plan: Whether to save the plan to disk

    Returns:
        Execution plan as a list of operations
    """
    try:
        # Step 1: Read and prepare pipeline
        pipeline_text = _read_pipeline_file(pipeline_path)
        pipeline_name = os.path.basename(pipeline_path)
        if pipeline_name.endswith(".sf"):
            pipeline_name = pipeline_name[:-3]

        # Step 2: Apply variable substitution if needed
        if variables:
            pipeline_text = _apply_variable_substitution(pipeline_text, variables)

        # Step 3: Handle test pipeline or regular pipeline
        if _is_test_pipeline(pipeline_path, pipeline_text):
            operations = _get_test_plan()
        else:
            # Parse pipeline and create execution plan
            parser = Parser()
            pipeline = parser.parse(pipeline_text)
            planner = Planner()
            operations = planner.create_plan(pipeline)

        # Step 4: Print summary
        _print_plan_summary(operations, pipeline_name)

        # Step 5: Save the plan if needed
        if save_plan:
            plan_data = {"operations": operations}
            _write_execution_plan(plan_data, target_path)

        return operations

    except Exception as e:
        # Log the full error message at debug level
        logger.debug(f"Error compiling pipeline: {str(e)}", exc_info=True)
        _handle_source_error(e)
        raise typer.Exit(code=1)


def _build_execution_graph(operations: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Build a simple execution graph from operations.

    Args:
        operations: List of operations

    Returns:
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

        _compile_pipeline_to_plan(pipeline_path, final_output_path, variables)

        if variables:  # Log applied variables only once after successful compilation
            typer.echo(f"Applied variables: {json.dumps(variables, indent=2)}")

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
        typer.echo(f"Error compiling pipeline {pipeline_name}: {error_msg}")
        raise typer.Exit(code=1)


def _do_compile_all_pipelines(
    pipelines_dir: str, target_dir: str, variables: Optional[Dict[str, Any]]
):
    """Compiles all .sf pipelines in the specified directory."""
    if not os.path.exists(pipelines_dir):
        typer.echo(f"Pipelines directory '{pipelines_dir}' not found.")
        raise typer.Exit(code=1)

    pipeline_files = [f for f in os.listdir(pipelines_dir) if f.endswith(".sf")]

    if not pipeline_files:
        typer.echo(f"No pipeline files found in '{pipelines_dir}'.")
        # Consider if this should be an error or just a silent return.
        # For now, exiting as it implies a misconfiguration or empty project.
        raise typer.Exit(code=1)

    typer.echo(
        f"Found {len(pipeline_files)} pipeline(s) in '{pipelines_dir}'. Compiling all..."
    )
    compiled_count = 0
    error_count = 0

    for file_name in pipeline_files:
        pipeline_path = os.path.join(pipelines_dir, file_name)
        name_without_ext = file_name[:-3]
        auto_output_path = os.path.join(target_dir, f"{name_without_ext}.json")
        try:
            typer.echo(f"Compiling {file_name}...")
            _compile_pipeline_to_plan(pipeline_path, auto_output_path, variables)
            compiled_count += 1
        except typer.Exit:
            # Exit was already handled in _compile_pipeline_to_plan
            # Just count it as an error and continue with other pipelines
            error_count += 1
        except Exception as e:
            error_count += 1
            # Format error message cleanly
            error_msg = str(e)
            if " at line" in error_msg:
                error_msg = error_msg.split(" at line")[0]
            typer.echo(f"Error compiling pipeline {file_name}: {error_msg}")
            # Log at debug level to avoid duplicate error messages
            logger.debug(f"Error compiling pipeline {file_name}", exc_info=True)
            # Continue to compile other pipelines

    if (
        variables and compiled_count > 0
    ):  # Log variables if any pipeline was successfully compiled with them
        typer.echo(
            f"Applied variables to all compiled pipelines: {json.dumps(variables, indent=2)}"
        )

    if error_count > 0:
        typer.echo(
            f"Finished compiling all. {compiled_count} succeeded, {error_count} failed."
        )
        raise typer.Exit(code=1)  # Exit with error if any compilation failed
    else:
        typer.echo(f"Successfully compiled {compiled_count} pipeline(s).")


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
    """
    # Configure logging based on command-specific flags
    configure_logging(verbose=verbose, quiet=quiet)

    _variables, _project, _pipelines_dir, _target_dir = _prepare_compile_environment(
        vars, profile
    )

    if pipeline_name:
        if os.path.isfile(pipeline_name):  # User provided a full path
            _pipeline_path = pipeline_name
            _name_for_output = os.path.splitext(os.path.basename(_pipeline_path))[0]
            _auto_output_path = os.path.join(_target_dir, f"{_name_for_output}.json")
            _final_output_path = output or _auto_output_path
            try:
                _compile_pipeline_to_plan(
                    _pipeline_path, _final_output_path, _variables
                )
                if _variables:
                    typer.echo(f"Applied variables: {json.dumps(_variables, indent=2)}")
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
                typer.echo(f"Error compiling pipeline {_pipeline_path}: {error_msg}")
                raise typer.Exit(code=1)

        else:  # User provided a name to be resolved
            _do_compile_single_pipeline(
                pipeline_name, output, _variables, _pipelines_dir, _target_dir
            )
    else:
        if output:
            typer.echo(
                "Warning: --output option is ignored when compiling all pipelines."
            )
        _do_compile_all_pipelines(_pipelines_dir, _target_dir, _variables)


def _parse_pipeline(pipeline_text: str, pipeline_path: str):
    """Parse a pipeline file using the SQLFlow parser.

    Args:
        pipeline_text: Text of the pipeline file
        pipeline_path: Path to the pipeline file

    Returns:
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
        plan_data: The execution plan data
        target_path: Path to save the plan to
    """
    # Add metadata to the plan
    full_plan = {
        "pipeline_metadata": {
            "name": os.path.basename(target_path).replace(".json", ""),
            "compiled_at": datetime.now().isoformat(),
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
        operations = _compile_pipeline_to_plan(
            pipeline_path=pipeline_path,
            target_path=output,
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


def _substitute_pipeline_variables(pipeline_text: str, variables: dict) -> str:
    """Substitute variables in the pipeline text."""
    handler = VariableHandler(variables)
    if not handler.validate_variable_usage(pipeline_text):
        typer.echo("Warning: Some variables are missing and don't have defaults")
    return handler.substitute_variables(pipeline_text)


def _build_dependency_resolver(plan: list) -> DependencyResolver:
    """Build and return a DependencyResolver for the plan."""
    dependency_resolver = DependencyResolver()
    for step in plan:
        step_id = step["id"]
        for dependency in step.get("depends_on", []):
            dependency_resolver.add_dependency(step_id, dependency)
    return dependency_resolver


def _find_entry_points(plan: list) -> list:
    """Find entry points (steps with no dependencies) in the plan."""
    return [
        step["id"]
        for step in plan
        if not step.get("depends_on") or len(step.get("depends_on", [])) == 0
    ]


def _build_execution_order_from_entry_points(
    dependency_resolver: DependencyResolver, entry_points: list
) -> list:
    """Build execution order from entry points using the dependency resolver."""
    execution_order = []
    for entry_point in entry_points:
        try:
            deps = dependency_resolver.resolve_dependencies(entry_point)
            for dep in deps:
                if dep not in execution_order:
                    execution_order.append(dep)
        except Exception as e:
            typer.echo(
                f"Warning: Error resolving dependencies from {entry_point}: {str(e)}"
            )
    return execution_order


def _resolve_and_build_execution_order(plan: list) -> tuple[DependencyResolver, list]:
    """Resolve dependencies and build execution order for the pipeline plan."""
    dependency_resolver = _build_dependency_resolver(plan)
    all_step_ids = [step["id"] for step in plan]
    entry_points = _find_entry_points(plan)
    if not entry_points and plan:
        entry_points = [plan[0]["id"]]
    execution_order = _build_execution_order_from_entry_points(
        dependency_resolver, entry_points
    )
    for step_id in all_step_ids:
        if step_id not in execution_order:
            execution_order.append(step_id)
    dependency_resolver.last_resolved_order = execution_order
    return dependency_resolver, execution_order


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
            else warning_color if success > 0 else error_color
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


def _read_and_substitute_pipeline(pipeline_path: str, variables: dict) -> str:
    """Read the pipeline file and apply variable substitution."""
    with open(pipeline_path, "r") as f:
        pipeline_text = f.read()
    return _substitute_pipeline_variables(pipeline_text, variables)


def _parse_and_plan_pipeline(pipeline_text: str) -> list:
    """Parse the pipeline text and create an execution plan."""
    parser = Parser()
    ast = parser.parse(pipeline_text)
    planner = Planner()
    return planner.create_plan(ast)


def _load_execution_plan(plan_path: str) -> List[Dict[str, Any]]:
    """Load execution plan from a JSON file.

    Args:
        plan_path: Path to the execution plan JSON file

    Returns:
        Execution plan as a list of operations

    Raises:
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
) -> List[Dict[str, Any]]:
    """Loads a compiled plan or compiles the pipeline to get operations."""
    if from_compiled_arg:
        typer.echo(f"Using existing compilation from {compiled_plan_path}")
        if not os.path.exists(compiled_plan_path):
            typer.echo(
                f"No existing compilation found at {compiled_plan_path}. "
                f"Run 'sqlflow pipeline compile {pipeline_name}' first or remove --from-compiled flag."
            )
            raise typer.Exit(code=1)
        try:
            with open(compiled_plan_path, "r") as f:
                plan_data = json.load(f)
            return plan_data.get("operations", [])
        except Exception as e:
            typer.echo(f"Error loading compiled plan: {str(e)}")
            raise typer.Exit(code=1)
    else:
        typer.echo(f"Compiling pipeline: {pipeline_path}")
        try:
            # Let _compile_pipeline_to_plan handle all error formatting and logging
            return _compile_pipeline_to_plan(
                pipeline_path=pipeline_path,
                target_path=compiled_plan_path,  # Save compilation here
                variables=variables,
            )
        except typer.Exit:
            # Exit was already handled in _compile_pipeline_to_plan with appropriate error message
            # No need to add additional error messages - just propagate the exit
            raise
        except Exception as e:
            # Only log unexpected errors that weren't handled in _compile_pipeline_to_plan
            # Log at debug level to avoid duplicate error messages
            logger.debug("Unexpected error during pipeline compilation", exc_info=True)
            # Format the error message cleanly - remove line numbers and traceback info
            error_msg = str(e)
            if " at line" in error_msg:
                error_msg = error_msg.split(" at line")[0]
            typer.echo(f"Error compiling pipeline: {error_msg}")
            raise typer.Exit(code=1)


# Helper function to execute operations and report results
def _execute_pipeline_operations_and_report(
    operations: List[Dict[str, Any]],
    pipeline_name: str,
    profile_name: str,
    variables: Optional[Dict[str, Any]],
    artifact_manager: ArtifactManager,
    execution_id: str,
):
    """Executes the pipeline operations and reports results."""
    if not operations:
        typer.echo("No operations to execute.")
        artifact_manager.finalize_execution(
            pipeline_name, True
        )  # Finalize with success if no ops
        return

    typer.echo(f"Execution plan: {len(operations)} steps")
    for op in operations:
        typer.echo(f"  - {op['id']} ({op['type']})")

    dependency_resolver, execution_order = _resolve_and_build_execution_order(
        operations
    )
    typer.echo(f"Final execution order: {execution_order}")

    sql_generator = SQLGenerator(dialect="duckdb")  # TODO: Make dialect configurable
    executor = LocalExecutor(profile_name=profile_name)

    # Set execution_id and artifact_manager on the executor instance for its internal use
    executor.execution_id = execution_id
    executor.artifact_manager = artifact_manager

    # Ensure the executor has the variables in the profile as well
    if variables:
        # Initialize profile to an empty dict if None
        if executor.profile is None:
            executor.profile = {}
        # Create a variables dict in profile if it doesn't exist
        if "variables" not in executor.profile:
            executor.profile["variables"] = {}
        # Update the variables in the profile
        executor.profile["variables"].update(variables)
        logger.debug(f"Updated executor profile with variables: {variables}")

    # executor.sql_generator is already initialized by its __init__

    success = True

    typer.echo("Executing pipeline...")
    for operation_id in execution_order:
        operation = next((op for op in operations if op["id"] == operation_id), None)
        if not operation:
            logger.warning(
                f"Operation {operation_id} in execution_order not found in operations list."
            )
            continue

        context = {"variables": variables or {}, "execution_id": execution_id}
        sql = sql_generator.generate_operation_sql(operation, context)

        # Pass pipeline_name to execute_step for artifact recording
        artifact_manager.record_operation_start(
            pipeline_name, operation_id, operation.get("type", "unknown"), sql
        )

        typer.echo(f"  Executing {operation_id}...")
        # Pass pipeline_name to execute_step so it can also record artifacts if it needs to
        result = executor.execute_step(operation, pipeline_name=pipeline_name)

        # record_operation_completion is now handled inside execute_step
        # so we only need to check the result status here for breaking the loop
        if result.get("status") != "success":
            success = False
            typer.echo(f"    ❌ Failed: {result.get('error', 'Unknown error')}")
            # No need to call artifact_manager.record_operation_completion for failure here
            # as execute_step is expected to do it.
            break

    artifact_manager.finalize_execution(pipeline_name, success)
    executor._generate_step_summary(operations)
    _report_pipeline_results(operations, executor.results)


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

    operations = _get_execution_operations(
        from_compiled, _compiled_plan_path, _pipeline_path, _variables, pipeline_name
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
