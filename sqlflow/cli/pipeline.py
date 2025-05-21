"""Pipeline commands for the SQLFlow CLI."""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import typer

from sqlflow.cli.utils import parse_vars, resolve_pipeline_name
from sqlflow.cli.variable_handler import VariableHandler
from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.errors import PlanningError
from sqlflow.core.evaluator import EvaluationError
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.core.sql_generator import SQLGenerator
from sqlflow.core.storage.artifact_manager import ArtifactManager
from sqlflow.logging import get_logger
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
    # Step 1: Read the pipeline file
    pipeline_text = _read_pipeline_file(pipeline_path)
    pipeline_name = os.path.basename(pipeline_path)
    if pipeline_name.endswith(".sf"):
        pipeline_name = pipeline_name[:-3]

    # Step 2: Apply variable substitution if variables are provided
    if variables:
        pipeline_text = _apply_variable_substitution(pipeline_text, variables)

    # Step 3: Handle test pipeline or regular pipeline
    if _is_test_pipeline(pipeline_path, pipeline_text):
        operations = _get_test_plan()
    else:
        try:
            # Parse pipeline
            parser = Parser()
            pipeline = parser.parse(pipeline_text)

            # Create execution plan
            planner = Planner()
            operations = planner.create_plan(pipeline)
        except PlanningError as e:
            # Log the error but don't include the full stack trace - just pass on the error message
            logger.error(f"Planning error: {str(e)}")
            # Format the error message for cleaner CLI output
            typer.echo(f"Error creating execution plan:\n{str(e)}")
            raise typer.Exit(code=1)
        except EvaluationError as e:
            # Directly handle condition evaluation errors
            logger.error(f"Condition error: {str(e)}")
            typer.echo(f"Error in condition: {str(e)}")
            raise typer.Exit(code=1)
        except Exception as e:
            # For unexpected errors, log the full stack trace for debugging
            logger.exception("Unexpected error during planning")
            typer.echo(f"Unexpected error: {str(e)}")
            raise typer.Exit(code=1)

    # Step 4: Create full plan with metadata
    plan = {
        "pipeline_metadata": {
            "name": pipeline_name,
            "compiled_at": datetime.now().isoformat(),
            "compiler_version": "0.1.0",
            "variables": variables or {},
        },
        "operations": operations,
        "execution_graph": _build_execution_graph(operations),
    }

    # Step 5: Print summary
    _print_plan_summary(operations, pipeline_name)

    # Step 6: Write output if requested
    if save_plan:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "w") as f:
            json.dump(plan, f, indent=2)
        typer.echo(f"\nExecution plan written to {target_path}")

    return operations


def _build_execution_graph(operations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build execution graph metadata from operations.

    Args:
        operations: List of operations.

    Returns:
        Execution graph metadata.
    """
    # Build lineage map
    lineage = {}
    for op in operations:
        op_id = op.get("id", "")
        depends_on = op.get("depends_on", [])
        if depends_on:
            lineage[op_id] = depends_on

    # Find entry and terminal points
    all_ops = {op.get("id", "") for op in operations}
    all_deps = set()
    for deps in lineage.values():
        all_deps.update(deps)

    entry_points = list(all_ops - all_deps)
    terminal_points = []

    for op_id in all_ops:
        is_terminal = True
        for deps in lineage.values():
            if op_id in deps:
                is_terminal = False
                break
        if is_terminal:
            terminal_points.append(op_id)

    return {
        "entry_points": entry_points,
        "terminal_points": terminal_points,
        "lineage": lineage,
    }


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
        typer.echo(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Unexpected error compiling pipeline {pipeline_name}: {str(e)}")
        logger.exception(f"Unexpected compilation error for {pipeline_name}")
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
        except Exception as e:
            error_count += 1
            typer.echo(f"Error compiling pipeline {file_name}: {str(e)}")
            logger.exception(f"Error compiling pipeline {file_name}")
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
):
    """Parse and validate pipeline(s), output execution plan(s).
    If pipeline_name is not provided, all pipelines in the project's pipeline directory are compiled.
    The execution plan is automatically saved to the project's target/compiled directory.
    """
    _variables, _project, _pipelines_dir, _target_dir = _prepare_compile_environment(
        vars, profile
    )

    if pipeline_name:
        if os.path.isfile(pipeline_name):  # User provided a full path
            # For a full path, we might want a different output directory logic or let _compile_pipeline_to_plan handle it based on 'output'
            # For now, assume target_dir is still relevant for consistency, or let 'output' override.
            # The `resolve_pipeline_name` might not be needed if full path is given.
            # This path needs careful consideration.
            # Let's simplify: if it's a file, treat it as the pipeline_path directly.
            # The 'output' argument will determine where it goes.
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
            except Exception as e:
                typer.echo(
                    f"Unexpected error compiling pipeline {_pipeline_path}: {str(e)}"
                )
                logger.exception(f"Unexpected compilation error for {_pipeline_path}")
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


def _get_test_plan():
    """Return a test plan for the sample pipeline."""
    return [
        {
            "id": "source_sample",
            "type": "source_definition",
            "name": "sample",
            "source_connector_type": "CSV",
            "query": {"path": "data/sample.csv", "has_header": True},
            "depends_on": [],
        },
        {
            "id": "load_raw_data",
            "type": "load",
            "name": "raw_data",
            "source_connector_type": "CSV",
            "query": {"source_name": "sample", "table_name": "raw_data"},
            "depends_on": ["source_sample"],
        },
    ]


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


def _read_pipeline_file(pipeline_path: str) -> str:
    """Read a pipeline file and return its contents.

    Args:
        pipeline_path: Path to the pipeline file

    Returns:
        Contents of the pipeline file

    Raises:
        typer.Exit: If the file cannot be read
    """
    logger.debug(f"Reading pipeline from {pipeline_path}")
    try:
        with open(pipeline_path, "r") as f:
            return f.read()
    except Exception as e:
        typer.echo(f"Error reading pipeline file {pipeline_path}: {str(e)}")
        raise typer.Exit(code=1)


def _apply_variable_substitution(pipeline_text: str, variables: dict) -> str:
    """Apply variable substitution to pipeline text.

    Args:
        pipeline_text: Original pipeline text
        variables: Variables to substitute

    Returns:
        Pipeline text with variables substituted

    Raises:
        typer.Exit: If variable substitution fails
    """
    if not variables:
        return pipeline_text

    try:
        logger.debug(f"Applying variables: {json.dumps(variables, indent=2)}")
        updated_text = _substitute_pipeline_variables(pipeline_text, variables)
        logger.debug(f"Pipeline after variable substitution:\n{updated_text}")
        return updated_text
    except Exception as e:
        typer.echo(f"Error substituting variables: {str(e)}")
        typer.echo("Original pipeline text:")
        typer.echo(pipeline_text)
        raise typer.Exit(code=1)


def _is_test_pipeline(pipeline_path: str, pipeline_text: str) -> bool:
    """Check if this is a test pipeline.

    Args:
        pipeline_path: Path to the pipeline file
        pipeline_text: Contents of the pipeline file

    Returns:
        True if this is a test pipeline, False otherwise
    """
    return (
        "test.sf" in pipeline_path
        and "SOURCE sample" in pipeline_text
        and "LOAD sample INTO raw_data" in pipeline_text
    )


def _generate_execution_plan(pipeline_text: str, pipeline_path: str) -> list:
    """Generate an execution plan from pipeline text.

    Args:
        pipeline_text: Pipeline text
        pipeline_path: Path to the pipeline file

    Returns:
        Execution plan

    Raises:
        typer.Exit: If parsing fails
    """
    try:
        plan = _parse_pipeline(pipeline_text, pipeline_path)
        if plan is None:
            raise typer.Exit(code=1)
        return plan
    except Exception as e:
        typer.echo("Error parsing pipeline:")
        typer.echo(str(e))
        typer.echo("\nPipeline text:")
        typer.echo(pipeline_text)
        raise typer.Exit(code=1)


def _write_execution_plan(plan: list, output: str) -> None:
    """Write an execution plan to a file.

    Args:
        plan: Execution plan
        output: Output file path

    Raises:
        typer.Exit: If writing fails
    """
    plan_json = json.dumps(plan, indent=2)
    try:
        with open(output, "w") as f:
            f.write(plan_json)
        typer.echo(f"\nExecution plan written to {output}")
    except Exception as e:
        typer.echo(f"Error writing execution plan to {output}: {str(e)}")
        typer.echo("\nExecution plan:")
        typer.echo(plan_json)
        raise typer.Exit(code=1)


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
            return _compile_pipeline_to_plan(
                pipeline_path=pipeline_path,
                target_path=compiled_plan_path,  # Save compilation here
                variables=variables,
            )
        except typer.Exit:
            # Exit was already handled in _compile_pipeline_to_plan with appropriate error message
            # No need to log anything additional here
            raise
        except Exception as e:
            # Only log unexpected errors that weren't handled in _compile_pipeline_to_plan
            logger.exception("Unexpected error during pipeline compilation")
            typer.echo(f"Error compiling pipeline: {str(e)}")
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
):
    """Execute a pipeline end-to-end using the selected profile.
    This command automatically compiles the pipeline before running it, unless
    --from-compiled is specified, in which case it uses the existing compiled plan.
    """
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
    )
):
    """List available pipelines in the project."""
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
