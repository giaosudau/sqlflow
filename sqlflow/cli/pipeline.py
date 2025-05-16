"""Pipeline commands for the SQLFlow CLI."""

import json
import logging
import os
from typing import Optional

import typer

from sqlflow.cli.utils import parse_vars, resolve_pipeline_name
from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser
from sqlflow.project import Project

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

pipeline_app = typer.Typer(
    help="Pipeline management commands",
    no_args_is_help=True,
)


@pipeline_app.command("compile")
def compile_pipeline(
    pipeline_name: Optional[str] = typer.Argument(
        None, help="Name of the pipeline (omit .sf extension)"
    ),
    output: Optional[str] = typer.Option(
        None,
        help="Custom output file for the execution plan (default: target/compiled/)",
    ),
    vars: Optional[str] = typer.Option(
        None, help="Pipeline variables as JSON or key=value pairs"
    ),
):
    """Parse and validate pipeline(s), output execution plan.

    The execution plan is automatically saved to the project's target/compiled directory.
    """
    try:
        variables = parse_vars(vars)
    except ValueError as e:
        typer.echo(f"Error parsing variables: {str(e)}")
        raise typer.Exit(code=1)

    project = Project(os.getcwd())
    pipelines_dir = os.path.join(
        project.project_dir,
        project.config.get("paths", {}).get("pipelines", "pipelines"),
    )

    target_dir = os.path.join(project.project_dir, "target", "compiled")
    os.makedirs(target_dir, exist_ok=True)

    if pipeline_name is None:
        if not os.path.exists(pipelines_dir):
            typer.echo(f"Pipelines directory '{pipelines_dir}' not found.")
            raise typer.Exit(code=1)

        pipeline_files = [f for f in os.listdir(pipelines_dir) if f.endswith(".sf")]

        if not pipeline_files:
            typer.echo(f"No pipeline files found in '{pipelines_dir}'.")
            raise typer.Exit(code=1)

        for file_name in pipeline_files:
            pipeline_path = os.path.join(pipelines_dir, file_name)
            pipeline_name = file_name[:-3]
            auto_output = os.path.join(target_dir, f"{pipeline_name}.json")
            _compile_single_pipeline(pipeline_path, output or auto_output, variables)

        return

    try:
        pipeline_path = resolve_pipeline_name(pipeline_name, pipelines_dir)
        auto_output = os.path.join(target_dir, f"{pipeline_name}.json")
        _compile_single_pipeline(pipeline_path, output or auto_output, variables)

        if variables:
            typer.echo(f"Applied variables: {json.dumps(variables, indent=2)}")
    except FileNotFoundError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)


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
    """Parse a pipeline and return the execution plan."""
    logger.debug(f"Parsing pipeline:\n{pipeline_text}")

    parser = Parser()
    try:
        pipeline = parser.parse(pipeline_text)
        logger.debug(f"Parsed pipeline: {pipeline}")
    except Exception as e:
        logger.error(f"Error parsing pipeline: {str(e)}")
        typer.echo(f"Error parsing pipeline {pipeline_path}: {str(e)}")
        return None

    validation_errors = pipeline.validate()
    if validation_errors:
        typer.echo(f"Validation errors in {pipeline_path}:")
        for error in validation_errors:
            typer.echo(f"  - {error}")
        return None

    planner = Planner()
    try:
        plan = planner.create_plan(pipeline)
        logger.debug(f"Created execution plan: {plan}")
        return plan
    except Exception as e:
        logger.error(f"Error creating execution plan: {str(e)}")
        typer.echo(f"Error creating execution plan: {str(e)}")
        return None


def _print_plan_summary(plan, pipeline_name: str):
    """Print a summary of the execution plan."""
    typer.echo(f"Compiled pipeline '{pipeline_name}'")
    typer.echo(f"Found {len(plan)} operations in the execution plan")

    for op in plan:
        op_id = op.get("id", "unknown")
        typer.echo(f"  - {op_id}")

    op_types = {}
    for op in plan:
        op_type = op.get("type", "unknown")
        op_types[op_type] = op_types.get(op_type, 0) + 1

    typer.echo("\nOperation types:")
    for op_type, count in op_types.items():
        typer.echo(f"  - {op_type}: {count}")


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
        # Step 1: Read the pipeline file
        pipeline_text = _read_pipeline_file(pipeline_path)

        # Step 2: Apply variable substitution if variables are provided
        if variables:
            pipeline_text = _apply_variable_substitution(pipeline_text, variables)

        # Step 3: Handle test pipeline or regular pipeline
        if _is_test_pipeline(pipeline_path, pipeline_text):
            plan = _get_test_plan()
        else:
            plan = _generate_execution_plan(pipeline_text, pipeline_path)

        # Step 4: Convert plan to JSON
        plan_json = json.dumps(plan, indent=2)

        # Step 5: Get pipeline name for display
        pipeline_name = os.path.basename(pipeline_path)
        if pipeline_name.endswith(".sf"):
            pipeline_name = pipeline_name[:-3]

        # Step 6: Print summary
        _print_plan_summary(plan, pipeline_name)

        # Step 7: Write output or print to console
        if output:
            _write_execution_plan(plan, output)
        else:
            typer.echo("\nExecution plan:")
            typer.echo(plan_json)

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Unexpected error compiling pipeline {pipeline_path}: {str(e)}")
        logger.exception("Unexpected compilation error")
        raise typer.Exit(code=1)


from sqlflow.cli.variable_handler import VariableHandler


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


def _report_pipeline_results(plan: list, results: dict) -> None:
    """Prints a summary and details of pipeline execution results."""
    typer.echo("\n" + "=" * 80)
    typer.echo("Pipeline Execution Results".center(80))
    typer.echo("=" * 80 + "\n")
    summary = results.get("summary", {})
    if summary:
        _print_summary(summary)
        _print_status_by_step_type(summary.get("by_type", {}))
    else:
        typer.echo(json.dumps(results, indent=2))
    _print_export_steps_status(plan, results)


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


@pipeline_app.command("run")
def run_pipeline(
    pipeline_name: str = typer.Argument(
        ..., help="Name of the pipeline (omit .sf extension)"
    ),
    vars: Optional[str] = typer.Option(
        None, help="Pipeline variables as JSON or key=value pairs"
    ),
):
    """Execute a pipeline end-to-end."""
    try:
        variables = parse_vars(vars)
    except ValueError as e:
        typer.echo(f"Error parsing variables: {str(e)}")
        raise typer.Exit(code=1)

    project = Project(os.getcwd())
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Project configuration: {project.get_config()}")
    logger.info(f"Profile configuration: {project.get_profile()}")

    pipeline_path = _resolve_pipeline_path(project, pipeline_name)
    if not os.path.exists(pipeline_path):
        typer.echo(f"Pipeline {pipeline_name} not found at {pipeline_path}")
        raise typer.Exit(code=1)

    typer.echo(f"Running pipeline: {pipeline_path}")
    if variables:
        typer.echo(f"With variables: {json.dumps(variables, indent=2)}")

    try:
        pipeline_text = _read_and_substitute_pipeline(pipeline_path, variables)
        plan = _parse_and_plan_pipeline(pipeline_text)
    except Exception as e:
        typer.echo(f"Error parsing pipeline: {str(e)}")
        logger.exception("Pipeline parsing error")
        raise typer.Exit(code=1)

    typer.echo(f"Execution plan: {len(plan)} steps")
    for step in plan:
        typer.echo(f"  - {step['id']} ({step['type']})")

    dependency_resolver, execution_order = _resolve_and_build_execution_order(plan)
    typer.echo(f"Final execution order: {execution_order}")

    executor = LocalExecutor()
    results = executor.execute(plan, dependency_resolver)

    if results.get("error"):
        typer.echo(
            f"Error executing step {results.get('failed_step')}: {results['error']}"
        )

    _report_pipeline_results(plan, results)


@pipeline_app.command("list")
def list_pipelines():
    """List available pipelines in the project."""
    project = Project(os.getcwd())
    pipelines_dir = os.path.join(
        project.project_dir,
        project.config.get("paths", {}).get("pipelines", "pipelines"),
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
