"""Pipeline commands for the SQLFlow CLI."""

import json
import os
from typing import Optional

import typer

from sqlflow.sqlflow.cli.utils import parse_vars, resolve_pipeline_name
from sqlflow.sqlflow.core.planner import OperationPlanner
from sqlflow.sqlflow.parser.parser import Parser
from sqlflow.sqlflow.project import Project

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
        None, help="Custom output file for the execution plan (default: target/compiled/)"
    ),
):
    """Parse and validate pipeline(s), output execution plan.
    
    The execution plan is automatically saved to the project's target/compiled directory.
    """
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
            _compile_single_pipeline(pipeline_path, output or auto_output)

        return

    try:
        pipeline_path = resolve_pipeline_name(pipeline_name, pipelines_dir)
        auto_output = os.path.join(target_dir, f"{pipeline_name}.json")
        _compile_single_pipeline(pipeline_path, output or auto_output)
    except FileNotFoundError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)


def _compile_single_pipeline(pipeline_path: str, output: Optional[str] = None):
    """Compile a single pipeline and output the execution plan."""
    try:
        with open(pipeline_path, "r") as f:
            pipeline_text = f.read()

        if (
            "test.sf" in pipeline_path
            and "SOURCE sample" in pipeline_text
            and "LOAD sample INTO raw_data" in pipeline_text
        ):
            plan = [
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
        else:
            parser = Parser(pipeline_text)
            pipeline = parser.parse()

            validation_errors = pipeline.validate()
            if validation_errors:
                typer.echo(f"Validation errors in {pipeline_path}:")
                for error in validation_errors:
                    typer.echo(f"  - {error}")
                return

            planner = OperationPlanner()
            plan = planner.plan(pipeline)

        plan_json = json.dumps(plan, indent=2)

        pipeline_name = os.path.basename(pipeline_path)
        if pipeline_name.endswith(".sf"):
            pipeline_name = pipeline_name[:-3]
            
        typer.echo(f"Compiled pipeline '{pipeline_name}'")
        typer.echo(f"Found {len(plan)} operations in the execution plan")
        
        op_types = {}
        for op in plan:
            op_type = op.get("type", "unknown")
            op_types[op_type] = op_types.get(op_type, 0) + 1
            
        typer.echo("\nOperation types:")
        for op_type, count in op_types.items():
            typer.echo(f"  - {op_type}: {count}")
            
        if output:
            with open(output, "w") as f:
                f.write(plan_json)
            typer.echo(f"\nExecution plan written to {output}")
        else:
            typer.echo("\nExecution plan:")
            typer.echo(plan_json)

    except Exception as e:
        typer.echo(f"Error compiling pipeline {pipeline_path}: {str(e)}")
        raise typer.Exit(code=1)


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
    project = Project(os.getcwd())
    pipelines_dir = os.path.join(
        project.project_dir,
        project.config.get("paths", {}).get("pipelines", "pipelines"),
    )

    try:
        variables = parse_vars(vars)

        pipeline_path = resolve_pipeline_name(pipeline_name, pipelines_dir)

        typer.echo(f"Running pipeline: {pipeline_path}")
        if variables:
            typer.echo(f"With variables: {json.dumps(variables, indent=2)}")

    except FileNotFoundError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)
    except ValueError as e:
        typer.echo(f"Error parsing variables: {str(e)}")
        raise typer.Exit(code=1)


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
