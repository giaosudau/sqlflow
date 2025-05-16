"""Main entry point for SQLFlow CLI."""

import os
import sys
from typing import Optional

import typer

from sqlflow import __version__
from sqlflow.cli import connect
from sqlflow.cli.pipeline import pipeline_app
from sqlflow.project import Project

app = typer.Typer(
    help="SQLFlow - SQL-based data pipeline tool.",
    no_args_is_help=True,
)

app.add_typer(pipeline_app, name="pipeline")
app.add_typer(connect.app, name="connect")


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        typer.echo(f"SQLFlow version: {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback, help="Show version and exit."
    ),
):
    """SQLFlow - SQL-based data pipeline tool."""


@app.command()
def init(
    project_name: str = typer.Argument(..., help="Name of the project"),
):
    """Initialize a new SQLFlow project."""
    project_dir = os.path.abspath(project_name)
    if os.path.exists(project_dir):
        typer.echo(f"Directory '{project_name}' already exists.")
        if not typer.confirm(
            "Do you want to initialize the project in this directory?"
        ):
            typer.echo("Project initialization cancelled.")
            raise typer.Exit(code=1)
    else:
        os.makedirs(project_dir)

    Project.init(project_dir, project_name)

    pipelines_dir = os.path.join(project_dir, "pipelines")
    example_pipeline_path = os.path.join(pipelines_dir, "example.sf")

    with open(example_pipeline_path, "w") as f:
        f.write(
            """-- Example SQLFlow pipeline

SET date = '${run_date|2023-10-25}';

SOURCE sample TYPE CSV PARAMS {
  "path": "data/sample_${date}.csv",
  "has_header": true
};

LOAD sample INTO raw_data;

CREATE TABLE processed_data AS
SELECT 
  *,
  UPPER(name) AS name_upper
FROM raw_data;

EXPORT
  SELECT * FROM processed_data
TO "output/processed_${date}.csv"
TYPE CSV
OPTIONS { "header": true, "delimiter": "," };
"""
        )

    typer.echo(f"✅ Project '{project_name}' initialized successfully!")
    typer.echo("\nNext steps:")
    typer.echo(f"  cd {project_name}")
    typer.echo("  sqlflow pipeline list")
    typer.echo("  sqlflow pipeline compile example")
    typer.echo('  sqlflow pipeline run example --vars \'{"date": "2023-10-25"}\'')


def cli():
    """Entry point for the command line."""
    # Fix for the help command issue with Typer
    if len(sys.argv) == 1 or "--help" in sys.argv or "-h" in sys.argv:
        print("SQLFlow - SQL-based data pipeline tool.")
        print("\nCommands:")
        print("  pipeline    Work with SQLFlow pipelines.")
        print("  connect     Manage and test connection profiles.")
        print("  init        Initialize a new SQLFlow project.")
        print("\nOptions:")
        print("  --version   Show version and exit.")
        print("  --help      Show this message and exit.")

        if len(sys.argv) == 1:
            # No arguments provided, exit with standard help code
            return 0

        # Check if help is requested for a specific command
        if len(sys.argv) > 2 and ("--help" in sys.argv or "-h" in sys.argv):
            command = sys.argv[1]
            if command == "pipeline":
                print("\nPipeline Commands:")
                print("  list        List available pipelines.")
                print("  compile     Compile a pipeline.")
                print("  run         Run a pipeline.")
                print("  validate    Validate a pipeline.")
            elif command == "connect":
                print("\nConnect Commands:")
                print("  list        List available connections.")
                print("  test        Test a connection.")
            return 0

        return 0

    # For non-help commands, attempt to run the app
    try:
        app()
    except Exception as e:
        typer.echo(f"Error: {str(e)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(cli())
