"""Main entry point for SQLFlow CLI."""

import os
from typing import Optional

import typer

from sqlflow import __version__
from sqlflow.cli.pipeline import pipeline_app
from sqlflow.project import Project

app = typer.Typer(
    help="SQLFlow - SQL-based data pipeline tool.",
    no_args_is_help=True,
)

app.add_typer(pipeline_app, name="pipeline")


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
    app()


if __name__ == "__main__":
    app()
