"""Main entry point for SQLFlow CLI."""

import os
import sys

import typer

from sqlflow import __version__
from sqlflow.cli import connect
from sqlflow.cli.commands.udf import app as udf_app
from sqlflow.cli.pipeline import pipeline_app
from sqlflow.logging import configure_logging, suppress_third_party_loggers
from sqlflow.project import Project

app = typer.Typer(
    help="SQLFlow - SQL-based data pipeline tool.",
    no_args_is_help=True,
)

app.add_typer(pipeline_app, name="pipeline")
app.add_typer(connect.app, name="connect")
app.add_typer(udf_app, name="udf")


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        typer.echo(f"SQLFlow version: {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", callback=version_callback, help="Show version and exit."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output with technical details"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
):
    """SQLFlow CLI main entrypoint."""
    # Configure logging based on command line flags
    configure_logging(verbose=verbose, quiet=quiet)

    # Suppress noisy third-party logs
    suppress_third_party_loggers()


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


@app.command("logging_status")
def show_logging_status():
    """Show the current logging status of all modules."""
    from sqlflow.logging import get_logging_status

    status = get_logging_status()

    typer.echo("SQLFlow Logging Status")
    typer.echo(f"Root level: {status['root_level']}")
    typer.echo("\nModule levels:")

    # Get sorted module names for consistent output
    module_names = sorted(status["modules"].keys())
    for name in module_names:
        info = status["modules"][name]
        typer.echo(f"  {name}: {info['level']}")


def cli():
    """Entry point for the command line."""
    # Fix for the help command issue with Typer
    if len(sys.argv) == 1 or "--help" in sys.argv or "-h" in sys.argv:
        print("SQLFlow - SQL-based data pipeline tool.")
        print("\nCommands:")
        print("  pipeline    Work with SQLFlow pipelines.")
        print("  connect     Manage and test connection profiles.")
        print("  udf         Manage Python User-Defined Functions.")
        print("  init        Initialize a new SQLFlow project.")
        print("  logging_status  Show the current logging configuration.")
        print("\nOptions:")
        print("  --version   Show version and exit.")
        print("  --quiet     Reduce output to essential information only.")
        print("  --verbose   Enable verbose output with technical details.")
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
            elif command == "udf":
                print("\nUDF Commands:")
                print("  list        List available Python UDFs.")
                print("  info        Show detailed information about a specific UDF.")
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
