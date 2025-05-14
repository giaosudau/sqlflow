"""Main entry point for SQLFlow CLI."""

import click


@click.group()
@click.version_option()
def main():
    """SQLFlow - SQL-based data pipeline tool."""
    pass


@main.command()
@click.argument("project_name")
def init(project_name):
    """Initialize a new SQLFlow project."""
    click.echo(f"Initializing project: {project_name}")


@main.command()
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option("--profile", default="default", help="Profile to use")
def run(pipeline_file, profile):
    """Run a SQLFlow pipeline."""
    click.echo(f"Running pipeline: {pipeline_file} with profile: {profile}")


@main.command()
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option("--profile", default="default", help="Profile to use")
def compile(pipeline_file, profile):
    """Compile a SQLFlow pipeline."""
    click.echo(f"Compiling pipeline: {pipeline_file} with profile: {profile}")


@main.command()
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option("--format", default="html", help="Output format (html, png, dot)")
def visualize(pipeline_file, format):
    """Visualize a SQLFlow pipeline."""
    click.echo(f"Visualizing pipeline: {pipeline_file} in format: {format}")


@main.command()
def list():
    """List available pipelines."""
    click.echo("Listing pipelines")


if __name__ == "__main__":
    main()
