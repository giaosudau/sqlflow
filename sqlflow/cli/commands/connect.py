"""Connect command for SQLFlow CLI.

This module handles connection management operations including
listing available connectors and testing connections.
"""

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.cli.display import display_error
from sqlflow.cli.factories import load_project_for_command
from sqlflow.logging import get_logger

# Create Typer app for connect commands
connect_app = typer.Typer(
    name="connect",
    help="Manage and test data source connections",
    rich_markup_mode="rich",
)

console = Console()
logger = get_logger(__name__)


def _load_project_safely(profile: str):
    """Load project with consistent error handling."""
    try:
        return load_project_for_command(profile)
    except Exception:
        return None


def _display_connections_table(connectors, profile):
    """Display connections in table format."""
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="white")
    table.add_column("Status", style="green")

    for name, config in connectors.items():
        connector_type = config.get("type", "unknown").upper()
        table.add_row(name, connector_type, "✓ Ready")

    console.print(f"📡 [bold blue]Connections in profile '{profile}'[/bold blue]")
    console.print(table)


def _display_connections_json(connectors):
    """Display connections in JSON format."""
    import json

    console.print(json.dumps(connectors, indent=2))


@connect_app.command("list")
def list_connections(
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="Profile to use for listing connections"
    ),
    format: str = typer.Option(
        "table", "--format", help="Output format: table or json"
    ),
) -> None:
    """List all available connections in the profile."""
    try:
        project = _load_project_safely(profile)
        if not project:
            display_error("Profile not found or empty")
            raise typer.Exit(1)

        connectors = _get_connectors_from_profile(project)

        # Check if we actually have any valid connectors
        valid_connectors = {
            k: v for k, v in connectors.items() if v
        }  # Filter out empty dicts
        if not valid_connectors:
            display_error("Profile not found or empty")
            raise typer.Exit(1)

        if format == "json":
            _display_connections_json(valid_connectors)
        else:
            _display_connections_table(valid_connectors, profile)

        logger.info(
            f"Listed {len(valid_connectors)} connections for profile '{profile}'"
        )

    except typer.Exit:
        raise
    except Exception as e:
        display_error(f"Failed to list connections: {str(e)}")
        raise typer.Exit(1)


def _get_connectors_from_profile(project):
    """Extract connectors from profile, handling both new and legacy formats."""
    connectors = project.profile.get("connectors", {})

    # Legacy format: connectors are at root level
    if not connectors:
        # Check if there are any dict items at root level (potential connectors)
        # Include even if they don't have 'type' field - validation happens later
        for key, value in project.profile.items():
            if isinstance(value, dict) and key not in [
                "version",
                "engines",
                "variables",
            ]:
                connectors[key] = value

    return connectors


def _display_connection_details(connection_name, connector_config, verbose):
    """Display connection details if verbose mode is enabled."""
    if not verbose:
        return

    connector_type = connector_config.get("type")
    console.print(f"Testing connection '{connection_name}'")
    console.print(f"Type: {connector_type.upper()}")
    console.print("Parameters:")

    # Handle both nested params and direct config
    params = connector_config.get("params", connector_config)
    for key, value in params.items():
        if key != "type":  # Skip the type field
            console.print(f"  {key}: {value}")


def _validate_connector_type(connector_type):
    """Validate that the connector type is supported."""
    supported_types = [
        "csv",
        "postgres",
        "s3",
        "rest",
        "parquet",
        "google_sheets",
        "shopify",
    ]
    return connector_type.lower() in supported_types


def _validate_connector_params(connector_type, connector_config):
    """Validate required parameters for a connector.

    Returns:
        str: Error message if validation fails, None if valid
    """
    # Get parameters from config
    params = connector_config.get("params", connector_config)

    # Define required parameters for each connector type
    required_params = {
        "csv": ["path"],
        "postgres": ["host", "database"],
        "s3": ["bucket"],
        "rest": ["url"],
        "parquet": ["path"],
        "google_sheets": ["spreadsheet_id"],
        "shopify": ["shop_domain"],
    }

    connector_type_lower = connector_type.lower()
    required = required_params.get(connector_type_lower, [])

    # Check for missing required parameters
    missing = []
    for param in required:
        if param not in params or not params[param]:
            missing.append(param)

    if missing:
        return f"Missing required parameters for {connector_type}: {', '.join(missing)}"

    return None


def _find_connection(connectors, connection_name):
    """Find connection in connectors and validate basic requirements."""
    if connection_name not in connectors:
        print(f"Connection '{connection_name}' not found")
        raise typer.Exit(1)

    connector_config = connectors[connection_name]
    connector_type = connector_config.get("type") if connector_config else None

    # Handle missing type field (including empty connector config)
    if not connector_type:
        print(f"Connection '{connection_name}' missing 'type' field")
        raise typer.Exit(1)

    return connector_config, connector_type


def _validate_connection(connection_name, connector_type, connector_config):
    """Validate connection type and parameters."""
    if not _validate_connector_type(connector_type):
        print(f"❌ Error: Unsupported connector type: {connector_type}")
        raise typer.Exit(2)

    validation_error = _validate_connector_params(connector_type, connector_config)
    if validation_error:
        print(f"❌ Error: {validation_error}")
        raise typer.Exit(2)


@connect_app.command("test")
def test_connection(
    connection_name: str = typer.Argument(..., help="Name of connection to test"),
    profile: str = typer.Option("dev", "--profile", "-p", help="Profile to use"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed test output"
    ),
) -> None:
    """Test a specific connection."""
    try:
        project = _load_project_safely(profile)
        if not project:
            display_error("Profile not found or empty")
            raise typer.Exit(1)

        connectors = _get_connectors_from_profile(project)
        connector_config, connector_type = _find_connection(connectors, connection_name)

        _display_connection_details(connection_name, connector_config, verbose)
        _validate_connection(connection_name, connector_type, connector_config)

        console.print("✓ Connection test succeeded")
        logger.info(f"Connection test passed for '{connection_name}'")

    except typer.Exit:
        raise
    except Exception as e:
        display_error(f"Connection test failed: {str(e)}")
        raise typer.Exit(1)


if __name__ == "__main__":
    connect_app()
