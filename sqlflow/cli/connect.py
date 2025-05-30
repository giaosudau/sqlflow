import logging
import os
from typing import Dict, List, Optional, Tuple

import typer

from sqlflow.connectors.base import Connector, ConnectorError
from sqlflow.connectors.registry import get_connector_class
from sqlflow.project import Project

app = typer.Typer(help="Manage and test connection profiles.")
logger = logging.getLogger("sqlflow.cli.connect")


def _get_profile_and_params(
    project: Project, profile_name: str
) -> tuple[Optional[Dict], Optional[Dict]]:
    """Get profile configuration and connection parameters.

    Args:
    ----
        project: Project instance
        profile_name: Name of the connection profile

    Returns:
    -------
        Tuple of (profile_dict, connection_params)

    """
    try:
        profile_dict = project.get_profile()
        if not profile_dict:
            return None, None

        params = profile_dict.get(profile_name)
        return profile_dict, params
    except Exception as e:
        logger.warning(f"Error loading profile: {e}")
        return None, None


def _get_connectors(profile_dict: Dict) -> Dict:
    """Get connectors from profile dictionary.

    Args:
    ----
        profile_dict: Profile configuration dictionary

    Returns:
    -------
        Dictionary of connectors

    """
    # For backward compatibility with tests, if there's no "connectors" section
    # treat the entire profile as a connector map
    return profile_dict.get("connectors", profile_dict)


def _get_connector_status(name: str, params: Dict) -> Tuple[str, str, str]:
    """Get connector type, parameters, and status.

    Args:
    ----
        name: Name of the connector
        params: Connector parameters

    Returns:
    -------
        Tuple of (connector_type, readable_status)

    """
    # Handle both old and new format
    if "type" in params:
        conn_type = params.get("type")
        conn_params = params.get("params", params)
    else:
        conn_type = name
        conn_params = params

    try:
        # Try to instantiate connector to check status
        connector_class = get_connector_class(conn_type)
        connector: Connector = connector_class()
        connector.configure(conn_params)
        result = connector.test_connection()
        status = "✓ Ready" if result.success else "✗ Error"
    except Exception:
        status = "? Unknown"

    return conn_type, status


def _print_connectors_table(connectors_info: List[Tuple[str, str, str]]) -> None:
    """Print connectors table.

    Args:
    ----
        connectors_info: List of tuples of (name, type, status)

    """
    # Print header
    typer.echo("-" * 40)
    typer.echo(f"{'NAME':20} {'TYPE':15} {'STATUS'}")
    typer.echo("-" * 40)

    for name, conn_type, status in connectors_info:
        typer.echo(f"{name:20} {conn_type:15} {status}")


@app.command("list")
def connect_list(
    profile: str = typer.Option("dev", help="Profile to use (default: dev)"),
) -> None:
    """List all available connection profiles in the selected profile.

    Args:
    ----
        profile: Name of the profile to use (default: dev)

    Raises:
    ------
        typer.Exit: With appropriate exit code

    """
    try:
        project = Project(os.getcwd(), profile_name=profile)
        profile_dict = project.get_profile()

        if not profile_dict:
            typer.echo(f"Profile '{profile}' not found or empty.")
            raise typer.Exit(code=1)

        connectors = _get_connectors(profile_dict)
        if not connectors:
            typer.echo(f"No connectors defined in profile '{profile}'.")
            raise typer.Exit(code=1)

        # Print header
        typer.echo(f"\nConnections in profile '{profile}':")

        # Collect connector information
        connectors_info = []
        for name, params in connectors.items():
            if not isinstance(params, dict):
                continue

            conn_type, status = _get_connector_status(name, params)
            connectors_info.append((name, conn_type, status))

        _print_connectors_table(connectors_info)

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error listing connections: {e}")
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)


def _validate_profile_and_connection(
    profile_dict: Optional[Dict],
    params: Optional[Dict],
    profile: str,
    profile_name: str,
) -> Optional[Dict]:
    """Validate profile and connection exist and are correctly formatted.

    Args:
    ----
        profile_dict: Profile configuration dictionary
        params: Connection parameters dictionary
        profile: Profile name
        profile_name: Connection profile name

    Returns:
    -------
        Connection parameters if validation passes

    Raises:
    ------
        typer.Exit: With appropriate exit code if validation fails

    """
    if not profile_dict:
        typer.echo(f"Profile '{profile}' not found or empty.")
        raise typer.Exit(code=1)

    # For backward compatibility with tests, check both formats
    # First try with connectors section
    connectors = profile_dict.get("connectors", {})
    conn_params = connectors.get(profile_name)

    # If not found, try the old format where the entire profile is a connector map
    if conn_params is None:
        conn_params = profile_dict.get(profile_name)

    if conn_params is None:
        typer.echo(f"Connection '{profile_name}' not found in profile '{profile}'.")
        raise typer.Exit(code=1)

    if not isinstance(conn_params, dict):
        typer.echo(f"Connection '{profile_name}' must be a dictionary.")
        raise typer.Exit(code=1)

    if "type" not in conn_params and profile_name != conn_params.get("type"):
        typer.echo(f"Connection '{profile_name}' missing 'type' field.")
        raise typer.Exit(code=1)

    return conn_params


def _get_connector_class(connector_type: str) -> type[Connector]:
    """Get connector class for the specified type.

    Args:
    ----
        connector_type: Type of connector to get

    Returns:
    -------
        Connector class

    Raises:
    ------
        typer.Exit: With appropriate exit code if connector not found

    """
    try:
        return get_connector_class(connector_type)
    except ValueError as e:
        logger.error(f"Error testing connection: {e}")
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=2)


def _test_connection(
    connector_class: type[Connector],
    params: Dict,
    profile_name: str,
    connector_type: str,
    verbose: bool,
) -> None:
    """Test a connection using the provided connector class and parameters.

    Args:
    ----
        connector_class: Connector class to use
        params: Connection parameters
        profile_name: Name of the connection profile
        connector_type: Type of connector
        verbose: Whether to show detailed information

    Raises:
    ------
        typer.Exit: With appropriate exit code if connection fails

    """
    try:
        connector: Connector = connector_class()
        connector.configure(params)

        if verbose:
            _print_connection_details(profile_name, connector_type, params)

        result = connector.test_connection()
        if result.success:
            typer.echo(
                f"✓ Connection to '{profile_name}' ({connector_type}) succeeded."
            )
            if verbose and result.message:
                typer.echo(f"Details: {result.message}")
        else:
            typer.echo(
                f"✗ Connection to '{profile_name}' ({connector_type}) failed: {result.message}"
            )
            raise typer.Exit(code=2)
    except ConnectorError as e:
        logger.error(f"Connector error testing connection: {e}")
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=2)
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=2)


def _print_connection_details(
    profile_name: str, connector_type: str, params: Dict
) -> None:
    """Print detailed connection information.

    Args:
    ----
        profile_name: Name of the connection profile
        connector_type: Type of connector
        params: Connection parameters

    """
    typer.echo(f"\nTesting connection '{profile_name}':")
    typer.echo(f"Type: {connector_type}")
    typer.echo("Parameters:")
    for k, v in params.items():
        if k.lower() not in ("password", "secret", "key", "token"):
            typer.echo(f"  {k}: {v}")
    typer.echo("\nResult:")


@app.command("test")
def connect_test(
    profile_name: str,
    profile: str = typer.Option("dev", help="Profile to use (default: dev)"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed connection info"
    ),
) -> None:
    """Test a connection profile in the selected profile file.

    Args:
    ----
        profile_name: Name of the connection to test
        profile: Name of the profile to use (default: dev)
        verbose: Show detailed connection information

    Raises:
    ------
        typer.Exit: With appropriate exit code (1 for error, 2 for connection failure)

    """
    try:
        project = Project(os.getcwd(), profile_name=profile)
        profile_dict = project.get_profile()

        conn_params = _validate_profile_and_connection(
            profile_dict, None, profile, profile_name
        )

        # Support both new and old formats
        if "type" in conn_params:
            connector_type = conn_params.get("type")
            connector_params = conn_params.get("params", conn_params)
        else:
            connector_type = profile_name
            connector_params = conn_params

        connector_class = _get_connector_class(connector_type)
        _test_connection(
            connector_class, connector_params, profile_name, connector_type, verbose
        )

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
