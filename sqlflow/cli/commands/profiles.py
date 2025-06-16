"""Profile management CLI commands for SQLFlow.

Implements Task 2.3: Complete Command Integration for profile management
using Typer framework with Rich display and type safety.
"""

import datetime
import os
from typing import Any, Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.cli.business_operations import (
    list_profiles_operation,
)
from sqlflow.cli.display import (
    display_json_output,
    display_profile_not_found_error,
    display_profile_validation_error,
    display_profile_validation_success,
    display_profiles_list,
)
from sqlflow.cli.errors import ProfileNotFoundError, ProfileValidationError
from sqlflow.logging import get_logger

logger = get_logger(__name__)
console = Console()

# Create the profiles command group
profiles_app = typer.Typer(
    name="profiles",
    help="Manage SQLFlow profiles and configurations",
)


@profiles_app.command("list")
def list_profiles(
    project_dir: Optional[str] = typer.Option(
        None,
        "--project-dir",
        help="Project directory (default: current directory)",
    ),
    format: str = typer.Option(
        "table", "--format", help="Output format: table or json"
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Reduce output to essential information only",
    ),
) -> None:
    """List all available profiles in the project."""
    try:
        profiles = list_profiles_operation(project_dir)

        if format == "json":
            # Output JSON format
            display_json_output(profiles)
        elif quiet:
            # Output only profile names in quiet mode
            for profile in profiles:
                console.print(profile["name"])
        else:
            # Output table format
            display_profiles_list(profiles)

        logger.debug(f"Listed {len(profiles)} profiles")

    except ProfileNotFoundError as e:
        # Handle specific profile directory errors
        error_msg = str(e)
        if "Profiles directory not found" in error_msg:
            console.print("Profiles directory not found")
            raise typer.Exit(1)
        elif "No profile files found" in error_msg:
            console.print("No profile files found")
            raise typer.Exit(1)
        else:
            display_profile_not_found_error(e)
            raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error listing profiles: {e}")
        console.print(f"❌ [bold red]Failed to list profiles: {str(e)}[/bold red]")
        raise typer.Exit(1)


@profiles_app.command("validate")
def validate_profile(
    profile_name: Optional[str] = typer.Argument(
        None,
        help="Name of the profile to validate (omit .yml extension)",
    ),
    project_dir: Optional[str] = typer.Option(
        None,
        "--project-dir",
        help="Project directory (default: current directory)",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed validation information"
    ),
) -> None:
    """Validate profile configuration."""
    try:
        with _change_directory_context(project_dir):
            if profile_name:
                _handle_single_profile_validation(profile_name, project_dir, verbose)
            else:
                _handle_all_profiles_validation(project_dir)

    except ProfileNotFoundError as e:
        display_profile_not_found_error(e)
        raise typer.Exit(1)
    except ProfileValidationError as e:
        display_profile_validation_error(e)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error validating profile: {e}")
        console.print(f"❌ [bold red]Validation failed: {str(e)}[/bold red]")
        raise typer.Exit(1)


def _change_directory_context(project_dir: Optional[str]):
    """Context manager for changing directory temporarily."""
    from contextlib import contextmanager

    @contextmanager
    def context():
        if project_dir:
            original_cwd = os.getcwd()
            os.chdir(project_dir)
            try:
                yield
            finally:
                os.chdir(original_cwd)
        else:
            yield

    return context()


def _handle_single_profile_validation(
    profile_name: str, project_dir: Optional[str], verbose: bool
) -> None:
    """Handle validation of a single profile."""
    is_valid, errors, warnings = _validate_single_profile_detailed(
        profile_name, project_dir or os.getcwd()
    )

    if is_valid:
        display_profile_validation_success(profile_name)
        if warnings and verbose:
            console.print("\n⚠️ [yellow]Warnings:[/yellow]")
            for warning in warnings:
                console.print(f"  • {warning}")
        logger.info(f"Profile '{profile_name}' validation passed")
    else:
        console.print(f"❌ [red]Profile '{profile_name}' validation failed[/red]")
        console.print("\n❌ [red]Errors:[/red]")
        for error in errors:
            console.print(f"  • {error}")
        logger.warning(f"Profile '{profile_name}' validation failed")
        raise typer.Exit(1)


def _handle_all_profiles_validation(project_dir: Optional[str]) -> None:
    """Handle validation of all profiles."""
    results = _validate_all_profiles_detailed(project_dir or os.getcwd())
    if not results:
        console.print("❌ [red]Profiles directory not found or empty[/red]")
        raise typer.Exit(1)

    # Display results
    valid_count = sum(1 for r in results if r["is_valid"])
    total_count = len(results)

    console.print("\n📋 [bold blue]Validation Summary:[/bold blue]")
    for result in results:
        status_icon = "✅" if result["is_valid"] else "❌"
        console.print(f"{status_icon} {result['name']}")

    console.print(
        f"\n📊 [bold blue]Results: {valid_count}/{total_count} profiles valid[/bold blue]"
    )

    # Also show count in expected format for tests
    if total_count > 0:
        console.print(f"{total_count} profile(s) processed")

    # Exit with error if any profile failed
    if valid_count < total_count:
        raise typer.Exit(1)


@profiles_app.command("show")
def show_profile(
    profile_name: str = typer.Argument(
        ...,
        help="Name of the profile to show (omit .yml extension)",
    ),
    project_dir: Optional[str] = typer.Option(
        None,
        "--project-dir",
        help="Project directory (default: current directory)",
    ),
    section: Optional[str] = typer.Option(
        None,
        "--section",
        help="Show only specific section (connectors, variables, engines)",
    ),
) -> None:
    """Show detailed profile configuration."""
    try:
        current_dir = project_dir or os.getcwd()

        # Find and load profile
        profile_path = _find_profile_for_show(profile_name, current_dir)
        profile_data = _load_profile_for_show(profile_path, profile_name)

        # Display profile
        _display_profile_data(profile_data, profile_name, section)

    except typer.Exit:
        # Re-raise typer.Exit to let it propagate naturally
        raise
    except ProfileNotFoundError as e:
        display_profile_not_found_error(e)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error showing profile: {e}")
        console.print(f"❌ [bold red]Failed to show profile: {str(e)}[/bold red]")
        raise typer.Exit(1)


def _find_profile_for_show(profile_name: str, current_dir: str) -> str:
    """Find profile file for show command."""
    profiles_dir = os.path.join(current_dir, "profiles")

    # Search for profile with different extensions
    for ext in [".yml", ".yaml"]:
        potential_path = os.path.join(profiles_dir, f"{profile_name}{ext}")
        if os.path.exists(potential_path):
            return potential_path

    # Profile not found - show helpful error
    _show_profile_not_found_error(profile_name, profiles_dir)
    raise typer.Exit(1)


def _show_profile_not_found_error(profile_name: str, profiles_dir: str) -> None:
    """Show helpful error when profile is not found."""
    console.print(f"❌ [red]Profile '{profile_name}' not found[/red]")

    # Show available profiles
    available_profiles = []
    if os.path.exists(profiles_dir):
        for filename in os.listdir(profiles_dir):
            if filename.endswith((".yml", ".yaml")):
                available_profiles.append(filename.rsplit(".", 1)[0])

    if available_profiles:
        console.print(
            f"💡 [yellow]Available profiles: {', '.join(available_profiles)}[/yellow]"
        )


def _load_profile_for_show(profile_path: str, profile_name: str) -> Dict:
    """Load profile data for show command."""
    import yaml

    with open(profile_path, "r") as f:
        profile_data = yaml.safe_load(f)

    if not profile_data:
        console.print(f"❌ [red]Profile '{profile_name}' is empty[/red]")
        raise typer.Exit(1)

    return profile_data


def _display_profile_data(
    profile_data: Dict, profile_name: str, section: Optional[str]
) -> None:
    """Display profile data based on requested section."""
    # Display profile header
    console.print(f"\nProfile: [bold cyan]{profile_name}[/bold cyan]")

    if section:
        # Show specific section
        _show_profile_section(profile_data, section, profile_name)
    else:
        # Show complete profile
        _show_complete_profile(profile_data, profile_name)


def _validate_single_profile(
    profile_name: str, profile_path: str
) -> Tuple[bool, str, List[str]]:
    """Validate a single profile and return results.

    Args:
        profile_name: Name of the profile
        profile_path: Path to the profile file

    Returns:
        Tuple of (is_valid, status, errors)
    """
    try:
        from sqlflow.project import Project

        project_root = os.getcwd()
        Project(project_root, profile_name)
        return True, "valid", []
    except Exception as e:
        return False, "invalid", [str(e)]


def _get_profile_files(profiles_dir: str) -> List[Tuple[str, str]]:
    """Get list of profile files from profiles directory.

    Args:
        profiles_dir: Directory containing profile files

    Returns:
        List of (profile_name, profile_path) tuples
    """
    profile_files = []
    if os.path.exists(profiles_dir):
        for filename in os.listdir(profiles_dir):
            if filename.endswith((".yml", ".yaml")):
                profile_name = filename.rsplit(".", 1)[0]
                profile_path = os.path.join(profiles_dir, filename)
                profile_files.append((profile_name, profile_path))
    return sorted(profile_files)


def _validate_all_profiles() -> List[Dict[str, Any]]:
    """Validate all profiles in the project.

    Returns:
        List of profile validation results
    """
    # Find profiles directory
    profiles_dir = os.path.join(os.getcwd(), "profiles")
    profile_files = _get_profile_files(profiles_dir)

    if not profile_files:
        return []

    # Validate each profile
    results = []
    for profile_name, profile_path in profile_files:
        is_valid, status, errors = _validate_single_profile(profile_name, profile_path)

        # Get file stats
        try:
            stat = os.stat(profile_path)
            size = f"{stat.st_size} bytes"
            modified = datetime.datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M"
            )
        except OSError:
            size = "Unknown"
            modified = "Unknown"

        result = {
            "name": profile_name,
            "path": profile_path,
            "status": status,
            "size": size,
            "modified": modified,
            "errors": errors if not is_valid else [],
        }
        results.append(result)

    return results


def _show_profile_section(profile_data: Dict, section: str, profile_name: str) -> None:
    """Show specific section of profile configuration."""
    # Define valid section names
    valid_sections = {"connectors", "variables", "engines", "version"}

    # Check if section name is valid
    is_valid_section = section.lower() in valid_sections

    # Find section with case-insensitive matching
    found_section = None
    actual_key = None

    for key in profile_data.keys():
        if key.lower() == section.lower():
            found_section = profile_data[key]
            actual_key = key
            break

    if found_section is not None:
        console.print(f"\n📋 [bold blue]{section.title()} Configuration:[/bold blue]")

        if actual_key == "connectors":
            _show_connectors_section(found_section)
        elif actual_key == "variables":
            _show_variables_section(found_section)
        elif actual_key == "engines":
            _show_engines_section(found_section)
        else:
            # Generic display for other sections
            display_json_output(found_section)
    else:
        if is_valid_section:
            # Valid section name but not present in profile - just inform, don't error
            console.print(f"Section '{section}' not found in profile")
            available_sections = list(profile_data.keys())
            if available_sections:
                console.print(f"Available sections: {', '.join(available_sections)}")
        else:
            # Invalid section name - this is an error
            console.print(f"Invalid section '{section}'")
            console.print(f"Valid sections: {', '.join(sorted(valid_sections))}")
            raise typer.Exit(1)


def _show_complete_profile(profile_data: Dict, profile_name: str) -> None:
    """Show complete profile configuration."""
    # Show variables first
    if "variables" in profile_data:
        console.print("\n📋 [bold blue]Variables:[/bold blue]")
        _show_variables_section(profile_data["variables"])

    # Show connectors
    if "connectors" in profile_data:
        console.print("\n📋 [bold blue]Connectors:[/bold blue]")
        _show_connectors_section(profile_data["connectors"])

    # Show engines
    if "engines" in profile_data:
        console.print("\n📋 [bold blue]Engines:[/bold blue]")
        _show_engines_section(profile_data["engines"])

    # Show any other sections
    other_sections = {
        k: v
        for k, v in profile_data.items()
        if k not in ["connectors", "variables", "engines"]
    }
    if other_sections:
        console.print("\n📋 [bold blue]Other Configuration:[/bold blue]")
        display_json_output(other_sections)


def _show_connectors_section(connectors: Dict) -> None:
    """Show connectors configuration."""
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="white")
    table.add_column("Description", style="dim")

    for name, config in connectors.items():
        connector_type = config.get("type", "Unknown")
        description = config.get("description", "No description")[:40]
        if len(config.get("description", "")) > 40:
            description += "..."

        table.add_row(name, connector_type, description)

    console.print(table)


def _show_variables_section(variables: Dict) -> None:
    """Show variables configuration."""
    for name, value in variables.items():
        # Mask sensitive values
        if any(
            sensitive in name.lower()
            for sensitive in ["password", "secret", "key", "token"]
        ):
            display_value = "***masked***"
        else:
            display_value = str(value)[:50]
            if len(str(value)) > 50:
                display_value += "..."

        console.print(f"  • [cyan]{name}[/cyan]: [white]{display_value}[/white]")


def _show_engines_section(engines: Dict) -> None:
    """Show engines configuration."""
    for name, config in engines.items():
        engine_type = config.get("type", "Unknown")
        console.print(f"  • [cyan]{name}[/cyan] ([white]{engine_type}[/white])")

        # Show key configuration items
        for key, value in config.items():
            if key != "type":
                console.print(f"    [dim]{key}: {value}[/dim]")


def _validate_single_profile_detailed(
    profile_name: str, project_dir: str
) -> Tuple[bool, List[str], List[str]]:
    """Validate a single profile with detailed error and warning reporting.

    Args:
        profile_name: Name of the profile
        project_dir: Directory containing the profiles

    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    errors = []
    warnings = []

    # Find profile file
    profile_path = _find_profile_file(profile_name, project_dir)
    if not profile_path:
        errors.append(f"Profile '{profile_name}' not found")
        return False, errors, warnings

    # Load and parse profile data
    profile_data, load_errors = _load_profile_data(profile_path)
    if load_errors:
        errors.extend(load_errors)
        return False, errors, warnings

    # Validate profile structure
    structure_errors, structure_warnings = _validate_profile_structure(profile_data)
    errors.extend(structure_errors)
    warnings.extend(structure_warnings)

    # Check if validation passed
    is_valid = len(errors) == 0
    return is_valid, errors, warnings


def _find_profile_file(profile_name: str, project_dir: str) -> Optional[str]:
    """Find profile file by name in project directory."""
    profiles_dir = os.path.join(project_dir, "profiles")

    for ext in [".yml", ".yaml"]:
        potential_path = os.path.join(profiles_dir, f"{profile_name}{ext}")
        if os.path.exists(potential_path):
            return potential_path
    return None


def _load_profile_data(profile_path: str) -> Tuple[Optional[Dict], List[str]]:
    """Load and parse profile YAML data."""
    import yaml

    errors = []
    try:
        with open(profile_path, "r") as f:
            profile_data = yaml.safe_load(f)

        if not profile_data:
            errors.append("Profile file is empty")
            return None, errors

        return profile_data, []

    except yaml.YAMLError as e:
        errors.append(f"Invalid YAML syntax: {str(e)}")
        return None, errors
    except Exception as e:
        errors.append(f"Error reading profile: {str(e)}")
        return None, errors


def _validate_profile_structure(profile_data: Dict) -> Tuple[List[str], List[str]]:
    """Validate the structure of profile data."""
    errors = []
    warnings = []

    # Validate version
    version_warnings = _validate_profile_version(profile_data)
    warnings.extend(version_warnings)

    # Validate connectors
    connector_errors, connector_warnings = _validate_profile_connectors(profile_data)
    errors.extend(connector_errors)
    warnings.extend(connector_warnings)

    return errors, warnings


def _validate_profile_version(profile_data: Dict) -> List[str]:
    """Validate profile version."""
    warnings = []
    if "version" in profile_data:
        version = profile_data["version"]
        if version not in ["1.0", "1"]:
            warnings.append(f"Unknown version '{version}', expected '1.0'")
    return warnings


def _validate_profile_connectors(profile_data: Dict) -> Tuple[List[str], List[str]]:
    """Validate profile connectors section."""
    errors = []
    warnings = []

    if "connectors" not in profile_data:
        return errors, warnings

    connectors = profile_data["connectors"]
    if not isinstance(connectors, dict):
        errors.append("'connectors' must be a dictionary")
        return errors, warnings

    for conn_name, conn_config in connectors.items():
        conn_errors, conn_warnings = _validate_single_connector(conn_name, conn_config)
        errors.extend(conn_errors)
        warnings.extend(conn_warnings)

    return errors, warnings


def _validate_single_connector(
    conn_name: str, conn_config: Any
) -> Tuple[List[str], List[str]]:
    """Validate a single connector configuration."""
    errors = []
    warnings = []

    if not isinstance(conn_config, dict):
        errors.append(f"Connector '{conn_name}' configuration must be a dictionary")
        return errors, warnings

    if "type" not in conn_config:
        errors.append(f"Connector '{conn_name}' is missing required 'type' field")

    if "params" not in conn_config:
        warnings.append(f"Connector '{conn_name}' has no 'params' section")

    return errors, warnings


def _validate_all_profiles_detailed(project_dir: str) -> List[Dict[str, Any]]:
    """Validate all profiles in the project with detailed reporting.

    Args:
        project_dir: Directory containing the project

    Returns:
        List of profile validation results
    """
    profiles_dir = os.path.join(project_dir, "profiles")

    if not os.path.exists(profiles_dir):
        return []

    profile_files = _get_profile_files(profiles_dir)

    if not profile_files:
        return []

    # Validate each profile
    results = []
    for profile_name, profile_path in profile_files:
        is_valid, errors, warnings = _validate_single_profile_detailed(
            profile_name, project_dir
        )

        result = {
            "name": profile_name,
            "path": profile_path,
            "is_valid": is_valid,
            "errors": errors,
            "warnings": warnings,
        }
        results.append(result)

    return results


if __name__ == "__main__":
    profiles_app()
