"""Profile management CLI commands for SQLFlow.

Implements Task 5.1: Profile Management CLI from the profile enhancement plan.
Provides commands to list, validate, and show profile configurations.
"""

import os
from typing import Dict, List, Optional

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.core.profiles import ProfileManager, ValidationResult
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
        None, "--project-dir", help="Project directory (default: current directory)"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
) -> None:
    """List all available profiles in the current project.

    Scans the profiles/ directory and shows available profile files
    with basic validation status.
    """
    try:
        project_path = project_dir or os.getcwd()
        profiles_dir = os.path.join(project_path, "profiles")

        _check_profiles_directory_exists(profiles_dir)
        profile_files = _find_profile_files(profiles_dir)

        if quiet:
            _print_quiet_profile_list(profile_files)
        else:
            _print_formatted_profile_table(profiles_dir, profile_files)

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error listing profiles: {e}")
        console.print(f"❌ Error: {e}")
        raise typer.Exit(code=1)


@profiles_app.command("validate")
def validate_profile(
    profile_name: Optional[str] = typer.Argument(
        None, help="Name of the profile to validate (omit .yml extension)"
    ),
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", help="Project directory (default: current directory)"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed validation information"
    ),
) -> None:
    """Validate profile YAML structure and configuration.

    Validates profile syntax, connector configurations, and schema compliance.
    If no profile name is provided, validates all profiles in the project.
    """
    try:
        project_path = project_dir or os.getcwd()
        profiles_dir = os.path.join(project_path, "profiles")

        if not os.path.exists(profiles_dir):
            console.print(f"❌ Profiles directory not found: {profiles_dir}")
            raise typer.Exit(code=1)

        if profile_name:
            # Validate single profile
            _validate_single_profile(profiles_dir, profile_name, verbose)
        else:
            # Validate all profiles
            _validate_all_profiles(profiles_dir, verbose)

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error validating profile: {e}")
        console.print(f"❌ Error: {e}")
        raise typer.Exit(code=1)


@profiles_app.command("show")
def show_profile(
    profile_name: str = typer.Argument(
        ..., help="Name of the profile to show (omit .yml extension)"
    ),
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", help="Project directory (default: current directory)"
    ),
    section: Optional[str] = typer.Option(
        None,
        "--section",
        help="Show only specific section (connectors, variables, engines)",
    ),
) -> None:
    """Show detailed profile configuration.

    Displays the complete profile configuration or a specific section
    with formatted output and connector details.
    """
    try:
        project_path = project_dir or os.getcwd()
        profiles_dir = os.path.join(project_path, "profiles")

        if not os.path.exists(profiles_dir):
            console.print(f"❌ Profiles directory not found: {profiles_dir}")
            raise typer.Exit(code=1)

        # Load and display profile
        try:
            profile_manager = ProfileManager(profiles_dir, profile_name)
            profile_data = profile_manager.load_profile(profile_name)

            console.print(f"\n📋 Profile: [bold cyan]{profile_name}[/bold cyan]")

            if section:
                _show_profile_section(profile_data, section, profile_name)
            else:
                _show_complete_profile(profile_data, profile_name)

        except FileNotFoundError:
            console.print(f"❌ Profile '{profile_name}' not found in {profiles_dir}")
            raise typer.Exit(code=1)
        except Exception as e:
            console.print(f"❌ Error loading profile '{profile_name}': {e}")
            raise typer.Exit(code=1)

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Error showing profile: {e}")
        console.print(f"❌ Error: {e}")
        raise typer.Exit(code=1)


def _validate_single_profile(
    profiles_dir: str, profile_name: str, verbose: bool
) -> None:
    """Validate a single profile and display results."""
    try:
        profile_manager, profile_path = _get_profile_manager_and_path(
            profiles_dir, profile_name
        )
        result = profile_manager.validate_profile(profile_path)
        _display_single_validation_result(profile_name, result, verbose)

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"❌ Error validating profile '{profile_name}': {e}")
        raise typer.Exit(code=1)


def _validate_all_profiles(profiles_dir: str, verbose: bool) -> None:
    """Validate all profiles in the directory."""
    profile_files = _get_profile_names_from_directory(profiles_dir)

    if not profile_files:
        console.print(f"❌ No profile files found in {profiles_dir}")
        raise typer.Exit(code=1)

    console.print(f"🔍 Validating {len(profile_files)} profile(s)...")

    valid_count, invalid_count = _validate_profile_batch(
        profiles_dir, profile_files, verbose
    )

    _display_validation_summary(profile_files, valid_count, invalid_count)


def _get_profile_manager_and_path(profiles_dir: str, profile_name: str) -> tuple:
    """Get profile manager and validate profile path exists."""
    profile_manager = ProfileManager(profiles_dir, profile_name)
    profile_path = os.path.join(profiles_dir, f"{profile_name}.yml")

    if not os.path.exists(profile_path):
        console.print(f"❌ Profile '{profile_name}' not found")
        raise typer.Exit(code=1)

    return profile_manager, profile_path


def _display_single_validation_result(
    profile_name: str, result: ValidationResult, verbose: bool
) -> None:
    """Display validation result for a single profile."""
    if result.is_valid:
        console.print(f"✅ Profile '{profile_name}' validation passed!")
        _display_warnings_if_verbose(result.warnings, verbose)
    else:
        console.print(f"❌ Profile '{profile_name}' validation failed!")
        _display_errors_and_warnings(result.errors, result.warnings)
        raise typer.Exit(code=1)


def _display_warnings_if_verbose(warnings: List[str], verbose: bool) -> None:
    """Display warnings if verbose mode is enabled."""
    if verbose and warnings:
        console.print("\n⚠️  Warnings:")
        for warning in warnings:
            console.print(f"  - {warning}")


def _display_errors_and_warnings(errors: List[str], warnings: List[str]) -> None:
    """Display errors and warnings for failed validation."""
    console.print("\n📋 Errors:")
    for error in errors:
        console.print(f"  - {error}")

    if warnings:
        console.print("\n⚠️  Warnings:")
        for warning in warnings:
            console.print(f"  - {warning}")


def _get_profile_names_from_directory(profiles_dir: str) -> List[str]:
    """Get list of profile names from directory."""
    profile_files = []
    for file in os.listdir(profiles_dir):
        if file.endswith(".yml") or file.endswith(".yaml"):
            profile_name = os.path.splitext(file)[0]
            profile_files.append(profile_name)
    return profile_files


def _validate_profile_batch(
    profiles_dir: str, profile_files: List[str], verbose: bool
) -> tuple:
    """Validate a batch of profiles and return counts."""
    valid_count = 0
    invalid_count = 0

    for profile_name in sorted(profile_files):
        try:
            profile_manager = ProfileManager(profiles_dir, profile_name)
            profile_path = os.path.join(profiles_dir, f"{profile_name}.yml")
            result = profile_manager.validate_profile(profile_path)

            if result.is_valid:
                console.print(f"✅ {profile_name}")
                valid_count += 1
                _display_verbose_warnings(result.warnings, verbose)
            else:
                console.print(f"❌ {profile_name}")
                invalid_count += 1
                _display_verbose_errors_and_warnings(
                    result.errors, result.warnings, verbose
                )
        except Exception as e:
            console.print(f"❌ {profile_name}: Error - {e}")
            invalid_count += 1

    return valid_count, invalid_count


def _display_verbose_warnings(warnings: List[str], verbose: bool) -> None:
    """Display warnings in verbose mode during batch validation."""
    if verbose and warnings:
        for warning in warnings:
            console.print(f"    ⚠️  {warning}")


def _display_verbose_errors_and_warnings(
    errors: List[str], warnings: List[str], verbose: bool
) -> None:
    """Display errors and warnings in verbose mode during batch validation."""
    if verbose:
        for error in errors:
            console.print(f"    ❌ {error}")
        for warning in warnings:
            console.print(f"    ⚠️  {warning}")


def _display_validation_summary(
    profile_files: List[str], valid_count: int, invalid_count: int
) -> None:
    """Display validation summary and exit if there are invalid profiles."""
    console.print("\n📊 Validation Summary:")
    console.print(f"  Total profiles: {len(profile_files)}")
    console.print(f"  ✅ Valid: {valid_count}")
    console.print(f"  ❌ Invalid: {invalid_count}")

    if invalid_count > 0:
        raise typer.Exit(code=1)


def _show_profile_section(profile_data: Dict, section: str, profile_name: str) -> None:
    """Show a specific section of the profile."""
    valid_sections = ["connectors", "variables", "engines"]

    if section not in valid_sections:
        console.print(
            f"❌ Invalid section '{section}'. "
            f"Valid sections: {', '.join(valid_sections)}"
        )
        raise typer.Exit(code=1)

    if section not in profile_data:
        console.print(f"⚠️  Section '{section}' not found in profile '{profile_name}'")
        return

    section_data = profile_data[section]

    if section == "connectors":
        _show_connectors_section(section_data)
    elif section == "variables":
        _show_variables_section(section_data)
    elif section == "engines":
        _show_engines_section(section_data)


def _show_complete_profile(profile_data: Dict, profile_name: str) -> None:
    """Show the complete profile configuration."""
    # Profile version
    version = profile_data.get("version", "Not specified")
    console.print(f"Version: {version}")

    # Variables section
    if "variables" in profile_data:
        console.print("\n🔧 Variables:")
        _show_variables_section(profile_data["variables"])

    # Connectors section
    if "connectors" in profile_data:
        console.print("\n🔌 Connectors:")
        _show_connectors_section(profile_data["connectors"])

    # Engines section
    if "engines" in profile_data:
        console.print("\n⚙️  Engines:")
        _show_engines_section(profile_data["engines"])


def _show_connectors_section(connectors: Dict) -> None:
    """Display connectors section in a formatted table."""
    if not connectors:
        console.print("  No connectors defined")
        return

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Parameters")

    for name, config in connectors.items():
        conn_type = config.get("type", "Unknown")
        params = config.get("params", {})
        param_count = len(params)
        param_text = f"{param_count} parameter{'s' if param_count != 1 else ''}"

        table.add_row(name, conn_type, param_text)

    console.print(table)


def _show_variables_section(variables: Dict) -> None:
    """Display variables section."""
    if not variables:
        console.print("  No variables defined")
        return

    for name, value in variables.items():
        console.print(f"  {name}: {value}")


def _show_engines_section(engines: Dict) -> None:
    """Display engines section."""
    if not engines:
        console.print("  No engines configured")
        return

    for engine_name, config in engines.items():
        console.print(f"  {engine_name}:")
        if isinstance(config, dict):
            for key, value in config.items():
                console.print(f"    {key}: {value}")
        else:
            console.print(f"    {str(config)}")


# Helper functions to reduce complexity in main commands
def _check_profiles_directory_exists(profiles_dir: str) -> None:
    """Check if profiles directory exists and raise error if not."""
    if not os.path.exists(profiles_dir):
        console.print(f"❌ Profiles directory not found: {profiles_dir}")
        raise typer.Exit(code=1)


def _find_profile_files(profiles_dir: str) -> List[tuple]:
    """Find all profile files in the directory."""
    profile_files = []
    for file in os.listdir(profiles_dir):
        if file.endswith(".yml") or file.endswith(".yaml"):
            profile_name = os.path.splitext(file)[0]
            profile_path = os.path.join(profiles_dir, file)
            profile_files.append((profile_name, profile_path, file))

    if not profile_files:
        console.print(f"❌ No profile files found in {profiles_dir}")
        raise typer.Exit(code=1)

    return profile_files


def _print_quiet_profile_list(profile_files: List[tuple]) -> None:
    """Print profile names in quiet mode."""
    for profile_name, _, _ in sorted(profile_files):
        console.print(profile_name)


def _print_formatted_profile_table(
    profiles_dir: str, profile_files: List[tuple]
) -> None:
    """Print formatted table of profiles with status."""
    console.print(f"\n📋 Available profiles in {profiles_dir}:")

    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Profile", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Connectors")

    for profile_name, profile_path, _ in sorted(profile_files):
        status, status_style, connector_text = _get_profile_status(
            profiles_dir, profile_name, profile_path
        )
        table.add_row(
            profile_name,
            f"[{status_style}]{status}[/{status_style}]",
            connector_text,
        )

    console.print(table)


def _get_profile_status(
    profiles_dir: str, profile_name: str, profile_path: str
) -> tuple:
    """Get profile validation status and connector count."""
    try:
        profile_manager = ProfileManager(profiles_dir, profile_name)
        result = profile_manager.validate_profile(profile_path)

        if result.is_valid:
            status = "✅ Valid"
            status_style = "green"
        else:
            status = "❌ Invalid"
            status_style = "red"

        # Get connector count
        profile_data = profile_manager.load_profile(profile_name)
        connectors = profile_data.get("connectors", {})
        connector_count = len(connectors)
        connector_text = (
            f"{connector_count} connector{'s' if connector_count != 1 else ''}"
        )

        return status, status_style, connector_text

    except Exception as e:
        return "⚠️  Error", "yellow", f"Error: {str(e)[:30]}..."


def _print_validation_result(
    profile_name: str, result: ValidationResult, verbose: bool
) -> None:
    """Print validation result with appropriate formatting."""
    if result.is_valid:
        console.print(f"✅ Profile '{profile_name}' validation passed!")

        if verbose and result.warnings:
            console.print("\n⚠️  Warnings:")
            for warning in result.warnings:
                console.print(f"  - {warning}")
    else:
        console.print(f"❌ Profile '{profile_name}' validation failed!")
        console.print("\n📋 Errors:")
        for error in result.errors:
            console.print(f"  - {error}")

        if result.warnings:
            console.print("\n⚠️  Warnings:")
            for warning in result.warnings:
                console.print(f"  - {warning}")

        raise typer.Exit(code=1)


def _validate_profile_file(profiles_dir: str, profile_name: str) -> ValidationResult:
    """Validate a profile file and return the result."""
    profile_manager = ProfileManager(profiles_dir, profile_name)
    profile_path = os.path.join(profiles_dir, f"{profile_name}.yml")
    return profile_manager.validate_profile(profile_path)
