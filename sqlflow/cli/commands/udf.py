"""CLI commands for Python UDFs."""

import re
from typing import Any, Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sqlflow.udfs.manager import PythonUDFManager, UDFDiscoveryError

app = typer.Typer(help="Manage Python UDFs")
console = Console()


def print_error(error_msg: str, plain: bool = False) -> None:
    """Print error message in plain or rich format."""
    if plain:
        print(f"Error: {error_msg}")
    else:
        console.print(f"[bold red]Error:[/bold red] {error_msg}")


def print_discovery_errors(manager: PythonUDFManager, plain: bool = False) -> None:
    """Print discovery errors if any exist."""
    errors = manager.get_discovery_errors()
    if not errors:
        return

    if plain:
        print("\nDiscovery Errors:")
        for file_path, error_msg in errors.items():
            first_line = error_msg.split("\n")[0]
            print(f"  {file_path}: {first_line}")
    else:
        console.print("\n[bold red]Discovery Errors:[/bold red]")
        error_table = Table(show_header=True)
        error_table.add_column("File", style="red")
        error_table.add_column("Error", style="red")

        for file_path, error_msg in errors.items():
            short_msg = error_msg.split("\n")[0]  # First line only for brevity
            error_table.add_row(file_path, short_msg)

        console.print(error_table)


def create_udf_list_table(udfs: List[Dict[str, Any]]) -> Table:
    """Create a rich table for UDF list display."""
    table = Table(title="Python UDFs")

    # Use improved column structure with better information
    table.add_column("Name", style="green")
    table.add_column("Type", style="blue")
    table.add_column("Signature", style="cyan")
    table.add_column("Summary", style="yellow")
    table.add_column("Module", style="magenta")

    for udf in sorted(udfs, key=lambda u: u["full_name"]):
        # Use formatted signature for better readability
        signature = udf.get("formatted_signature", udf.get("signature", ""))

        # Get a clean summary
        summary = udf.get("docstring_summary", "").strip()

        table.add_row(udf["name"], udf["type"], signature, summary, udf["module"])

    return table


def print_plain_udf_list(udfs: List[Dict[str, Any]]) -> None:
    """Print UDF list in plain text format."""
    for udf in udfs:
        type_label = f"({udf['type']})"
        summary = udf.get("docstring_summary", "")
        print(f"{udf['full_name']} {type_label}: {summary}")


@app.command("list")
def list_udfs(
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", "-p", help="Project directory"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed information"
    ),
    plain: bool = typer.Option(
        False, "--plain", help="Use plain text output (for testing)"
    ),
):
    """List available Python UDFs."""
    try:
        # Use regular UDF manager without enhancement for CLI operations
        # CLI should only show actual UDFs, not auto-generated specialized versions
        manager = PythonUDFManager(project_dir=project_dir)
        manager.discover_udfs()
        udfs = manager.list_udfs()

        if not udfs:
            if plain:
                print("No Python UDFs found in the project")
            else:
                console.print("[yellow]No Python UDFs found in the project[/yellow]")
            return

        if plain:
            print_plain_udf_list(udfs)
        else:
            table = create_udf_list_table(udfs)
            console.print(table)
            print_discovery_errors(manager, plain)

    except UDFDiscoveryError as e:
        print_error(str(e), plain)


def print_plain_udf_info(
    udf_info: Dict[str, Any], manager: PythonUDFManager, udf_name: str
) -> None:
    """Print UDF info in plain text format."""
    print(f"UDF: {udf_info['full_name']}")
    print(f"Type: {udf_info['type']}")
    print(f"File: {udf_info['file_path']}")
    print(
        f"Signature: {udf_info.get('formatted_signature', udf_info.get('signature', ''))}"
    )
    print(f"Docstring: {udf_info['docstring']}")

    # Print parameter details
    if "param_details" in udf_info:
        print("\nParameters:")
        for name, details in udf_info["param_details"].items():
            param_type = details.get("type", "Any")
            default = f" = {details['default']}" if details.get("has_default") else ""
            print(f"  - {name}: {param_type}{default}")

    # Print required columns for table UDFs
    if udf_info["type"] == "table" and "required_columns" in udf_info:
        req_cols = ", ".join(udf_info["required_columns"])
        print(f"\nRequired Columns: {req_cols}")

    # Print validation warnings
    print_plain_validation_warnings(manager, udf_name)


def print_plain_validation_warnings(manager: PythonUDFManager, udf_name: str) -> None:
    """Print validation warnings in plain text format."""
    warnings = manager.validate_udf_metadata(udf_name)
    if warnings:
        print("\nValidation Warnings:")
        for warning in warnings:
            print(f"  - {warning}")


def extract_param_description(param_name: str, docstring: str) -> str:
    """Extract parameter description from docstring."""
    if not docstring:
        return "-"

    # Look for parameter in docstring (assumes format like "name: description")
    param_pattern = r"{0}:\s*(.+?)(?:\n\s+\w+:|$)".format(param_name)
    param_match = re.search(param_pattern, docstring, re.DOTALL)
    if param_match:
        return param_match.group(1).strip()
    return "-"


def create_parameter_table(udf_info: Dict[str, Any]) -> Table:
    """Create a rich table for UDF parameters."""
    param_table = Table(show_header=True, box=None)
    param_table.add_column("Name", style="cyan")
    param_table.add_column("Type", style="blue")
    param_table.add_column("Default", style="yellow")
    param_table.add_column("Description", style="green")

    for name, details in udf_info["param_details"].items():
        param_type = details.get("type", "Any")
        default = str(details["default"]) if details.get("has_default") else "-"
        description = extract_param_description(name, udf_info.get("docstring", ""))
        param_table.add_row(name, param_type, default, description)

    return param_table


def format_rich_udf_info(
    udf_info: Dict[str, Any], manager: PythonUDFManager, udf_name: str
) -> Panel:
    """Format UDF info as a rich panel."""
    title = (
        f"[bold green]{udf_info['name']}[/bold green] ([blue]{udf_info['type']}[/blue])"
    )

    content = []
    content.append(f"[bold]Full Name:[/bold] {udf_info['full_name']}")
    content.append(f"[bold]File:[/bold] {udf_info['file_path']}")
    content.append(
        f"[bold]Signature:[/bold] {udf_info.get('formatted_signature', udf_info.get('signature', ''))}"
    )

    if "docstring" in udf_info and udf_info["docstring"]:
        # Format docstring as markdown for better display
        content.append("\n[bold]Description:[/bold]")
        doc_md = Markdown(udf_info["docstring"])
        content.append(doc_md)

    # Parameter details section with a table
    if "param_details" in udf_info:
        content.append("\n[bold]Parameters:[/bold]")
        param_table = create_parameter_table(udf_info)
        content.append(param_table)

    # Required columns for table UDFs
    if udf_info["type"] == "table" and "required_columns" in udf_info:
        content.append("\n[bold]Required Columns:[/bold]")
        req_cols = ", ".join(
            f"[cyan]{col}[/cyan]" for col in udf_info["required_columns"]
        )
        content.append(Text.from_markup(req_cols))

    # Validation warnings
    warnings = manager.validate_udf_metadata(udf_name)
    if warnings:
        content.append("\n[bold red]Validation Warnings:[/bold red]")
        for warning in warnings:
            content.append(f"[red]• {warning}[/red]")

    # Render the panel with all content
    return Panel.fit(
        "\n".join(str(item) for item in content),
        title=title,
        border_style="green",
    )


@app.command("info")
def udf_info(
    udf_name: str = typer.Argument(..., help="UDF name (module.function)"),
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", "-p", help="Project directory"
    ),
    plain: bool = typer.Option(
        False, "--plain", help="Use plain text output (for testing)"
    ),
):
    """Show detailed information about a Python UDF."""
    try:
        manager = PythonUDFManager(project_dir=project_dir)
        manager.discover_udfs()

        # Get UDF information
        udf_info_dict = manager.get_udf_info(udf_name)

        if not udf_info_dict:
            if plain:
                print(f"UDF '{udf_name}' not found")
            else:
                console.print(f"[bold red]UDF '{udf_name}' not found[/bold red]")
            return

        if plain:
            print_plain_udf_info(udf_info_dict, manager, udf_name)
        else:
            panel = format_rich_udf_info(udf_info_dict, manager, udf_name)
            console.print(panel)

    except UDFDiscoveryError as e:
        print_error(str(e), plain)


def validate_and_print_plain_results(
    manager: PythonUDFManager, udfs: List[Dict[str, Any]]
) -> int:
    """Validate UDFs and print results in plain text format."""
    print(f"Validating {len(udfs)} UDFs...")
    invalid_count = 0

    for udf in udfs:
        warnings = manager.validate_udf_metadata(udf["full_name"])
        if warnings:
            invalid_count += 1
            print(f"\n{udf['full_name']}:")
            for warning in warnings:
                print(f"  - {warning}")

    if invalid_count == 0:
        print("\nAll UDFs are valid!")
    else:
        print(f"\n{invalid_count} UDFs have validation issues.")

    return invalid_count


def create_validation_table(
    manager: PythonUDFManager, udfs: List[Dict[str, Any]]
) -> Tuple[Table, int]:
    """Create a validation results table and return invalid count."""
    validation_table = Table(title="UDF Validation Results")
    validation_table.add_column("UDF Name", style="cyan")
    validation_table.add_column("Status", style="bold")
    validation_table.add_column("Warnings", style="yellow")

    invalid_count = 0

    for udf in sorted(udfs, key=lambda u: u["full_name"]):
        warnings = manager.validate_udf_metadata(udf["full_name"])
        if warnings:
            invalid_count += 1
            status = "[red]Invalid[/red]"
            warning_text = "\n".join(f"• {w}" for w in warnings)
        else:
            status = "[green]Valid[/green]"
            warning_text = ""

        validation_table.add_row(udf["full_name"], status, warning_text)

    return validation_table, invalid_count


def print_validation_summary(invalid_count: int, plain: bool = False) -> None:
    """Print validation summary based on results."""
    if plain:
        if invalid_count == 0:
            print("\nAll UDFs are valid!")
        else:
            print(f"\n{invalid_count} UDFs have validation issues.")
    else:
        if invalid_count == 0:
            console.print("\n[bold green]All UDFs are valid![/bold green]")
        else:
            console.print(
                f"\n[bold red]{invalid_count} UDFs have validation issues.[/bold red]"
            )


@app.command("validate")
def validate_udfs(
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", "-p", help="Project directory"
    ),
    plain: bool = typer.Option(
        False, "--plain", help="Use plain text output (for testing)"
    ),
):
    """Validate all UDFs in the project."""
    try:
        manager = PythonUDFManager(project_dir=project_dir)
        manager.discover_udfs()
        udfs = manager.list_udfs()

        if not udfs:
            if plain:
                print("No UDFs found to validate")
            else:
                console.print("[yellow]No UDFs found to validate[/yellow]")
            return

        invalid_count = 0

        if plain:
            invalid_count = validate_and_print_plain_results(manager, udfs)
        else:
            console.print(f"Validating [bold]{len(udfs)}[/bold] UDFs...\n")
            validation_table, invalid_count = create_validation_table(manager, udfs)
            console.print(validation_table)
            print_discovery_errors(manager, plain)

        print_validation_summary(invalid_count, plain)

    except UDFDiscoveryError as e:
        print_error(str(e), plain)
