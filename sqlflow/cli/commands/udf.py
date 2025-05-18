"""CLI commands for Python UDFs."""

from typing import Optional

import typer
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sqlflow.udfs.manager import PythonUDFManager, UDFDiscoveryError

app = typer.Typer(help="Manage Python UDFs")
console = Console()


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
            for udf in udfs:
                print(
                    f"{udf['full_name']} (scalar): {udf['docstring_summary']}" if udf['type'] == 'scalar' else f"{udf['full_name']} (table): {udf['docstring_summary']}"
                )
        else:
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

                table.add_row(
                    udf["name"], udf["type"], signature, summary, udf["module"]
                )

            console.print(table)

            # Show discovery errors if any
            errors = manager.get_discovery_errors()
            if errors:
                console.print("\n[bold red]Discovery Errors:[/bold red]")
                error_table = Table(show_header=True)
                error_table.add_column("File", style="red")
                error_table.add_column("Error", style="red")

                for file_path, error_msg in errors.items():
                    short_msg = error_msg.split("\n")[0]  # First line only for brevity
                    error_table.add_row(file_path, short_msg)

                console.print(error_table)

    except UDFDiscoveryError as e:
        if plain:
            print(f"Error: {str(e)}")
        else:
            console.print(f"[bold red]Error:[/bold red] {str(e)}")


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
        udf_info = manager.get_udf_info(udf_name)

        if not udf_info:
            if plain:
                print(f"UDF '{udf_name}' not found")
            else:
                console.print(f"[bold red]UDF '{udf_name}' not found[/bold red]")
            return

        if plain:
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
                    default = (
                        f" = {details['default']}" if details.get("has_default") else ""
                    )
                    print(f"  - {name}: {param_type}{default}")

            # Print required columns for table UDFs
            if udf_info["type"] == "table" and "required_columns" in udf_info:
                req_cols = ", ".join(udf_info["required_columns"])
                print(f"\nRequired Columns: {req_cols}")

            # Print validation warnings
            warnings = manager.validate_udf_metadata(udf_name)
            if warnings:
                print("\nValidation Warnings:")
                for warning in warnings:
                    print(f"  - {warning}")
        else:
            # Rich panel with detailed UDF information
            title = f"[bold green]{udf_info['name']}[/bold green] ([blue]{udf_info['type']}[/blue])"

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
                param_table = Table(show_header=True, box=None)
                param_table.add_column("Name", style="cyan")
                param_table.add_column("Type", style="blue")
                param_table.add_column("Default", style="yellow")
                param_table.add_column("Description", style="green")

                for name, details in udf_info["param_details"].items():
                    param_type = details.get("type", "Any")
                    default = (
                        str(details["default"]) if details.get("has_default") else "-"
                    )

                    # Try to extract parameter description from docstring
                    description = "-"
                    if udf_info["docstring"]:
                        import re

                        # Look for parameter in docstring (assumes format like "name: description")
                        param_pattern = rf"{name}:\s*(.+?)(?:\n\s+\w+:|$)"
                        param_match = re.search(
                            param_pattern, udf_info["docstring"], re.DOTALL
                        )
                        if param_match:
                            description = param_match.group(1).strip()

                    param_table.add_row(name, param_type, default, description)

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
            panel = Panel.fit(
                "\n".join(str(item) for item in content),
                title=title,
                border_style="green",
            )
            console.print(panel)

    except UDFDiscoveryError as e:
        if plain:
            print(f"Error: {str(e)}")
        else:
            console.print(f"[bold red]Error:[/bold red] {str(e)}")


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

        # Count UDFs with warnings
        invalid_count = 0

        if plain:
            print(f"Validating {len(udfs)} UDFs...")
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
        else:
            console.print(f"Validating [bold]{len(udfs)}[/bold] UDFs...\n")

            validation_table = Table(title="UDF Validation Results")
            validation_table.add_column("UDF Name", style="cyan")
            validation_table.add_column("Status", style="bold")
            validation_table.add_column("Warnings", style="yellow")

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

            console.print(validation_table)

            # Show summary
            if invalid_count == 0:
                console.print("\n[bold green]All UDFs are valid![/bold green]")
            else:
                console.print(
                    f"\n[bold red]{invalid_count} UDFs have validation issues.[/bold red]"
                )

            # Show discovery errors if any
            errors = manager.get_discovery_errors()
            if errors:
                console.print("\n[bold red]Discovery Errors:[/bold red]")
                error_table = Table(show_header=True)
                error_table.add_column("File", style="red")
                error_table.add_column("Error", style="red")

                for file_path, error_msg in errors.items():
                    short_msg = error_msg.split("\n")[0]  # First line only for brevity
                    error_table.add_row(file_path, short_msg)

                console.print(error_table)

    except UDFDiscoveryError as e:
        if plain:
            print(f"Error: {str(e)}")
        else:
            console.print(f"[bold red]Error:[/bold red] {str(e)}")
