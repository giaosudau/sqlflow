"""CLI commands for Python UDFs."""

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.udfs.manager import PythonUDFManager

app = typer.Typer(help="Manage Python UDFs")
console = Console()


@app.command("list")
def list_udfs(
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", "-p", help="Project directory"
    ),
    plain: bool = typer.Option(
        False, "--plain", help="Use plain text output (for testing)"
    ),
):
    """List available Python UDFs in the project."""
    project_dir = project_dir or "."
    udf_manager = PythonUDFManager(project_dir)
    udfs = udf_manager.discover_udfs()

    if not udfs:
        console.print("No Python UDFs found in the project.")
        return

    # For testing, use plain text output
    if plain:
        for udf_name, udf_func in udfs.items():
            udf_type = getattr(udf_func, "_udf_type", "unknown")
            doc = udf_func.__doc__ or ""
            description = doc.strip().split("\n")[0] if doc else ""
            console.print(f"{udf_name} ({udf_type}): {description}")
        return

    # Normal rich table output
    table = Table(title="Python UDFs")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Signature", style="yellow")
    table.add_column("Description", style="white")

    udf_info_list = []
    for udf_name, udf_func in udfs.items():
        udf_type = getattr(udf_func, "_udf_type", "unknown")
        signature = str(getattr(udf_func, "__annotations__", {}))
        doc = udf_func.__doc__ or ""

        # Get first line of docstring as description
        description = doc.strip().split("\n")[0] if doc else ""

        udf_info_list.append(
            {
                "name": udf_name,
                "type": udf_type,
                "signature": signature,
                "description": description,
            }
        )

    # Sort by name for consistent output
    for udf_info in sorted(udf_info_list, key=lambda x: x["name"]):
        table.add_row(
            udf_info["name"],
            udf_info["type"],
            udf_info["signature"],
            udf_info["description"],
        )

    console.print(table)


def _print_udf_not_found(udf_name: str, udfs: dict, plain: bool):
    """Print message when UDF is not found and suggest alternatives."""
    console.print(f"UDF '{udf_name}' not found.")

    # Suggest similar UDFs if available
    import difflib

    matches = difflib.get_close_matches(udf_name, udfs.keys(), n=3, cutoff=0.6)
    if matches:
        console.print("\nDid you mean one of these?")
        for match in matches:
            if plain:
                console.print(f"  {match}")
            else:
                console.print(f"  [cyan]{match}[/cyan]")


def _format_udf_signature(udf_func):
    """Format the signature of a UDF for display."""
    signature = str(getattr(udf_func, "__annotations__", {}))

    # Better format the signature for display
    try:
        import inspect

        signature = str(inspect.signature(udf_func))
    except Exception:
        # Fallback to simpler signature if inspect fails
        pass

    return signature


def _print_udf_info_plain(udf_name: str, udf_func, signature: str, doc: str):
    """Print UDF info in plain text format."""
    udf_type = getattr(udf_func, "_udf_type", "unknown")

    console.print(f"UDF: {udf_name}")
    console.print(f"Type: {udf_type}")
    console.print(f"Signature: {signature}")

    if doc:
        console.print("\nDescription:")
        console.print(doc)

    console.print("\nUsage in SQL:")
    if udf_type == "scalar":
        console.print(
            f'SELECT PYTHON_FUNC("{udf_name}", column1, column2) FROM table_name;'
        )
    elif udf_type == "table":
        console.print(f'SELECT * FROM PYTHON_FUNC("{udf_name}", table_name);')


def _print_udf_info_rich(udf_name: str, udf_func, signature: str, doc: str):
    """Print UDF info with rich formatting."""
    udf_type = getattr(udf_func, "_udf_type", "unknown")

    console.print(f"[bold cyan]UDF:[/bold cyan] {udf_name}")
    console.print(f"[bold green]Type:[/bold green] {udf_type}")
    console.print(f"[bold yellow]Signature:[/bold yellow] {signature}")

    if doc:
        console.print("\n[bold]Description:[/bold]")
        console.print(doc)

    console.print("\n[bold]Usage in SQL:[/bold]")
    if udf_type == "scalar":
        console.print(
            f'SELECT PYTHON_FUNC("{udf_name}", column1, column2) FROM table_name;'
        )
    elif udf_type == "table":
        console.print(f'SELECT * FROM PYTHON_FUNC("{udf_name}", table_name);')


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
    """Show detailed information about a specific UDF."""
    project_dir = project_dir or "."
    udf_manager = PythonUDFManager(project_dir)
    udfs = udf_manager.discover_udfs()

    if not udfs:
        console.print("No Python UDFs found in the project.")
        return

    if udf_name not in udfs:
        _print_udf_not_found(udf_name, udfs, plain)
        return

    udf_func = udfs[udf_name]
    signature = _format_udf_signature(udf_func)
    doc = udf_func.__doc__ or ""

    if plain:
        _print_udf_info_plain(udf_name, udf_func, signature, doc)
    else:
        _print_udf_info_rich(udf_name, udf_func, signature, doc)
