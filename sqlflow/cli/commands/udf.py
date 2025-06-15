"""Minimal UDF commands module stub for test compatibility."""

import typer

# Create a minimal app for test compatibility
app = typer.Typer(help="User-defined function (UDF) management commands")


@app.command("list")
def list_udfs():
    """List available UDFs."""
    print("UDF listing not implemented in this version")


if __name__ == "__main__":
    app()
