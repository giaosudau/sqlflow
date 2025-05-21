"""Main entry point for SQLFlow CLI."""

from sqlflow.cli.main import cli
from sqlflow.logging import configure_logging


def main() -> None:
    """Run the SQLFlow CLI."""
    # Configure basic logging - CLI flags will be handled in cli.main
    configure_logging()

    # Run the CLI
    cli()


if __name__ == "__main__":
    main()
