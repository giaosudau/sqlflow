"""Main entry point for SQLFlow CLI."""

import sys
from typing import Optional

from sqlflow.cli.main import cli
from sqlflow.logging import configure_logging


def main(log_level: Optional[str] = None) -> None:
    """Run the SQLFlow CLI with configurable logging.

    Args:
        log_level: Optional log level to use, defaults to environment variable or INFO
    """
    # Configure logging based on argument or environment variable
    configure_logging(log_level=log_level)

    # Run the CLI
    cli()


if __name__ == "__main__":
    # Allow setting log level via command line arg
    log_level_arg = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--log-level" and i < len(sys.argv):
            log_level_arg = sys.argv[i + 1]
            # Remove these arguments so they don't interfere with the CLI
            sys.argv.pop(i)
            sys.argv.pop(i)
            break

    main(log_level=log_level_arg)
