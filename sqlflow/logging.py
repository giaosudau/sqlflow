import logging
import sys
from typing import Any, Dict

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Map string log levels to logging constants
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

# Modules that produce verbose, technical output
# These will be set to WARNING by default unless verbose mode is enabled
TECHNICAL_MODULES = [
    "sqlflow.core.engines.duckdb_engine",
    "sqlflow.core.sql_generator",
    "sqlflow.core.storage.artifact_manager",
    "sqlflow.parser.parser",
    "sqlflow.core.planner",
    "sqlflow.udfs.manager",
]


def get_logger(name: str) -> logging.Logger:
    """Return a logger with the specified name.

    Args:
        name: The name for the logger, typically __name__

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)

    # Only add a handler if it doesn't have one already
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(DEFAULT_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Don't propagate to root logger to avoid duplicate logging
        logger.propagate = False

    return logger


def configure_logging(
    verbose: bool = False,
    quiet: bool = False,
) -> None:
    """Configure logging settings based on command line flags.

    Args:
        verbose: Whether to enable verbose mode (shows all debug logs)
        quiet: Whether to enable quiet mode (only shows warnings and errors)
    """
    # Determine the root logging level based on flags
    if quiet:
        root_level = logging.WARNING  # Only warnings and errors
    elif verbose:
        root_level = logging.DEBUG  # All debug logs
    else:
        root_level = logging.INFO  # Default level - information messages

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)

    # Clear existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add a single handler
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(DEFAULT_FORMAT)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Set specific levels for technical modules
    for module_name in TECHNICAL_MODULES:
        module_logger = logging.getLogger(module_name)

        # In verbose mode, show all logs from technical modules
        # Otherwise, only show warnings and above
        if verbose:
            module_logger.setLevel(logging.DEBUG)
        else:
            module_logger.setLevel(logging.WARNING)

        # Add a handler if there isn't one already
        if not module_logger.handlers:
            module_logger.addHandler(handler)
            module_logger.propagate = False


def suppress_third_party_loggers():
    """Suppress noisy third-party loggers."""
    noisy_loggers = [
        "boto3",
        "botocore",
        "urllib3",
        "s3transfer",
        "aiohttp",
        "fsspec",
        "s3fs",
        "aiobotocore",
    ]

    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_logging_status() -> Dict[str, Any]:
    """Get the current logging status of all modules.

    Returns:
        Dictionary with logging status information
    """
    # Get root logger level
    root_logger = logging.getLogger()
    root_level = logging.getLevelName(root_logger.level)

    # Collect all module loggers
    modules = {}

    for name in logging.root.manager.loggerDict:
        logger = logging.getLogger(name)
        modules[name] = {
            "level": logging.getLevelName(logger.level),
            "propagate": logger.propagate,
            "has_handlers": bool(logger.handlers),
        }

    return {"root_level": root_level, "modules": modules}


def get_level_name(level: int) -> str:
    """Get a formatted name for a logging level.

    Args:
        level: Logging level (e.g., logging.INFO)

    Returns:
        Formatted name (e.g., "INFO")
    """
    level_names = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }
    return level_names.get(level, str(level))
