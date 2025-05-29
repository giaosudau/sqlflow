import logging
import sys
from typing import Any, Dict

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
USER_FRIENDLY_FORMAT = "%(message)s"

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
    "sqlflow.udfs.udf_patch",
    "sqlflow.udfs.enhanced_manager",
    "sqlflow.project",
    "sqlflow.core.engines",
]

# Modules that should be completely hidden from users unless in verbose mode
NOISY_MODULES = [
    "sqlflow.udfs.udf_patch",
    "sqlflow.udfs.enhanced_manager",
    "sqlflow.project",
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
    root_level = _determine_root_log_level(quiet, verbose)
    handler = _setup_root_logger(root_level, verbose)
    _configure_noisy_modules(verbose, handler)
    _configure_technical_modules(verbose, handler)


def _determine_root_log_level(quiet: bool, verbose: bool) -> int:
    """Determine the appropriate root logging level based on flags.

    Args:
        quiet: Whether quiet mode is enabled
        verbose: Whether verbose mode is enabled

    Returns:
        Appropriate logging level constant
    """
    if quiet:
        return logging.WARNING  # Only warnings and errors
    elif verbose:
        return logging.DEBUG  # All debug logs
    else:
        return logging.INFO  # Default level - information messages


def _setup_root_logger(root_level: int, verbose: bool) -> logging.Handler:
    """Setup the root logger with appropriate level and handler.

    Args:
        root_level: The logging level to set
        verbose: Whether verbose mode is enabled

    Returns:
        The configured handler for reuse by other loggers
    """
    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)

    # Clear existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add a single handler with appropriate format
    handler = logging.StreamHandler(sys.stdout)
    formatter = _create_formatter(verbose)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    return handler


def _create_formatter(verbose: bool) -> logging.Formatter:
    """Create the appropriate formatter based on mode.

    Args:
        verbose: Whether verbose mode is enabled

    Returns:
        Configured logging formatter
    """
    if verbose:
        # Technical format for verbose mode
        return logging.Formatter(DEFAULT_FORMAT)
    else:
        # Clean format for normal users
        return logging.Formatter(USER_FRIENDLY_FORMAT)


def _configure_noisy_modules(verbose: bool, handler: logging.Handler) -> None:
    """Configure noisy modules that should be suppressed unless verbose.

    Args:
        verbose: Whether verbose mode is enabled
        handler: The logging handler to use
    """
    for module_name in NOISY_MODULES:
        module_logger = logging.getLogger(module_name)

        if verbose:
            module_logger.setLevel(logging.DEBUG)
        else:
            module_logger.setLevel(logging.CRITICAL)  # Effectively silent

        _ensure_logger_handler(module_logger, handler)


def _configure_technical_modules(verbose: bool, handler: logging.Handler) -> None:
    """Configure technical modules with appropriate log levels.

    Args:
        verbose: Whether verbose mode is enabled
        handler: The logging handler to use
    """
    for module_name in TECHNICAL_MODULES:
        if module_name in NOISY_MODULES:
            continue  # Already handled by _configure_noisy_modules

        module_logger = logging.getLogger(module_name)

        # In verbose mode, show all logs from technical modules
        # Otherwise, only show warnings and above
        if verbose:
            module_logger.setLevel(logging.DEBUG)
        else:
            module_logger.setLevel(logging.WARNING)

        _ensure_logger_handler(module_logger, handler)


def _ensure_logger_handler(logger: logging.Logger, handler: logging.Handler) -> None:
    """Ensure a logger has the appropriate handler and propagation settings.

    Args:
        logger: The logger to configure
        handler: The handler to add if needed
    """
    if not logger.handlers:
        logger.addHandler(handler)
        logger.propagate = False


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
