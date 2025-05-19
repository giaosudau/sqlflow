import logging
import os
from typing import Any, Dict, Optional

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


def get_logger(name: str) -> logging.Logger:
    """Return a logger with the specified name.

    Args:
        name: The name for the logger, typically __name__

    Returns:
        A configured logger instance
    """
    fmt = DEFAULT_FORMAT
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))
    logger = logging.getLogger(name)

    # Only add handler if logger doesn't already have handlers
    if not logger.handlers:
        logger.addHandler(handler)

    # Don't set level here - it will be set by configure_logging
    return logger


def configure_logging(
    log_level: Optional[str] = None, config: Optional[Dict[str, Any]] = None
) -> None:
    """Configure the logging system for SQLFlow.

    Args:
        log_level: Optional string with log level ("debug", "info", "warning", "error", "critical")
                  This overrides any level in the config dictionary
        config: Optional dictionary with configuration values
    """
    # Get level from environment variable first, fallback to parameter
    env_level = os.environ.get("SQLFLOW_LOG_LEVEL", "").lower()
    if env_level and env_level in LOG_LEVELS:
        level = LOG_LEVELS[env_level]
    elif log_level and log_level.lower() in LOG_LEVELS:
        level = LOG_LEVELS[log_level.lower()]
    elif config and "log_level" in config and config["log_level"].lower() in LOG_LEVELS:
        level = LOG_LEVELS[config["log_level"].lower()]
    else:
        level = DEFAULT_LOG_LEVEL

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Set sqlflow package logger level
    sqlflow_logger = logging.getLogger("sqlflow")
    sqlflow_logger.setLevel(level)

    # Configure module-specific logging if provided in config
    if config and "module_log_levels" in config:
        module_levels = config["module_log_levels"]
        for module_name, module_level in module_levels.items():
            if module_level.lower() in LOG_LEVELS:
                module_logger = logging.getLogger(module_name)
                module_logger.setLevel(LOG_LEVELS[module_level.lower()])


def get_all_loggers() -> Dict[str, logging.Logger]:
    """Return a dictionary of all loggers in the system.

    Returns:
        A dictionary mapping logger names to logger instances
    """
    return logging.root.manager.loggerDict
