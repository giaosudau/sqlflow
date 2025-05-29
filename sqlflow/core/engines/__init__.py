"""SQLFlow engines module."""

# Import all engines
from sqlflow.core.engines.base import SQLEngine
from sqlflow.core.engines.duckdb import DuckDBEngine

__all__ = ["SQLEngine", "DuckDBEngine", "get_engine"]

# Set up logging
import logging

logger = logging.getLogger(__name__)


def get_engine(engine_type: str = "duckdb", **kwargs) -> SQLEngine:
    """Get an engine instance based on type.

    Args:
        engine_type: Type of engine to get (duckdb, etc.)
        **kwargs: Arguments to pass to the engine constructor

    Returns:
        Engine instance
    """
    logger.info(f"Initializing engine: {engine_type}")
    logger.debug(f"Initializing engine: {engine_type} with kwargs: {kwargs}")

    if engine_type.lower() == "duckdb":
        return DuckDBEngine(**kwargs)
    else:
        raise ValueError(f"Unsupported engine type: {engine_type}")
