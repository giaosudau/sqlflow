"""Data handling module for DuckDB engine."""

from .handlers import ArrowDataHandler, DataHandlerFactory, PandasDataHandler
from .registration import DataRegistrationManager

__all__ = [
    "DataHandlerFactory",
    "PandasDataHandler",
    "ArrowDataHandler",
    "DataRegistrationManager",
]
