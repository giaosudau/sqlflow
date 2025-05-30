"""Data handlers for DuckDB engine."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
import pyarrow as pa

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class DataHandler(ABC):
    """Abstract base class for data handlers."""

    @abstractmethod
    def register(self, name: str, data: Any, connection: Any) -> None:
        """Register data with the database connection.

        Args:
        ----
            name: Name to register the data as
            data: Data to register
            connection: Database connection

        """


class PandasDataHandler(DataHandler):
    """Handler for pandas DataFrame registration."""

    def register(self, name: str, data: Any, connection: Any) -> None:
        """Register a pandas DataFrame with DuckDB.

        Args:
        ----
            name: Name to register the data as
            data: Pandas DataFrame
            connection: DuckDB connection

        """
        logger.debug(f"Registering pandas DataFrame: {name}")
        connection.register(name, data)


class ArrowDataHandler(DataHandler):
    """Handler for Apache Arrow table registration."""

    def register(self, name: str, data: Any, connection: Any) -> None:
        """Register an Arrow table with DuckDB.

        Args:
        ----
            name: Name to register the data as
            data: Arrow table
            connection: DuckDB connection

        """
        logger.debug(f"Registering Arrow table: {name}")
        connection.register(name, data)


class DataHandlerFactory:
    """Factory for creating appropriate data handlers."""

    @staticmethod
    def create(data: Any) -> DataHandler:
        """Create appropriate data handler based on data type.

        Args:
        ----
            data: Data to create handler for

        Returns:
        -------
            Appropriate data handler

        Raises:
        ------
            TypeError: If data type is not supported

        """
        # Check data type and return appropriate handler
        if isinstance(data, pd.DataFrame):
            return PandasDataHandler()
        elif isinstance(data, pa.Table):
            return ArrowDataHandler()
        else:
            # Raise an error for unsupported data types
            raise TypeError(
                f"Unsupported data type: {type(data)}. "
                f"Supported types are pandas.DataFrame and pyarrow.Table."
            )
