"""Base class for SQL engines in SQLFlow."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict

import pyarrow as pa


class SQLEngine(ABC):
    """Abstract base class for SQL engines in SQLFlow.

    This class defines the interface that all SQL engines must implement
    to be compatible with SQLFlow.
    """

    @abstractmethod
    def configure(
        self, config: Dict[str, Any], profile_variables: Dict[str, Any]
    ) -> None:
        """Configure the engine with settings from the profile.

        Args:
        ----
            config: Engine configuration from the profile
            profile_variables: Variables defined in the profile

        """

    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """Execute a SQL query.

        Args:
        ----
            query: SQL query to execute

        Returns:
        -------
            Query result object (engine-specific)

        """

    @abstractmethod
    def create_temp_table(self, name: str, data: Any) -> None:
        """Create a temporary table with the given data.

        Args:
        ----
            name: Name of the temporary table
            data: Data to insert into the table

        """

    @abstractmethod
    def register_arrow(self, table_name: str, arrow_table: pa.Table) -> None:
        """Register an Arrow table with the engine.

        Args:
        ----
            table_name: Name to register the table as
            arrow_table: PyArrow table to register

        """

    @abstractmethod
    def register_python_udf(self, name: str, function: Callable) -> None:
        """Register a Python UDF with the engine.

        Args:
        ----
            name: Name to register the UDF as
            function: Python function to register

        """

    @abstractmethod
    def process_query_for_udfs(self, query: str, udfs: Dict[str, Callable]) -> str:
        """Process a query to replace UDF references with engine-specific syntax.

        Args:
        ----
            query: Original SQL query with UDF references
            udfs: Dictionary of UDF names to functions

        Returns:
        -------
            Processed query with engine-specific UDF references

        """

    @abstractmethod
    def supports_feature(self, feature: str) -> bool:
        """Check if the engine supports a specific feature.

        Args:
        ----
            feature: Feature to check for support

        Returns:
        -------
            True if the feature is supported, False otherwise

        """

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
        ----
            table_name: Name of the table to check

        Returns:
        -------
            True if the table exists, False otherwise

        """

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table.

        Args:
        ----
            table_name: Name of the table

        Returns:
        -------
            Dict mapping column names to their types

        """

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the engine."""
