"""PostgreSQL placeholder connector for SQLFlow.

This module provides a minimal placeholder for the PostgreSQL connector
when the psycopg2 dependency is not available. This allows tests that check
for connector registration to pass even without all dependencies installed.

The placeholder ensures that:
1. The POSTGRES connector is registered in the CONNECTOR_REGISTRY
2. Tests checking for the existence of required connectors pass
3. Users attempting to use the PostgreSQL connector without installing psycopg2
   will receive clear error messages indicating the missing dependency

In production environments, users should install the psycopg2 package before
using the PostgreSQL connector:

    pip install psycopg2-binary
"""

from typing import Any, Dict, Iterator, List, Optional

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector
from sqlflow.core.errors import ConnectorError


@register_connector("POSTGRES")
class PostgresPlaceholderConnector(Connector):
    """Placeholder connector for PostgreSQL databases when psycopg2 is not available."""

    def __init__(self):
        """Initialize a PlaceholderPostgresConnector."""
        super().__init__()
        self.state = ConnectorState.ERROR

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: Always raised because this is a placeholder
        """
        raise ConnectorError(
            "POSTGRES",
            "PostgreSQL connector requires psycopg2 package. Please install it with: pip install psycopg2-binary",
        )

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the PostgreSQL database.

        Returns:
            Result of the connection test (always failure)
        """
        return ConnectionTestResult(
            False,
            "PostgreSQL connector requires psycopg2 package. Please install it with: pip install psycopg2-binary",
        )

    def discover(self) -> List[str]:
        """Discover available tables in the PostgreSQL database.

        Returns:
            List of table names

        Raises:
            ConnectorError: Always raised because this is a placeholder
        """
        raise ConnectorError(
            "POSTGRES",
            "PostgreSQL connector requires psycopg2 package. Please install it with: pip install psycopg2-binary",
        )

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for a PostgreSQL table.

        Args:
            object_name: Table name

        Returns:
            Schema for the table

        Raises:
            ConnectorError: Always raised because this is a placeholder
        """
        raise ConnectorError(
            "POSTGRES",
            "PostgreSQL connector requires psycopg2 package. Please install it with: pip install psycopg2-binary",
        )

    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from a PostgreSQL table.

        Args:
            object_name: Table name
            columns: Optional list of columns to read
            filters: Optional filters to apply
            batch_size: Number of rows per batch

        Yields:
            DataChunk objects

        Raises:
            ConnectorError: Always raised because this is a placeholder
        """
        raise ConnectorError(
            "POSTGRES",
            "PostgreSQL connector requires psycopg2 package. Please install it with: pip install psycopg2-binary",
        )
