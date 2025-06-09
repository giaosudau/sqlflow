from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict, Iterator, List, Optional


class ConnectorState(Enum):
    """State of a connector."""

    CREATED = auto()
    CONFIGURED = auto()
    READY = auto()
    ERROR = auto()


class Connector(ABC):
    """Standardized base class for all source connectors."""

    def __init__(self):
        """Initialize a connector."""
        self.state = ConnectorState.CREATED
        self.name: Optional[str] = None
        self.connection_params: Dict[str, Any] = {}
        self.is_connected: bool = False
        self.health_status: str = "unknown"

    @abstractmethod
    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
        ----
            params: Configuration parameters

        Raises:
        ------
            ConnectorError: If configuration fails

        """

    @abstractmethod
    def test_connection(self) -> "ConnectionTestResult":
        """Test the connection to the data source.

        Returns
        -------
            Result of the connection test

        """

    @abstractmethod
    def discover(self) -> List[str]:
        """Discover available objects in the data source.

        Returns
        -------
            List of object names (tables, files, etc.)

        Raises
        ------
            ConnectorError: If discovery fails

        """

    @abstractmethod
    def get_schema(self, object_name: str) -> "Schema":
        """Get schema for an object.

        Args:
        ----
            object_name: Name of the object

        Returns:
        -------
            Schema for the object

        Raises:
        ------
            ConnectorError: If schema retrieval fails

        """

    @abstractmethod
    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator["DataChunk"]:
        """Read data from the source in chunks.

        Args:
        ----
            object_name: Name of the object to read
            columns: Optional list of columns to read
            filters: Optional filters to apply
            batch_size: Number of rows per batch

        Returns:
        -------
            Iterator yielding DataChunk objects

        Raises:
        ------
            ConnectorError: If reading fails

        """


class BidirectionalConnector(Connector):
    """Base class for connectors that support both source and export operations."""

    @abstractmethod
    def write(
        self, object_name: str, data_chunk: "DataChunk", mode: str = "append"
    ) -> None:
        """Write data to the destination.

        Args:
        ----
            object_name: Name of the object to write to
            data_chunk: Data to write
            mode: Write mode (append, overwrite, etc.)

        Raises:
        ------
            ConnectorError: If writing fails

        """
