"""Base connector interfaces for SQLFlow."""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.errors import ConnectorError


class ConnectorState(Enum):
    """State of a connector."""

    CREATED = auto()
    CONFIGURED = auto()
    READY = auto()
    ERROR = auto()


class ConnectorType(Enum):
    """Type of connector based on data flow direction."""

    SOURCE = "source"  # Can only read data (source connector)
    EXPORT = "export"  # Can only write data (export connector)
    BIDIRECTIONAL = "bidirectional"  # Can both read and write data


class ConnectionTestResult:
    """Result of a connection test."""

    def __init__(self, success: bool, message: Optional[str] = None):
        """Initialize a ConnectionTestResult.

        Args:
            success: Whether the test was successful
            message: Optional message with details
        """
        self.success = success
        self.message = message


class Schema:
    """Schema information for a data source or destination."""

    def __init__(self, arrow_schema: pa.Schema):
        """Initialize a Schema.

        Args:
            arrow_schema: Arrow schema
        """
        self.arrow_schema = arrow_schema

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, str]) -> "Schema":
        """Create a Schema from a dictionary.

        Args:
            schema_dict: Dictionary mapping field names to types

        Returns:
            Schema instance
        """
        fields = []
        for name, type_str in schema_dict.items():
            if type_str.lower() == "string":
                pa_type = pa.string()
            elif type_str.lower() == "int" or type_str.lower() == "integer":
                pa_type = pa.int64()
            elif type_str.lower() == "float":
                pa_type = pa.float64()
            elif type_str.lower() == "bool" or type_str.lower() == "boolean":
                pa_type = pa.bool_()
            elif type_str.lower() == "date":
                pa_type = pa.date32()
            elif type_str.lower() == "timestamp":
                pa_type = pa.timestamp("ns")
            else:
                pa_type = pa.string()  # Default to string for unknown types

            fields.append(pa.field(name, pa_type))

        return cls(pa.schema(fields))


class Connector(ABC):
    """Base class for all source connectors."""

    def __init__(self):
        """Initialize a connector."""
        self.state = ConnectorState.CREATED
        self.name: Optional[str] = None
        self.connector_type = ConnectorType.SOURCE

    @abstractmethod
    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: If configuration fails
        """

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the data source.

        Returns:
            Result of the connection test
        """

    @abstractmethod
    def discover(self) -> List[str]:
        """Discover available objects in the data source.

        Returns:
            List of object names (tables, files, etc.)

        Raises:
            ConnectorError: If discovery fails
        """

    @abstractmethod
    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an object.

        Args:
            object_name: Name of the object

        Returns:
            Schema for the object

        Raises:
            ConnectorError: If schema retrieval fails
        """

    @abstractmethod
    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from the source in chunks.

        Args:
            object_name: Name of the object to read
            columns: Optional list of columns to read
            filters: Optional filters to apply
            batch_size: Number of rows per batch

        Returns:
            Iterator yielding DataChunk objects

        Raises:
            ConnectorError: If reading fails
        """

    def validate_state(self, expected_state: ConnectorState) -> None:
        """Validate that the connector is in the expected state.

        Args:
            expected_state: Expected state

        Raises:
            ConnectorError: If the connector is not in the expected state
        """
        valid_states = {expected_state, ConnectorState.READY}
        if self.state not in valid_states:
            raise ConnectorError(
                self.name or "unknown",
                f"Invalid state: expected one of {valid_states}, got {self.state}",
            )


class ExportConnector(ABC):
    """Base class for all export connectors."""

    def __init__(self):
        """Initialize an export connector."""
        self.state = ConnectorState.CREATED
        self.name: Optional[str] = None
        self.connector_type = ConnectorType.EXPORT

    @abstractmethod
    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: If configuration fails
        """

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the destination.

        Returns:
            Result of the connection test
        """

    @abstractmethod
    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to the destination.

        Args:
            object_name: Name of the object to write to
            data_chunk: Data to write
            mode: Write mode (append, overwrite, etc.)

        Raises:
            ConnectorError: If writing fails
        """

    def validate_state(self, expected_state: ConnectorState) -> None:
        """Validate that the connector is in the expected state.

        Args:
            expected_state: Expected state

        Raises:
            ConnectorError: If the connector is not in the expected state
        """
        valid_states = {expected_state, ConnectorState.READY}
        if self.state not in valid_states:
            raise ConnectorError(
                self.name or "unknown",
                f"Invalid state: expected one of {valid_states}, got {self.state}",
            )


class BidirectionalConnector(Connector, ExportConnector):
    """Base class for connectors that support both source and export operations."""

    def __init__(self):
        """Initialize a bidirectional connector."""
        # Call both parent __init__ methods explicitly
        Connector.__init__(self)
        # Don't re-initialize state in ExportConnector.__init__
        # since it's already initialized in Connector.__init__
        # and we want to avoid potential state conflicts

        # Set the connector type explicitly
        self.connector_type = ConnectorType.BIDIRECTIONAL

    # Explicitly declare abstract methods that must be implemented by concrete classes
    # This ensures they're properly checked even if the class overrides parent methods

    @abstractmethod
    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: If configuration fails
        """

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection.

        Returns:
            Result of the connection test
        """

    @abstractmethod
    def discover(self) -> List[str]:
        """Discover available objects.

        Returns:
            List of object names

        Raises:
            ConnectorError: If discovery fails
        """

    @abstractmethod
    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an object.

        Args:
            object_name: Name of the object

        Returns:
            Schema for the object

        Raises:
            ConnectorError: If schema retrieval fails
        """

    @abstractmethod
    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from the source.

        Args:
            object_name: Name of the object to read
            columns: Optional list of columns to read
            filters: Optional filters to apply
            batch_size: Number of rows per batch

        Returns:
            Iterator yielding DataChunk objects

        Raises:
            ConnectorError: If reading fails
        """

    @abstractmethod
    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to the destination.

        Args:
            object_name: Name of the object to write to
            data_chunk: Data to write
            mode: Write mode (append, overwrite, etc.)

        Raises:
            ConnectorError: If writing fails
        """
