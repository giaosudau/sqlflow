"""Base connector interfaces for SQLFlow."""

import time
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.errors import ConnectorError


class SyncMode(Enum):
    """Synchronization modes for connectors."""

    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"
    CDC = "cdc"


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


# Standardized Exception Hierarchy
class ParameterError(ConnectorError):
    """Parameter validation errors."""

    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Parameter error: {message}")


class IncrementalError(ConnectorError):
    """Incremental loading errors."""

    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Incremental error: {message}")


class HealthCheckError(ConnectorError):
    """Health check errors."""

    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Health check error: {message}")


# Industry-Standard Parameters
STANDARD_PARAMS = {
    # Connection parameters
    "host": str,
    "port": int,
    "database": str,
    "username": str,
    "password": str,
    # Incremental loading parameters
    "sync_mode": str,  # "full_refresh", "incremental", "cdc"
    "cursor_field": str,  # Field for incremental loading
    "primary_key": Union[str, List[str]],  # Primary key(s)
    # Performance parameters
    "batch_size": int,  # Default: 10000
    "timeout_seconds": int,  # Default: 300
    "max_retries": int,  # Default: 3
    # Data handling parameters
    "schema": str,  # Schema/namespace
    "table": str,  # Table/object name
    "query": str,  # Custom query override
    "path": str,  # File path for file-based connectors
    "has_header": bool,  # CSV header flag
}


class ParameterValidator:
    """Standardized parameter validation for all connectors."""

    def __init__(self, connector_type: str):
        self.connector_type = connector_type
        self.required_params = self._get_required_params()
        self.optional_params = self._get_optional_params()

    def validate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters and return normalized version."""
        # Check required parameters
        missing = set(self.required_params) - set(params.keys())
        if missing:
            raise ParameterError(f"Missing required parameters: {missing}")

        # Validate parameter types
        validated = {}
        for key, value in params.items():
            validated[key] = self._validate_param_type(key, value)

        # Set defaults for optional parameters
        for key, default in self.optional_params.items():
            if key not in validated:
                validated[key] = default

        return validated

    def _validate_param_type(self, key: str, value: Any) -> Any:
        """Validate individual parameter type."""
        expected_type = STANDARD_PARAMS.get(key, str)

        # Handle special cases for complex types
        if hasattr(expected_type, "__origin__"):
            # Handle Union types (e.g., Union[str, List[str]])
            if expected_type.__origin__ is Union:
                for arg_type in expected_type.__args__:
                    try:
                        return self._convert_to_type(value, arg_type)
                    except (ValueError, TypeError):
                        continue
                # If no type worked, raise error
                raise ParameterError(
                    f"Parameter '{key}' must be one of {expected_type.__args__}"
                )

            # Handle List types (e.g., List[str])
            elif expected_type.__origin__ is list:
                if not isinstance(value, list):
                    # Try to convert single value to list
                    if isinstance(value, str):
                        return [value]
                    else:
                        raise ParameterError(f"Parameter '{key}' must be a list")
                return value

        # Handle simple types
        return self._convert_to_type(value, expected_type)

    def _convert_to_type(self, value: Any, target_type: type) -> Any:
        """Convert value to target type."""
        if isinstance(value, target_type):
            return value

        # Special handling for dict types - they shouldn't be converted to strings automatically
        if target_type == str and isinstance(value, dict):
            raise ParameterError(f"Cannot convert dict to string: {value}")

        try:
            return target_type(value)
        except (ValueError, TypeError):
            raise ParameterError(
                f"Cannot convert {type(value).__name__} to {target_type.__name__}"
            )

    def _get_required_params(self) -> List[str]:
        """Get required parameters for connector type."""
        # Override in subclasses for connector-specific requirements
        return []

    def _get_optional_params(self) -> Dict[str, Any]:
        """Get optional parameters with defaults."""
        return {
            "batch_size": 10000,
            "timeout_seconds": 300,
            "max_retries": 3,
        }


class ConnectionTestResult:
    """Result of a connection test."""

    def __init__(self, success: bool, message: Optional[str] = None):
        """Initialize a ConnectionTestResult.

        Args:
        ----
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
        ----
            arrow_schema: Arrow schema

        """
        self.arrow_schema = arrow_schema

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, str]) -> "Schema":
        """Create a Schema from a dictionary.

        Args:
        ----
            schema_dict: Dictionary mapping field names to types

        Returns:
        -------
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
    """Standardized base class for all source connectors."""

    def __init__(self):
        """Initialize a connector."""
        self.state = ConnectorState.CREATED
        self.name: Optional[str] = None
        self.connector_type = ConnectorType.SOURCE
        self.connection_params: Dict[str, Any] = {}
        self.is_connected: bool = False
        self.health_status: str = "unknown"
        self._parameter_validator: Optional[ParameterValidator] = None

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

    def validate_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize parameters using standardized framework.

        Args:
        ----
            params: Parameters to validate

        Returns:
        -------
            Validated and normalized parameters

        Raises:
        ------
            ParameterError: If validation fails
        """
        if self._parameter_validator is None:
            self._parameter_validator = ParameterValidator(self.__class__.__name__)
        return self._parameter_validator.validate(params)

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the data source.

        Returns
        -------
            Result of the connection test

        """

    def check_health(self) -> Dict[str, Any]:
        """Comprehensive health check with performance metrics.

        Returns:
        -------
            Health status dictionary with metrics
        """
        start_time = time.time()

        try:
            # Test basic connectivity
            self._test_connection()

            # Test read capability
            self._test_read_capability()

            # Calculate response time
            response_time = (time.time() - start_time) * 1000

            self.health_status = "healthy"
            return {
                "status": "healthy",
                "connected": True,
                "response_time_ms": response_time,
                "last_check": datetime.utcnow().isoformat(),
                "capabilities": {
                    "incremental": self.supports_incremental(),
                    "batch_reading": True,
                    "health_monitoring": True,
                },
            }
        except Exception as e:
            self.health_status = "unhealthy"
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e),
                "last_check": datetime.utcnow().isoformat(),
            }

    def _test_connection(self) -> None:
        """Test basic connection - override in subclasses."""
        if not self.is_connected:
            raise ConnectionError("Not connected")

    def _test_read_capability(self) -> None:
        """Test read capability - override in subclasses."""
        # Default: no-op, connectors should implement specific tests

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for monitoring.

        Returns:
        -------
            Performance metrics dictionary
        """
        return {
            "connection_time_ms": 0,
            "query_time_ms": 0,
            "rows_per_second": 0,
            "bytes_transferred": 0,
        }

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
    def get_schema(self, object_name: str) -> Schema:
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
    ) -> Iterator[DataChunk]:
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

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data incrementally from the source using cursor-based approach.

        Args:
        ----
            object_name: Name of the object to read
            cursor_field: Field to use for incremental reading
            cursor_value: Last cursor value (watermark) for incremental reading
            columns: Optional list of columns to read
            batch_size: Number of rows per batch

        Returns:
        -------
            Iterator yielding DataChunk objects

        Raises:
        ------
            IncrementalError: If incremental reading fails

        """
        if not self.supports_incremental():
            raise IncrementalError("Connector does not support incremental loading")

        # Default implementation filters by cursor field
        if cursor_value is not None:
            filters = {cursor_field: {">=": cursor_value}}
        else:
            filters = None

        return self.read(
            object_name=object_name,
            columns=columns,
            filters=filters,
            batch_size=batch_size,
        )

    def supports_incremental(self) -> bool:
        """Check if the connector supports incremental reading.

        Returns:
        -------
            True if incremental reading is supported, False otherwise

        """
        return True  # Most connectors can support basic incremental reading

    def get_cursor_value(
        self, data_chunk: DataChunk, cursor_field: str
    ) -> Optional[Any]:
        """Extract the maximum cursor value from a data chunk.

        Args:
        ----
            data_chunk: Data chunk to extract cursor value from
            cursor_field: Field to use as cursor

        Returns:
        -------
            Maximum cursor value or None if not found

        """
        try:
            df = data_chunk.pandas_df
            if cursor_field in df.columns and not df.empty:
                return df[cursor_field].max()
        except Exception:
            pass
        return None

    def validate_incremental_params(self, params: Dict[str, Any]) -> None:
        """Validate incremental loading parameters.

        Args:
        ----
            params: Parameters to validate

        Raises:
        ------
            IncrementalError: If validation fails

        """
        sync_mode = params.get("sync_mode")

        if sync_mode == SyncMode.INCREMENTAL.value:
            cursor_field = params.get("cursor_field")
            if not cursor_field:
                raise IncrementalError(
                    "cursor_field is required for incremental sync mode"
                )
            if not self.supports_incremental():
                raise IncrementalError(
                    f"{self.__class__.__name__} does not support incremental loading"
                )

    def validate_state(self, expected_state: ConnectorState) -> None:
        """Validate that the connector is in the expected state.

        Args:
        ----
            expected_state: Expected state

        Raises:
        ------
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
        ----
            params: Configuration parameters

        Raises:
        ------
            ConnectorError: If configuration fails

        """

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the destination.

        Returns
        -------
            Result of the connection test

        """

    @abstractmethod
    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
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

    def validate_state(self, expected_state: ConnectorState) -> None:
        """Validate that the connector is in the expected state.

        Args:
        ----
            expected_state: Expected state

        Raises:
        ------
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
        ----
            params: Configuration parameters

        Raises:
        ------
            ConnectorError: If configuration fails

        """

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection.

        Returns
        -------
            Result of the connection test

        """

    @abstractmethod
    def discover(self) -> List[str]:
        """Discover available objects.

        Returns
        -------
            List of object names

        Raises
        ------
            ConnectorError: If discovery fails

        """

    @abstractmethod
    def get_schema(self, object_name: str) -> Schema:
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
    ) -> Iterator[DataChunk]:
        """Read data from the source.

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

    @abstractmethod
    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
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
