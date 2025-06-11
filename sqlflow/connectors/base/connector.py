from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict, Iterator, List, Optional

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk


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
        self.resilience_manager: Optional[Any] = (
            None  # Will be ResilienceManager when configured
        )

    @property
    def is_resilient(self) -> bool:
        """Check if resilience is configured for this connector."""
        return self.resilience_manager is not None

    def _needs_resilience(self) -> bool:
        """Determine if this connector type requires resilience patterns.

        Override in subclasses to customize resilience behavior.
        Default: Enable resilience for all connectors except in-memory types.
        """
        # A more robust check than string matching
        if getattr(self, "_is_resilient_by_design", True) is False:
            return False

        class_name = self.__class__.__name__.lower()
        if "memory" in class_name or "mock" in class_name:
            return False
        return True

    def _configure_resilience(self, params: Dict[str, Any]) -> None:
        """Configure resilience patterns based on connector type."""
        if not self._needs_resilience():
            return

        # Import here to avoid circular dependencies
        from sqlflow.connectors.resilience import ResilienceManager
        from sqlflow.connectors.resilience_profiles import (
            get_resilience_config_for_connector,
        )

        # Get resilience configuration from params or use defaults
        resilience_config = params.get("resilience")
        if resilience_config is None:
            # Use connector-specific default configuration
            resilience_config = get_resilience_config_for_connector(
                self.__class__.__name__
            )

        if resilience_config is not None:
            self.resilience_manager = ResilienceManager(
                config=resilience_config, name=f"{self.__class__.__name__}_{id(self)}"
            )

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
