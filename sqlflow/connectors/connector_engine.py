"""Connector engine for SQLFlow."""

from typing import Any, Dict, Iterator, List, Optional

from sqlflow.connectors import (
    CONNECTOR_REGISTRY,
    get_connector_class,
    get_export_connector_class,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.errors import ConnectorError


class ConnectorEngine:
    """Orchestrates data flow from connectors to database."""

    def __init__(self):
        """Initialize a ConnectorEngine."""
        self.registered_connectors: Dict[str, Dict[str, Any]] = {}

    def register_connector(
        self, name: str, connector_type: str, params: Dict[str, Any]
    ) -> None:
        """Register a connector.

        Args:
            name: Name of the connector
            connector_type: Type of the connector
            params: Parameters for the connector

        Raises:
            ValueError: If connector already exists or type is unknown
        """
        if name in self.registered_connectors:
            raise ValueError(f"Connector '{name}' already registered")

        if connector_type not in CONNECTOR_REGISTRY:
            raise ValueError(f"Unknown connector type: {connector_type}")

        self.registered_connectors[name] = {
            "type": connector_type,
            "params": params,
            "instance": None,
        }

    def load_data(
        self,
        connector_name: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Iterator[DataChunk]:
        """Load data from a connector.

        Args:
            connector_name: Name of the connector
            table_name: Name of the table to load data into
            columns: Optional list of columns to load
            filters: Optional filters to apply

        Returns:
            Iterator of DataChunk objects

        Raises:
            ValueError: If connector is not registered
            ConnectorError: If loading fails
        """
        if connector_name not in self.registered_connectors:
            raise ValueError(f"Connector '{connector_name}' not registered")

        connector_info = self.registered_connectors[connector_name]

        if connector_info["instance"] is None:
            connector_class = get_connector_class(connector_info["type"])
            connector = connector_class()
            connector.name = connector_name
            connector.configure(connector_info["params"])
            connector_info["instance"] = connector

        connector = connector_info["instance"]

        test_result = connector.test_connection()
        if not test_result.success:
            raise ConnectorError(
                connector_name, f"Connection test failed: {test_result.message}"
            )

        return connector.read(table_name, columns=columns, filters=filters)

    def export_data(
        self,
        data: Any,
        destination: str,
        connector_type: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Export data using an export connector.

        Args:
            data: Data to export
            destination: Destination to export to
            connector_type: Type of export connector to use
            options: Options for the export connector

        Raises:
            ValueError: If connector type is unknown
            ConnectorError: If export fails
        """
        try:
            if not isinstance(data, DataChunk):
                data = DataChunk(data)

            export_connector_class = get_export_connector_class(connector_type)

            # Create and configure export connector
            export_connector = export_connector_class()
            export_connector.name = f"{connector_type}_EXPORT"
            export_connector.configure(options or {})

            test_result = export_connector.test_connection()
            if not test_result.success:
                raise ConnectorError(
                    export_connector.name,
                    f"Connection test failed: {test_result.message}",
                )

            export_connector.write(destination, data)
        except Exception as e:
            raise ConnectorError(f"{connector_type}_EXPORT", f"Export failed: {str(e)}")
