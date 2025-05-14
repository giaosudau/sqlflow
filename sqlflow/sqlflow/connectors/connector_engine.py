"""Connector engine for SQLFlow."""

from typing import Any, Dict

from sqlflow.sqlflow.connectors import CONNECTOR_REGISTRY


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

    def load_data(self, connector_name: str, table_name: str) -> None:
        """Load data from a connector into a table.

        Args:
            connector_name: Name of the connector
            table_name: Name of the table to load data into
        """
        if connector_name not in self.registered_connectors:
            raise ValueError(f"Connector '{connector_name}' not registered")

        connector_info = self.registered_connectors[connector_name]

        if connector_info["instance"] is None:
            connector_class = CONNECTOR_REGISTRY[connector_info["type"]]
            connector = connector_class()
            connector.configure(connector_info["params"])
            connector_info["instance"] = connector

        connector = connector_info["instance"]
