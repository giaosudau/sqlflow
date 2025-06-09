from typing import Dict, Type

from sqlflow.connectors.base.destination_connector import DestinationConnector


class DestinationConnectorRegistry:
    """Registry for destination connectors."""

    def __init__(self):
        self._connectors: Dict[str, Type[DestinationConnector]] = {}

    def register(
        self, connector_type: str, connector_class: Type[DestinationConnector]
    ):
        """Register a destination connector."""
        self._connectors[connector_type] = connector_class

    def get(self, connector_type: str) -> Type[DestinationConnector]:
        """Get a destination connector class."""
        if connector_type not in self._connectors:
            raise ValueError(f"Unknown destination connector type: {connector_type}")
        return self._connectors[connector_type]


destination_registry = DestinationConnectorRegistry()
