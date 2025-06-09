from typing import Dict, Type

from sqlflow.connectors.base.source_connector import SourceConnector


class SourceConnectorRegistry:
    """Registry for source connectors."""

    def __init__(self):
        self._connectors: Dict[str, Type[SourceConnector]] = {}

    def register(self, connector_type: str, connector_class: Type[SourceConnector]):
        """Register a source connector."""
        self._connectors[connector_type] = connector_class

    def get(self, connector_type: str) -> Type[SourceConnector]:
        """Get a source connector class."""
        if connector_type not in self._connectors:
            raise ValueError(f"Unknown source connector type: {connector_type}")
        return self._connectors[connector_type]


source_registry = SourceConnectorRegistry()
