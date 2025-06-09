from sqlflow.connectors.registry.destination_registry import destination_registry
from sqlflow.connectors.registry.source_registry import source_registry

from .destination import CSVDestination
from .source import CSVSource

source_registry.register("csv", CSVSource)
destination_registry.register("csv", CSVDestination)

__all__ = ["CSVSource", "CSVDestination"]
