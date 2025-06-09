from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.rest.source import RestSource

source_registry.register("rest", RestSource)

__all__ = [
    "RestSource",
]
