from sqlflow.connectors.in_memory.in_memory_connector import (
    IN_MEMORY_DATA_STORE,
    InMemoryDestination,
    InMemorySource,
)
from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.source_registry import source_registry

source_registry.register("in_memory", InMemorySource)
destination_registry.register("in_memory", InMemoryDestination)

__all__ = [
    "InMemorySource",
    "InMemoryDestination",
    "IN_MEMORY_DATA_STORE",
]
