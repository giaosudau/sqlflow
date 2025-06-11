from sqlflow.connectors.in_memory.in_memory_connector import (
    IN_MEMORY_DATA_STORE,
    InMemoryDestination,
    InMemorySource,
)
from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry

# Register with old registries for backward compatibility
source_registry.register("in_memory", InMemorySource)
destination_registry.register("in_memory", InMemoryDestination)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "in_memory",
    InMemorySource,
    description="In-memory data source for testing",
)
enhanced_registry.register_destination(
    "in_memory",
    InMemoryDestination,
    description="In-memory data destination for testing",
)

__all__ = [
    "InMemorySource",
    "InMemoryDestination",
    "IN_MEMORY_DATA_STORE",
]
