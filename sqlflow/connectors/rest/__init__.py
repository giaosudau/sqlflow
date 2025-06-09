from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.rest.source import RestApiSource

source_registry.register("rest", RestApiSource)
