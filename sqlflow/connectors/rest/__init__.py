from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.rest.source import RestSource

# Register with old registries for backward compatibility
source_registry.register("rest", RestSource)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "rest",
    RestSource,
    default_config={
        "method": "GET",
        "timeout": 30,
        "max_retries": 3,
        "flatten_response": True,
    },
    required_params=["url"],
    optional_params={
        "method": "GET",
        "headers": {},
        "params": {},
        "auth": {},
        "pagination": {},
        "timeout": 30,
        "max_retries": 3,
        "retry_delay": 1.0,
        "data_path": "",
        "flatten_response": True,
    },
    description="REST API source connector",
)

__all__ = [
    "RestSource",
]
