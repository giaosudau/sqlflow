# This file is intentionally left blank.

from .destination_registry import destination_registry
from .source_registry import source_registry


def get_connector_class(connector_type: str):
    try:
        return source_registry.get(connector_type)
    except ValueError:
        # For source operations, we should only check source registry
        # and provide the correct error message
        try:
            return destination_registry.get(connector_type)
        except ValueError:
            # Re-raise the original source registry error for clarity
            raise ValueError(f"Unknown source connector type: {connector_type}")


def register_connector(connector_type: str):
    def decorator(connector_class):
        if hasattr(connector_class, "read"):
            source_registry.register(connector_type, connector_class)
        if hasattr(connector_class, "write"):
            destination_registry.register(connector_type, connector_class)
        return connector_class

    return decorator


__all__ = [
    "source_registry",
    "destination_registry",
    "get_connector_class",
    "register_connector",
]
