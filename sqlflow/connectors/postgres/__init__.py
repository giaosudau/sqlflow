from sqlflow.connectors.postgres.destination import PostgresDestination
from sqlflow.connectors.postgres.source import PostgresSource
from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry

# Register with old registries for backward compatibility
source_registry.register("postgres", PostgresSource)
destination_registry.register("postgres", PostgresDestination)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "postgres",
    PostgresSource,
    default_config={"port": 5432},
    required_params=["host", "database", "username"],
    optional_params={
        "password": "",
        "port": 5432,
        "connect_timeout": 10,
        "schema": "public",
    },
    description="PostgreSQL database source connector",
)
enhanced_registry.register_destination(
    "postgres",
    PostgresDestination,
    default_config={"port": 5432, "if_exists": "replace"},
    required_params=["host", "database", "username", "table"],
    optional_params={
        "password": "",
        "port": 5432,
        "schema": "public",
        "if_exists": "replace",
    },
    description="PostgreSQL database destination connector",
)

__all__ = [
    "PostgresSource",
    "PostgresDestination",
]
