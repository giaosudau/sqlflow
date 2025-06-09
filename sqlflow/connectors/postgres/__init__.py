from sqlflow.connectors.postgres.destination import PostgresDestination
from sqlflow.connectors.postgres.source import PostgresSource
from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.source_registry import source_registry

source_registry.register("postgres", PostgresSource)
destination_registry.register("postgres", PostgresDestination)
