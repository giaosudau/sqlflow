from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.source_registry import source_registry

from .destination import ParquetDestination
from .source import ParquetSource

source_registry.register("parquet", ParquetSource)
destination_registry.register("parquet", ParquetDestination)

__all__ = ["ParquetSource", "ParquetDestination"]
