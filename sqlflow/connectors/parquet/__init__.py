from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry

from .destination import ParquetDestination
from .source import ParquetSource

# Register with old registries for backward compatibility
source_registry.register("parquet", ParquetSource)
destination_registry.register("parquet", ParquetDestination)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "parquet",
    ParquetSource,
    required_params=["path"],
    description="Parquet file source connector",
)
enhanced_registry.register_destination(
    "parquet",
    ParquetDestination,
    required_params=["path"],
    description="Parquet file destination connector",
)

__all__ = ["ParquetSource", "ParquetDestination"]
