from sqlflow.connectors.registry.destination_registry import destination_registry
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry

from .destination import CSVDestination
from .source import CSVSource

# Register with old registries for backward compatibility
source_registry.register("csv", CSVSource)
destination_registry.register("csv", CSVDestination)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "csv",
    CSVSource,
    default_config={
        "has_header": True,
        "delimiter": ",",
        "encoding": "utf-8",
    },
    required_params=["path"],
    optional_params={
        "has_header": True,
        "delimiter": ",",
        "encoding": "utf-8",
        "quote_char": '"',
        "sync_mode": "full_refresh",
    },
    description="CSV file source connector for reading CSV files",
)

enhanced_registry.register_destination(
    "csv",
    CSVDestination,
    default_config={
        "index": False,
        "header": True,
    },
    required_params=["path"],
    optional_params={
        "index": False,
        "header": True,
        "delimiter": ",",
        "encoding": "utf-8",
    },
    description="CSV file destination connector for writing CSV files",
)

__all__ = ["CSVSource", "CSVDestination"]
