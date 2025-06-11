from sqlflow.connectors.google_sheets.source import GoogleSheetsSource
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry

# Register with old registries for backward compatibility
source_registry.register("google_sheets", GoogleSheetsSource)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "google_sheets",
    GoogleSheetsSource,
    default_config={"has_header": True},
    required_params=["spreadsheet_id", "sheet_name"],
    optional_params={
        "credentials_file": "",
        "range": "",
        "has_header": True,
    },
    description="Google Sheets source connector",
)

# Export for backward compatibility
__all__ = ["GoogleSheetsSource"]
