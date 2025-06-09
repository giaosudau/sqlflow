from sqlflow.connectors.google_sheets.source import GoogleSheetsSource
from sqlflow.connectors.registry.source_registry import source_registry

# Register the Google Sheets source connector
source_registry.register("google_sheets", GoogleSheetsSource)

# Export for backward compatibility
__all__ = ["GoogleSheetsSource"]
