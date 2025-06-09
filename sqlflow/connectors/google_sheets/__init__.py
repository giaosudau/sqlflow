from sqlflow.connectors.google_sheets.google_sheets_connector import (
    GoogleSheetsConnector,
)
from sqlflow.connectors.registry.source_registry import source_registry

source_registry.register("google_sheets", GoogleSheetsConnector)
