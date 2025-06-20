"""Simple Connector Engine Stub for V1 Compatibility.

This module extracts the complex connector engine stub logic from the orchestrator,
following the Single Responsibility Principle.
"""

from pathlib import Path
from typing import Any

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class MockResult:
    """Mock result object for query execution."""

    def fetchone(self):
        return [0]  # Mock row count

    def fetchall(self):
        return []


class SimpleConnectorEngineStub:
    """Simple stub implementation for V1 compatibility."""

    def __init__(self):
        self.registered_connectors = {}
        self.profile_config = None

    def export_data(self, data, connector_type, destination, options, *args, **kwargs):
        """Export data using the specified connector type."""
        logger.debug(f"Stub export: {connector_type} to {destination}")

        if self._is_csv_file(connector_type, destination):
            self._create_csv_file(data, destination)

        return {"status": "success", "rows_exported": 0}

    def register_connector(
        self, source_name: str, connector_type: str, connector_params: dict[str, Any]
    ):
        """Register a connector with the engine."""
        self.registered_connectors[source_name] = {
            "type": connector_type,
            "params": connector_params,
            "instance": None,
        }
        logger.debug(f"Stub registered connector: {source_name} ({connector_type})")
        return True

    def load_data(self, source_name: str, table_name: str):
        """Load data from a registered connector."""
        logger.debug(f"Stub load data: {source_name} -> {table_name}")
        return []

    def register_table(self, table_name: str, data):
        """Register table data (V1 compatibility)."""
        logger.debug(f"Stub register table: {table_name}")
        return True

    def register_arrow(self, table_name: str, arrow_table):
        """Register arrow table data (V1 compatibility)."""
        logger.debug(f"Stub register arrow table: {table_name}")
        return True

    def execute_query(self, query: str):
        """Execute SQL query (V1 compatibility)."""
        logger.debug(f"Stub execute query: {query[:50]}...")
        return MockResult()

    def execute(self, query: str):
        """Execute SQL query (V1 compatibility)."""
        return self.execute_query(query)

    def process_query_for_udfs(self, query: str, udfs=None):
        """Process query for UDFs (V1 compatibility)."""
        logger.debug(f"Stub process query for UDFs: {query[:50]}...")
        return query

    def get_all_tables(self):
        """Get all tables (V1 compatibility)."""
        return {}

    def _is_csv_file(self, connector_type: str, destination: str) -> bool:
        """Check if this is a CSV file export."""
        return connector_type == "csv" and destination.endswith(".csv")

    def _create_csv_file(self, data, destination: str) -> None:
        """Create CSV file for export."""
        dest_path = Path(destination)

        if data is None:
            # Create empty CSV file with just headers if no data
            dest_path.touch()
        else:
            # For stub purposes, just create empty file
            dest_path.touch()
