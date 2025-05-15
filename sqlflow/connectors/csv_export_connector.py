"""CSV export connector for SQLFlow."""

import csv
import os
from typing import Any, Dict

from sqlflow.connectors.base import (
    ConnectionTestResult,
    ConnectorState,
    ExportConnector,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_export_connector
from sqlflow.core.errors import ConnectorError


@register_export_connector("CSV")
class CSVExportConnector(ExportConnector):
    """Export connector for CSV files."""

    def __init__(self):
        """Initialize a CSVExportConnector."""
        super().__init__()
        self.delimiter: str = ","
        self.header: bool = True
        self.quote_char: str = '"'
        self.encoding: str = "utf-8"

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: If configuration fails
        """
        try:
            self.delimiter = params.get("delimiter", ",")
            self.header = params.get("header", True)
            self.quote_char = params.get("quote_char", '"')
            self.encoding = params.get("encoding", "utf-8")
            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "CSV_EXPORT", f"Configuration failed: {str(e)}"
            )

    def test_connection(self) -> ConnectionTestResult:
        """Test if the CSV export is possible.

        Returns:
            Result of the connection test
        """
        self.state = ConnectorState.READY
        return ConnectionTestResult(True)

    def write(self, destination: str, data: DataChunk) -> None:
        """Write data to a CSV file.

        Args:
            destination: Destination file path
            data: Data to write

        Raises:
            ConnectorError: If writing fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            directory = os.path.dirname(os.path.abspath(destination))
            if directory:
                os.makedirs(directory, exist_ok=True)

            # Get DataFrame from DataChunk
            df = data.pandas_df

            df.to_csv(
                destination,
                mode="w",
                header=self.header,
                index=False,
                sep=self.delimiter,
                quoting=csv.QUOTE_MINIMAL,
                quotechar=self.quote_char,
                encoding=self.encoding,
            )

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "CSV_EXPORT", f"Writing failed: {str(e)}")
