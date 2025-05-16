"""Google Sheets connector for SQLFlow."""

from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from sqlflow.connectors.base import (
    BidirectionalConnector,
    ConnectionTestResult,
    ConnectorState,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import (
    register_bidirectional_connector,
)
from sqlflow.core.errors import ConnectorError


@register_bidirectional_connector("GOOGLE_SHEETS")
class GoogleSheetsConnector(BidirectionalConnector):
    """Connector for Google Sheets.

    This is a bidirectional connector that supports both reading from and writing to
    Google Sheets.
    """

    def __init__(self):
        """Initialize a GoogleSheetsConnector."""
        super().__init__()
        self.credentials_file: Optional[str] = None
        self.spreadsheet_id: Optional[str] = None
        self.sheet_name: Optional[str] = None
        self.range: Optional[str] = None
        self.has_header: bool = True
        self.service = None

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: If configuration fails
        """
        try:
            self.credentials_file = params.get("credentials_file")
            if not self.credentials_file:
                raise ValueError("Credentials file is required")

            self.spreadsheet_id = params.get("spreadsheet_id")
            if not self.spreadsheet_id:
                raise ValueError("Spreadsheet ID is required")

            self.sheet_name = params.get("sheet_name", "Sheet1")
            self.range = params.get("range")
            self.has_header = params.get("has_header", True)

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "GOOGLE_SHEETS", f"Configuration failed: {str(e)}"
            )

    def _initialize_service(self):
        """Initialize the Google Sheets service."""
        try:
            credentials = Credentials.from_service_account_file(
                self.credentials_file,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )

            # Create client options based on the version of google-api-core
            # Some versions might not have universe_domain attribute
            client_options = {}
            if hasattr(credentials, "universe_domain"):
                client_options["universe_domain"] = credentials.universe_domain

            self.service = build(
                "sheets", "v4", credentials=credentials, client_options=client_options
            )
        except Exception as e:
            raise ConnectorError(
                self.name or "GOOGLE_SHEETS", f"Failed to initialize service: {str(e)}"
            )

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to Google Sheets.

        Returns:
            Result of the connection test
        """
        try:
            if self.service is None:
                self._initialize_service()

            # Try to get spreadsheet info
            self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()

            self.state = ConnectorState.READY
            return ConnectionTestResult(True, "Successfully connected to Google Sheets")
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, f"Connection failed: {str(e)}")

    def discover(self) -> List[str]:
        """Discover available sheets in the spreadsheet.

        Returns:
            List of sheet names

        Raises:
            ConnectorError: If discovery fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.service is None:
                self._initialize_service()

            spreadsheet = (
                self.service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id)
                .execute()
            )

            sheets = []
            for sheet in spreadsheet.get("sheets", []):
                sheet_name = sheet["properties"]["title"]
                sheets.append(sheet_name)

            return sheets
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "GOOGLE_SHEETS", f"Discovery failed: {str(e)}"
            )

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for a sheet.

        Args:
            object_name: Sheet name

        Returns:
            Schema for the sheet

        Raises:
            ConnectorError: If schema retrieval fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.service is None:
                self._initialize_service()

            # Get first row to determine schema
            sheet_range = (
                f"{object_name}!A1:ZZ1"
                if self.range is None
                else f"{object_name}!{self.range}"
            )
            result = (
                self.service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=self.spreadsheet_id,
                    range=sheet_range,
                    valueRenderOption="UNFORMATTED_VALUE",
                )
                .execute()
            )

            values = result.get("values", [])
            if not values:
                # Return a basic schema if no data
                return Schema(pa.schema([]))

            # Use first row as column names if has_header is True
            if self.has_header:
                columns = values[0]
            else:
                # Generate column names (A, B, C, etc.)
                columns = [chr(65 + i) for i in range(len(values[0]))]

            # Create schema with string type for all columns
            fields = [pa.field(str(col), pa.string()) for col in columns]
            return Schema(pa.schema(fields))

        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "GOOGLE_SHEETS", f"Schema retrieval failed: {str(e)}"
            )

    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from a Google Sheet.

        Args:
            object_name: Sheet name
            columns: Optional list of columns to read
            filters: Optional filters to apply (not supported)
            batch_size: Number of rows per batch

        Yields:
            DataChunk objects

        Raises:
            ConnectorError: If reading fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.service is None:
                self._initialize_service()

            if filters:
                import logging

                logging.warning(
                    "Filters are not supported for Google Sheets and will be ignored"
                )

            # Construct range
            sheet_range = f"{object_name}!{self.range}" if self.range else object_name

            # Get values from sheet
            result = (
                self.service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=self.spreadsheet_id,
                    range=sheet_range,
                    valueRenderOption="UNFORMATTED_VALUE",
                )
                .execute()
            )

            values = result.get("values", [])
            if not values:
                return

            # Convert to DataFrame
            if self.has_header:
                header = values[0]
                data = values[1:]
                df = pd.DataFrame(data, columns=header)
            else:
                df = pd.DataFrame(values)

            if columns:
                df = df[columns]

            # Convert to Arrow table and yield as DataChunk
            table = pa.Table.from_pandas(df)
            yield DataChunk(table)

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "GOOGLE_SHEETS", f"Reading failed: {str(e)}"
            )

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to a Google Sheet.

        Args:
            object_name: Sheet name
            data_chunk: Data to write
            mode: Write mode (ignored for Google Sheets)

        Raises:
            ConnectorError: If writing fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.service is None:
                self._initialize_service()

            # Convert DataChunk to list of lists for Google Sheets API
            df = data_chunk.pandas_df
            values = [df.columns.tolist()]  # Header row
            values.extend(df.values.tolist())  # Data rows

            # Write to sheet
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{object_name}!A1",  # Start from A1
                valueInputOption="RAW",
                body={"values": values},
            ).execute()

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "GOOGLE_SHEETS", f"Writing failed: {str(e)}"
            )

    def close(self) -> None:
        """Close the Google Sheets service."""
        self.service = None
