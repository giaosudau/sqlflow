from typing import Any, Dict, Optional

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from sqlflow.connectors.base.source_connector import SourceConnector


class GoogleSheetsConnector(SourceConnector):
    """
    Connector for reading data from Google Sheets.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.credentials_file = self.config.get("credentials_file")
        self.spreadsheet_id = self.config.get("spreadsheet_id")
        self.sheet_name = self.config.get("sheet_name")

        if not all([self.credentials_file, self.spreadsheet_id, self.sheet_name]):
            raise ValueError(
                "GoogleSheetsConnector: 'credentials_file', 'spreadsheet_id', and 'sheet_name' are required"
            )

        self.service = self._initialize_service()

    def _initialize_service(self):
        """Initialize the Google Sheets service."""
        credentials = Credentials.from_service_account_file(
            self.credentials_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        client_options = {"universe_domain": "googleapis.com"}
        return build(
            "sheets", "v4", credentials=credentials, client_options=client_options
        )

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the Google Sheet.
        """
        read_options = options or {}
        sheet_range = read_options.get("range", f"{self.sheet_name}")

        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=sheet_range)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            return pd.DataFrame()

        # Assume first row is header
        return pd.DataFrame(values[1:], columns=values[0])
