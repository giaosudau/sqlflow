import os
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector


class CSVDestination(DestinationConnector):
    """
    Connector for writing data to CSV files using a safe "stage-and-swap" pattern.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.path = self.config.get("path")
        if not self.path:
            raise ValueError("CSVDestination: 'path' not specified in config")

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "replace",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the CSV file safely.

        - `replace`: Atomically replaces the file.
        - `append`: Appends to the file. Note: This is not atomic.
        - `upsert`: Not supported for CSV.
        """
        if mode.lower() == "upsert":
            raise NotImplementedError(
                "UPSERT mode is not supported for CSVDestination."
            )

        write_options = options or {}

        if mode.lower() == "replace":
            self._replace_safe(df, write_options)
        else:  # append
            self._append(df, write_options)

    def _replace_safe(self, df: pd.DataFrame, write_options: Dict[str, Any]):
        """Write to a temporary file and then atomically rename it."""
        temp_path = f"{os.path.dirname(self.path)}/._temp_{uuid.uuid4()}_{os.path.basename(self.path)}"

        # Set default write options for replace if not provided
        if "index" not in write_options:
            write_options["index"] = False
        if "header" not in write_options:
            write_options["header"] = True

        try:
            df.to_csv(temp_path, **write_options)
            os.rename(temp_path, self.path)
        except Exception:
            # Cleanup the temporary file on failure
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _append(self, df: pd.DataFrame, write_options: Dict[str, Any]):
        """Append data to the CSV file."""
        # For append, we usually don't want a header if the file exists
        if "header" not in write_options:
            write_options["header"] = not os.path.exists(self.path)
        if "index" not in write_options:
            write_options["index"] = False

        df.to_csv(self.path, mode="a", **write_options)
