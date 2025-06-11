import os
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector


class ParquetDestination(DestinationConnector):
    """
    Connector for writing data to Parquet files using a safe "stage-and-swap" pattern.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.path = self.config.get("path")
        if not self.path:
            raise ValueError("ParquetDestination: 'path' not specified in config")

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "replace",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the Parquet file safely.

        - `replace`: Atomically replaces the file.
        - `append`: Appends to the file. This creates a new file in a directory for partitioned data,
                    but is not well-defined for single-file destinations. We will overwrite.
        - `upsert`: Not supported for Parquet.
        """
        if mode.lower() == "upsert":
            raise NotImplementedError(
                "UPSERT mode is not supported for ParquetDestination."
            )

        write_options = options or {}

        # For Parquet, 'append' is ambiguous for a single file. The safe default is to replace.
        # True append for Parquet is usually done by writing new files to a partitioned dataset,
        # which is handled by setting the `path` to a directory and using `partition_cols`.
        if mode.lower() in ["replace", "append"]:
            self._replace_safe(df, write_options)
        else:
            raise ValueError(f"Unsupported write mode for ParquetDestination: {mode}")

    def _replace_safe(self, df: pd.DataFrame, write_options: Dict[str, Any]):
        """Write to a temporary file and then atomically rename it."""
        # If path is a directory (for partitioning), we write directly.
        # The atomic operation is the creation of the final file by the engine.
        if os.path.isdir(self.path) or "partition_cols" in write_options:
            df.to_parquet(self.path, **write_options)
            return

        temp_path = f"{os.path.dirname(self.path)}/._temp_{uuid.uuid4()}_{os.path.basename(self.path)}"

        if "index" not in write_options:
            write_options["index"] = False

        try:
            df.to_parquet(temp_path, **write_options)
            os.rename(temp_path, self.path)
        except Exception:
            # Cleanup the temporary file on failure
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
