from typing import Any, Dict

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector


class ParquetDestination(DestinationConnector):
    """
    Connector for writing data to Parquet files.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.path = self.config.get("path")
        if not self.path:
            raise ValueError("ParquetDestination: 'path' not specified in config")

    def write(self, data: pd.DataFrame, options: Dict[str, Any] = None) -> None:
        """
        Write data to the Parquet file.
        """
        write_options = options or {}
        data.to_parquet(self.path, **write_options)
