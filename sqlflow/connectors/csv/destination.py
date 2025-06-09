from typing import Any, Dict, Optional

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector


class CSVDestination(DestinationConnector):
    """
    Connector for writing data to CSV files.
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
    ) -> None:
        """
        Write data to the CSV file.
        """
        write_options = options or {}
        df.to_csv(self.path, **write_options)
