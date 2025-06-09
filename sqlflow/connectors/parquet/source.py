from typing import Any, Dict, Optional

import pandas as pd

from sqlflow.connectors.base.source_connector import SourceConnector


class ParquetSource(SourceConnector):
    """
    Connector for reading data from Parquet files.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.path = self.config.get("path")
        if not self.path:
            raise ValueError("ParquetSource: 'path' not specified in config")

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the Parquet file.
        """
        read_options = options or {}
        return pd.read_parquet(self.path, **read_options)
