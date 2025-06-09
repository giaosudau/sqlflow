from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.connectors.base.source_connector import SourceConnector

IN_MEMORY_DATA_STORE: Dict[str, pd.DataFrame] = {}


class InMemorySource(SourceConnector):
    """
    Connector for reading data from memory.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.table_name = self.config.get("table_name")
        if not self.table_name:
            raise ValueError("InMemorySource: 'table_name' not specified in config")

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the in-memory store.
        """
        if self.table_name not in IN_MEMORY_DATA_STORE:
            raise ValueError(f"Table '{self.table_name}' not found in in-memory store")
        return IN_MEMORY_DATA_STORE[self.table_name]


class InMemoryDestination(DestinationConnector):
    """
    Connector for writing data to memory.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.table_name = self.config.get("table_name")
        if not self.table_name:
            raise ValueError(
                "InMemoryDestination: 'table_name' not specified in config"
            )

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to the in-memory store.
        """
        IN_MEMORY_DATA_STORE[self.table_name] = df
