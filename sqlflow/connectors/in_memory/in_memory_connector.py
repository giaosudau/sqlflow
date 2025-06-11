from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk

IN_MEMORY_DATA_STORE: Dict[str, pd.DataFrame] = {}


class InMemorySource(Connector):
    """
    Connector for reading data from memory, primarily for testing.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        if config is not None:
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector."""
        self.connection_params = params
        self.table_name = params.get("table_name")
        if not self.table_name:
            raise ValueError("'table_name' is required for InMemorySource")
        self.state = ConnectorState.CONFIGURED

    def test_connection(self) -> ConnectionTestResult:
        """Test connection."""
        if self.table_name in IN_MEMORY_DATA_STORE:
            return ConnectionTestResult(
                True, f"Table '{self.table_name}' exists in memory."
            )
        return ConnectionTestResult(
            False, f"Table '{self.table_name}' not found in memory."
        )

    def discover(self) -> List[str]:
        """Discover available tables."""
        return list(IN_MEMORY_DATA_STORE.keys())

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an in-memory table."""
        if object_name not in IN_MEMORY_DATA_STORE:
            raise ValueError(f"Table '{object_name}' not found in in-memory store")
        import pyarrow as pa

        arrow_schema = pa.Schema.from_pandas(IN_MEMORY_DATA_STORE[object_name])
        return Schema(arrow_schema)

    def read(
        self,
        object_name: str = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from the in-memory store."""
        target_table = object_name or self.table_name
        if target_table not in IN_MEMORY_DATA_STORE:
            raise ValueError(f"Table '{target_table}' not found in in-memory store")

        df = IN_MEMORY_DATA_STORE[target_table]
        if columns:
            df = df[columns]

        # Filters are not implemented for in-memory, but we don't error

        yield DataChunk(df)


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
