"""Data chunk container for SQLFlow connectors."""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa


class DataChunk:
    """Container for batches of data exchanged with connectors.

    DataChunk provides a standardized format for data interchange between
    connectors and the SQLFlow engine. It supports both Arrow tables and
    pandas DataFrames with automatic conversion between them.
    """

    def __init__(
        self,
        data: Union[pa.Table, pa.RecordBatch, pd.DataFrame, List[Dict[str, Any]]],
        schema: Optional[pa.Schema] = None,
    ):
        """Initialize a DataChunk.

        Args:
            data: The data for this chunk, either as PyArrow Table, PyArrow RecordBatch,
                pandas DataFrame, or list of dictionaries.
            schema: Optional schema for the data. If not provided and
                data is not a PyArrow Table, schema will be inferred.
        """
        self._arrow_table: Optional[pa.Table] = None
        self._pandas_df: Optional[pd.DataFrame] = None

        if isinstance(data, pa.Table):
            self._arrow_table = data
        elif isinstance(data, pa.RecordBatch):
            self._arrow_table = pa.Table.from_batches([data])
        elif isinstance(data, pd.DataFrame):
            self._pandas_df = data
        elif isinstance(data, list):
            self._pandas_df = pd.DataFrame(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

        self._schema = schema

    @property
    def arrow_table(self) -> pa.Table:
        """Get data as PyArrow Table.

        Returns:
            PyArrow Table representation of the data.
        """
        if self._arrow_table is None:
            assert self._pandas_df is not None
            self._arrow_table = pa.Table.from_pandas(self._pandas_df)
        return self._arrow_table

    @property
    def pandas_df(self) -> pd.DataFrame:
        """Get data as pandas DataFrame.

        Returns:
            pandas DataFrame representation of the data.
        """
        if self._pandas_df is None:
            assert self._arrow_table is not None
            self._pandas_df = self._arrow_table.to_pandas()
        return self._pandas_df

    @property
    def schema(self) -> pa.Schema:
        """Get the schema of the data.

        Returns:
            PyArrow Schema for the data.
        """
        if self._schema is not None:
            return self._schema
        return self.arrow_table.schema

    def __len__(self) -> int:
        """Get the number of rows in this chunk.

        Returns:
            Row count.
        """
        if self._arrow_table is not None:
            return len(self._arrow_table)
        assert self._pandas_df is not None
        return len(self._pandas_df)
