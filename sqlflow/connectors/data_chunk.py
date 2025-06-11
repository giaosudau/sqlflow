"""Data chunk container for SQLFlow connectors."""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import pandas as pd
import pyarrow as pa

T = TypeVar("T", pd.DataFrame, pa.Table)


class DataChunk:
    """Container for batches of data exchanged with connectors.

    DataChunk provides a standardized format for data interchange between
    connectors and the SQLFlow engine. It supports both Arrow tables and
    pandas DataFrames with automatic conversion between them.

    Optimized for memory efficiency and performance with:
    - Zero-copy operations where possible
    - Deferred schema computation
    - Vectorized operations
    - Efficient type conversions
    """

    __slots__ = (
        "_arrow_table",
        "_pandas_df",
        "_schema",
        "_original_column_names",
        "_cached_schema",
        "_cached_arrow_table",
        "_cached_pandas_df",
        "_conversion_cache",
    )

    def __init__(
        self,
        data: Union[pa.Table, pa.RecordBatch, pd.DataFrame, List[Dict[str, Any]]],
        schema: Optional[pa.Schema] = None,
        original_column_names: Optional[List[str]] = None,
    ):
        """Initialize a DataChunk.

        Args:
        ----
            data: The data for this chunk, either as PyArrow Table, PyArrow RecordBatch,
                pandas DataFrame, or list of dictionaries.
            schema: Optional schema for the data. If not provided and
                data is not a PyArrow Table, schema will be inferred.
            original_column_names: Optional list of original column names, used to preserve
                column names from source data (e.g., CSV headers).

        """
        self._arrow_table: Optional[pa.Table] = None
        self._pandas_df: Optional[pd.DataFrame] = None
        self._original_column_names: Optional[List[str]] = original_column_names
        self._schema: Optional[pa.Schema] = schema
        self._cached_schema: Optional[pa.Schema] = None
        self._cached_arrow_table: Optional[pa.Table] = None
        self._cached_pandas_df: Optional[pd.DataFrame] = None
        self._conversion_cache: Dict[str, Any] = {}

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

    @property
    def arrow_table(self) -> pa.Table:
        """Get data as PyArrow Table.

        Returns
        -------
            PyArrow Table representation of the data.

        """
        if self._arrow_table is None:
            assert self._pandas_df is not None
            # Use zero-copy conversion when possible
            self._arrow_table = self._convert_pandas_to_arrow(self._pandas_df)
        return self._arrow_table

    @property
    def pandas_df(self) -> pd.DataFrame:
        """Get data as pandas DataFrame.

        Returns
        -------
            pandas DataFrame representation of the data.

        """
        if self._pandas_df is None:
            assert self._arrow_table is not None
            # Use zero-copy conversion when possible
            self._pandas_df = self._convert_arrow_to_pandas(self._arrow_table)
        return self._pandas_df

    @property
    def schema(self) -> pa.Schema:
        """Get the schema of the data.

        Returns
        -------
            PyArrow Schema for the data.

        """
        if self._schema is not None:
            return self._schema
        if self._cached_schema is not None:
            return self._cached_schema
        self._cached_schema = self.arrow_table.schema
        return self._cached_schema

    @property
    def original_column_names(self) -> Optional[List[str]]:
        """Get the original column names if available.

        Returns
        -------
            List of original column names or None if not available.

        """
        return self._original_column_names

    def __len__(self) -> int:
        """Get the number of rows in this chunk.

        Returns
        -------
            Row count.

        """
        if self._arrow_table is not None:
            return len(self._arrow_table)
        assert self._pandas_df is not None
        return len(self._pandas_df)

    def to_pandas(self) -> pd.DataFrame:
        """Get data as pandas DataFrame (alias for pandas_df property).

        Returns
        -------
            pandas DataFrame representation of the data.

        """
        return self.pandas_df

    def _convert_pandas_to_arrow(self, df: pd.DataFrame) -> pa.Table:
        """Convert pandas DataFrame to Arrow Table with optimizations.

        Args:
        ----
            df: pandas DataFrame to convert

        Returns
        -------
            PyArrow Table

        """
        cache_key = f"pandas_to_arrow_{id(df)}"
        if cache_key in self._conversion_cache:
            return self._conversion_cache[cache_key]

        # Use optimal conversion settings
        table = pa.Table.from_pandas(
            df,
            preserve_index=False,  # Usually not needed for data processing
            safe=False,  # Allow unsafe conversions for performance
        )
        self._conversion_cache[cache_key] = table
        return table

    def _convert_arrow_to_pandas(self, table: pa.Table) -> pd.DataFrame:
        """Convert Arrow Table to pandas DataFrame with optimizations.

        Args:
        ----
            table: PyArrow Table to convert

        Returns
        -------
            pandas DataFrame

        """
        cache_key = f"arrow_to_pandas_{id(table)}"
        if cache_key in self._conversion_cache:
            return self._conversion_cache[cache_key]

        # Use optimal conversion settings
        df = table.to_pandas(
            use_threads=True,  # Enable parallel processing
            date_as_object=False,  # Use native datetime types
            strings_to_categorical=False,  # Avoid categorical conversion overhead
        )
        self._conversion_cache[cache_key] = df
        return df

    def vectorized_operation(
        self, operation: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "DataChunk":
        """Apply a vectorized operation to the data.

        Args:
        ----
            operation: A function that takes a pandas DataFrame and returns a transformed DataFrame

        Returns
        -------
            A new DataChunk with the transformed data

        """
        df = self.pandas_df.copy()  # Create a copy to avoid modifying original
        result_df = operation(df)
        return DataChunk(result_df)

    def filter(self, condition: Callable[[pd.DataFrame], pd.Series]) -> "DataChunk":
        """Filter the data using a vectorized condition.

        Args:
        ----
            condition: A function that takes a pandas DataFrame and returns a boolean Series

        Returns
        -------
            A new DataChunk with the filtered data

        """
        df = self.pandas_df
        mask = condition(df)
        filtered_df = df[mask].copy()  # Create a copy to avoid modifying original
        return DataChunk(filtered_df)

    def select_columns(self, columns: List[str]) -> "DataChunk":
        """Select specific columns from the data.

        Args:
        ----
            columns: List of column names to select

        Returns
        -------
            A new DataChunk with only the selected columns

        """
        df = self.pandas_df
        available_columns = [col for col in columns if col in df.columns]
        if not available_columns:
            raise ValueError(f"None of the specified columns {columns} found in data")
        return DataChunk(
            df[available_columns].copy()
        )  # Create a copy to avoid modifying original

    def apply_transform(
        self, transform_func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "DataChunk":
        """Apply a transformation function to the data.

        Args:
        ----
            transform_func: A function that takes a pandas DataFrame and returns a transformed DataFrame

        Returns
        -------
            A new DataChunk with the transformed data

        """
        df = self.pandas_df.copy()  # Create a copy to avoid modifying original
        result_df = transform_func(df)
        return DataChunk(result_df)

    def __eq__(self, other: Any) -> bool:
        """Compare two DataChunks for equality.

        Args:
        ----
            other: Another DataChunk to compare with

        Returns
        -------
            True if the DataChunks are equal, False otherwise

        """
        if not isinstance(other, DataChunk):
            return False
        return self.arrow_table.equals(other.arrow_table)
