import os
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk


class CSVSource(Connector):
    """
    Connector for reading data from CSV files.

    This connector implements the full Connector interface for CLI compatibility
    while maintaining backward compatibility with the legacy SourceConnector interface.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        if config:
            self.configure(config)
        elif config is not None:  # Empty dict was passed
            # If empty config dict is passed, still validate it
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
        ----
            params: Configuration parameters including 'path'

        Raises:
        ------
            ValueError: If required parameters are missing
        """
        self.connection_params = params
        self.path = params.get("path")
        if not self.path:
            raise ValueError("CSVSource: 'path' parameter is required")

        # Extract additional CSV-specific parameters
        self.has_header = params.get("has_header", True)
        self.delimiter = params.get("delimiter", ",")
        self.encoding = params.get("encoding", "utf-8")

        self.state = ConnectorState.CONFIGURED

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the CSV file.

        Returns:
        -------
            Result of the connection test
        """
        if not self.path:
            return ConnectionTestResult(False, "CSV path not configured")

        try:
            # Check if file exists and is readable
            if not os.path.exists(self.path):
                return ConnectionTestResult(False, f"CSV file not found: {self.path}")

            if not os.access(self.path, os.R_OK):
                return ConnectionTestResult(
                    False, f"CSV file not readable: {self.path}"
                )

            # Try to read a small sample to validate the file format
            try:
                sample_df = pd.read_csv(
                    self.path,
                    nrows=1,
                    header=0 if self.has_header else None,
                    delimiter=self.delimiter,
                    encoding=self.encoding,
                )
                if sample_df.empty and self.has_header:
                    # File might be valid but empty (header only)
                    return ConnectionTestResult(
                        True, "CSV file is accessible (empty data)"
                    )

                return ConnectionTestResult(
                    True,
                    f"CSV file is accessible with {len(sample_df.columns)} columns",
                )

            except pd.errors.EmptyDataError:
                return ConnectionTestResult(False, "CSV file is empty")
            except Exception as e:
                return ConnectionTestResult(False, f"CSV file format error: {str(e)}")

        except Exception as e:
            return ConnectionTestResult(False, f"Error accessing CSV file: {str(e)}")

    def discover(self) -> List[str]:
        """Discover available objects (files) in the CSV source.

        For CSV, this returns the configured file path.

        Returns:
        -------
            List containing the CSV file path
        """
        if self.path and os.path.exists(self.path):
            return [self.path]
        return []

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for the CSV file.

        Args:
        ----
            object_name: Path to the CSV file (should match self.path)

        Returns:
        -------
            Schema for the CSV file
        """
        try:
            # Read a small sample to infer schema
            sample_df = pd.read_csv(
                object_name or self.path,
                nrows=100,  # Sample for schema inference
                header=0 if self.has_header else None,
                delimiter=self.delimiter,
                encoding=self.encoding,
            )

            # Convert pandas dtypes to Arrow schema
            import pyarrow as pa

            arrow_schema = pa.Schema.from_pandas(sample_df)
            return Schema(arrow_schema)

        except Exception:
            # Return empty schema if we can't read the file
            import pyarrow as pa

            return Schema(pa.schema([]))

    # Legacy interface support for backward compatibility
    @property
    def config(self) -> Dict[str, Any]:
        """Backward compatibility property."""
        return getattr(self, "connection_params", {})

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Read data from the CSV file.

        Args:
            object_name: Name/path of the CSV file (optional, uses self.path if not provided)
            columns: List of columns to read (optional)
            batch_size: Batch size for reading (not used for CSV, but kept for interface compatibility)
            options: Additional options for pandas.read_csv
            **kwargs: Additional keyword arguments
        """
        # Use object_name if provided, otherwise use self.path
        file_path = object_name or self.path

        read_options = options or {}
        read_options.update(kwargs)

        # Set default CSV reading options
        if "header" not in read_options:
            read_options["header"] = 0 if self.has_header else None
        if "delimiter" not in read_options:
            read_options["delimiter"] = self.delimiter
        if "encoding" not in read_options:
            read_options["encoding"] = self.encoding

        # Read the CSV file
        df = pd.read_csv(file_path, **read_options)

        # Filter columns if specified
        if columns:
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        return df

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        batch_size: int = 10000,
        **kwargs,
    ) -> Iterator[DataChunk]:
        """
        Read data incrementally from the CSV file based on cursor field.

        Args:
            object_name: Name/path of the CSV file (matches self.path)
            cursor_field: Column name to use for incremental filtering
            cursor_value: Last value of cursor field from previous run
            batch_size: Number of rows per batch (for memory efficiency)
            **kwargs: Additional parameters

        Yields:
            DataChunk objects containing incremental data
        """
        df = self.read(object_name=object_name, **kwargs)

        # If no cursor field in the data, return full data
        if cursor_field not in df.columns:
            yield DataChunk(df)
            return

        # Filter data based on last cursor value
        if cursor_value is not None:
            # Try datetime conversion first
            try:
                # Convert DataFrame column to datetime
                df_cursor_col = pd.to_datetime(df[cursor_field])

                # Convert cursor_value to datetime with the same format
                if isinstance(cursor_value, str):
                    cursor_value_dt = pd.to_datetime(cursor_value)
                else:
                    cursor_value_dt = pd.to_datetime(cursor_value)

                # Filter using datetime comparison
                filtered_df = df[df_cursor_col > cursor_value_dt]

            except (ValueError, TypeError):
                # If datetime conversion fails, use original data types for comparison
                try:
                    # Try numeric comparison
                    filtered_df = df[
                        pd.to_numeric(df[cursor_field]) > pd.to_numeric(cursor_value)
                    ]
                except (ValueError, TypeError):
                    # Fall back to string comparison
                    filtered_df = df[df[cursor_field].astype(str) > str(cursor_value)]
        else:
            # No previous cursor value, return all data
            filtered_df = df

        yield DataChunk(filtered_df)

    def get_cursor_value(self, chunk: DataChunk, cursor_field: str) -> Optional[Any]:
        """
        Get the maximum cursor value from a data chunk.

        Args:
            chunk: DataChunk object containing data
            cursor_field: Name of the cursor field

        Returns:
            Maximum value of the cursor field in the chunk, or None if not found
        """
        df = chunk.pandas_df

        if cursor_field not in df.columns or df.empty:
            return None

        # Get the maximum value of the cursor field
        max_value = df[cursor_field].max()

        # Convert pandas null values to None
        if pd.isna(max_value):
            return None

        return max_value

    def supports_incremental(self) -> bool:
        """CSV connector supports incremental reading."""
        return True
