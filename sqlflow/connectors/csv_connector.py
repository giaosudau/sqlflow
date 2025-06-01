"""CSV connector for SQLFlow."""

import csv
import os
from typing import Any, Dict, Iterator, List, Optional

import pyarrow.csv as csv_arrow

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector
from sqlflow.core.errors import ConnectorError
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@register_connector("CSV")
class CSVConnector(Connector):
    """Connector for CSV files."""

    def __init__(self):
        """Initialize a CSVConnector."""
        super().__init__()
        self.path: Optional[str] = None
        self.delimiter: str = ","
        self.has_header: bool = True
        self.quote_char: str = '"'
        self.encoding: str = "utf-8"

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
        ----
            params: Configuration parameters

        Raises:
        ------
            ConnectorError: If configuration fails

        """
        try:
            self.path = params.get("path")
            if not self.path:
                raise ValueError("Path is required")

            self.delimiter = params.get("delimiter", ",")

            # Handle variations of header parameter naming
            self.has_header = params.get("has_header", params.get("header", True))
            logger.debug(f"CSV Connector configured with has_header: {self.has_header}")

            self.quote_char = params.get("quote_char", '"')
            self.encoding = params.get("encoding", "utf-8")

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "CSV", f"Configuration failed: {str(e)}")

    def test_connection(self) -> ConnectionTestResult:
        """Test if the CSV file exists and is readable.

        Returns
        -------
            Result of the connection test

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                return ConnectionTestResult(False, "Path not configured")

            if not os.path.exists(self.path):
                self.state = ConnectorState.ERROR
                return ConnectionTestResult(False, f"File not found: {self.path}")

            with open(self.path, "r", encoding=self.encoding) as f:
                f.read(1)

            self.state = ConnectorState.READY
            return ConnectionTestResult(True)
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, str(e))

    def discover(self) -> List[str]:
        """Discover available objects in the data source.

        For CSV, this returns a single object representing the file.

        Returns
        -------
            List with a single object name

        Raises
        ------
            ConnectorError: If discovery fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                raise ValueError("Path not configured")

            if not os.path.exists(self.path):
                raise ValueError(f"File not found: {self.path}")

            base_name = os.path.basename(self.path)
            name, _ = os.path.splitext(base_name)

            return [name]
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "CSV", f"Discovery failed: {str(e)}")

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for the CSV file.

        Args:
        ----
            object_name: Name of the object (ignored for CSV)

        Returns:
        -------
            Schema for the CSV file

        Raises:
        ------
            ConnectorError: If schema retrieval fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                raise ValueError("Path not configured")

            if not os.path.exists(self.path):
                raise ValueError(f"File not found: {self.path}")

            if self.has_header:
                with open(self.path, "r", encoding=self.encoding) as f:
                    reader = csv.reader(
                        f, delimiter=self.delimiter, quotechar=self.quote_char
                    )
                    header_row = next(reader)

                read_options = csv_arrow.ReadOptions(
                    skip_rows=1, encoding=self.encoding
                )
                parse_options = csv_arrow.ParseOptions(
                    delimiter=self.delimiter, quote_char=self.quote_char
                )
                convert_options = csv_arrow.ConvertOptions()

                table = csv_arrow.read_csv(
                    self.path,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options,
                )

                import pyarrow as pa

                fields = [
                    pa.field(name, dtype)
                    for name, dtype in zip(header_row, table.schema.types)
                ]
                schema = pa.schema(fields)

                table = pa.Table.from_arrays(table.columns, schema=schema)
            else:
                read_options = csv_arrow.ReadOptions(
                    skip_rows=0, encoding=self.encoding
                )
                parse_options = csv_arrow.ParseOptions(
                    delimiter=self.delimiter, quote_char=self.quote_char
                )
                convert_options = csv_arrow.ConvertOptions()

                table = csv_arrow.read_csv(
                    self.path,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options,
                )

            self.state = ConnectorState.READY
            return Schema(table.schema)
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "CSV", f"Schema retrieval failed: {str(e)}"
            )

    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from the CSV file in chunks.

        Args:
        ----
            object_name: Name of the object (ignored for CSV)
            columns: Optional list of columns to read
            filters: Optional filters to apply (not supported for CSV)
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects

        Raises:
        ------
            ConnectorError: If reading fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                raise ValueError("Path not configured")

            if not os.path.exists(self.path):
                raise ValueError(f"File not found: {self.path}")

            if filters:
                import logging

                logging.warning("Filters are not supported for CSV and will be ignored")

            import pandas as pd

            # Add debug output
            logger.debug(f"CSV Connector reading file: {self.path}")
            logger.debug(f"Has header: {self.has_header}")

            # Read the first few lines to debug the content
            with open(self.path, "r", encoding=self.encoding) as f:
                first_lines = [
                    f.readline().strip()
                    for _ in range(min(5, sum(1 for _ in open(self.path))))
                ]
                logger.debug(f"First lines of CSV: {first_lines}")

            # Initialize original_column_names
            original_column_names = None

            # Read CSV file with pandas - explicitly set header=0 when has_header is True
            # This ensures pandas uses the first row as column names
            header_row = 0 if self.has_header else None
            logger.debug(f"Using header_row={header_row} for pandas.read_csv")

            df = pd.read_csv(
                self.path,
                sep=self.delimiter,
                header=header_row,  # Use 0 for first row as header, None for no header
                quotechar=self.quote_char,
                encoding=self.encoding,
                dtype=None,  # Allow pandas to automatically infer data types
            )

            # Store original column names if headers are available
            if self.has_header:
                original_column_names = df.columns.tolist()
                logger.debug(f"Storing original column names: {original_column_names}")

            # Print DataFrame info for debugging
            logger.debug(f"CSV loaded with columns: {df.columns.tolist()}")
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(
                f"DataFrame first row: {df.iloc[0].tolist() if len(df) > 0 else 'empty'}"
            )

            if columns:
                df = df[columns]
                # Update original_column_names to match filtered columns
                if original_column_names:
                    original_column_names = [
                        col for col in original_column_names if col in columns
                    ]

            import pyarrow as pa

            # Create a PyArrow table preserving the column names from pandas
            table = pa.Table.from_pandas(df, preserve_index=False)

            # Print PyArrow table info for debugging
            logger.debug(f"PyArrow table schema: {table.schema}")
            logger.debug(f"PyArrow column names: {table.column_names}")
            logger.debug(f"Original column names being passed: {original_column_names}")

            # Create and yield the DataChunk with the table and original column names
            yield DataChunk(table, original_column_names=original_column_names)

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "CSV", f"Reading failed: {str(e)}")

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read CSV with automatic filtering based on cursor field.

        For CSV files, we implement a simple in-memory filtering approach.
        In production, this could be optimized for large files.

        Args:
        ----
            object_name: Name of the object to read (ignored for CSV)
            cursor_field: Field to use for incremental filtering
            cursor_value: Last cursor value for filtering (None for initial load)
            columns: Optional list of columns to read
            batch_size: Number of rows per batch

        Returns:
        -------
            Iterator yielding DataChunk objects with filtered data
        """
        logger.info(
            f"CSV incremental read: cursor_field={cursor_field}, cursor_value={cursor_value}"
        )

        # Read all data first (simple approach for CSV)
        chunks = list(self.read(object_name, columns, None, batch_size))

        if not chunks:
            # Return empty DataFrame if no data
            import pandas as pd

            yield DataChunk(pd.DataFrame())
            return

        # For CSV, we'll process all chunks and filter
        for chunk in chunks:
            df = chunk.pandas_df

            if cursor_value is not None and cursor_field in df.columns and len(df) > 0:
                try:
                    # Simple filtering - let pandas handle type coercion
                    filtered_df = df[df[cursor_field] > cursor_value]
                    logger.info(
                        f"CSV incremental: filtered {len(df)} → {len(filtered_df)} rows"
                    )
                    yield DataChunk(filtered_df)
                except Exception as e:
                    logger.warning(
                        f"Incremental filtering failed: {e}, returning full data"
                    )
                    yield chunk
            else:
                # No filtering needed or possible
                yield chunk

    def supports_incremental(self) -> bool:
        """CSV connector supports incremental reading."""
        return True

    def get_cursor_value(
        self, data_chunk: DataChunk, cursor_field: str
    ) -> Optional[Any]:
        """Extract the maximum cursor value from a data chunk.

        Args:
        ----
            data_chunk: Data chunk to extract cursor value from
            cursor_field: Name of the cursor field

        Returns:
        -------
            Maximum cursor value in the chunk, or None if not found
        """
        try:
            df = data_chunk.pandas_df
            if cursor_field not in df.columns or len(df) == 0:
                return None

            # Get the maximum value in the cursor field, excluding NaN values
            series = df[cursor_field].dropna()  # Remove NaN values first
            if len(series) == 0:
                return None

            max_value = series.max()

            # Safe check for NaN values - convert to string and check
            try:
                if str(max_value).lower() in ["nan", "nat", "none"]:
                    return None
            except Exception:
                pass  # If conversion fails, continue with max_value

            logger.debug(f"CSV cursor value extracted: {cursor_field}={max_value}")
            return max_value

        except Exception as e:
            logger.warning(f"Failed to extract cursor value for {cursor_field}: {e}")
            return None

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to a CSV file.

        Args:
        ----
            object_name: Name of the object (used to create filename if path not set)
            data_chunk: Data to write
            mode: Write mode (append or overwrite)

        Raises:
        ------
            ConnectorError: If writing fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            write_path = self.path
            if not write_path:
                write_path = f"{object_name}.csv"

            directory = os.path.dirname(os.path.abspath(write_path))
            if directory:
                os.makedirs(directory, exist_ok=True)

            # Get DataFrame from DataChunk
            df = data_chunk.pandas_df

            file_exists = os.path.exists(write_path) and os.path.getsize(write_path) > 0
            file_mode = "a" if mode == "append" and file_exists else "w"
            write_header = file_mode == "w" or not file_exists

            df.to_csv(
                write_path,
                mode=file_mode,
                header=write_header,
                index=False,
                sep=self.delimiter,
                quoting=csv.QUOTE_MINIMAL,
                quotechar=self.quote_char,
                encoding=self.encoding,
            )

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "CSV", f"Writing failed: {str(e)}")
