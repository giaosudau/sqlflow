import os
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk

# Performance constants following Zen of Python: "Simple is better than complex"
# These defaults are optimized for typical use cases but can be overridden
DEFAULT_CHUNK_SIZE = 10000  # Optimal for most CSV files
LARGE_FILE_THRESHOLD_MB = 50  # Switch to PyArrow for files larger than this
MEMORY_EFFICIENT_CHUNK_SIZE = 5000  # For memory-constrained environments


class CSVSource(Connector):
    """
    Connector for reading data from CSV files.

    This connector implements the full Connector interface for CLI compatibility
    while maintaining backward compatibility with the legacy SourceConnector interface.
    Optimized for chunked reading, PyArrow support, and column selection at read time.

    Performance optimizations:
    - Automatic chunk size optimization based on file size
    - PyArrow engine selection for large files
    - Column selection at read time to minimize memory usage
    - Intelligent fallback from PyArrow to pandas when needed
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        if config:
            self.configure(config)
        elif config is not None:  # Empty dict was passed
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters including 'path'
        Raises:
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
        self.engine = params.get("engine", "auto")  # 'pandas', 'pyarrow', or 'auto'
        self.state = ConnectorState.CONFIGURED

    def _get_optimal_chunk_size(
        self, file_path: str, requested_size: Optional[int] = None
    ) -> int:
        """Calculate optimal chunk size based on file characteristics.

        Following Zen of Python: "Practicality beats purity"
        We choose chunk sizes that work well in practice rather than theoretical perfection.

        Args:
            file_path: Path to the CSV file
            requested_size: User-requested chunk size (takes precedence if provided)

        Returns:
            Optimal chunk size for the file
        """
        if requested_size and requested_size > 0:
            return requested_size

        try:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

            # "Simple is better than complex" - use straightforward rules
            if file_size_mb < 10:
                return DEFAULT_CHUNK_SIZE
            elif file_size_mb < 100:
                return DEFAULT_CHUNK_SIZE * 2
            else:
                return MEMORY_EFFICIENT_CHUNK_SIZE  # Larger files need smaller chunks for memory efficiency

        except OSError:
            # "Errors should never pass silently" - but we can provide a sensible default
            return DEFAULT_CHUNK_SIZE

    def _should_use_pyarrow(self, file_path: str, engine: str) -> bool:
        """Determine whether to use PyArrow based on file characteristics and user preference.

        Args:
            file_path: Path to the CSV file
            engine: Engine preference ('pandas', 'pyarrow', or 'auto')

        Returns:
            True if PyArrow should be used, False otherwise
        """
        if engine == "pandas":
            return False
        elif engine == "pyarrow":
            return True
        else:  # auto
            try:
                # "Explicit is better than implicit" - clearly state our logic
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                return file_size_mb >= LARGE_FILE_THRESHOLD_MB
            except OSError:
                return False  # Default to pandas if we can't determine file size

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

    def _prepare_read_options(
        self, options: Optional[Dict[str, Any]], columns: Optional[List[str]], **kwargs
    ) -> Dict[str, Any]:
        """Prepare options for CSV reading.

        Args:
            options: Base options dictionary
            columns: List of columns to read
            **kwargs: Additional keyword arguments

        Returns:
            Prepared options dictionary
        """
        read_options = options.copy() if options else {}
        read_options.update(kwargs)

        # Set default CSV reading options
        if "header" not in read_options:
            read_options["header"] = 0 if self.has_header else None
        if "delimiter" not in read_options:
            read_options["delimiter"] = self.delimiter
        if "encoding" not in read_options:
            read_options["encoding"] = self.encoding
        if columns:
            read_options["usecols"] = columns

        return read_options

    def _read_with_pyarrow(
        self, file_path: str, read_options: Dict[str, Any], batch_size: Optional[int]
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Read CSV using PyArrow engine.

        Args:
            file_path: Path to the CSV file
            read_options: Prepared read options
            batch_size: Optional batch size for chunked reading

        Returns:
            DataFrame or iterator of DataFrames
        """
        import pyarrow.csv as pacsv

        arrow_opts = pacsv.ReadOptions()
        parse_opts = pacsv.ParseOptions(delimiter=read_options["delimiter"])
        convert_opts = pacsv.ConvertOptions()
        if "usecols" in read_options:
            convert_opts = pacsv.ConvertOptions(include_columns=read_options["usecols"])

        if batch_size and batch_size > 0:

            def pyarrow_chunk_iter():
                """
                Efficient PyArrow chunked reading.
                "Readability counts" - clear iterator implementation.
                """
                try:
                    # Use PyArrow's streaming reader for memory efficiency
                    table = pacsv.read_csv(
                        file_path,
                        read_options=arrow_opts,
                        parse_options=parse_opts,
                        convert_options=convert_opts,
                    )

                    # Convert to pandas and yield in chunks
                    df = table.to_pandas()
                    total_rows = len(df)

                    for start_idx in range(0, total_rows, batch_size):
                        end_idx = min(start_idx + batch_size, total_rows)
                        yield df.iloc[start_idx:end_idx]

                except Exception:
                    # "Errors should never pass silently" - but handle gracefully
                    # If PyArrow chunking fails, fall back to reading entire file
                    table = pacsv.read_csv(
                        file_path,
                        read_options=arrow_opts,
                        parse_options=parse_opts,
                        convert_options=convert_opts,
                    )
                    yield table.to_pandas()

            return pyarrow_chunk_iter()

        table = pacsv.read_csv(
            file_path,
            read_options=arrow_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )
        return table.to_pandas()

    def _read_with_pandas(
        self, file_path: str, read_options: Dict[str, Any], batch_size: Optional[int]
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Read CSV using pandas engine.

        Args:
            file_path: Path to the CSV file
            read_options: Prepared read options
            batch_size: Optional batch size for chunked reading

        Returns:
            DataFrame or iterator of DataFrames
        """
        if batch_size and batch_size > 0:
            return pd.read_csv(file_path, chunksize=batch_size, **read_options)
        return pd.read_csv(file_path, **read_options)

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
        as_iterator: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Read data from the CSV file, with support for chunked reading and PyArrow engine.

        Performance optimizations:
        - Automatic optimal chunk size calculation based on file characteristics
        - Intelligent engine selection (PyArrow for large files, pandas for smaller ones)
        - Column selection at read time to minimize memory usage

        Args:
            object_name: Name/path of the CSV file (optional, uses self.path if not provided)
            columns: List of columns to read (optional)
            batch_size: Batch size for reading (if >0, enables chunked reading)
            options: Additional options for pandas.read_csv or pyarrow.csv.read_csv
            as_iterator: If True, always return an iterator (even for single chunk)
            **kwargs: Additional keyword arguments
        Returns:
            DataFrame (default) or iterator of DataFrames (if chunked or as_iterator)
        """
        file_path = object_name or self.path
        read_options = self._prepare_read_options(options, columns, **kwargs)

        # Engine selection: automatic based on file size, or user preference
        engine = read_options.pop("engine", self.engine)
        use_pyarrow = self._should_use_pyarrow(file_path, engine)

        # Optimize chunk size based on file characteristics - only if chunking is requested
        optimal_batch_size = None
        if batch_size is not None or as_iterator:
            optimal_batch_size = self._get_optimal_chunk_size(file_path, batch_size)

        try:
            if use_pyarrow:
                result = self._read_with_pyarrow(
                    file_path, read_options, optimal_batch_size
                )
            else:
                result = self._read_with_pandas(
                    file_path, read_options, optimal_batch_size
                )

            # Convert single DataFrame to iterator if requested
            if as_iterator and not isinstance(result, Iterator):

                def single_chunk_iter():
                    yield result

                return single_chunk_iter()

            return result

        except ImportError:
            if use_pyarrow:
                # "Errors should never pass silently" - fallback gracefully
                return self._read_with_pandas(
                    file_path, read_options, optimal_batch_size
                )
            raise

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        batch_size: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
        engine: Optional[str] = None,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> Iterator[DataChunk]:
        """
        Read data incrementally from the CSV file based on cursor field, with chunked reading and PyArrow support.

        Performance optimizations applied:
        - Uses optimal chunk size calculation automatically
        - Intelligent engine selection based on file size
        - Efficient cursor-based filtering

        Args:
            object_name: Name/path of the CSV file (matches self.path)
            cursor_field: Column name to use for incremental filtering
            cursor_value: Last value of cursor field from previous run
            batch_size: Number of rows per batch (for memory efficiency)
            options: Additional options for pandas.read_csv or pyarrow.csv.read_csv
            engine: 'pandas' or 'pyarrow' (optional)
            columns: List of columns to read (optional, default: all columns)
            **kwargs: Additional parameters
        Yields:
            DataChunk objects containing incremental data
        """
        read_opts = options.copy() if options else {}
        if engine:
            read_opts["engine"] = engine

        # "Explicit is better than implicit" - let the read method handle optimization
        chunk_iter = self.read(
            object_name=object_name,
            columns=columns,  # None means all columns - optimized at read time
            batch_size=batch_size,  # Will be optimized by _get_optimal_chunk_size
            options=read_opts,
            as_iterator=True,
            **kwargs,
        )
        for chunk_df in chunk_iter:
            if cursor_field not in chunk_df.columns:
                continue
            filtered_df = chunk_df
            if cursor_value is not None:
                try:
                    df_cursor_col = pd.to_datetime(chunk_df[cursor_field])
                    cursor_value_dt = pd.to_datetime(cursor_value)
                    filtered_df = chunk_df[df_cursor_col > cursor_value_dt]
                except (ValueError, TypeError):
                    try:
                        filtered_df = chunk_df[
                            pd.to_numeric(chunk_df[cursor_field])
                            > pd.to_numeric(cursor_value)
                        ]
                    except (ValueError, TypeError):
                        filtered_df = chunk_df[
                            chunk_df[cursor_field].astype(str) > str(cursor_value)
                        ]
            if not filtered_df.empty:
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
