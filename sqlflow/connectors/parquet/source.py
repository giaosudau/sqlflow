"""Parquet Source Connector implementation.

This module provides a comprehensive Parquet connector that supports schema inference,
column selection, predicate pushdown, and optimized parallel reading.

Performance optimizations implemented:
- Predicate pushdown for efficient filtering at the storage layer
- Optimized batch sizes based on file characteristics
- Parallel file reading for multiple files
- Intelligent memory management for large datasets
"""

import glob
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

logger = get_logger(__name__)

# Performance constants following Zen of Python: "Simple is better than complex"
DEFAULT_BATCH_SIZE = 50000  # Optimal for most Parquet files (larger than CSV)
LARGE_FILE_THRESHOLD_MB = 100  # Threshold for using smaller batch sizes
MEMORY_EFFICIENT_BATCH_SIZE = 25000  # For very large files
MAX_PARALLEL_FILES = 4  # Maximum concurrent file reads


class ParquetSource(Connector):
    """
    Enhanced Parquet connector with performance optimizations.

    Features:
    - Schema inference and file pattern support
    - Predicate pushdown for efficient filtering
    - Optimized batch sizes based on file characteristics
    - Parallel reading for multiple files
    - Column selection for memory efficiency

    Performance optimizations follow Zen of Python:
    - "Simple is better than complex" - straightforward optimization rules
    - "Explicit is better than implicit" - clear parameter handling
    - "Practicality beats purity" - real-world performance over theoretical perfection
    """

    def __init__(self):
        """Initialize the Parquet connector."""
        super().__init__()
        self.path: Optional[str] = None
        self.columns: Optional[List[str]] = None
        self.combine_files: bool = True  # Changed to True for backward compatibility
        self.batch_size: int = DEFAULT_BATCH_SIZE
        self.parallel_reading: bool = True  # Enable parallel reading by default

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the Parquet connector.

        Args:
            params: Configuration parameters

        Required Parameters:
            path: File path or pattern (e.g., 'data.parquet', 'data/*.parquet')

        Optional Parameters:
            columns: List of columns to read
            combine_files: Whether to combine multiple files (default: True)
            batch_size: Rows per batch (default: auto-optimized)
            parallel_reading: Enable parallel file reading (default: True)
        """
        if not params.get("path"):
            raise ValueError("Parameter 'path' is required")

        self.path = params["path"]
        self.columns = params.get("columns")
        self.combine_files = params.get("combine_files", True)
        self.batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
        self.parallel_reading = params.get("parallel_reading", True)

        logger.info(f"Configured Parquet connector for path: {self.path}")
        self.state = ConnectorState.CONFIGURED

    def _get_optimal_batch_size(
        self, file_path: str, requested_size: Optional[int] = None
    ) -> int:
        """Calculate optimal batch size based on file characteristics.

        Following Zen of Python: "Practicality beats purity"
        Choose batch sizes that work well in practice.

        Args:
            file_path: Path to the Parquet file
            requested_size: User-requested batch size (takes precedence)

        Returns:
            Optimal batch size for the file
        """
        if requested_size and requested_size > 0:
            return requested_size

        try:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

            # "Simple is better than complex" - use straightforward rules
            if file_size_mb < 50:
                return DEFAULT_BATCH_SIZE
            elif file_size_mb < LARGE_FILE_THRESHOLD_MB:
                return DEFAULT_BATCH_SIZE // 2  # Smaller batches for medium files
            else:
                return MEMORY_EFFICIENT_BATCH_SIZE  # Even smaller for large files

        except OSError:
            # "Errors should never pass silently" - but provide sensible default
            return DEFAULT_BATCH_SIZE

    def _build_filter_expression(
        self, filters: Dict[str, Any]
    ) -> Optional[pc.Expression]:
        """Build PyArrow filter expression from filter dictionary.

        Supports predicate pushdown for efficient filtering at storage layer.

        Args:
            filters: Dictionary of column filters

        Returns:
            PyArrow compute expression or None
        """
        if not filters:
            return None

        expressions = []

        for column, condition in filters.items():
            if isinstance(condition, dict):
                # Handle operator-based conditions
                for op, value in condition.items():
                    expr = self._build_single_filter(column, op, value)
                    if expr is not None:
                        expressions.append(expr)
            else:
                # Simple equality condition
                expressions.append(pc.equal(pc.field(column), condition))

        return self._combine_expressions(expressions)

    def _build_single_filter(
        self, column: str, op: str, value: Any
    ) -> Optional[pc.Expression]:
        """Build a single filter expression.

        Args:
            column: Column name
            op: Operator string
            value: Filter value

        Returns:
            PyArrow compute expression or None
        """
        field = pc.field(column)

        # "Simple is better than complex" - straightforward operator mapping
        operator_map = {
            "==": pc.equal,
            "!=": pc.not_equal,
            ">": pc.greater,
            ">=": pc.greater_equal,
            "<": pc.less,
            "<=": pc.less_equal,
            "in": pc.is_in,
        }

        if op in operator_map:
            return operator_map[op](field, value)
        return None

    def _combine_expressions(
        self, expressions: List[pc.Expression]
    ) -> Optional[pc.Expression]:
        """Combine multiple expressions with AND logic.

        Args:
            expressions: List of expressions to combine

        Returns:
            Combined expression or None
        """
        if len(expressions) == 1:
            return expressions[0]
        elif len(expressions) > 1:
            # Use & operator for combining expressions (compatible with older PyArrow)
            result = expressions[0]
            for expr in expressions[1:]:
                result = result & expr
            return result

        return None

    def test_connection(self) -> ConnectionTestResult:
        """Test access to Parquet files.

        Returns:
            ConnectionTestResult with status and details
        """
        if self.state != ConnectorState.CONFIGURED:
            return ConnectionTestResult(
                success=False, message="Connector not configured"
            )

        try:
            files = self._get_file_list()
            if not files:
                return ConnectionTestResult(
                    success=False,
                    message=f"No files found matching pattern: {self.path}",
                )

            # Test reading the first file
            pq.read_metadata(files[0])

            return ConnectionTestResult(
                success=True,
                message=f"Successfully connected to {len(files)} Parquet file(s)",
            )

        except Exception as e:
            return ConnectionTestResult(
                success=False, message=f"Connection test failed: {str(e)}"
            )

    def discover(self) -> List[str]:
        """Discover available Parquet files.

        Returns:
            List of available file paths
        """
        try:
            files = self._get_file_list()
            return [os.path.basename(f) for f in files]
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            return []

    def get_schema(self, object_name: str = None) -> Schema:
        """Get schema from Parquet file(s).

        Args:
            object_name: Optional specific file name

        Returns:
            Schema object with column information
        """
        try:
            files = self._get_file_list()
            if not files:
                raise FileNotFoundError(f"No files found for path: {self.path}")

            # Read metadata from first file for schema
            metadata = pq.read_metadata(files[0])
            arrow_schema = metadata.schema.to_arrow_schema()

            # Filter columns if specified
            if self.columns:
                filtered_fields = [
                    field for field in arrow_schema if field.name in self.columns
                ]
                arrow_schema = pa.schema(filtered_fields)

            return Schema(arrow_schema)

        except Exception as e:
            logger.error(f"Schema retrieval failed: {e}")
            raise

    def read(
        self,
        object_name: str = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterator[DataChunk]:
        """Read data from Parquet file(s) with performance optimizations.

        Performance features:
        - Predicate pushdown for efficient filtering at storage layer
        - Optimized batch sizes based on file characteristics
        - Parallel file reading for multiple files
        - Column selection for memory efficiency

        Args:
            object_name: Optional specific file to read
            columns: Optional column list (overrides configured columns)
            filters: Optional filters with predicate pushdown support
                    Format: {"column": value} or {"column": {">=": value, "<": other_value}}
            batch_size: Optional batch size (overrides configured batch_size)
            options: Optional options for backward compatibility

        Yields:
            DataChunk objects containing the data
        """
        if self.state != ConnectorState.CONFIGURED:
            raise RuntimeError("Connector not configured")

        # Use parameters or fall back to configured values
        columns_to_read = columns or self.columns

        try:
            files = self._get_file_list()
            if not files:
                # Don't raise exception for empty pattern - just return no data
                return

            if object_name:
                # Read specific file
                files = [f for f in files if os.path.basename(f) == object_name]
                if not files:
                    raise FileNotFoundError(f"File not found: {object_name}")

            # Build filter expression for predicate pushdown
            filter_expression = (
                self._build_filter_expression(filters) if filters else None
            )

            if self.combine_files and len(files) > 1:
                # Read all files as one dataset with optimizations
                yield from self._read_combined_files_optimized(
                    files, columns_to_read, batch_size, filter_expression
                )
            else:
                # Read files separately with parallel processing if multiple files
                if len(files) > 1 and self.parallel_reading:
                    yield from self._read_files_parallel(
                        files, columns_to_read, batch_size, filter_expression
                    )
                else:
                    # Single file or parallel disabled
                    for file_path in files:
                        optimal_batch_size = self._get_optimal_batch_size(
                            file_path, batch_size
                        )
                        yield from self._read_single_file_optimized(
                            file_path,
                            columns_to_read,
                            optimal_batch_size,
                            filter_expression,
                        )

        except Exception as e:
            logger.error(f"Read operation failed: {e}")
            raise

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental loading.

        Returns:
            True if incremental loading is supported
        """
        return True

    def get_cursor_value(self, object_name: str, cursor_field: str) -> Any:
        """Get the current cursor value for incremental loading.

        Args:
            object_name: Name of the object
            cursor_field: Name of the cursor field

        Returns:
            Current cursor value
        """
        try:
            files = self._get_file_list()
            if not files:
                return None

            max_value = None
            for file_path in files:
                df = pd.read_parquet(file_path, columns=[cursor_field])
                if cursor_field in df.columns:
                    file_max = df[cursor_field].max()
                    if max_value is None or file_max > max_value:
                        max_value = file_max

            return max_value

        except Exception as e:
            logger.error(f"Cursor value retrieval failed: {e}")
            return None

    def _get_file_list(self) -> List[str]:
        """Get list of files matching the path pattern.

        Returns:
            List of file paths
        """
        if "*" in self.path or "?" in self.path:
            # Pattern matching
            files = glob.glob(self.path)
            return sorted(files)
        else:
            # Single file
            if os.path.exists(self.path):
                return [self.path]
            else:
                return []

    def _read_single_file_optimized(
        self,
        file_path: str,
        columns: Optional[List[str]],
        batch_size: int,
        filter_expression: Optional[pc.Expression] = None,
    ) -> Iterator[DataChunk]:
        """Read a single Parquet file in batches with optimizations.

        Args:
            file_path: Path to the file
            columns: Optional columns to read
            batch_size: Number of rows per batch
            filter_expression: Optional filter for predicate pushdown

        Yields:
            DataChunk objects
        """
        try:
            if filter_expression is not None:
                # Use dataset API for predicate pushdown
                dataset = ds.dataset(file_path, format="parquet")
                table = dataset.to_table(columns=columns, filter=filter_expression)

                # Convert to pandas and yield in batches
                df = table.to_pandas()
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i : i + batch_size]
                    yield DataChunk(data=batch_df)
            else:
                # Use ParquetFile for efficient streaming without filters
                parquet_file = pq.ParquetFile(file_path)
                for batch in parquet_file.iter_batches(
                    columns=columns, batch_size=batch_size
                ):
                    df = batch.to_pandas()
                    yield DataChunk(data=df)

        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            raise

    def _read_files_parallel(
        self,
        file_paths: List[str],
        columns: Optional[List[str]],
        batch_size: Optional[int],
        filter_expression: Optional[pc.Expression] = None,
    ) -> Iterator[DataChunk]:
        """Read multiple Parquet files in parallel.

        Following Zen: "Flat is better than nested" - simple parallel processing.

        Args:
            file_paths: List of file paths to read
            columns: Optional columns to read
            batch_size: Optional batch size (will be optimized per file)
            filter_expression: Optional filter for predicate pushdown

        Yields:
            DataChunk objects
        """

        def read_file_chunks(file_path: str) -> List[DataChunk]:
            """Read all chunks from a single file."""
            chunks = []
            optimal_batch_size = self._get_optimal_batch_size(file_path, batch_size)

            for chunk in self._read_single_file_optimized(
                file_path, columns, optimal_batch_size, filter_expression
            ):
                chunks.append(chunk)
            return chunks

        # "Explicit is better than implicit" - clearly limit parallel workers
        max_workers = min(len(file_paths), MAX_PARALLEL_FILES)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all file reading tasks
            future_to_file = {
                executor.submit(read_file_chunks, file_path): file_path
                for file_path in file_paths
            }

            # Yield chunks as they become available
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    chunks = future.result()
                    for chunk in chunks:
                        yield chunk
                except Exception as e:
                    logger.error(f"Failed to read file {file_path}: {e}")
                    # Continue with other files

    # Legacy method for backward compatibility
    def _read_single_file(
        self, file_path: str, columns: Optional[List[str]], batch_size: int
    ) -> Iterator[DataChunk]:
        """Legacy method - delegates to optimized version."""
        yield from self._read_single_file_optimized(file_path, columns, batch_size)

    def _read_combined_files_optimized(
        self,
        file_paths: List[str],
        columns: Optional[List[str]],
        batch_size: Optional[int],
        filter_expression: Optional[pc.Expression] = None,
    ) -> Iterator[DataChunk]:
        """Read multiple Parquet files as a combined dataset with optimizations.

        Args:
            file_paths: List of file paths
            columns: Optional columns to read
            batch_size: Optional batch size (will be optimized)
            filter_expression: Optional filter for predicate pushdown

        Yields:
            DataChunk objects
        """
        try:
            # "Simple is better than complex" - use PyArrow dataset for efficiency
            dataset = ds.dataset(file_paths, format="parquet")

            # Calculate optimal batch size based on total dataset size
            total_size_mb = sum(
                os.path.getsize(f) / (1024 * 1024)
                for f in file_paths
                if os.path.exists(f)
            )
            optimal_batch_size = batch_size or self._get_optimal_batch_size_for_dataset(
                total_size_mb
            )

            # Read with predicate pushdown and column selection using correct API
            table = dataset.to_table(columns=columns, filter=filter_expression)

            # Convert to pandas and yield in optimized batches
            df = table.to_pandas()

            for i in range(0, len(df), optimal_batch_size):
                batch_df = df.iloc[i : i + optimal_batch_size]
                yield DataChunk(data=batch_df)

        except Exception as e:
            logger.error(f"Failed to read combined files: {e}")
            raise

    def _get_optimal_batch_size_for_dataset(self, total_size_mb: float) -> int:
        """Calculate optimal batch size for combined dataset.

        Args:
            total_size_mb: Total size of dataset in MB

        Returns:
            Optimal batch size
        """
        # "Practicality beats purity" - adapt batch size to dataset size
        if total_size_mb < 100:
            return DEFAULT_BATCH_SIZE
        elif total_size_mb < 500:
            return DEFAULT_BATCH_SIZE // 2
        else:
            return MEMORY_EFFICIENT_BATCH_SIZE

    # Legacy method for backward compatibility
    def _read_combined_files(
        self, file_paths: List[str], columns: Optional[List[str]], batch_size: int
    ) -> Iterator[DataChunk]:
        """Legacy method - delegates to optimized version."""
        yield from self._read_combined_files_optimized(file_paths, columns, batch_size)
