"""Parquet Source Connector implementation.

This module provides a comprehensive Parquet connector that supports schema inference,
column selection, and file pattern matching.
"""

import glob
import os
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ParquetSource(Connector):
    """
    Enhanced Parquet connector with schema inference and file pattern support.

    Supports single files, file patterns, and column selection for optimized loading.
    """

    def __init__(self):
        """Initialize the Parquet connector."""
        super().__init__()
        self.path: Optional[str] = None
        self.columns: Optional[List[str]] = None
        self.combine_files: bool = True  # Changed to True for backward compatibility
        self.batch_size: int = 10000

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the Parquet connector.

        Args:
            params: Configuration parameters

        Required Parameters:
            path: File path or pattern (e.g., 'data.parquet', 'data/*.parquet')

        Optional Parameters:
            columns: List of columns to read
            combine_files: Whether to combine multiple files (default: False)
            batch_size: Rows per batch (default: 10000)
        """
        if not params.get("path"):
            raise ValueError("Parameter 'path' is required")

        self.path = params["path"]
        self.columns = params.get("columns")
        self.combine_files = params.get("combine_files", True)
        self.batch_size = params.get("batch_size", 10000)

        logger.info(f"Configured Parquet connector for path: {self.path}")
        self.state = ConnectorState.CONFIGURED

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
    ) -> Iterator[DataChunk]:
        """Read data from Parquet file(s).

        Args:
            object_name: Optional specific file to read
            columns: Optional column list (overrides configured columns)
            filters: Optional filters (not implemented yet)
            batch_size: Optional batch size (overrides configured batch_size)

        Yields:
            DataChunk objects containing the data
        """
        if self.state != ConnectorState.CONFIGURED:
            raise RuntimeError("Connector not configured")

        # Use parameters or fall back to configured values
        columns_to_read = columns or self.columns
        batch_size = batch_size or self.batch_size

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

            if self.combine_files and len(files) > 1:
                # Read all files as one dataset
                yield from self._read_combined_files(files, columns_to_read, batch_size)
            else:
                # Read files separately
                for file_path in files:
                    yield from self._read_single_file(
                        file_path, columns_to_read, batch_size
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

    def _read_single_file(
        self, file_path: str, columns: Optional[List[str]], batch_size: int
    ) -> Iterator[DataChunk]:
        """Read a single Parquet file in batches.

        Args:
            file_path: Path to the file
            columns: Optional columns to read
            batch_size: Number of rows per batch

        Yields:
            DataChunk objects
        """
        try:
            parquet_file = pq.ParquetFile(file_path)

            for batch in parquet_file.iter_batches(
                columns=columns, batch_size=batch_size
            ):
                df = batch.to_pandas()
                yield DataChunk(data=df)

        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            raise

    def _read_combined_files(
        self, file_paths: List[str], columns: Optional[List[str]], batch_size: int
    ) -> Iterator[DataChunk]:
        """Read multiple Parquet files as a combined dataset.

        Args:
            file_paths: List of file paths
            columns: Optional columns to read
            batch_size: Number of rows per batch

        Yields:
            DataChunk objects
        """
        try:
            # Create dataset from multiple files
            dataset = pq.ParquetDataset(file_paths)
            table = dataset.read(columns=columns)

            # Convert to pandas and yield in batches
            df = table.to_pandas()

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                yield DataChunk(data=batch_df)

        except Exception as e:
            logger.error(f"Failed to read combined files: {e}")
            raise
