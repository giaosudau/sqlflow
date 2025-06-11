import os
import uuid
from io import BytesIO
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ParquetWriteBuffer:
    """Reusable buffer for Parquet writing operations."""

    def __init__(self, initial_size: int = 16384):
        self._buffer = BytesIO()
        self._initial_size = initial_size

    def get_buffer(self) -> BytesIO:
        """Get a clean buffer for writing."""
        self._buffer.seek(0)
        self._buffer.truncate(0)
        return self._buffer

    def write_to_file(self, file_path: str) -> None:
        """Write buffer contents to file efficiently."""
        self._buffer.seek(0)
        with open(file_path, "wb", buffering=self._initial_size) as f:
            # Use efficient chunk-based writing
            while True:
                chunk = self._buffer.read(self._initial_size)
                if not chunk:
                    break
                f.write(chunk)


class ParquetDestination(DestinationConnector):
    """
    Connector for writing data to Parquet files using optimized "stage-and-swap" pattern.
    """

    # Class-level buffer pool for efficient reuse
    _buffer_pool: List[ParquetWriteBuffer] = []
    _max_pool_size = 3  # Smaller pool for Parquet due to larger memory usage

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.path = self.config.get("path")
        if not self.path:
            raise ValueError("ParquetDestination: 'path' not specified in config")

    @classmethod
    def _get_buffer(cls) -> ParquetWriteBuffer:
        """Get a buffer from the pool or create a new one."""
        if cls._buffer_pool:
            return cls._buffer_pool.pop()
        return ParquetWriteBuffer()

    @classmethod
    def _return_buffer(cls, buffer: ParquetWriteBuffer) -> None:
        """Return a buffer to the pool for reuse."""
        if len(cls._buffer_pool) < cls._max_pool_size:
            cls._buffer_pool.append(buffer)

    def _get_write_strategy(self, df: pd.DataFrame) -> str:
        """Determine optimal write strategy based on data characteristics."""
        # Estimate memory usage for Parquet (more efficient than CSV)
        estimated_size = (
            len(df) * len(df.columns) * 30
        )  # ~30 bytes per value estimate for Parquet

        if estimated_size > 100 * 1024 * 1024:  # > 100MB
            return "chunked"
        elif estimated_size > 20 * 1024 * 1024:  # > 20MB
            return "buffered"
        else:
            return "direct"

    def _optimize_write_options(
        self, write_options: Dict[str, Any], df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Optimize write options based on data characteristics."""
        optimized = write_options.copy()

        # Set optimal compression based on data size
        if "compression" not in optimized:
            if len(df) > 100000:  # Large datasets benefit from compression
                optimized["compression"] = "snappy"  # Fast compression
            else:
                optimized["compression"] = None  # No compression for small data

        # Optimize row group size for better performance
        if "row_group_size" not in optimized:
            if len(df) > 50000:
                optimized["row_group_size"] = min(50000, len(df) // 4)
            else:
                optimized["row_group_size"] = len(df)

        # Enable dictionary encoding for string columns if beneficial
        if "use_dictionary" not in optimized and len(df) > 1000:
            # Check if we have string columns that might benefit from dictionary encoding
            string_cols = df.select_dtypes(include=["object", "string"]).columns
            if len(string_cols) > 0:
                # Sample data to estimate cardinality
                sample_size = min(1000, len(df))
                sample = df.head(sample_size)
                for col in string_cols:
                    unique_ratio = sample[col].nunique() / len(sample)
                    if unique_ratio < 0.5:  # Low cardinality, good for dictionary
                        optimized["use_dictionary"] = True
                        break

        return optimized

    def _write_chunked(
        self, df: pd.DataFrame, file_path: str, write_options: Dict[str, Any]
    ) -> None:
        """Write large DataFrames in chunks to manage memory efficiently."""
        # Adaptive chunk sizing based on data size and columns
        chunk_size = max(
            5000, min(50000, (100 * 1024 * 1024) // (len(df.columns) * 50))
        )
        logger.debug("Writing %d rows in chunks of %d", len(df), chunk_size)

        # Use PyArrow for efficient chunked writing
        schema = pa.Schema.from_pandas(df)

        with pq.ParquetWriter(file_path, schema, **write_options) as writer:
            for i in range(0, len(df), chunk_size):
                chunk_df = df.iloc[i : i + chunk_size]
                table = pa.Table.from_pandas(chunk_df, schema=schema)
                writer.write_table(table)

    def _write_buffered(
        self, df: pd.DataFrame, file_path: str, write_options: Dict[str, Any]
    ) -> None:
        """Write using buffer pool for medium-sized DataFrames."""
        buffer = self._get_buffer()
        try:
            # Write to buffer first using PyArrow for efficiency
            parquet_buffer = buffer.get_buffer()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_buffer, **write_options)

            # Write buffer to file efficiently
            buffer.write_to_file(file_path)

        finally:
            self._return_buffer(buffer)

    def _write_direct(
        self, df: pd.DataFrame, file_path: str, write_options: Dict[str, Any]
    ) -> None:
        """Direct write for small DataFrames using pandas."""
        df.to_parquet(file_path, **write_options)

    def _create_optimized_temp_path(self, base_path: str) -> str:
        """Create optimized temporary file path in same directory as target."""
        dir_path = os.path.dirname(base_path)
        base_name = os.path.basename(base_path)
        # Use shorter UUID and more descriptive name
        temp_name = f".tmp_{uuid.uuid4().hex[:8]}_{base_name}"
        return os.path.join(dir_path, temp_name)

    def _handle_partitioned_write(
        self, df: pd.DataFrame, write_options: Dict[str, Any]
    ) -> None:
        """Handle partitioned dataset writes."""
        optimized_options = self._optimize_write_options(write_options, df)

        if "partition_cols" in optimized_options:
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=self.path,
                partition_cols=optimized_options.pop("partition_cols"),
                **optimized_options,
            )
        else:
            df.to_parquet(self.path, **optimized_options)

    def _execute_write_strategy(
        self, df: pd.DataFrame, temp_path: str, optimized_options: Dict[str, Any]
    ) -> None:
        """Execute the appropriate write strategy based on data size."""
        strategy = self._get_write_strategy(df)
        logger.debug("Using %s write strategy for %d rows", strategy, len(df))

        if strategy == "chunked":
            self._write_chunked(df, temp_path, optimized_options)
        elif strategy == "buffered":
            self._write_buffered(df, temp_path, optimized_options)
        else:
            self._write_direct(df, temp_path, optimized_options)

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "replace",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the Parquet file safely with performance optimizations.

        - `replace`: Atomically replaces the file.
        - `append`: Appends to the file. This creates a new file in a directory for partitioned data,
                    but is not well-defined for single-file destinations. We will overwrite.
        - `upsert`: Not supported for Parquet.
        """
        if mode.lower() == "upsert":
            raise NotImplementedError(
                "UPSERT mode is not supported for ParquetDestination."
            )

        write_options = options or {}

        # For Parquet, 'append' is ambiguous for a single file. The safe default is to replace.
        # True append for Parquet is usually done by writing new files to a partitioned dataset,
        # which is handled by setting the `path` to a directory and using `partition_cols`.
        if mode.lower() in ["replace", "append"]:
            self._replace_safe(df, write_options)
        else:
            raise ValueError(f"Unsupported write mode for ParquetDestination: {mode}")

    def _replace_safe(self, df: pd.DataFrame, write_options: Dict[str, Any]):
        """Write to a temporary file and then atomically rename it with optimizations."""
        # If path is a directory (for partitioning), we write directly.
        # The atomic operation is the creation of the final file by the engine.
        if os.path.isdir(self.path) or "partition_cols" in write_options:
            self._handle_partitioned_write(df, write_options)
            return

        temp_path = self._create_optimized_temp_path(self.path)

        if "index" not in write_options:
            write_options["index"] = False

        # Optimize write options based on data characteristics
        optimized_options = self._optimize_write_options(write_options, df)

        # Ensure target directory exists
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        try:
            self._execute_write_strategy(df, temp_path, optimized_options)

            # Atomic rename
            os.rename(temp_path, self.path)
            logger.debug("Successfully wrote %d rows to %s", len(df), self.path)

        except Exception:
            # Cleanup the temporary file on failure
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    logger.warning("Failed to cleanup temporary file: %s", temp_path)
            raise
