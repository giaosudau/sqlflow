import os
import uuid
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class WriteBuffer:
    """Reusable buffer for CSV writing operations."""

    def __init__(self, initial_size: int = 8192):
        self._buffer = StringIO()
        self._initial_size = initial_size

    def get_buffer(self) -> StringIO:
        """Get a clean buffer for writing."""
        self._buffer.seek(0)
        self._buffer.truncate(0)
        return self._buffer

    def write_to_file(self, file_path: str, encoding: str = "utf-8") -> None:
        """Write buffer contents to file efficiently."""
        self._buffer.seek(0)
        with open(file_path, "w", encoding=encoding, buffering=self._initial_size) as f:
            # Use efficient chunk-based writing
            while True:
                chunk = self._buffer.read(self._initial_size)
                if not chunk:
                    break
                f.write(chunk)


class CSVDestination(DestinationConnector):
    """
    Connector for writing data to CSV files using optimized "stage-and-swap" pattern.
    """

    # Class-level buffer pool for efficient reuse
    _buffer_pool: List[WriteBuffer] = []
    _max_pool_size = 5

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.path = self.config.get("path")
        if not self.path:
            raise ValueError("CSVDestination: 'path' not specified in config")

    @classmethod
    def _get_buffer(cls) -> WriteBuffer:
        """Get a buffer from the pool or create a new one."""
        if cls._buffer_pool:
            return cls._buffer_pool.pop()
        return WriteBuffer()

    @classmethod
    def _return_buffer(cls, buffer: WriteBuffer) -> None:
        """Return a buffer to the pool for reuse."""
        if len(cls._buffer_pool) < cls._max_pool_size:
            cls._buffer_pool.append(buffer)

    def _get_write_strategy(self, df: pd.DataFrame) -> str:
        """Determine optimal write strategy based on data characteristics."""
        # Estimate memory usage: num_rows * num_cols * avg_bytes_per_value
        estimated_size = len(df) * len(df.columns) * 50  # ~50 bytes per value estimate

        if estimated_size > 50 * 1024 * 1024:  # > 50MB
            return "chunked"
        elif estimated_size > 5 * 1024 * 1024:  # > 5MB
            return "buffered"
        else:
            return "direct"

    def _write_chunked(
        self, df: pd.DataFrame, file_path: str, write_options: Dict[str, Any]
    ) -> None:
        """Write large DataFrames in chunks to manage memory."""
        chunk_size = min(10000, max(1000, len(df) // 10))  # Adaptive chunk sizing
        logger.debug("Writing %d rows in chunks of %d", len(df), chunk_size)

        write_header = write_options.get("header", True)
        encoding = write_options.get("encoding", "utf-8")

        with open(file_path, "w", encoding=encoding, buffering=65536) as f:
            for i, chunk in enumerate(df.groupby(df.index // chunk_size)):
                chunk_df = chunk[1]

                # Only write header for first chunk
                chunk_options = write_options.copy()
                chunk_options["header"] = write_header and i == 0

                # Write chunk to string buffer first
                chunk_csv = chunk_df.to_csv(None, **chunk_options)
                f.write(chunk_csv)

    def _write_buffered(
        self, df: pd.DataFrame, file_path: str, write_options: Dict[str, Any]
    ) -> None:
        """Write using buffer pool for medium-sized DataFrames."""
        buffer = self._get_buffer()
        try:
            # Write to buffer first
            csv_buffer = buffer.get_buffer()
            df.to_csv(csv_buffer, **write_options)

            # Write buffer to file efficiently
            encoding = write_options.get("encoding", "utf-8")
            buffer.write_to_file(file_path, encoding)

        finally:
            self._return_buffer(buffer)

    def _write_direct(
        self, df: pd.DataFrame, file_path: str, write_options: Dict[str, Any]
    ) -> None:
        """Direct write for small DataFrames."""
        df.to_csv(file_path, **write_options)

    def _create_optimized_temp_path(self, base_path: str) -> str:
        """Create optimized temporary file path in same directory as target."""
        dir_path = os.path.dirname(base_path)
        base_name = os.path.basename(base_path)
        # Use shorter UUID and more descriptive name
        temp_name = f".tmp_{uuid.uuid4().hex[:8]}_{base_name}"
        return os.path.join(dir_path, temp_name)

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "replace",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the CSV file safely with performance optimizations.

        - `replace`: Atomically replaces the file.
        - `append`: Appends to the file. Note: This is not atomic.
        - `upsert`: Not supported for CSV.
        """
        if mode.lower() == "upsert":
            raise NotImplementedError(
                "UPSERT mode is not supported for CSVDestination."
            )

        write_options = options or {}

        if mode.lower() == "replace":
            self._replace_safe(df, write_options)
        else:  # append
            self._append(df, write_options)

    def _replace_safe(self, df: pd.DataFrame, write_options: Dict[str, Any]):
        """Write to a temporary file and then atomically rename it with optimizations."""
        temp_path = self._create_optimized_temp_path(self.path)

        # Set default write options for replace if not provided
        if "index" not in write_options:
            write_options["index"] = False
        if "header" not in write_options:
            write_options["header"] = True

        # Ensure target directory exists
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        try:
            # Choose optimal write strategy
            strategy = self._get_write_strategy(df)
            logger.debug("Using %s write strategy for %d rows", strategy, len(df))

            if strategy == "chunked":
                self._write_chunked(df, temp_path, write_options)
            elif strategy == "buffered":
                self._write_buffered(df, temp_path, write_options)
            else:
                self._write_direct(df, temp_path, write_options)

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

    def _append(self, df: pd.DataFrame, write_options: Dict[str, Any]):
        """Append data to the CSV file with optimization."""
        # For append, we usually don't want a header if the file exists
        if "header" not in write_options:
            write_options["header"] = not os.path.exists(self.path)
        if "index" not in write_options:
            write_options["index"] = False

        # Ensure target directory exists
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        # Choose optimal write strategy for append
        strategy = self._get_write_strategy(df)
        logger.debug("Using %s append strategy for %d rows", strategy, len(df))

        if strategy == "chunked":
            # For chunked append, we need to manage the header carefully
            write_header = write_options.get("header", False)
            chunk_size = min(10000, max(1000, len(df) // 10))

            with open(
                self.path,
                "a",
                encoding=write_options.get("encoding", "utf-8"),
                buffering=65536,
            ) as f:
                for i, chunk in enumerate(df.groupby(df.index // chunk_size)):
                    chunk_df = chunk[1]

                    # Only write header for first chunk if file didn't exist
                    chunk_options = write_options.copy()
                    chunk_options["header"] = write_header and i == 0

                    chunk_csv = chunk_df.to_csv(None, **chunk_options)
                    f.write(chunk_csv)
        else:
            # Use pandas' built-in append mode for smaller files
            df.to_csv(self.path, mode="a", **write_options)
