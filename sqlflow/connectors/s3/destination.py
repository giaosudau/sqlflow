import io
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class S3WriteBuffer:
    """Reusable buffer for S3 writing operations."""

    def __init__(self, initial_size: int = 1024 * 1024):  # 1MB default for S3
        self._buffer = io.BytesIO()
        self._initial_size = initial_size

    def get_buffer(self) -> io.BytesIO:
        """Get a clean buffer for writing."""
        self._buffer.seek(0)
        self._buffer.truncate(0)
        return self._buffer

    def get_buffer_contents(self) -> bytes:
        """Get buffer contents as bytes."""
        self._buffer.seek(0)
        return self._buffer.getvalue()


class S3Destination(DestinationConnector):
    """
    Connector for writing data to S3 with performance optimizations.
    """

    # Class-level buffer pool for efficient reuse
    _buffer_pool: List[S3WriteBuffer] = []
    _max_pool_size = 3  # Smaller pool for S3 due to larger memory usage

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.uri = self.config.get("uri")
        if not self.uri:
            raise ValueError("S3Destination: 'uri' not specified in config")

        parsed_uri = urlparse(self.uri)
        self.bucket = parsed_uri.netloc
        self.key = parsed_uri.path.lstrip("/")

        # Initialize S3 client with optimized configuration
        s3_config = boto3.session.Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            max_pool_connections=50,
            region_name=config.get("region", "us-east-1"),
        )

        # Support custom endpoint for S3-compatible services
        endpoint_url = config.get("endpoint_url")
        if endpoint_url:
            self.s3_client = boto3.client(
                "s3", config=s3_config, endpoint_url=endpoint_url
            )
        else:
            self.s3_client = boto3.client("s3", config=s3_config)

    @classmethod
    def _get_buffer(cls) -> S3WriteBuffer:
        """Get a buffer from the pool or create a new one."""
        if cls._buffer_pool:
            return cls._buffer_pool.pop()
        return S3WriteBuffer()

    @classmethod
    def _return_buffer(cls, buffer: S3WriteBuffer) -> None:
        """Return a buffer to the pool for reuse."""
        if len(cls._buffer_pool) < cls._max_pool_size:
            cls._buffer_pool.append(buffer)

    def _get_write_strategy(self, df: pd.DataFrame, file_format: str) -> str:
        """Determine optimal write strategy based on data characteristics."""
        # Estimate buffer size based on format
        if file_format == "parquet":
            estimated_size = len(df) * len(df.columns) * 20  # Parquet is more efficient
        elif file_format == "csv":
            estimated_size = len(df) * len(df.columns) * 40  # CSV is less efficient
        else:  # json
            estimated_size = len(df) * len(df.columns) * 60  # JSON is least efficient

        # Use multipart upload for files > 100MB
        if estimated_size > 100 * 1024 * 1024:
            return "multipart"
        elif estimated_size > 10 * 1024 * 1024:  # > 10MB
            return "buffered"
        else:
            return "direct"

    def _optimize_format_options(
        self, file_format: str, df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Get optimized options for different file formats."""
        options = {}

        if file_format == "csv":
            options.update(
                {
                    "index": False,
                    "float_format": "%.6g",  # Reduce precision for smaller files
                    "encoding": "utf-8",
                }
            )
        elif file_format == "parquet":
            # PyArrow parquet options (not pandas options)
            if len(df) > 100000:
                options.update(
                    {
                        "compression": "snappy",
                        "row_group_size": min(50000, len(df) // 4),
                    }
                )
            else:
                options.update({"compression": None})
            # Note: 'index' is not a valid PyArrow option, handled during table creation
        elif file_format == "json":
            options.update({"orient": "records", "lines": False, "compression": None})

        return options

    def _write_csv_optimized(
        self, df: pd.DataFrame, buffer: io.BytesIO, options: Dict[str, Any]
    ) -> None:
        """Write CSV with optimizations."""
        # Use StringIO first for CSV, then encode
        string_buffer = io.StringIO()
        df.to_csv(string_buffer, **options)
        string_buffer.seek(0)

        # Encode to bytes efficiently
        encoded_data = string_buffer.getvalue().encode(options.get("encoding", "utf-8"))
        buffer.write(encoded_data)

    def _write_parquet_optimized(
        self, df: pd.DataFrame, buffer: io.BytesIO, options: Dict[str, Any]
    ) -> None:
        """Write Parquet with PyArrow optimizations."""
        # Use PyArrow for better performance
        # Exclude index from the table (equivalent to pandas index=False)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, buffer, **options)

    def _write_json_optimized(
        self, df: pd.DataFrame, buffer: io.BytesIO, options: Dict[str, Any]
    ) -> None:
        """Write JSON with optimizations."""
        # Use built-in pandas to_json which is reasonably optimized
        json_data = df.to_json(**options)
        buffer.write(json_data.encode("utf-8"))

    def _write_direct(self, df: pd.DataFrame, file_format: str) -> None:
        """Direct write for small files."""
        buffer = self._get_buffer()
        try:
            buffer_io = buffer.get_buffer()
            options = self._optimize_format_options(file_format, df)

            if file_format == "csv":
                self._write_csv_optimized(df, buffer_io, options)
            elif file_format == "parquet":
                self._write_parquet_optimized(df, buffer_io, options)
            elif file_format == "json":
                self._write_json_optimized(df, buffer_io, options)

            # Single PUT operation
            buffer_contents = buffer.get_buffer_contents()
            self.s3_client.put_object(
                Bucket=self.bucket, Key=self.key, Body=buffer_contents
            )

            logger.debug("Direct upload completed: %d bytes", len(buffer_contents))

        finally:
            self._return_buffer(buffer)

    def _write_buffered(self, df: pd.DataFrame, file_format: str) -> None:
        """Buffered write for medium-sized files."""
        # Similar to direct but with optimized buffer management
        self._write_direct(df, file_format)

    def _upload_multipart_chunk(
        self,
        chunk_df: pd.DataFrame,
        file_format: str,
        part_number: int,
        upload_id: str,
        is_first_chunk: bool,
    ) -> Dict[str, Any]:
        """Upload a single chunk for multipart upload."""
        buffer = self._get_buffer()
        try:
            buffer_io = buffer.get_buffer()
            options = self._optimize_format_options(file_format, chunk_df)

            # Handle headers for CSV in multipart upload
            if file_format == "csv":
                if not is_first_chunk:  # No header for subsequent parts
                    options["header"] = False
                self._write_csv_optimized(chunk_df, buffer_io, options)
            elif file_format == "parquet":
                # For parquet multipart, we need to handle schema consistency
                # Use the same schema for all chunks
                table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                pq.write_table(table, buffer_io, **options)
            elif file_format == "json":
                # For JSON, we need to handle array structure
                if is_first_chunk:
                    buffer_io.write(b"[")
                else:
                    buffer_io.write(b",")
                self._write_json_optimized(chunk_df, buffer_io, options)

            buffer_contents = buffer.get_buffer_contents()

            # Upload part
            part_response = self.s3_client.upload_part(
                Bucket=self.bucket,
                Key=self.key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer_contents,
            )

            logger.debug(
                "Uploaded part %d: %d bytes", part_number, len(buffer_contents)
            )

            return {"ETag": part_response["ETag"], "PartNumber": part_number}

        finally:
            self._return_buffer(buffer)

    def _complete_json_multipart(
        self, upload_id: str, parts: List[Dict[str, Any]]
    ) -> None:
        """Complete JSON multipart upload by adding closing bracket."""
        buffer = self._get_buffer()
        try:
            buffer_io = buffer.get_buffer()
            buffer_io.write(b"]")
            buffer_contents = buffer.get_buffer_contents()

            part_response = self.s3_client.upload_part(
                Bucket=self.bucket,
                Key=self.key,
                PartNumber=len(parts) + 1,
                UploadId=upload_id,
                Body=buffer_contents,
            )

            parts.append({"ETag": part_response["ETag"], "PartNumber": len(parts) + 1})
        finally:
            self._return_buffer(buffer)

    def _write_multipart(self, df: pd.DataFrame, file_format: str) -> None:
        """Multipart upload for large files."""
        logger.debug("Using multipart upload for large dataset")

        # Initiate multipart upload
        response = self.s3_client.create_multipart_upload(
            Bucket=self.bucket, Key=self.key
        )
        upload_id = response["UploadId"]

        try:
            # Split dataframe into chunks for multipart upload
            chunk_size = max(10000, len(df) // 10)  # At least 10k rows per part
            parts = []

            for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
                chunk_df = df.iloc[chunk_start : chunk_start + chunk_size]

                part = self._upload_multipart_chunk(
                    chunk_df, file_format, i + 1, upload_id, i == 0
                )
                parts.append(part)

            # Handle JSON array closing
            if file_format == "json":
                self._complete_json_multipart(upload_id, parts)

            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            logger.debug("Multipart upload completed with %d parts", len(parts))

        except Exception:
            # Abort multipart upload on failure
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket, Key=self.key, UploadId=upload_id
                )
            except Exception as cleanup_error:
                logger.warning("Failed to abort multipart upload: %s", cleanup_error)
            raise

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to the S3 file with performance optimizations.
        """
        file_format = self.key.split(".")[-1].lower()

        if file_format not in ["csv", "parquet", "json"]:
            raise ValueError(f"Unsupported file format: {file_format}")

        # Determine optimal write strategy
        strategy = self._get_write_strategy(df, file_format)
        logger.debug(
            "Using %s strategy for %d rows, format: %s", strategy, len(df), file_format
        )

        try:
            if strategy == "multipart":
                self._write_multipart(df, file_format)
            elif strategy == "buffered":
                self._write_buffered(df, file_format)
            else:
                self._write_direct(df, file_format)

            logger.debug(
                "Successfully wrote %d rows to s3://%s/%s",
                len(df),
                self.bucket,
                self.key,
            )

        except Exception as e:
            logger.error("Failed to write to S3: %s", str(e))
            raise
