"""S3 Source Connector implementation.

This module provides a comprehensive S3 connector that supports multiple file formats,
cost management, partition awareness, and advanced features like incremental loading.
"""

import io
import logging
from typing import Any, Dict, Iterator, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk

logger = logging.getLogger(__name__)


class S3Source(Connector):
    """
    Enhanced S3 connector with cost management, partition awareness, and multi-format support.

    This connector implements the full Connector interface for CLI compatibility
    while providing advanced S3 features like discovery, cost limits, and format detection.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        # Initialize S3-specific attributes
        self.bucket: Optional[str] = None
        self.key: Optional[str] = None
        self.path_prefix: Optional[str] = None
        self.region: str = "us-east-1"
        self.file_format: str = "csv"
        self.s3_client: Optional[Any] = None

        # Cost management attributes
        self.cost_limit_usd: float = 100.0
        self.max_files_per_run: int = 10000
        self.max_data_size_gb: float = 1000.0
        self.dev_sampling: Optional[float] = None
        self.dev_max_files: Optional[int] = None

        # Partition attributes
        self.partition_keys: Optional[List[str]] = None
        self.partition_filter: Optional[Dict[str, Any]] = None

        # Format-specific attributes
        self.csv_delimiter: str = ","
        self.csv_header: bool = True
        self.csv_encoding: str = "utf-8"
        self.json_flatten: bool = True
        self.json_max_depth: int = 10

        if config is not None:
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
        ----
            params: Configuration parameters

        Raises:
        ------
            ValueError: If required parameters are missing
        """
        self.connection_params = params

        # Handle both old 'uri' interface and new separate parameters
        uri = params.get("uri")
        if uri:
            # Parse URI in format s3://bucket/key
            if not uri.startswith("s3://"):
                raise ValueError("S3Source: 'uri' must start with 's3://'")

            from urllib.parse import urlparse

            parsed_uri = urlparse(uri)
            self.bucket = parsed_uri.netloc
            self.key = parsed_uri.path.lstrip("/")

            if not self.bucket:
                raise ValueError("S3Source: 'uri' must contain a valid bucket name")
        else:
            # Extract S3 connection parameters (new interface)
            self.bucket = params.get("bucket")
            if not self.bucket:
                raise ValueError("S3Source: 'bucket' parameter is required")

            self.key = params.get("key")

        self.path_prefix = params.get("path_prefix", "")
        self.region = params.get("region", "us-east-1")

        # Authentication parameters
        access_key_id = params.get("access_key_id") or params.get("access_key")
        secret_access_key = params.get("secret_access_key") or params.get("secret_key")
        session_token = params.get("session_token")
        endpoint_url = params.get("endpoint_url")

        # File format parameters
        self.file_format = params.get("file_format", params.get("format", "csv"))

        # Cost management parameters
        self.cost_limit_usd = params.get("cost_limit_usd", 100.0)
        self.max_files_per_run = params.get("max_files_per_run", 10000)
        self.max_data_size_gb = params.get("max_data_size_gb", 1000.0)
        self.dev_sampling = params.get("dev_sampling")
        self.dev_max_files = params.get("dev_max_files")

        # Partition parameters
        partition_keys = params.get("partition_keys")
        if isinstance(partition_keys, str):
            self.partition_keys = [k.strip() for k in partition_keys.split(",")]
        elif isinstance(partition_keys, list):
            self.partition_keys = partition_keys

        self.partition_filter = params.get("partition_filter")

        # Format-specific parameters
        self.csv_delimiter = params.get("csv_delimiter", ",")
        self.csv_header = params.get("csv_header", True)
        self.csv_encoding = params.get("csv_encoding", "utf-8")
        self.json_flatten = params.get("json_flatten", True)
        self.json_max_depth = params.get("json_max_depth", 10)

        # Create S3 client
        s3_kwargs = {
            "region_name": self.region,
        }

        if access_key_id and secret_access_key:
            s3_kwargs["aws_access_key_id"] = access_key_id
            s3_kwargs["aws_secret_access_key"] = secret_access_key
            if session_token:
                s3_kwargs["aws_session_token"] = session_token

        if endpoint_url:
            s3_kwargs["endpoint_url"] = endpoint_url

        try:
            self.s3_client = boto3.client("s3", **s3_kwargs)
            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            raise ValueError(f"Failed to configure S3 client: {str(e)}")

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to S3.

        Returns:
        -------
            Result of the connection test
        """
        if not self.s3_client:
            return ConnectionTestResult(False, "S3 client not configured")

        try:
            # Test basic S3 connectivity
            self.s3_client.list_buckets()

            # Test bucket access
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
                return ConnectionTestResult(
                    True, f"Successfully connected to S3 bucket '{self.bucket}'"
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                if error_code == "404":
                    return ConnectionTestResult(
                        False, f"S3 bucket '{self.bucket}' not found"
                    )
                elif error_code == "403":
                    return ConnectionTestResult(
                        False, f"Access denied to S3 bucket '{self.bucket}'"
                    )
                else:
                    return ConnectionTestResult(
                        False, f"Error accessing S3 bucket: {error_code}"
                    )

        except NoCredentialsError:
            return ConnectionTestResult(False, "S3 credentials not found or invalid")
        except Exception as e:
            return ConnectionTestResult(False, f"S3 connection failed: {str(e)}")

    def discover(self) -> List[str]:
        """Discover available objects in the S3 bucket.

        Returns:
        -------
            List of S3 object keys
        """
        if not self.s3_client:
            return []

        if self.key:
            # If specific key is provided, return it if it exists
            try:
                self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
                return [self.key]
            except ClientError:
                return []

        # Discover objects with path prefix
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.bucket, Prefix=self.path_prefix
            )

            objects = []
            file_count = 0
            total_size = 0

            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        size = obj["Size"]

                        # Apply file format filtering
                        if self._matches_file_format(key):
                            objects.append(key)
                            file_count += 1
                            total_size += size

                            # Apply cost management limits
                            if file_count >= self.max_files_per_run:
                                logger.warning(
                                    f"Reached max files limit: {self.max_files_per_run}"
                                )
                                break

                            if total_size >= self.max_data_size_gb * 1024**3:
                                logger.warning(
                                    f"Reached max data size limit: {self.max_data_size_gb}GB"
                                )
                                break

                if file_count >= self.max_files_per_run:
                    break

            # Apply development sampling
            if self.dev_sampling and 0 < self.dev_sampling < 1:
                import random

                sample_size = max(1, int(len(objects) * self.dev_sampling))
                objects = random.sample(objects, sample_size)

            if self.dev_max_files and len(objects) > self.dev_max_files:
                objects = objects[: self.dev_max_files]

            return objects

        except Exception as e:
            logger.error(f"Error discovering S3 objects: {str(e)}")
            return []

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an S3 object.

        Args:
        ----
            object_name: S3 object key

        Returns:
        -------
            Schema for the object
        """
        try:
            # Read a small sample to infer schema
            sample_df = self._read_object_sample(object_name, nrows=100)

            if sample_df is not None and not sample_df.empty:
                import pyarrow as pa

                arrow_schema = pa.Schema.from_pandas(sample_df)
                return Schema(arrow_schema)

        except Exception as e:
            logger.error(f"Error getting schema for {object_name}: {str(e)}")

        # Return empty schema if we can't read the file
        import pyarrow as pa

        return Schema(pa.schema([]))

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from an S3 object.

        Args:
            object_name: S3 object key to read
            columns: List of columns to read (optional)
            filters: Filters to apply (not implemented for S3)
            batch_size: Batch size (not used for single file read)
            options: Additional read options
        """
        target_key = object_name or self.key
        if not target_key:
            raise ValueError("No S3 object key specified")

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=target_key)
            file_content = response["Body"].read()

            # Detect file format from key if not explicitly set
            detected_format = self._detect_file_format(target_key)

            df = self._parse_file_content(file_content, detected_format, options or {})

            # Filter columns if specified
            if columns and df is not None:
                available_columns = [col for col in columns if col in df.columns]
                if available_columns:
                    df = df[available_columns]

            return df if df is not None else pd.DataFrame()

        except ValueError as e:
            # Re-raise ValueError for unsupported formats and other validation errors
            raise e
        except Exception as e:
            logger.error(f"Error reading S3 object {target_key}: {str(e)}")
            return pd.DataFrame()

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        batch_size: int = 10000,
        **kwargs,
    ) -> Iterator[DataChunk]:
        """
        Read data incrementally from S3 object based on cursor field.

        Args:
            object_name: S3 object key
            cursor_field: Column name to use for incremental filtering
            cursor_value: Last value of cursor field from previous run
            batch_size: Number of rows per batch
        """
        df = self.read(object_name, **kwargs)

        if cursor_field and cursor_field in df.columns and cursor_value is not None:
            # Filter data based on cursor value
            df = df[df[cursor_field] > cursor_value]

        # Yield data in chunks
        for i in range(0, len(df), batch_size):
            chunk_df = df.iloc[i : i + batch_size]
            yield DataChunk(chunk_df)

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental loading."""
        return True

    def get_cursor_value(self, chunk: DataChunk, cursor_field: str) -> Optional[Any]:
        """Get the maximum cursor value from a data chunk."""
        df = chunk.pandas_df
        if cursor_field in df.columns and not df.empty:
            return df[cursor_field].max()
        return None

    # Private helper methods

    def _matches_file_format(self, key: str) -> bool:
        """Check if S3 key matches the expected file format."""
        if not key:
            return False

        # Get file extension
        extension = key.split(".")[-1].lower()

        # Map formats to extensions
        format_extensions = {
            "csv": ["csv", "tsv"],
            "json": ["json"],
            "jsonl": ["jsonl"],
            "parquet": ["parquet"],
            "tsv": ["tsv"],
        }

        expected_extensions = format_extensions.get(
            self.file_format, [self.file_format]
        )
        return extension in expected_extensions

    def _detect_file_format(self, key: str) -> str:
        """Detect file format from S3 key extension."""
        if not key:
            return self.file_format

        extension = key.split(".")[-1].lower()

        if extension in ["csv", "tsv"]:
            return "csv"
        elif extension in ["json", "jsonl"]:
            return "json" if extension == "json" else "jsonl"
        elif extension == "parquet":
            return "parquet"
        else:
            # For unsupported extensions, raise an error during file reading
            # rather than defaulting to a format
            supported_extensions = ["csv", "tsv", "json", "jsonl", "parquet"]
            if extension not in supported_extensions:
                raise ValueError(
                    f"Unsupported file format: {extension}. Supported formats: {supported_extensions}"
                )
            return self.file_format

    def _read_object_sample(self, key: str, nrows: int = 100) -> Optional[pd.DataFrame]:
        """Read a sample of rows from an S3 object for schema inference."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            file_content = response["Body"].read()

            detected_format = self._detect_file_format(key)

            # For CSV, we can read just a few rows efficiently
            if detected_format == "csv":
                content_str = file_content.decode(self.csv_encoding)
                lines = content_str.split("\n")
                if self.csv_header:
                    sample_lines = lines[: nrows + 1]  # +1 for header
                else:
                    sample_lines = lines[:nrows]
                sample_content = "\n".join(sample_lines).encode(self.csv_encoding)
                return self._parse_file_content(
                    sample_content, detected_format, {"nrows": nrows}
                )
            else:
                # For other formats, read normally (could be optimized)
                return self._parse_file_content(
                    file_content, detected_format, {"nrows": nrows}
                )

        except Exception as e:
            logger.warning(f"Could not read sample from {key}: {str(e)}")
            return None

    def _parse_file_content(
        self, content: bytes, file_format: str, options: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Parse file content based on format."""
        try:
            if file_format == "csv" or file_format == "tsv":
                delimiter = "\t" if file_format == "tsv" else self.csv_delimiter
                return pd.read_csv(
                    io.BytesIO(content),
                    delimiter=delimiter,
                    header=0 if self.csv_header else None,
                    encoding=self.csv_encoding,
                    **options,
                )
            elif file_format == "parquet":
                return pd.read_parquet(io.BytesIO(content), **options)
            elif file_format == "json":
                return pd.read_json(io.BytesIO(content), **options)
            elif file_format == "jsonl":
                # Read JSONL (newline-delimited JSON)
                content_str = content.decode("utf-8")
                lines = [
                    line.strip() for line in content_str.split("\n") if line.strip()
                ]

                if "nrows" in options and options["nrows"]:
                    lines = lines[: options["nrows"]]

                if not lines:
                    return pd.DataFrame()

                import json

                records = []
                for line in lines:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

                if records:
                    max_level = self.json_max_depth if self.json_flatten else None
                    df = pd.json_normalize(records, max_level=max_level)
                    return df
                else:
                    return pd.DataFrame()
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

        except Exception as e:
            logger.error(f"Error parsing {file_format} content: {str(e)}")
            return None

    # Legacy interface support for backward compatibility
    @property
    def config(self) -> Dict[str, Any]:
        """Backward compatibility property."""
        return getattr(self, "connection_params", {})
