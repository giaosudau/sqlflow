"""S3 Source Connector implementation.

This module provides a comprehensive S3 connector that supports multiple file formats,
cost management, partition awareness, and advanced features like incremental loading.
"""

import io
import logging
from typing import Any, Dict, Iterator, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, NoCredentialsError

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.resilience import resilient_operation

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

        # Extract S3 connection parameters
        self._configure_s3_parameters(params)

        # Extract file and cost management parameters
        self._configure_file_parameters(params)

        # Create S3 client
        self._create_s3_client(params)

        # Configure resilience patterns
        self._configure_resilience(params)

    def _configure_s3_parameters(self, params: Dict[str, Any]) -> None:
        """Configure S3-specific connection parameters."""
        # Handle both old 'uri' interface and new separate parameters
        uri = params.get("uri")
        if uri:
            self._parse_s3_uri(uri)
        else:
            # Extract S3 connection parameters (new interface)
            self.bucket = params.get("bucket")
            if not self.bucket:
                raise ValueError("S3Source: 'bucket' parameter is required")
            self.key = params.get("key")

        self.path_prefix = params.get("path_prefix", "")
        self.region = params.get("region", "us-east-1")

    def _parse_s3_uri(self, uri: str) -> None:
        """Parse S3 URI in format s3://bucket/key."""
        if not uri.startswith("s3://"):
            raise ValueError("S3Source: 'uri' must start with 's3://'")

        from urllib.parse import urlparse

        parsed_uri = urlparse(uri)
        self.bucket = parsed_uri.netloc
        self.key = parsed_uri.path.lstrip("/")

        if not self.bucket:
            raise ValueError("S3Source: 'uri' must contain a valid bucket name")

    def _configure_file_parameters(self, params: Dict[str, Any]) -> None:
        """Configure file format and cost management parameters."""
        # File format parameters
        self.file_format = params.get("file_format", params.get("format", "csv"))

        # Cost management parameters
        self.cost_limit_usd = params.get("cost_limit_usd", 100.0)
        self.max_files_per_run = params.get("max_files_per_run", 10000)
        self.max_data_size_gb = params.get("max_data_size_gb", 1000.0)
        self.dev_sampling = params.get("dev_sampling")
        self.dev_max_files = params.get("dev_max_files")

        # Partition parameters
        self._configure_partition_parameters(params)

        # Format-specific parameters
        self.csv_delimiter = params.get("csv_delimiter", ",")
        self.csv_header = params.get("csv_header", True)
        self.csv_encoding = params.get("csv_encoding", "utf-8")
        self.json_flatten = params.get("json_flatten", True)
        self.json_max_depth = params.get("json_max_depth", 10)

    def _configure_partition_parameters(self, params: Dict[str, Any]) -> None:
        """Configure partition-related parameters."""
        partition_keys = params.get("partition_keys")
        if isinstance(partition_keys, str):
            self.partition_keys = [k.strip() for k in partition_keys.split(",")]
        elif isinstance(partition_keys, list):
            self.partition_keys = partition_keys

        self.partition_filter = params.get("partition_filter")

    def _create_s3_client(self, params: Dict[str, Any]) -> None:
        """Create and configure the S3 client."""
        # Authentication parameters
        access_key_id = params.get("access_key_id") or params.get("access_key")
        secret_access_key = params.get("secret_access_key") or params.get("secret_key")
        session_token = params.get("session_token")
        endpoint_url = params.get("endpoint_url")

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

    @resilient_operation()
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
            self.s3_client.head_bucket(Bucket=self.bucket)
            return ConnectionTestResult(
                True, f"Successfully connected to S3 bucket '{self.bucket}'"
            )
        except Exception as e:
            msg = f"S3 connection test failed: {str(e)}"
            if isinstance(e, ClientError):
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "404":
                    msg = f"S3 bucket '{self.bucket}' not found"
                elif error_code == "403" or error_code == "AccessDenied":
                    msg = f"Access denied to S3 bucket '{self.bucket}'"
                elif error_code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
                    msg = "S3 authentication failed: Invalid credentials."
            elif isinstance(e, NoCredentialsError):
                msg = "S3 credentials not found."

            logger.error(f"S3Source: {msg}")
            return ConnectionTestResult(success=False, message=msg)

    @resilient_operation()
    def discover(self) -> List[str]:
        """Discover available objects in S3 with resilience.

        Returns:
        -------
            List of object keys in the bucket
        """
        if self.key:
            return self._discover_single_key()
        else:
            return self._discover_with_prefix()

    def _discover_single_key(self) -> List[str]:
        """Discover a single specific key."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
            return [self.key]
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return []
            raise

    @resilient_operation()
    def _discover_with_prefix(self) -> List[str]:
        """Discover objects using prefix pattern with resilience."""
        objects = []
        file_count = 0
        total_size = 0

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.path_prefix)

        for page in page_iterator:
            if "Contents" not in page:
                continue

            page_contents = page["Contents"]
            objects, file_count, total_size = self._process_page_objects(
                page_contents, objects, file_count, total_size
            )

            if self._should_stop_discovery(file_count, total_size):
                break

        # Apply sampling filters if configured
        objects = self._apply_sampling_filters(objects)

        logger.info(
            f"Discovered {len(objects)} objects in S3 bucket '{self.bucket}' "
            f"with prefix '{self.path_prefix}'"
        )
        return objects

    def _process_page_objects(
        self,
        page_contents: List[Dict],
        objects: List[str],
        file_count: int,
        total_size: int,
    ) -> tuple:
        """Process objects from a single page of S3 list results."""
        for obj in page_contents:
            key = obj["Key"]
            size_bytes = obj["Size"]

            # Skip if file doesn't match format
            if not self._matches_file_format(key):
                continue

            objects.append(key)
            file_count += 1
            total_size += size_bytes

            # Check if we've hit any limits
            if self._should_stop_discovery(file_count, total_size):
                break

        return objects, file_count, total_size

    def _should_stop_discovery(self, file_count: int, total_size: int) -> bool:
        """Check if discovery should stop based on limits."""
        if file_count >= self.max_files_per_run:
            logger.warning(
                f"Stopping discovery: reached max files limit ({self.max_files_per_run})"
            )
            return True

        if total_size > self.max_data_size_gb * 1024**3:
            logger.warning(
                f"Stopping discovery: reached max data size limit ({self.max_data_size_gb} GB)"
            )
            return True

        return False

    def _apply_sampling_filters(self, objects: List[str]) -> List[str]:
        """Apply development sampling filters."""
        if self.dev_max_files and len(objects) > self.dev_max_files:
            logger.info(f"Applying dev_max_files filter: {self.dev_max_files}")
            objects = objects[: self.dev_max_files]

        if self.dev_sampling and 0 < self.dev_sampling < 1:
            import random

            sample_size = int(len(objects) * self.dev_sampling)
            logger.info(
                f"Applying dev_sampling filter: {self.dev_sampling} ({sample_size} files)"
            )
            objects = random.sample(objects, sample_size)

        return objects

    @resilient_operation()
    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an S3 object with resilience.

        Args:
        ----
            object_name: S3 object key

        Returns:
        -------
            Schema for the object
        """
        # Sample the file to detect schema
        sample_df = self._read_object_sample(object_name, nrows=100)
        if sample_df is None:
            raise ValueError(f"Could not determine schema for object: {object_name}")

        columns = []
        for col_name, dtype in sample_df.dtypes.items():
            try:
                if pd.api.types.is_string_dtype(dtype):
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                elif pd.api.types.is_bool_dtype(dtype):
                    pa_type = pa.bool_()
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    pa_type = pa.timestamp("ns")
                else:
                    pa_type = pa.string()  # Fallback for other types
            except Exception:
                pa_type = pa.string()  # Broad exception for safety

            columns.append(
                pa.field(
                    name=col_name,
                    type=pa_type,
                    nullable=bool(sample_df[col_name].isnull().any()),
                )
            )

        return Schema(pa.schema(columns))

    @resilient_operation()
    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterator[DataChunk]:
        """Read data from an S3 object in chunks with resilience."""
        # Determine file format from object_name extension if not provided
        file_format = (options or {}).get("file_format")
        if not file_format:
            if object_name.endswith(".csv"):
                file_format = "csv"
            elif object_name.endswith((".json", ".jsonl")):
                file_format = "json"
            elif object_name.endswith((".parquet", ".parq")):
                file_format = "parquet"
            else:
                raise ValueError(
                    f"Unsupported file format for '{object_name}'. "
                    "Please specify 'file_format' in options."
                )

        try:
            if file_format == "csv":
                reader = self._read_csv_chunks
            elif file_format == "json":
                reader = self._read_json_chunks
            elif file_format == "parquet":
                reader = self._read_parquet_chunks
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            yield from reader(object_name, batch_size)

        except Exception as e:
            logger.error(
                f"S3Source: Failed to read from '{object_name}' with format '{file_format}': {str(e)}"
            )
            raise

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        batch_size: int = 10000,
        **kwargs,
    ) -> Iterator[DataChunk]:
        """Read data incrementally using a cursor field."""
        # For S3, incremental loading is typically based on object modification time
        # or object key patterns, not data content
        filters = kwargs.get("filters", {})

        # For simplicity, we'll use the standard read method
        # In a real implementation, you might filter objects by modification time
        return self.read(
            object_name=object_name,
            columns=kwargs.get("columns"),
            filters=filters,
            batch_size=batch_size,
        )

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental reading."""
        return True

    def get_cursor_value(self, chunk: DataChunk, cursor_field: str) -> Optional[Any]:
        """Get the maximum cursor value from a data chunk."""
        df = chunk.table.to_pandas()
        if cursor_field in df.columns and not df.empty:
            return df[cursor_field].max()
        return None

    def _matches_file_format(self, key: str) -> bool:
        """Check if a file key matches the expected format."""
        if self.file_format == "auto":
            # Accept common data file formats
            extensions = {".csv", ".json", ".jsonl", ".parquet", ".txt"}
            return any(key.lower().endswith(ext) for ext in extensions)
        else:
            # Check specific format
            format_extensions = {
                "csv": [".csv", ".txt"],
                "json": [".json"],
                "jsonl": [".jsonl", ".ndjson"],
                "parquet": [".parquet"],
            }

            extensions = format_extensions.get(
                self.file_format, [f".{self.file_format}"]
            )
            return any(key.lower().endswith(ext) for ext in extensions)

    def _detect_file_format(self, key: str) -> str:
        """Detect file format from key extension."""
        if self.file_format != "auto":
            return self.file_format

        key_lower = key.lower()
        if key_lower.endswith((".csv", ".txt")):
            return "csv"
        elif key_lower.endswith(".json"):
            return "json"
        elif key_lower.endswith((".jsonl", ".ndjson")):
            return "jsonl"
        elif key_lower.endswith(".parquet"):
            return "parquet"
        else:
            # Default to CSV
            logger.warning(f"Unknown file extension for {key}, defaulting to CSV")
            return "csv"

    @resilient_operation()
    def _read_object_sample(self, key: str, nrows: int = 100) -> Optional[pd.DataFrame]:
        """Read a sample of an S3 object for schema detection with resilience."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read()

            file_format = self._detect_file_format(key)
            options = {"nrows": nrows} if file_format == "csv" else {}

            return self._parse_file_content(content, file_format, options)

        except Exception as e:
            logger.error(f"Failed to read sample from {key}: {str(e)}")
            return None

    def _parse_file_content(
        self, content: bytes, file_format: str, options: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Parse file content based on format."""
        try:
            if file_format == "csv":
                return self._parse_csv_content(content, file_format, options)
            elif file_format == "parquet":
                return self._parse_parquet_content(content, options)
            elif file_format == "json":
                return self._parse_json_content(content, options)
            elif file_format == "jsonl":
                return self._parse_jsonl_content(content, options)
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return None
        except Exception as e:
            logger.error(f"Failed to parse {file_format} content: {str(e)}")
            return None

    def _parse_csv_content(
        self, content: bytes, file_format: str, options: Dict[str, Any]
    ) -> pd.DataFrame:
        """Parse CSV content."""
        csv_options = {
            "delimiter": self.csv_delimiter,
            "header": 0 if self.csv_header else None,
            "encoding": self.csv_encoding,
        }
        csv_options.update(options)

        return pd.read_csv(io.BytesIO(content), **csv_options)

    def _parse_parquet_content(
        self, content: bytes, options: Dict[str, Any]
    ) -> pd.DataFrame:
        """Parse Parquet content."""
        with io.BytesIO(content) as buffer:
            pq.ParquetFile(buffer)
            return pd.read_parquet(buffer, **options)

    def _parse_json_content(
        self, content: bytes, options: Dict[str, Any]
    ) -> pd.DataFrame:
        """Parse JSON content."""
        json_options = {}
        json_options.update(options)

        return pd.read_json(io.BytesIO(content), **json_options)

    def _parse_jsonl_content(
        self, content: bytes, options: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Parse JSON Lines content."""
        try:
            text_content = content.decode("utf-8")
            lines = [line.strip() for line in text_content.split("\n") if line.strip()]

            if not lines:
                return pd.DataFrame()

            # Limit lines if nrows is specified
            if "nrows" in options:
                lines = lines[: options["nrows"]]

            records = self._parse_jsonl_lines(lines)
            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"Failed to parse JSONL content: {str(e)}")
            return None

    def _parse_jsonl_lines(self, lines: List[str]) -> List[Dict]:
        """Parse individual JSON lines."""
        import json

        records = []
        for line_num, line in enumerate(lines, 1):
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
                continue

        return records

    @property
    def config(self) -> Dict[str, Any]:
        """Get current configuration."""
        return getattr(self, "connection_params", {})

    def read_legacy(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from S3 object - legacy method for backward compatibility.
        """
        if object_name is None:
            if self.key:
                object_name = self.key
            else:
                raise ValueError(
                    "object_name must be provided or configured in connector"
                )

        # Use the chunked read method and combine results
        chunks = []
        for chunk in self.read(object_name, columns, filters, batch_size):
            chunks.append(chunk.table.to_pandas())

        if chunks:
            return pd.concat(chunks, ignore_index=True)
        else:
            return pd.DataFrame()

    def _read_csv_chunks(
        self, object_name: str, batch_size: int
    ) -> Iterator[DataChunk]:
        """Read a CSV file from S3 in chunks."""
        s3_object = self._get_s3_object(object_name)
        encoding = self.config.get("csv_encoding", "utf-8")
        text_stream = io.TextIOWrapper(s3_object["Body"], encoding=encoding)
        with pd.read_csv(
            text_stream,
            chunksize=batch_size,
            encoding=encoding,
            delimiter=self.config.get("csv_delimiter", ","),
            header="infer" if self.config.get("csv_header", True) else None,
        ) as reader:
            for chunk_df in reader:
                yield DataChunk(pa.Table.from_pandas(chunk_df))

    def _read_json_chunks(
        self, object_name: str, batch_size: int
    ) -> Iterator[DataChunk]:
        """Read a JSON file from S3 in chunks."""
        s3_object = self._get_s3_object(object_name)
        text_stream = io.TextIOWrapper(s3_object["Body"], encoding="utf-8")
        df_generator = pd.read_json(
            text_stream, lines=True, chunksize=batch_size, encoding="utf-8"
        )
        for chunk_df in df_generator:
            yield DataChunk(pa.Table.from_pandas(chunk_df))

    def _read_parquet_chunks(
        self, object_name: str, batch_size: int
    ) -> Iterator[DataChunk]:
        """Read a Parquet file from S3 in chunks."""
        s3_object = self._get_s3_object(object_name)
        # The Parquet reader needs a seekable file-like object.
        # The StreamingBody from S3 is not seekable, so we read it into a buffer.
        with io.BytesIO(s3_object["Body"].read()) as buffer:
            parquet_file = pq.ParquetFile(buffer)
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                yield DataChunk(pa.Table.from_batches([batch]))

    def _get_s3_object(self, object_name: str) -> Dict[str, Any]:
        """Get an S3 object with resilience."""
        try:
            return self.s3_client.get_object(Bucket=self.bucket, Key=object_name)
        except NoCredentialsError:
            logger.error("S3Source: AWS credentials not found.")
            raise
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(
                    f"S3 object '{object_name}' not found in bucket '{self.bucket}'"
                ) from e
            logger.error(f"S3Source: Failed to get object '{object_name}': {str(e)}")
            raise
