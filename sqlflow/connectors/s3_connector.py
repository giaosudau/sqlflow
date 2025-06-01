"""Enhanced S3 connector for SQLFlow with cost management and partition awareness."""

import dataclasses
import io
import json
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Pattern, Tuple

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.json as pj
import pyarrow.parquet as pq

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    ExportConnector,
    HealthCheckError,
    ParameterError,
    ParameterValidator,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector, register_export_connector
from sqlflow.core.errors import ConnectorError
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class CostLimitError(ConnectorError):
    """Error raised when cost limits are exceeded."""

    def __init__(self, message: str, connector_name: str = "S3"):
        super().__init__(connector_name, message)


class PartitionError(ConnectorError):
    """Error raised for partition-related issues."""

    def __init__(self, message: str, connector_name: str = "S3"):
        super().__init__(connector_name, message)


class FormatError(ConnectorError):
    """Error raised for file format issues."""

    def __init__(self, message: str, connector_name: str = "S3"):
        super().__init__(connector_name, message)


@dataclasses.dataclass
class S3Object:
    """Represents an S3 object with metadata."""

    key: str
    size: int
    last_modified: datetime
    e_tag: str


@dataclasses.dataclass
class PartitionPattern:
    """Represents a detected partition pattern."""

    pattern_type: str  # "hive", "date", "custom"
    keys: List[str]
    regex: Pattern[str]
    example: str


class S3ParameterValidator(ParameterValidator):
    """Parameter validator for S3 connector."""

    def __init__(self):
        super().__init__("S3")

    def _get_required_params(self) -> List[str]:
        """Get required parameters for S3 connector."""
        return ["bucket"]

    def _get_optional_params(self) -> Dict[str, Any]:
        """Get optional parameters with defaults."""
        return {
            **super()._get_optional_params(),
            "region": "us-east-1",
            "path_prefix": "",
            "file_format": "csv",
            "compression": None,
            "sync_mode": "full_refresh",
            # Authentication
            "access_key_id": None,
            "secret_access_key": None,
            "session_token": None,
            "endpoint_url": None,
            # Cost management
            "cost_limit_usd": 100.0,
            "max_files_per_run": 10000,
            "max_data_size_gb": 1000.0,
            # Development features
            "dev_sampling": None,
            "dev_max_files": None,
            # Partition awareness
            "partition_keys": None,
            "partition_filter": None,
            # Performance
            "batch_size": 10000,
            "parallel_workers": 4,
            # Security
            "use_ssl": True,
            "server_side_encryption": None,
            # Format-specific parameters
            "csv_delimiter": ",",
            "csv_header": True,
            "csv_encoding": "utf-8",
            "parquet_columns": None,
            "parquet_filters": None,
            "json_flatten": True,
            "json_max_depth": 10,
        }

    def validate(self, params: Dict[str, Any]) -> Dict[str, Any]:  # noqa: C901
        """Validate S3 parameters with enhanced checks."""
        validated = super().validate(params)

        # Handle backward compatibility for prefix parameter
        path_prefix = validated.get("path_prefix") or validated.get("prefix", "")
        validated["path_prefix"] = path_prefix

        # Handle backward compatibility for access keys
        access_key_id = validated.get("access_key_id") or validated.get("access_key")
        if access_key_id:
            validated["access_key_id"] = access_key_id

        secret_access_key = validated.get("secret_access_key") or validated.get(
            "secret_key"
        )
        if secret_access_key:
            validated["secret_access_key"] = secret_access_key

        # Validate file format
        file_format = validated.get("file_format") or validated.get("format", "csv")
        file_format = file_format.lower()
        supported_formats = ["csv", "parquet", "json", "jsonl", "tsv", "avro"]
        if file_format not in supported_formats:
            raise ParameterError(
                f"File format '{file_format}' not supported. "
                f"Supported formats: {supported_formats}"
            )
        validated["file_format"] = file_format

        # Validate cost limits
        try:
            if validated.get("cost_limit_usd") is not None:
                validated["cost_limit_usd"] = float(validated["cost_limit_usd"])
            if validated.get("max_data_size_gb") is not None:
                validated["max_data_size_gb"] = float(validated["max_data_size_gb"])
            if validated.get("max_files_per_run") is not None:
                validated["max_files_per_run"] = int(validated["max_files_per_run"])
        except (ValueError, TypeError) as e:
            raise ParameterError(f"Invalid cost limit parameter: {e}")

        # Validate development parameters
        if validated.get("dev_sampling") is not None:
            try:
                dev_sampling = float(validated["dev_sampling"])
                if not 0 < dev_sampling <= 1:
                    raise ParameterError(
                        "dev_sampling must be between 0 and 1 (exclusive of 0)"
                    )
                validated["dev_sampling"] = dev_sampling
            except (ValueError, TypeError):
                raise ParameterError("dev_sampling must be a float between 0 and 1")

        return validated


class S3CostManager:
    """Manages cost tracking and limits for S3 operations."""

    COST_PER_GB_REQUEST = 0.0004  # USD per GB for GET requests
    COST_PER_1000_REQUESTS = 0.0004  # USD per 1000 GET requests

    def __init__(self, cost_limit_usd: float):
        self.cost_limit_usd = cost_limit_usd
        self.current_cost = 0.0
        self.data_scanned_gb = 0.0
        self.requests_made = 0

    def estimate_cost(self, files: List[S3Object]) -> Dict[str, float]:
        """Estimate cost for processing files."""
        total_size_gb = sum(f.size for f in files) / (1024**3)
        num_requests = len(files)

        data_cost = total_size_gb * self.COST_PER_GB_REQUEST
        request_cost = (num_requests / 1000) * self.COST_PER_1000_REQUESTS
        total_cost = data_cost + request_cost

        return {
            "estimated_cost_usd": total_cost,
            "data_cost_usd": data_cost,
            "request_cost_usd": request_cost,
            "data_size_gb": total_size_gb,
            "num_requests": num_requests,
        }

    def check_cost_limit(self, estimated_cost: float) -> bool:
        """Check if operation would exceed cost limit."""
        return (self.current_cost + estimated_cost) <= self.cost_limit_usd

    def track_operation(self, data_size_bytes: int, num_requests: int) -> None:
        """Track actual operation costs."""
        data_gb = data_size_bytes / (1024**3)
        self.data_scanned_gb += data_gb
        self.requests_made += num_requests

        data_cost = data_gb * self.COST_PER_GB_REQUEST
        request_cost = (num_requests / 1000) * self.COST_PER_1000_REQUESTS
        self.current_cost += data_cost + request_cost

    def get_metrics(self) -> Dict[str, Any]:
        """Get current cost metrics."""
        return {
            "current_cost_usd": self.current_cost,
            "cost_limit_usd": self.cost_limit_usd,
            "data_scanned_gb": self.data_scanned_gb,
            "requests_made": self.requests_made,
            "cost_per_gb": self.COST_PER_GB_REQUEST,
            "remaining_budget_usd": self.cost_limit_usd - self.current_cost,
        }


class S3PartitionManager:
    """Manages partition detection and optimization for S3 keys."""

    def __init__(self):
        self.detected_pattern: Optional[PartitionPattern] = None
        self._pattern_cache: Dict[str, PartitionPattern] = {}

    def detect_partition_pattern(
        self, s3_keys: List[str]
    ) -> Optional[PartitionPattern]:
        """Automatically detect partition patterns from S3 keys."""
        if not s3_keys:
            return None

        # Try to detect Hive-style partitions (key=value)
        hive_pattern = self._detect_hive_pattern(s3_keys)
        if hive_pattern:
            return hive_pattern

        # Try to detect date-based partitions
        date_pattern = self._detect_date_pattern(s3_keys)
        if date_pattern:
            return date_pattern

        return None

    def _detect_hive_pattern(self, s3_keys: List[str]) -> Optional[PartitionPattern]:
        """Detect Hive-style partitions (year=2024/month=01/)."""
        sample_keys = s3_keys[:10]  # Sample first 10 keys

        # Look for key=value patterns
        hive_regex = re.compile(r"([^/=]+)=([^/]+)")

        partition_keys = set()
        for key in sample_keys:
            matches = hive_regex.findall(key)
            for partition_key, _ in matches:
                partition_keys.add(partition_key)

        if partition_keys:
            keys_list = sorted(list(partition_keys))
            # Build regex pattern for matching
            pattern_parts = []
            for pk in keys_list:
                pattern_parts.append(f"({pk})=([^/]+)")

            regex_pattern = "|".join(pattern_parts)
            compiled_regex = re.compile(regex_pattern)

            return PartitionPattern(
                pattern_type="hive",
                keys=keys_list,
                regex=compiled_regex,
                example=sample_keys[0] if sample_keys else "",
            )

        return None

    def _detect_date_pattern(self, s3_keys: List[str]) -> Optional[PartitionPattern]:
        """Detect date-based partitions (2024/01/15/)."""
        sample_keys = s3_keys[:10]

        # Look for YYYY/MM/DD or YYYY-MM-DD patterns
        date_patterns = [
            (r"(\d{4})/(\d{2})/(\d{2})", ["year", "month", "day"]),
            (r"(\d{4})-(\d{2})-(\d{2})", ["year", "month", "day"]),
            (r"(\d{4})/(\d{2})", ["year", "month"]),
            (r"(\d{4})-(\d{2})", ["year", "month"]),
        ]

        for pattern_str, keys in date_patterns:
            pattern = re.compile(pattern_str)
            matches = 0
            for key in sample_keys:
                if pattern.search(key):
                    matches += 1

            # If more than half the keys match, consider it a date pattern
            if matches > len(sample_keys) // 2:
                return PartitionPattern(
                    pattern_type="date",
                    keys=keys,
                    regex=pattern,
                    example=sample_keys[0] if sample_keys else "",
                )

        return None

    def build_optimized_prefix(
        self, base_prefix: str, cursor_value: Any, cursor_field: str
    ) -> str:
        """Build S3 prefix that minimizes files scanned for incremental loading."""
        if not self.detected_pattern or not cursor_value:
            return base_prefix

        # For date-based partitions, try to build an optimized prefix
        if self.detected_pattern.pattern_type == "date" and cursor_field in [
            "date",
            "timestamp",
            "event_time",
            "created_at",
            "updated_at",
        ]:
            try:
                # Convert cursor value to date components
                if isinstance(cursor_value, str):
                    cursor_date = pd.to_datetime(cursor_value)
                elif isinstance(cursor_value, datetime):
                    cursor_date = cursor_value
                else:
                    return base_prefix

                # Build optimized prefix based on date
                if "year" in self.detected_pattern.keys:
                    year_prefix = f"{base_prefix}year={cursor_date.year}/"
                    if "month" in self.detected_pattern.keys:
                        month_prefix = f"{year_prefix}month={cursor_date.month:02d}/"
                        if "day" in self.detected_pattern.keys:
                            return f"{month_prefix}day={cursor_date.day:02d}/"
                        return month_prefix
                    return year_prefix

            except Exception as e:
                logger.warning(f"Failed to build optimized prefix: {e}")

        return base_prefix


@register_connector("S3")
@register_export_connector("S3")
class S3Connector(Connector, ExportConnector):
    """Enhanced S3 connector with cost management, partition awareness, and multi-format support."""

    def __init__(self):
        """Initialize an Enhanced S3Connector."""
        Connector.__init__(self)
        ExportConnector.__init__(self)
        self.params: Optional[Dict[str, Any]] = None
        self.s3_client = None
        self.cost_manager: Optional[S3CostManager] = None
        self.partition_manager = S3PartitionManager()
        self._parameter_validator = S3ParameterValidator()

        # Legacy parameters for backward compatibility
        self.bucket: Optional[str] = None
        self.prefix: str = ""
        self.region: Optional[str] = None
        self.access_key: Optional[str] = None
        self.secret_key: Optional[str] = None
        self.session_token: Optional[str] = None
        self.endpoint_url: Optional[str] = None
        self.format: str = "csv"
        self.compression: Optional[str] = None
        self.part_size: int = 5 * 1024 * 1024
        self.max_retries: int = 3
        self.content_type: Optional[str] = None
        self.filename_template: str = "{prefix}{uuid}.{format}"
        self.use_multipart: bool = True

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the enhanced S3 connector with industry-standard parameters."""
        try:
            # Check for mock mode for testing (backward compatibility)
            self.mock_mode = params.get("mock_mode", False)
            if self.mock_mode:
                logger.debug("Using S3 connector in mock mode (dry run)")
                self.state = ConnectorState.CONFIGURED
                return

            # Validate parameters using standardized framework
            validated_params = self.validate_params(params)
            self.params = validated_params

            # Set up cost management
            cost_limit = validated_params.get("cost_limit_usd", 100.0)
            self.cost_manager = S3CostManager(cost_limit)

            # Configure legacy parameters for backward compatibility
            self._configure_legacy_params(validated_params)

            # Initialize S3 client
            self._initialize_s3_client()

            self.state = ConnectorState.CONFIGURED
            logger.info(
                f"S3 connector configured for bucket '{validated_params['bucket']}' "
                f"with cost limit ${cost_limit:.2f}"
            )

        except (ParameterError, CostLimitError):
            self.state = ConnectorState.ERROR
            raise
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "S3", f"Configuration failed: {str(e)}")

    def _configure_legacy_params(self, params: Dict[str, Any]) -> None:
        """Configure legacy parameters for backward compatibility."""
        self.bucket = params["bucket"]
        self.prefix = params.get("path_prefix", "")
        self.region = params.get("region", "us-east-1")
        self.access_key = params.get("access_key_id")
        self.secret_key = params.get("secret_access_key")
        self.session_token = params.get("session_token")
        self.endpoint_url = params.get("endpoint_url")
        self.format = params.get("file_format", "csv")
        self.compression = params.get("compression")
        self.part_size = int(params.get("part_size", 5 * 1024 * 1024))
        self.max_retries = int(params.get("max_retries", 3))
        self.filename_template = params.get(
            "filename_template", "{prefix}{uuid}.{format}"
        )
        self.use_multipart = params.get("use_multipart", True)

        # Set content type
        if self.format == "csv":
            self.content_type = "text/csv"
        elif self.format in ["parquet"]:
            self.content_type = "application/octet-stream"
        elif self.format in ["json", "jsonl"]:
            self.content_type = "application/json"
        else:
            self.content_type = "application/octet-stream"

    def _initialize_s3_client(self) -> None:
        """Initialize the S3 client with enhanced error handling."""
        try:
            session_kwargs = {}
            if self.region:
                session_kwargs["region_name"] = self.region
            if self.access_key and self.secret_key:
                session_kwargs["aws_access_key_id"] = self.access_key
                session_kwargs["aws_secret_access_key"] = self.secret_key
            if self.session_token:
                session_kwargs["aws_session_token"] = self.session_token

            session = boto3.Session(**session_kwargs)

            client_kwargs = {}
            if self.endpoint_url:
                client_kwargs["endpoint_url"] = self.endpoint_url
            if self.params and not self.params.get("use_ssl", True):
                client_kwargs["use_ssl"] = False

            self.s3_client = session.client("s3", **client_kwargs)

        except Exception as e:
            raise ConnectorError(
                self.name or "S3", f"Failed to initialize S3 client: {str(e)}"
            )

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental loading."""
        return True

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to S3 with enhanced validation."""
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.mock_mode:
                self.state = ConnectorState.READY
                return ConnectionTestResult(True, "Mock mode connection successful")

            if not self.s3_client or not self.bucket:
                return ConnectionTestResult(False, "Not configured properly")

            start_time = time.time()

            # Test bucket access
            self.s3_client.head_bucket(Bucket=self.bucket)

            # Test list access (minimal)
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=self.prefix, MaxKeys=1
            )

            connection_time = time.time() - start_time

            self.state = ConnectorState.READY

            # Get basic bucket info
            file_count = response.get("KeyCount", 0)
            message = (
                f"Connected to S3 bucket '{self.bucket}' "
                f"({file_count} objects with prefix '{self.prefix}') "
                f"in {connection_time:.2f}s"
            )

            return ConnectionTestResult(True, message)

        except Exception as e:
            self.state = ConnectorState.ERROR
            error_msg = str(e)
            logger.error(f"S3 connection test failed: {error_msg}")
            return ConnectionTestResult(False, error_msg)

    def check_health(self) -> Dict[str, Any]:
        """Comprehensive health check with S3-specific metrics."""
        start_time = time.time()

        try:
            if self.state != ConnectorState.READY:
                test_result = self.test_connection()
                if not test_result.success:
                    return {
                        "status": "unhealthy",
                        "connected": False,
                        "error": test_result.message,
                        "response_time_ms": (time.time() - start_time) * 1000,
                        "last_check": datetime.utcnow().isoformat(),
                        "capabilities": {
                            "incremental": False,
                            "cost_management": False,
                            "partition_awareness": False,
                        },
                    }

            if not self.s3_client or not self.bucket:
                raise HealthCheckError("S3 client not initialized", self.name or "S3")

            # Get bucket region and basic stats
            bucket_location = self.s3_client.get_bucket_location(Bucket=self.bucket)
            bucket_region = bucket_location.get("LocationConstraint") or "us-east-1"

            # Sample a few objects to detect patterns
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=self.prefix, MaxKeys=100
            )

            objects = response.get("Contents", [])
            total_size = sum(obj.get("Size", 0) for obj in objects)

            # Detect partition patterns
            keys = [obj["Key"] for obj in objects]
            self.partition_manager.detected_pattern = (
                self.partition_manager.detect_partition_pattern(keys)
            )

            response_time = (time.time() - start_time) * 1000

            health_info = {
                "status": "healthy",
                "connected": True,
                "response_time_ms": response_time,
                "last_check": datetime.utcnow().isoformat(),
                "bucket": self.bucket,
                "region": bucket_region,
                "prefix": self.prefix,
                "file_format": self.format,
                "object_count": len(objects),
                "total_size_bytes": total_size,
                "capabilities": {
                    "incremental": True,
                    "cost_management": True,
                    "partition_awareness": bool(
                        self.partition_manager.detected_pattern
                    ),
                    "multi_format": True,
                    "development_mode": True,
                },
            }

            # Add cost management metrics
            if self.cost_manager:
                health_info["cost_metrics"] = self.cost_manager.get_metrics()

            # Add partition information
            if self.partition_manager.detected_pattern:
                pattern = self.partition_manager.detected_pattern
                health_info["partition_info"] = {
                    "pattern_type": pattern.pattern_type,
                    "partition_keys": pattern.keys,
                    "example_key": pattern.example,
                }

            return health_info

        except Exception as e:
            logger.error(f"S3 health check failed: {e}")
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e),
                "response_time_ms": (time.time() - start_time) * 1000,
                "last_check": datetime.utcnow().isoformat(),
                "capabilities": {
                    "incremental": False,
                    "cost_management": False,
                    "partition_awareness": False,
                },
            }

    def discover(self) -> List[str]:
        """Discover available S3 objects with cost management and partition awareness."""
        self.validate_state(ConnectorState.CONFIGURED)

        if self.mock_mode:
            return ["mock/file1.csv", "mock/file2.csv", "mock/file3.csv"]

        try:
            if not self.s3_client or not self.bucket:
                raise ConnectorError(self.name or "S3", "S3 client not initialized")

            # Apply development limits and get paginator
            max_keys = self._get_discovery_limits()
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.bucket,
                Prefix=self.prefix,
                PaginationConfig={"MaxItems": max_keys},
            )

            # Process pages and collect objects
            objects, total_size = self._process_discovery_pages(page_iterator)

            # Apply development sampling if configured
            objects = self._apply_development_sampling(objects)

            # Detect partition patterns
            self._detect_and_log_partitions(objects)

            self.state = ConnectorState.READY
            logger.info(
                f"Discovered {len(objects)} S3 objects in bucket '{self.bucket}'"
            )
            return objects

        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "S3", f"Discovery failed: {str(e)}")

    def _get_discovery_limits(self) -> int:
        """Get discovery limits based on configuration."""
        max_keys = 1000
        if self.params and self.params.get("dev_max_files"):
            max_keys = min(max_keys, int(self.params["dev_max_files"]))
        return max_keys

    def _process_discovery_pages(self, page_iterator) -> Tuple[List[str], int]:
        """Process discovery pages and check cost/size limits."""
        objects = []
        total_size = 0

        for page in page_iterator:
            for obj in page.get("Contents", []):
                objects.append(obj["Key"])
                total_size += obj.get("Size", 0)

                # Check cost and size limits
                if not self._check_discovery_limits(total_size):
                    break

        return objects, total_size

    def _check_discovery_limits(self, total_size: int) -> bool:
        """Check if discovery should continue based on cost and size limits."""
        if not self.cost_manager:
            return True

        # Check cost limit
        size_gb = total_size / (1024**3)
        estimated_cost = size_gb * self.cost_manager.COST_PER_GB_REQUEST

        if not self.cost_manager.check_cost_limit(estimated_cost):
            logger.warning(
                f"Discovery stopped due to cost limit: "
                f"${estimated_cost:.4f} would exceed ${self.cost_manager.cost_limit_usd:.2f}"
            )
            return False

        # Check data size limit
        max_data_gb = (
            self.params.get("max_data_size_gb", 1000.0) if self.params else 1000.0
        )
        if size_gb > max_data_gb:
            logger.warning(
                f"Discovery stopped due to data size limit: "
                f"{size_gb:.2f}GB exceeds {max_data_gb:.2f}GB limit"
            )
            return False

        return True

    def _apply_development_sampling(self, objects: List[str]) -> List[str]:
        """Apply development sampling if configured."""
        if not self.params or not self.params.get("dev_sampling"):
            return objects

        import random

        sampling_rate = float(self.params["dev_sampling"])
        sample_count = max(1, int(len(objects) * sampling_rate))
        sampled_objects = random.sample(objects, sample_count)
        logger.info(
            f"Applied {sampling_rate*100:.1f}% sampling: {sample_count} objects selected"
        )
        return sampled_objects

    def _detect_and_log_partitions(self, objects: List[str]) -> None:
        """Detect partition patterns and log results."""
        if not objects:
            return

        self.partition_manager.detected_pattern = (
            self.partition_manager.detect_partition_pattern(objects)
        )
        if self.partition_manager.detected_pattern:
            pattern = self.partition_manager.detected_pattern
            logger.info(
                f"Detected {pattern.pattern_type} partition pattern with keys: {pattern.keys}"
            )

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an S3 object by sampling its content.

        Args:
        ----
            object_name: S3 object key

        Returns:
        -------
            Schema for the object

        Raises:
        ------
            ConnectorError: If schema detection fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        if self.mock_mode:
            # Return a mock schema
            fields = [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
            return Schema(pa.schema(fields))

        try:
            if not self.s3_client or not self.bucket:
                raise ConnectorError(self.name or "S3", "S3 client not initialized")

            # Parse object name if it's an S3 URI
            bucket_name, key = self._parse_s3_uri(object_name)

            # Read a small sample to infer schema
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)

            # Read first 1MB for schema detection
            sample_data = response["Body"].read(1024 * 1024)
            buffer = io.BytesIO(sample_data)

            if self.format == "parquet":
                parquet_file = pq.ParquetFile(buffer)
                schema = parquet_file.schema_arrow
            elif self.format in ["csv", "tsv"]:
                # Use pandas to infer schema from CSV sample
                delimiter = "," if self.format == "csv" else "\t"
                sample_df = pd.read_csv(
                    buffer,
                    delimiter=delimiter,
                    nrows=100,
                    encoding=(
                        self.params.get("csv_encoding", "utf-8")
                        if self.params
                        else "utf-8"
                    ),
                )
                schema = pa.Schema.from_pandas(sample_df)
            elif self.format in ["json", "jsonl"]:
                # Read JSON and infer schema
                buffer.seek(0)
                if self.format == "json":
                    sample_table = pj.read_json(buffer)
                else:
                    # JSONL - read line by line
                    lines = buffer.getvalue().decode("utf-8").strip().split("\n")[:100]
                    json_objects = [json.loads(line) for line in lines if line.strip()]
                    sample_df = pd.DataFrame(json_objects)
                    sample_table = pa.Table.from_pandas(sample_df)
                schema = sample_table.schema
            else:
                # Default to simple string schema
                fields = [pa.field("data", pa.string())]
                schema = pa.schema(fields)

            return Schema(schema)

        except Exception as e:
            # Fallback to basic schema
            logger.warning(f"Failed to detect schema for {object_name}: {e}")
            fields = [pa.field("data", pa.string())]
            return Schema(pa.schema(fields))

    def _read_parquet(
        self, buffer: io.BytesIO, columns: Optional[List[str]], batch_size: int
    ) -> Iterator[DataChunk]:
        """Read Parquet data with enhanced features."""
        try:
            parquet_file = pq.ParquetFile(buffer)

            # Apply column projection if specified
            read_columns = columns
            if self.params and self.params.get("parquet_columns"):
                read_columns = self.params["parquet_columns"]

            # Read in batches
            for batch in parquet_file.iter_batches(
                batch_size=batch_size, columns=read_columns
            ):
                # Convert RecordBatch to Table
                table = pa.Table.from_batches([batch])
                yield DataChunk(table)

        except Exception as e:
            logger.error(f"Failed to read Parquet data: {e}")
            # Fallback to basic read
            buffer.seek(0)
            table = pq.read_table(buffer, columns=columns)
            yield DataChunk(table)

    def _read_pandas_format(  # noqa: C901
        self,
        buffer: io.BytesIO,
        file_format: str,
        columns: Optional[List[str]],
        batch_size: int,
    ) -> Iterator[DataChunk]:
        """Read CSV, JSON, and other pandas-compatible formats."""
        try:
            if file_format in ["csv", "tsv"]:
                delimiter = "," if file_format == "csv" else "\t"
                encoding = (
                    self.params.get("csv_encoding", "utf-8") if self.params else "utf-8"
                )
                header = self.params.get("csv_header", True) if self.params else True

                # Read in chunks
                try:
                    # Handle column selection properly - convert to tuple for pandas compatibility
                    if columns:
                        use_cols = (
                            columns  # Use Any type to bypass pandas typing constraints
                        )
                    else:
                        use_cols = None

                    chunk_iter = pd.read_csv(
                        buffer,
                        delimiter=delimiter,
                        encoding=encoding,
                        header=0 if header else None,
                        chunksize=batch_size,
                        usecols=use_cols,  # type: ignore  # Bypass pandas typing constraints
                    )

                    for chunk_df in chunk_iter:
                        table = pa.Table.from_pandas(chunk_df)
                        yield DataChunk(table)
                except Exception as e:
                    logger.error(f"Failed to read CSV/TSV data: {e}")
                    # Return empty chunk
                    empty_df = pd.DataFrame()
                    yield DataChunk(pa.Table.from_pandas(empty_df))

            elif file_format in ["json", "jsonl"]:
                buffer.seek(0)
                content = buffer.getvalue().decode("utf-8")

                if file_format == "json":
                    # Single JSON object or array
                    data = json.loads(content)
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    else:
                        df = pd.DataFrame([data])
                else:
                    # JSONL - one JSON object per line
                    lines = content.strip().split("\n")
                    json_objects = [json.loads(line) for line in lines if line.strip()]
                    df = pd.DataFrame(json_objects)

                # Apply column selection
                if columns and all(col in df.columns for col in columns):
                    df = df[columns]

                # Yield in batches
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i : i + batch_size]
                    table = pa.Table.from_pandas(batch_df)
                    yield DataChunk(table)

        except Exception as e:
            logger.error(f"Failed to read {file_format} data: {e}")
            # Return empty chunk
            empty_df = pd.DataFrame()
            yield DataChunk(pa.Table.from_pandas(empty_df))

    def read(  # noqa: C901
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from S3 with enhanced cost management and format support.

        Args:
        ----
            object_name: S3 object key or URI
            columns: Optional list of columns to read
            filters: Optional filters (not implemented for S3)
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects

        Raises:
        ------
            ConnectorError: If read fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        if self.mock_mode:
            # Return mock data
            mock_data = {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, 20.0, 30.0],
            }
            df = pd.DataFrame(mock_data)
            if columns:
                df = df[columns] if all(col in df.columns for col in columns) else df
            table = pa.Table.from_pandas(df)
            yield DataChunk(table)
            return

        try:
            if not self.s3_client or not self.bucket:
                raise ConnectorError(self.name or "S3", "S3 client not initialized")

            bucket_name, key = self._parse_s3_uri(object_name)

            # Get object metadata for cost estimation
            obj_response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            obj_size = obj_response.get("ContentLength", 0)

            # Check cost limits
            if self.cost_manager:
                size_gb = obj_size / (1024**3)
                estimated_cost = size_gb * self.cost_manager.COST_PER_GB_REQUEST

                if not self.cost_manager.check_cost_limit(estimated_cost):
                    raise CostLimitError(
                        f"Reading object would cost ${estimated_cost:.4f}, "
                        f"exceeding limit ${self.cost_manager.cost_limit_usd:.2f}"
                    )

            # Read the object
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)

            # Handle compression
            body = response["Body"]
            if response.get("ContentEncoding") == "gzip":
                import gzip

                body = gzip.GzipFile(fileobj=body)

            buffer = io.BytesIO(body.read())

            # Track operation cost
            if self.cost_manager:
                self.cost_manager.track_operation(obj_size, 1)

            # Read based on format
            if self.format == "parquet":
                yield from self._read_parquet(buffer, columns, batch_size)
            elif self.format in ["csv", "tsv", "json", "jsonl"]:
                yield from self._read_pandas_format(
                    buffer, self.format, columns, batch_size
                )
            else:
                raise FormatError(f"Unsupported file format: {self.format}")

            self.state = ConnectorState.READY

        except (CostLimitError, FormatError):
            raise
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "S3", f"Reading failed: {str(e)}")

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data incrementally from S3 with partition awareness."""
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            # Build optimized prefix using partition awareness
            base_prefix = self._extract_base_prefix(object_name)
            optimized_prefix = self.partition_manager.build_optimized_prefix(
                base_prefix, cursor_value, cursor_field
            )

            logger.info(
                f"Using optimized prefix for incremental read: {optimized_prefix}"
            )

            # Discover candidate objects
            candidate_objects = self._discover_incremental_objects(
                optimized_prefix, cursor_value, cursor_field
            )

            # Read and filter objects
            total_rows = 0
            for obj_key in candidate_objects:
                try:
                    for chunk in self.read(obj_key, columns, batch_size=batch_size):
                        filtered_chunk = self._apply_cursor_filtering(
                            chunk, cursor_field, cursor_value
                        )
                        if filtered_chunk:
                            total_rows += len(filtered_chunk.pandas_df)
                            yield filtered_chunk

                except Exception as e:
                    logger.warning(
                        f"Failed to read object {obj_key} during incremental load: {e}"
                    )
                    continue

            logger.info(
                f"Incremental read processed {total_rows} rows from {len(candidate_objects)} objects"
            )

        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "S3", f"Incremental read failed: {str(e)}"
            )

    def _extract_base_prefix(self, object_name: str) -> str:
        """Extract base prefix from object name."""
        if object_name.startswith("s3://"):
            return object_name.replace("s3://", "").split("/", 1)[1]
        return object_name

    def _discover_incremental_objects(
        self, optimized_prefix: str, cursor_value: Optional[Any], cursor_field: str
    ) -> List[str]:
        """Discover objects for incremental loading."""
        if not self.s3_client or not self.bucket:
            raise ConnectorError(self.name or "S3", "S3 client not initialized")

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=optimized_prefix,
            PaginationConfig={"MaxItems": 1000},
        )

        candidate_objects = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                if self._should_include_object(obj, cursor_value, cursor_field):
                    candidate_objects.append(obj["Key"])

        return candidate_objects

    def _should_include_object(
        self, obj: Dict[str, Any], cursor_value: Optional[Any], cursor_field: str
    ) -> bool:
        """Check if object should be included based on cursor filtering."""
        if not cursor_value or cursor_field not in [
            "timestamp",
            "modified_time",
            "last_modified",
        ]:
            return True

        try:
            obj_modified = obj["LastModified"]
            if isinstance(cursor_value, str):
                cursor_datetime = pd.to_datetime(cursor_value)
            elif isinstance(cursor_value, datetime):
                cursor_datetime = cursor_value
            else:
                return True

            return obj_modified.replace(tzinfo=None) > cursor_datetime
        except Exception as e:
            logger.warning(f"Failed to filter object by timestamp: {e}")
            return True

    def _apply_cursor_filtering(
        self, chunk: DataChunk, cursor_field: str, cursor_value: Optional[Any]
    ) -> Optional[DataChunk]:
        """Apply cursor field filtering to a data chunk."""
        if not cursor_value or not cursor_field:
            return chunk

        try:
            df = chunk.pandas_df
            if cursor_field not in df.columns:
                return chunk

            cursor_series = df[cursor_field]
            if cursor_series.dtype == "object":
                mask = cursor_series.apply(
                    lambda x: x > str(cursor_value) if x is not None else False
                )
            else:
                mask = cursor_series.apply(
                    lambda x: x > cursor_value if pd.notna(x) else False
                )

            filtered_df = df[mask]
            if len(filtered_df) > 0:
                filtered_table = pa.Table.from_pandas(filtered_df)
                return DataChunk(filtered_table)
            return None

        except Exception as e:
            logger.warning(f"Failed to apply cursor filtering: {e}")
            return chunk

    def get_cursor_value(
        self, data_chunk: DataChunk, cursor_field: str
    ) -> Optional[Any]:
        """Extract the maximum cursor value from a data chunk.

        Args:
        ----
            data_chunk: DataChunk to extract cursor value from
            cursor_field: Name of the cursor field

        Returns:
        -------
            Maximum cursor value from the chunk, or None if not found

        """
        try:
            df = data_chunk.pandas_df
            if df.empty or cursor_field not in df.columns:
                return None

            max_value = df[cursor_field].max()

            # Handle pandas NaT or NaN values - avoid boolean evaluation on Series
            try:
                # Check if max_value is null using numpy/pandas compatible method
                if max_value is None:
                    return None
                if (
                    hasattr(max_value, "__class__")
                    and max_value.__class__.__name__ == "NaTType"
                ):
                    return None
                if isinstance(max_value, (int, float)) and np.isnan(max_value):
                    return None
            except (TypeError, ValueError, AttributeError):
                # For any checking issues, return None safely
                return None

            return max_value

        except Exception as e:
            logger.error(
                f"Failed to extract cursor value for field '{cursor_field}': {e}"
            )
            return None

    def _parse_s3_uri(self, object_name: str) -> Tuple[str, str]:
        """Parse S3 URI and extract bucket and key.

        Args:
        ----
            object_name: S3 URI or key

        Returns:
        -------
            Tuple of (bucket_name, key)

        """
        if object_name.startswith("s3://"):
            parts = object_name.replace("s3://", "").split("/", 1)
            if len(parts) == 2:
                bucket_name, key = parts
                return bucket_name, key
            bucket_name = parts[0]
            return bucket_name, ""

        # Not an S3 URI, use configured bucket
        if self.bucket:
            return self.bucket, object_name
        else:
            raise ConnectorError(
                self.name or "S3",
                "No bucket configured and object_name is not an S3 URI",
            )

    # Export functionality - simplified for now, keeping existing functionality
    def _generate_key(self, uuid_str: str) -> str:
        """Generate S3 key for export operations."""
        return self.filename_template.format(
            prefix=self.prefix, uuid=uuid_str, format=self.format
        )

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to S3 with enhanced error handling."""
        if self.mock_mode:
            logger.debug(
                f"[MOCK] Would write {len(data_chunk.pandas_df)} rows to {object_name}"
            )
            return

        try:
            # Generate a unique key for the file
            file_uuid = str(uuid.uuid4())
            key = self._generate_key(file_uuid)

            # Use existing export functionality
            self._export_data(data_chunk, key)

        except Exception as e:
            raise ConnectorError(self.name or "S3", f"Write failed: {str(e)}")

    def close(self) -> None:
        """Clean up S3 connector resources."""
        self.s3_client = None
        self.cost_manager = None

    def _export_data(self, data_chunk: DataChunk, key: str) -> None:
        """Export data to S3 in the configured format.

        Args:
        ----
            data_chunk: DataChunk to export
            key: S3 object key

        Raises:
        ------
            ConnectorError: If export fails

        """
        if not self.s3_client:
            raise ConnectorError(self.name or "S3", "S3 client not initialized")

        try:
            if self.format == "csv":
                self._export_csv(data_chunk, key)
            elif self.format == "parquet":
                self._export_parquet(data_chunk, key)
            elif self.format == "json":
                self._export_json(data_chunk, key)
            else:
                raise FormatError(f"Unsupported export format: {self.format}")
        except Exception as e:
            raise ConnectorError(self.name or "S3", f"Failed to export data: {str(e)}")

    def _export_csv(self, data: DataChunk, key: str) -> None:
        """Export data as CSV to S3."""
        try:
            df = data.pandas_df
            buffer = io.BytesIO()

            if self.compression == "gzip":
                import gzip

                with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                    csv_data = df.to_csv(index=False)
                    if csv_data:  # Check if csv_data is not None
                        gz.write(csv_data.encode("utf-8"))
            else:
                csv_data = df.to_csv(index=False)
                if csv_data:  # Check if csv_data is not None
                    buffer.write(csv_data.encode("utf-8"))

            buffer.seek(0)
            self._upload_single_part(buffer, key)
        except Exception as e:
            raise ConnectorError(self.name or "S3", f"Failed to export CSV: {str(e)}")

    def _export_parquet(self, data: DataChunk, key: str) -> None:
        """Export data as Parquet to S3."""
        try:
            table = data.arrow_table
            buffer = io.BytesIO()

            if self.compression:
                pq.write_table(table, buffer, compression=self.compression)
            else:
                pq.write_table(table, buffer)

            buffer.seek(0)
            self._upload_single_part(buffer, key)
        except Exception as e:
            raise ConnectorError(
                self.name or "S3", f"Failed to export Parquet: {str(e)}"
            )

    def _export_json(self, data: DataChunk, key: str) -> None:
        """Export data as JSON to S3."""
        try:
            df = data.pandas_df
            buffer = io.BytesIO()

            json_data = df.to_json(orient="records")
            if json_data:  # Check if json_data is not None
                if self.compression == "gzip":
                    import gzip

                    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                        gz.write(json_data.encode("utf-8"))
                else:
                    buffer.write(json_data.encode("utf-8"))

            buffer.seek(0)
            self._upload_single_part(buffer, key)
        except Exception as e:
            raise ConnectorError(self.name or "S3", f"Failed to export JSON: {str(e)}")

    def _upload_single_part(self, buffer: io.BytesIO, key: str) -> None:
        """Upload data to S3 in a single request."""
        try:
            if not self.s3_client or not self.bucket:
                raise ConnectorError(self.name or "S3", "S3 client not initialized")

            extra_args = {}
            if self.content_type:
                extra_args["ContentType"] = self.content_type

            if self.compression == "gzip" and self.format in ["csv", "json"]:
                extra_args["ContentEncoding"] = "gzip"

            for attempt in range(self.max_retries):
                try:
                    self.s3_client.put_object(
                        Bucket=self.bucket, Key=key, Body=buffer, **extra_args
                    )
                    break
                except Exception:
                    if attempt == self.max_retries - 1:
                        raise
                    buffer.seek(0)
        except Exception as e:
            raise ConnectorError(self.name or "S3", f"Failed to upload to S3: {str(e)}")
