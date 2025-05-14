"""S3 export connector for SQLFlow."""

import io
from typing import Any, Dict, Optional

import boto3
import pyarrow.parquet as pq

from sqlflow.connectors.base import (
    ConnectionTestResult,
    ConnectorState,
    ExportConnector,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_export_connector
from sqlflow.core.errors import ConnectorError


@register_export_connector("S3")
class S3ExportConnector(ExportConnector):
    """Export connector for AWS S3."""

    def __init__(self):
        """Initialize an S3ExportConnector."""
        super().__init__()
        self.bucket: Optional[str] = None
        self.prefix: str = ""
        self.region: Optional[str] = None
        self.access_key: Optional[str] = None
        self.secret_key: Optional[str] = None
        self.session_token: Optional[str] = None
        self.endpoint_url: Optional[str] = None
        self.format: str = "csv"  # csv, parquet, json
        self.compression: Optional[str] = None  # gzip, snappy, etc.
        self.s3_client = None
        self.part_size: int = 5 * 1024 * 1024  # 5MB default part size
        self.max_retries: int = 3
        self.content_type: Optional[str] = None
        self.filename_template: str = "{prefix}{uuid}.{format}"
        self.use_multipart: bool = True

    def _validate_bucket(self, params: Dict[str, Any]) -> None:
        """Validate bucket configuration.

        Args:
            params: Configuration parameters

        Raises:
            ValueError: If bucket is not provided
        """
        self.bucket = params.get("bucket")
        if not self.bucket:
            raise ValueError("Bucket is required")

    def _configure_connection_params(self, params: Dict[str, Any]) -> None:
        """Configure connection parameters.

        Args:
            params: Configuration parameters
        """
        self.prefix = params.get("prefix", "")
        self.region = params.get("region")
        self.access_key = params.get("access_key")
        self.secret_key = params.get("secret_key")
        self.session_token = params.get("session_token")
        self.endpoint_url = params.get("endpoint_url")

    def _validate_format(self, params: Dict[str, Any]) -> None:
        """Validate and set format and compression.

        Args:
            params: Configuration parameters

        Raises:
            ValueError: If format or compression is invalid
        """
        self.format = params.get("format", "csv").lower()
        if self.format not in ["csv", "parquet", "json"]:
            raise ValueError(
                f"Invalid format: {self.format}. " "Must be one of: csv, parquet, json"
            )

        self.compression = params.get("compression")
        if (
            self.compression
            and self.format == "csv"
            and self.compression not in ["gzip"]
        ):
            raise ValueError(
                f"Invalid compression for CSV: {self.compression}. "
                "Must be one of: gzip"
            )
        if (
            self.compression
            and self.format == "parquet"
            and self.compression not in ["snappy", "gzip", "brotli", "zstd"]
        ):
            raise ValueError(
                f"Invalid compression for Parquet: {self.compression}. "
                "Must be one of: snappy, gzip, brotli, zstd"
            )

    def _configure_upload_params(self, params: Dict[str, Any]) -> None:
        """Configure upload parameters.

        Args:
            params: Configuration parameters
        """
        self.part_size = int(params.get("part_size", 5 * 1024 * 1024))
        self.max_retries = int(params.get("max_retries", 3))
        self.filename_template = params.get(
            "filename_template", "{prefix}{uuid}.{format}"
        )
        self.use_multipart = params.get("use_multipart", True)

    def _set_content_type(self, params: Dict[str, Any]) -> None:
        """Set content type based on format.

        Args:
            params: Configuration parameters
        """
        if self.format == "csv":
            self.content_type = "text/csv"
        elif self.format == "parquet":
            self.content_type = "application/octet-stream"
        elif self.format == "json":
            self.content_type = "application/json"

        if "content_type" in params:
            self.content_type = params["content_type"]

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters including bucket, prefix, region,
                   access_key, secret_key, format, compression, etc.

        Raises:
            ConnectorError: If configuration fails
        """
        try:
            self._validate_bucket(params)
            self._configure_connection_params(params)
            self._validate_format(params)
            self._configure_upload_params(params)
            self._set_content_type(params)
            self._initialize_s3_client()

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Configuration failed: {str(e)}"
            )

    def _initialize_s3_client(self) -> None:
        """Initialize the S3 client.

        Raises:
            ConnectorError: If client initialization fails
        """
        try:
            session_kwargs = {}
            client_kwargs = {}

            if self.region:
                session_kwargs["region_name"] = self.region

            if self.access_key and self.secret_key:
                session_kwargs["aws_access_key_id"] = self.access_key
                session_kwargs["aws_secret_access_key"] = self.secret_key
                if self.session_token:
                    session_kwargs["aws_session_token"] = self.session_token

            if self.endpoint_url:
                client_kwargs["endpoint_url"] = self.endpoint_url

            session = boto3.Session(**session_kwargs)
            self.s3_client = session.client("s3", **client_kwargs)
        except Exception as e:
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Failed to initialize S3 client: {str(e)}"
            )

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to S3.

        Returns:
            Result of the connection test
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.s3_client is None:
                self._initialize_s3_client()
                if self.s3_client is None:
                    raise ConnectorError(
                        self.name or "S3_EXPORT", "Failed to initialize S3 client"
                    )

            self.s3_client.list_objects_v2(
                Bucket=self.bucket, MaxKeys=1, Prefix=self.prefix
            )

            self.state = ConnectorState.READY
            return ConnectionTestResult(True)
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, str(e))

    def _generate_key(self, uuid: str) -> str:
        """Generate a key for the S3 object.

        Args:
            uuid: Unique identifier for the file

        Returns:
            S3 object key
        """
        return self.filename_template.format(
            prefix=self.prefix, uuid=uuid, format=self.format
        )

    def _export_csv(self, data: DataChunk, key: str) -> None:
        """Export data as CSV to S3.

        Args:
            data: DataChunk to export
            key: S3 object key

        Raises:
            ConnectorError: If export fails
        """
        try:
            df = data.pandas_df

            buffer = io.BytesIO()

            if self.compression is None:
                df.to_csv(buffer, index=False, quoting=0)
            else:
                # For pandas, we need to use a dict for compression
                df.to_csv(
                    buffer,
                    index=False,
                    compression={"method": self.compression},
                    quoting=0,
                )

            buffer.seek(0)

            if self.use_multipart and buffer.getbuffer().nbytes > self.part_size:
                self._upload_multipart(buffer, key)
            else:
                self._upload_single_part(buffer, key)
        except Exception as e:
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Failed to export CSV: {str(e)}"
            )

    def _export_parquet(self, data: DataChunk, key: str) -> None:
        """Export data as Parquet to S3.

        Args:
            data: DataChunk to export
            key: S3 object key

        Raises:
            ConnectorError: If export fails
        """
        try:
            table = data.arrow_table

            buffer = io.BytesIO()

            if self.compression is None:
                pq.write_table(table, buffer)
            else:
                pq.write_table(table, buffer, compression=self.compression)

            buffer.seek(0)

            if self.use_multipart and buffer.getbuffer().nbytes > self.part_size:
                self._upload_multipart(buffer, key)
            else:
                self._upload_single_part(buffer, key)
        except Exception as e:
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Failed to export Parquet: {str(e)}"
            )

    def _export_json(self, data: DataChunk, key: str) -> None:
        """Export data as JSON to S3.

        Args:
            data: DataChunk to export
            key: S3 object key

        Raises:
            ConnectorError: If export fails
        """
        try:
            df = data.pandas_df

            buffer = io.BytesIO()

            if self.compression == "gzip":
                import gzip

                with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                    gz.write(df.to_json(orient="records").encode("utf-8"))
            else:
                buffer.write(df.to_json(orient="records").encode("utf-8"))

            buffer.seek(0)

            if self.use_multipart and buffer.getbuffer().nbytes > self.part_size:
                self._upload_multipart(buffer, key)
            else:
                self._upload_single_part(buffer, key)
        except Exception as e:
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Failed to export JSON: {str(e)}"
            )

    def _prepare_upload_args(self) -> Dict[str, str]:
        """Prepare extra arguments for S3 upload.

        Returns:
            Dictionary of extra arguments for S3 upload
        """
        extra_args = {}
        if self.content_type:
            extra_args["ContentType"] = self.content_type

        if self.compression == "gzip" and self.format in ["csv", "json"]:
            extra_args["ContentEncoding"] = "gzip"

        return extra_args

    def _ensure_s3_client(self) -> None:
        """Ensure S3 client is initialized.

        Raises:
            ConnectorError: If client initialization fails
        """
        if self.s3_client is None:
            self._initialize_s3_client()
            if self.s3_client is None:
                raise ConnectorError(
                    self.name or "S3_EXPORT", "Failed to initialize S3 client"
                )

    def _upload_single_part(self, buffer: io.BytesIO, key: str) -> None:
        """Upload data to S3 in a single request.

        Args:
            buffer: Data buffer
            key: S3 object key

        Raises:
            ConnectorError: If upload fails
        """
        try:
            self._ensure_s3_client()
            extra_args = self._prepare_upload_args()

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
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Failed to upload to S3: {str(e)}"
            )

    def _start_multipart_upload(self, key: str) -> str:
        """Start a multipart upload.

        Args:
            key: S3 object key

        Returns:
            Upload ID for the multipart upload

        Raises:
            ConnectorError: If starting the upload fails
        """
        extra_args = self._prepare_upload_args()
        response = self.s3_client.create_multipart_upload(
            Bucket=self.bucket, Key=key, **extra_args
        )
        return response["UploadId"]

    def _upload_part_with_retry(
        self, key: str, part_number: int, upload_id: str, data: bytes
    ) -> Dict[str, Any]:
        """Upload a part with retry logic.

        Args:
            key: S3 object key
            part_number: Part number
            upload_id: Upload ID
            data: Part data

        Returns:
            Part information including ETag

        Raises:
            Exception: If upload fails after all retries
        """
        for attempt in range(self.max_retries):
            try:
                response = self.s3_client.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data,
                )
                return {"PartNumber": part_number, "ETag": response["ETag"]}
            except Exception:
                if attempt == self.max_retries - 1:
                    raise

        raise Exception("Failed to upload part after all retries")

    def _complete_multipart_upload(self, key: str, upload_id: str, parts: list) -> None:
        """Complete a multipart upload.

        Args:
            key: S3 object key
            upload_id: Upload ID
            parts: List of parts information
        """
        if self.s3_client is not None:
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

    def _upload_multipart(self, buffer: io.BytesIO, key: str) -> None:
        """Upload data to S3 using multipart upload.

        Args:
            buffer: Data buffer
            key: S3 object key

        Raises:
            ConnectorError: If upload fails
        """
        try:
            self._ensure_s3_client()
            upload_id = self._start_multipart_upload(key)

            buffer.seek(0)
            parts = []
            part_number = 1

            try:
                while True:
                    data = buffer.read(self.part_size)
                    if not data:
                        break

                    part_info = self._upload_part_with_retry(
                        key, part_number, upload_id, data
                    )
                    parts.append(part_info)
                    part_number += 1

                self._complete_multipart_upload(key, upload_id, parts)
            except Exception:
                if self.s3_client is not None:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket, Key=key, UploadId=upload_id
                    )
                raise
        except Exception as e:
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Failed to upload to S3: {str(e)}"
            )

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to S3.

        Args:
            object_name: Name of the object to write to (used as part of the S3 key)
            data_chunk: Data to write
            mode: Write mode (ignored for S3)

        Raises:
            ConnectorError: If write fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.s3_client is None:
                self._initialize_s3_client()
                if self.s3_client is None:
                    raise ConnectorError(
                        self.name or "S3_EXPORT", "Failed to initialize S3 client"
                    )

            import uuid

            key = self._generate_key(str(uuid.uuid4()))

            if self.format == "csv":
                self._export_csv(data_chunk, key)
            elif self.format == "parquet":
                self._export_parquet(data_chunk, key)
            elif self.format == "json":
                self._export_json(data_chunk, key)

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "S3_EXPORT", f"Write operation failed: {str(e)}"
            )

    def close(self) -> None:
        """Close the S3 client."""
        self.s3_client = None
