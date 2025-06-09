import io
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import boto3
import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector

logger = logging.getLogger(__name__)


class S3Destination(DestinationConnector):
    """
    Connector for writing data to S3.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.uri = self.config.get("uri")
        if not self.uri:
            raise ValueError("S3Destination: 'uri' not specified in config")

        parsed_uri = urlparse(self.uri)
        self.bucket = parsed_uri.netloc
        self.key = parsed_uri.path.lstrip("/")

        self.s3_client = boto3.client("s3")

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to the S3 file.
        """
        file_format = self.key.split(".")[-1].lower()
        buffer = io.BytesIO()

        if file_format == "csv":
            df.to_csv(buffer, index=False)
        elif file_format == "parquet":
            df.to_parquet(buffer, index=False)
        elif file_format == "json":
            df.to_json(buffer, orient="records")
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        self.s3_client.put_object(
            Bucket=self.bucket, Key=self.key, Body=buffer.getvalue()
        )
