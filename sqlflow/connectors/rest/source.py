"""REST API Source Connector implementation.

This module provides a comprehensive REST API connector for consuming JSON data from
HTTP endpoints with authentication, pagination, and error handling.
"""

import json
import time
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class RestApiSource(Connector):
    """
    Enhanced REST API connector with authentication, pagination, and schema inference.

    Supports various authentication methods, response formats, and pagination patterns
    commonly used in REST APIs.
    """

    def __init__(self):
        super().__init__()
        self.url: str = ""
        self.method: str = "GET"
        self.headers: Dict[str, str] = {}
        self.params: Dict[str, Any] = {}
        self.auth_config: Optional[Dict[str, Any]] = None
        self.pagination_config: Optional[Dict[str, Any]] = None
        self.timeout: int = 30
        self.max_retries: int = 3
        self.retry_delay: float = 1.0
        self.data_path: Optional[str] = None  # JSONPath to extract data from response
        self.flatten_response: bool = True
        self.session: Optional[requests.Session] = None

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the REST API connector with connection parameters."""
        self.url = params.get("url", "")
        if not self.url:
            raise ValueError("REST API connector requires 'url' parameter")

        # Validate URL format
        parsed_url = urlparse(self.url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL format: {self.url}")

        self.method = params.get("method", "GET").upper()
        if self.method not in ["GET", "POST"]:
            raise ValueError(f"Unsupported HTTP method: {self.method}")

        self.headers = params.get("headers", {})
        self.params = params.get("params", {})
        self.auth_config = params.get("auth")
        self.pagination_config = params.get("pagination")
        self.timeout = params.get("timeout", 30)
        self.max_retries = params.get("max_retries", 3)
        self.retry_delay = params.get("retry_delay", 1.0)
        self.data_path = params.get("data_path")
        self.flatten_response = params.get("flatten_response", True)

        # Set default headers
        if "User-Agent" not in self.headers:
            self.headers["User-Agent"] = "SQLFlow-REST-Connector/1.0"

        self.state = ConnectorState.CONFIGURED

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the REST API endpoint."""
        if self.state != ConnectorState.CONFIGURED:
            return ConnectionTestResult(
                success=False, message="Connector not configured"
            )

        try:
            session = self._create_session()

            # Make a test request with limited data
            test_params = self.params.copy()
            if "limit" not in test_params and self.method == "GET":
                test_params["limit"] = 1  # Try to limit response size

            response = self._make_request(session, test_params)

            return self._process_test_response(response)

        except requests.exceptions.Timeout:
            return ConnectionTestResult(
                success=False,
                message=f"Connection timeout after {self.timeout} seconds",
            )
        except requests.exceptions.ConnectionError as e:
            return ConnectionTestResult(
                success=False, message=f"Connection failed: {str(e)}"
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False, message=f"Connection test failed: {str(e)}"
            )

    def discover(self) -> List[str]:
        """Discover available data objects/endpoints."""
        try:
            # For REST APIs, we typically only have the single endpoint
            # In more advanced cases, this could discover multiple endpoints
            # from an API specification (OpenAPI/Swagger)
            return [urlparse(self.url).path.split("/")[-1] or "api_data"]
        except Exception as e:
            logger.warning(f"Failed to discover objects: {e}")
            return ["api_data"]

    def get_schema(self, object_name: Optional[str] = None) -> Schema:
        """Infer schema from the REST API response."""
        try:
            session = self._create_session()

            # Get a sample of data to infer schema
            test_params = self.params.copy()
            if "limit" not in test_params and self.method == "GET":
                test_params["limit"] = 10  # Small sample for schema inference

            response = self._make_request(session, test_params)
            data = response.json()

            # Extract data using data_path if specified
            if self.data_path:
                data = self._extract_data_by_path(data, self.data_path)

            # Convert to DataFrame to infer schema
            if isinstance(data, list) and len(data) > 0:
                df = (
                    pd.json_normalize(data)
                    if self.flatten_response
                    else pd.DataFrame(data)
                )
            elif isinstance(data, dict):
                df = (
                    pd.json_normalize([data])
                    if self.flatten_response
                    else pd.DataFrame([data])
                )
            else:
                # Empty or unsupported data structure
                return Schema(columns=[])

            # Convert to arrow schema
            arrow_table = pa.Table.from_pandas(df)
            return Schema(arrow_table.schema)

        except Exception as e:
            logger.error(f"Failed to infer schema: {e}")
            # Return empty schema
            return Schema(pa.schema([]))

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 1000,
        cursor_value: Optional[Any] = None,
    ) -> Iterator[DataChunk]:
        """Read data from the REST API endpoint."""
        try:
            session = self._create_session()

            if self.pagination_config:
                yield from self._read_paginated(
                    session, columns, batch_size, cursor_value
                )
            else:
                yield from self._read_single_request(session, columns, cursor_value)

        except Exception as e:
            logger.error(f"Failed to read data from REST API: {e}")
            raise

    def get_cursor_value(self, object_name: str, cursor_column: str) -> Optional[Any]:
        """Get the maximum cursor value for incremental loading."""
        try:
            session = self._create_session()

            # Try to get the latest data point sorted by cursor column
            params = self.params.copy()

            # Some APIs support sorting - try common parameter names
            for sort_param in ["sort", "order_by", "orderBy"]:
                if sort_param not in params:
                    params[sort_param] = f"{cursor_column} desc"
                    break

            if "limit" not in params:
                params["limit"] = 1

            response = self._make_request(session, params)
            data = response.json()

            if self.data_path:
                data = self._extract_data_by_path(data, self.data_path)

            if isinstance(data, list) and len(data) > 0:
                df = (
                    pd.json_normalize(data)
                    if self.flatten_response
                    else pd.DataFrame(data)
                )
                if cursor_column in df.columns:
                    return df[cursor_column].iloc[0]

            return None

        except Exception as e:
            logger.warning(f"Failed to get cursor value: {e}")
            return None

    def _create_session(self) -> requests.Session:
        """Create a configured requests session."""
        if self.session is None:
            session = requests.Session()
            session.headers.update(self.headers)

            # Configure authentication
            if self.auth_config:
                auth_type = self.auth_config.get("type", "").lower()

                if auth_type == "basic":
                    session.auth = HTTPBasicAuth(
                        self.auth_config.get("username", ""),
                        self.auth_config.get("password", ""),
                    )
                elif auth_type == "digest":
                    session.auth = HTTPDigestAuth(
                        self.auth_config.get("username", ""),
                        self.auth_config.get("password", ""),
                    )
                elif auth_type == "bearer":
                    token = self.auth_config.get("token", "")
                    session.headers["Authorization"] = f"Bearer {token}"
                elif auth_type == "api_key":
                    key_name = self.auth_config.get("key_name", "X-API-Key")
                    key_value = self.auth_config.get("key_value", "")
                    session.headers[key_name] = key_value

            self.session = session

        return self.session

    def _make_request(
        self, session: requests.Session, params: Dict[str, Any]
    ) -> requests.Response:
        """Make HTTP request with retry logic."""
        for attempt in range(self.max_retries + 1):
            try:
                if self.method == "GET":
                    response = session.get(
                        self.url, params=params, timeout=self.timeout
                    )
                elif self.method == "POST":
                    response = session.post(self.url, json=params, timeout=self.timeout)
                else:
                    raise ValueError(f"Unsupported method: {self.method}")

                # Check status code before returning
                if response.status_code >= 400:
                    response.raise_for_status()

                return response

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}), retrying: {e}"
                    )
                    time.sleep(self.retry_delay * (2**attempt))  # Exponential backoff
                else:
                    raise

    def _read_single_request(
        self,
        session: requests.Session,
        columns: Optional[List[str]],
        cursor_value: Optional[Any],
    ) -> Iterator[DataChunk]:
        """Read data from a single API request."""
        params = self.params.copy()

        # Add cursor parameter for incremental loading
        if cursor_value is not None and self.pagination_config:
            cursor_param = self.pagination_config.get("cursor_param", "since")
            params[cursor_param] = cursor_value

        response = self._make_request(session, params)
        data = response.json()

        if self.data_path:
            data = self._extract_data_by_path(data, self.data_path)

        df = self._convert_to_dataframe(data)

        if columns:
            # Select only requested columns
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        if not df.empty:
            yield DataChunk(data=df)

    def _read_paginated(
        self,
        session: requests.Session,
        columns: Optional[List[str]],
        batch_size: int,
        cursor_value: Optional[Any],
    ) -> Iterator[DataChunk]:
        """Read data with pagination support."""
        page_param = self.pagination_config.get("page_param", "page")
        size_param = self.pagination_config.get("size_param", "limit")
        page_size = self.pagination_config.get("page_size", batch_size)

        page = 1
        params = self.params.copy()
        params[size_param] = page_size

        # Add cursor parameter for incremental loading
        if cursor_value is not None:
            cursor_param = self.pagination_config.get("cursor_param", "since")
            params[cursor_param] = cursor_value

        while True:
            params[page_param] = page

            try:
                response = self._make_request(session, params)
                data = response.json()

                if self.data_path:
                    data = self._extract_data_by_path(data, self.data_path)

                df = self._convert_to_dataframe(data)

                if df.empty:
                    break  # No more data

                if columns:
                    available_columns = [col for col in columns if col in df.columns]
                    if available_columns:
                        df = df[available_columns]

                yield DataChunk(data=df)

                # Check if we should continue pagination
                if len(df) < page_size:
                    break  # Last page (partial page)

                page += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"Pagination failed at page {page}: {e}")
                break

    def _convert_to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert API response data to pandas DataFrame."""
        if isinstance(data, list):
            if len(data) == 0:
                return pd.DataFrame()
            return (
                pd.json_normalize(data) if self.flatten_response else pd.DataFrame(data)
            )
        elif isinstance(data, dict):
            return (
                pd.json_normalize([data])
                if self.flatten_response
                else pd.DataFrame([data])
            )
        else:
            # Handle scalar values or unsupported types
            return pd.DataFrame([{"value": data}])

    def _extract_data_by_path(self, data: Any, path: str) -> Any:
        """Extract data from response using a simple JSONPath-like syntax."""
        try:
            current = data
            for key in path.split("."):
                if isinstance(current, dict):
                    current = current.get(key)
                elif isinstance(current, list) and key.isdigit():
                    idx = int(key)
                    current = current[idx] if 0 <= idx < len(current) else None
                else:
                    return None

                if current is None:
                    break

            return current
        except Exception as e:
            logger.warning(f"Failed to extract data using path '{path}': {e}")
            return data

    def _process_test_response(
        self, response: requests.Response
    ) -> ConnectionTestResult:
        """Process test response and return connection result."""
        try:
            data = response.json()
            message = f"Successfully connected to {self.url}"

            # Add info about response structure
            if isinstance(data, list):
                message += f" (found {len(data)} items)"
            elif isinstance(data, dict):
                if self.data_path:
                    message += f" (using data path: {self.data_path})"
                else:
                    message += f" (found {len(data)} keys)"

            return ConnectionTestResult(success=True, message=message)
        except json.JSONDecodeError as e:
            return ConnectionTestResult(
                success=False, message=f"Invalid JSON response: {str(e)}"
            )
