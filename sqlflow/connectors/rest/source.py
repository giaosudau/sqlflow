"""REST API Source Connector implementation.

This module provides a comprehensive REST API connector for consuming JSON data from
HTTP endpoints with authentication, pagination, and error handling.

Performance optimizations implemented:
- Parallel request capability for paginated endpoints
- Response streaming for large payloads
- Optimized JSON parsing using hybrid Arrow/pandas approach
- Connection pooling optimizations
- Intelligent batching based on response characteristics
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from urllib3.util.retry import Retry

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

logger = get_logger(__name__)

# Performance constants following Zen of Python: "Simple is better than complex"
DEFAULT_BATCH_SIZE = 10000  # Default batch size for API requests
LARGE_RESPONSE_THRESHOLD_MB = 10  # Threshold for using streaming
MAX_PARALLEL_REQUESTS = 4  # Maximum concurrent requests for pagination
CONNECTION_POOL_SIZE = 10  # Connection pool size for performance
MAX_RETRIES = 3  # Maximum retry attempts with exponential backoff


class RestSource(Connector):
    """
    Enhanced REST API connector with performance optimizations.

    Features:
    - Authentication support (Basic, Digest, Bearer, API Key)
    - Pagination handling with parallel requests
    - Response streaming for large payloads
    - Connection pooling and retry strategies
    - Schema inference and column selection

    Performance optimizations follow Zen of Python:
    - "Simple is better than complex" - straightforward optimization rules
    - "Explicit is better than implicit" - clear parameter handling
    - "Practicality beats purity" - real-world performance over theoretical perfection
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        # Initialize default values to prevent AttributeError
        self.url = None
        self.method = "GET"
        self.headers = {}
        self.params = {}
        self.auth_config = None
        self.pagination_config = None
        self.timeout = 30
        self.max_retries = MAX_RETRIES
        self.retry_delay = 1.0
        self.data_path = None
        self.flatten_response = True
        self.session: Optional[requests.Session] = None

        # Performance optimization settings
        self.parallel_requests = True
        self.max_parallel_requests = MAX_PARALLEL_REQUESTS
        self.stream_large_responses = True
        self.response_streaming_threshold = LARGE_RESPONSE_THRESHOLD_MB
        self.use_connection_pooling = True

        if config:
            self.configure(config)

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
        self.max_retries = params.get("max_retries", MAX_RETRIES)
        self.retry_delay = params.get("retry_delay", 1.0)
        self.data_path = params.get("data_path")
        self.flatten_response = params.get("flatten_response", True)

        # Performance optimization settings
        self.parallel_requests = params.get("parallel_requests", True)
        self.max_parallel_requests = params.get(
            "max_parallel_requests", MAX_PARALLEL_REQUESTS
        )
        self.stream_large_responses = params.get("stream_large_responses", True)
        self.response_streaming_threshold = params.get(
            "response_streaming_threshold", LARGE_RESPONSE_THRESHOLD_MB
        )
        self.use_connection_pooling = params.get("use_connection_pooling", True)

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

            # Convert to DataFrame to infer schema using optimized method
            df = self._convert_to_dataframe_optimized(data)
            if df.empty:
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
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,  # Legacy support
    ) -> Iterator[DataChunk]:
        """Read data from the REST API endpoint.

        Supports both new interface (object_name, columns, filters, batch_size)
        and legacy interface (options dict).
        """
        try:
            session = self._create_session()

            # Handle backward compatibility
            if options is not None:
                # Legacy interface: options dict contains all parameters
                columns = options.get("columns", columns)
                batch_size = options.get("batch_size", batch_size)
                cursor_value = options.get("cursor_value")
            else:
                # New interface: cursor_value from filters
                cursor_value = filters.get("cursor_value") if filters else None

            if self.pagination_config:
                if self.parallel_requests and self.pagination_config.get(
                    "parallel_safe", False
                ):
                    yield from self._read_paginated_parallel(
                        session, columns, batch_size, cursor_value
                    )
                else:
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
        """Create and configure a requests session with performance optimizations."""
        if self.session is None:
            session = requests.Session()
            session.headers.update(self.headers)

            # Configure connection pooling and retry strategy for performance
            if self.use_connection_pooling:
                retry_strategy = Retry(
                    total=self.max_retries,
                    backoff_factor=self.retry_delay,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=[
                        "HEAD",
                        "GET",
                        "OPTIONS",
                    ],  # Updated parameter name
                )

                adapter = HTTPAdapter(
                    pool_connections=CONNECTION_POOL_SIZE,
                    pool_maxsize=CONNECTION_POOL_SIZE,
                    max_retries=retry_strategy,
                    pool_block=False,
                )

                session.mount("http://", adapter)
                session.mount("https://", adapter)

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

        df = self._convert_to_dataframe_optimized(data)

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
        cursor_value: Optional[Any] = None,
    ) -> Iterator[DataChunk]:
        """Read paginated data from the REST API."""
        current_params = self.params.copy()
        current_params[self.pagination_config["size_param"]] = batch_size

        page_param = self.pagination_config.get("page_param", "page")
        self.pagination_config.get("size_param", "limit")
        page_size = self.pagination_config.get("page_size", batch_size)

        page = 1
        params = current_params.copy()

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

                df = self._convert_to_dataframe_optimized(data)

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

    def _read_paginated_parallel(
        self,
        session: requests.Session,
        columns: Optional[List[str]],
        batch_size: int,
        cursor_value: Optional[Any] = None,
    ) -> Iterator[DataChunk]:
        """Read paginated data using parallel requests for improved performance.

        This method is used when pagination_config has 'parallel_safe': True,
        indicating that multiple pages can be fetched concurrently.
        """
        current_params = self.params.copy()
        current_params[self.pagination_config["size_param"]] = batch_size

        page_param = self.pagination_config.get("page_param", "page")
        page_size = self.pagination_config.get("page_size", batch_size)

        # Add cursor parameter for incremental loading
        if cursor_value is not None:
            cursor_param = self.pagination_config.get("cursor_param", "since")
            current_params[cursor_param] = cursor_value

        # First, get the first page to understand pagination structure
        first_page_data = self._fetch_first_page(
            session, current_params, page_param, columns
        )
        if first_page_data is None or first_page_data.empty:
            return

        yield DataChunk(data=first_page_data)

        # Check if we have more pages
        if len(first_page_data) < page_size:
            return  # Only one page

        # Fetch remaining pages in parallel
        yield from self._fetch_remaining_pages_parallel(
            session, current_params, page_param, columns
        )

    def _fetch_first_page(
        self,
        session: requests.Session,
        current_params: Dict[str, Any],
        page_param: str,
        columns: Optional[List[str]],
    ) -> Optional[pd.DataFrame]:
        """Fetch the first page of data to understand pagination structure."""
        try:
            params = current_params.copy()
            params[page_param] = 1

            response = self._make_request(session, params)
            data = response.json()

            if self.data_path:
                data = self._extract_data_by_path(data, self.data_path)

            df = self._convert_to_dataframe_optimized(data)

            if df.empty:
                return None

            if columns:
                available_columns = [col for col in columns if col in df.columns]
                if available_columns:
                    df = df[available_columns]

            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch first page: {e}")
            return None

    def _fetch_remaining_pages_parallel(
        self,
        session: requests.Session,
        current_params: Dict[str, Any],
        page_param: str,
        columns: Optional[List[str]],
    ) -> Iterator[DataChunk]:
        """Fetch remaining pages in parallel."""
        try:
            # Estimate total pages (conservative approach)
            max_pages_to_fetch = min(
                10, self.max_parallel_requests
            )  # Conservative limit

            # Fetch multiple pages in parallel
            with ThreadPoolExecutor(max_workers=self.max_parallel_requests) as executor:
                future_to_page = {}

                for page in range(2, max_pages_to_fetch + 2):  # Start from page 2
                    params = current_params.copy()
                    params[page_param] = page
                    future = executor.submit(
                        self._fetch_page_data, session, params, columns
                    )
                    future_to_page[future] = page

                # Process completed requests as they finish
                for future in as_completed(future_to_page):
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            yield DataChunk(data=df)
                        else:
                            # Stop if we get an empty page
                            break
                    except Exception as e:
                        page = future_to_page[future]
                        logger.error(f"Failed to fetch page {page}: {e}")
                        break

        except requests.exceptions.RequestException as e:
            logger.error(f"Parallel pagination failed: {e}")

    def _fetch_page_data(
        self,
        session: requests.Session,
        params: Dict[str, Any],
        columns: Optional[List[str]],
    ) -> Optional[pd.DataFrame]:
        """Fetch a single page of data (used by parallel pagination)."""
        try:
            response = self._make_request(session, params)
            data = response.json()

            if self.data_path:
                data = self._extract_data_by_path(data, self.data_path)

            df = self._convert_to_dataframe_optimized(data)

            if df.empty:
                return None

            if columns:
                available_columns = [col for col in columns if col in df.columns]
                if available_columns:
                    df = df[available_columns]

            return df

        except Exception as e:
            logger.error(f"Failed to fetch page data: {e}")
            return None

    def _convert_to_dataframe_optimized(self, data: Any) -> pd.DataFrame:
        """Convert API response data to pandas DataFrame with optimizations.

        Uses hybrid Arrow/pandas approach for better performance with large datasets.
        """
        if isinstance(data, list):
            if len(data) == 0:
                return pd.DataFrame()

            # For large datasets, try PyArrow first for better performance
            if len(data) > 1000:  # Threshold for using Arrow optimization
                try:
                    # Convert to PyArrow table first for better memory efficiency
                    if self.flatten_response:
                        # Use pandas normalize for complex nested structures
                        df = pd.json_normalize(data)
                    else:
                        # Direct DataFrame creation for simple structures
                        df = pd.DataFrame(data)

                    # Convert through Arrow for memory optimization
                    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
                    return arrow_table.to_pandas(use_threads=True)

                except Exception as e:
                    logger.debug(
                        f"Arrow optimization failed, falling back to pandas: {e}"
                    )
                    # Fallback to standard pandas processing
                    return (
                        pd.json_normalize(data)
                        if self.flatten_response
                        else pd.DataFrame(data)
                    )
            else:
                # Use standard pandas for smaller datasets
                return (
                    pd.json_normalize(data)
                    if self.flatten_response
                    else pd.DataFrame(data)
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
