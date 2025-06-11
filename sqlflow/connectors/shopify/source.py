"""Shopify Source Connector implementation.

This module provides a comprehensive Shopify connector for extracting data from
Shopify stores using the Admin API with authentication, pagination, and error handling.
Optimized for performance with connection pooling, credential caching, and batched operations.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.rest.source import RestSource

logger = logging.getLogger(__name__)

# Performance optimization constants
MAX_BATCH_SIZE = 250  # Shopify API limit
CONNECTION_POOL_SIZE = 10  # Connection pool size
MAX_RETRIES = 3  # Maximum retry attempts
CREDENTIAL_CACHE_TTL = 1800  # 30 minutes credential cache
RATE_LIMIT_BUFFER = 0.1  # Buffer time for rate limiting
MAX_PARALLEL_ENDPOINTS = 4  # Maximum parallel endpoint requests


class ShopifySource(RestSource):
    """
    Connector for reading data from Shopify with performance optimizations.
    """

    # Shopify API endpoints mapping
    ENDPOINTS = {
        "orders": "orders",
        "customers": "customers",
        "products": "products",
        "inventory_items": "inventory_items",
        "inventory_levels": "inventory_levels",
        "locations": "locations",
        "transactions": "transactions",
        "fulfillments": "fulfillments",
        "refunds": "refunds",
        "discounts": "discounts",
        "collections": "collections",
        "metafields": "metafields",
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Call parent without config to avoid double configuration
        super().__init__()

        # Performance optimization attributes
        self.connection_pool_size = CONNECTION_POOL_SIZE
        self.max_parallel_endpoints = MAX_PARALLEL_ENDPOINTS
        self.enable_connection_pooling = True
        self.enable_parallel_requests = True

        # Credential caching and backward compatibility
        self._cached_session = None
        self._session_cache_time = 0
        self._credential_cache_ttl = CREDENTIAL_CACHE_TTL

        # Backward compatibility - maintain session attribute
        self.session = None

        if config:
            # Configure directly with the Shopify config
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the Shopify connector with connection parameters and optimizations."""
        # Validate required parameters
        shop_domain = params.get("shop_domain")
        if not shop_domain:
            raise ValueError("Shopify connector requires 'shop_domain' parameter")

        access_token = params.get("access_token")
        if not access_token:
            raise ValueError("Shopify connector requires 'access_token' parameter")

        # Normalize shop domain
        if not shop_domain.endswith(".myshopify.com"):
            shop_domain = f"{shop_domain}.myshopify.com"

        # Store Shopify-specific attributes
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = params.get("api_version", "2023-10")
        self.timeout = params.get("timeout", 30)
        self.max_retries = params.get("max_retries", MAX_RETRIES)
        self.retry_delay = params.get("retry_delay", 1.0)
        self.rate_limit_delay = params.get("rate_limit_delay", RATE_LIMIT_BUFFER)

        # Performance optimization parameters
        self.connection_pool_size = params.get(
            "connection_pool_size", CONNECTION_POOL_SIZE
        )
        self.max_parallel_endpoints = params.get(
            "max_parallel_endpoints", MAX_PARALLEL_ENDPOINTS
        )
        self.enable_connection_pooling = params.get("enable_connection_pooling", True)
        self.enable_parallel_requests = params.get("enable_parallel_requests", True)
        self._credential_cache_ttl = params.get(
            "credential_cache_ttl", CREDENTIAL_CACHE_TTL
        )

        # Build base URL
        self.base_url = f"https://{shop_domain}/admin/api/{self.api_version}/"

        # Create REST config and configure parent
        url = f"https://{shop_domain}/admin/api/{self.api_version}"
        auth = {
            "type": "api_key",
            "key_name": "X-Shopify-Access-Token",
            "key_value": access_token,
            "add_to": "header",
        }
        pagination = {
            "cursor_param": "since_id",
            "cursor_path": "id",
            "size_param": "limit",
            "page_size": MAX_BATCH_SIZE,
            "parallel_safe": True,  # Enable parallel requests
        }
        rest_config = {
            "url": url,
            "auth": auth,
            "pagination": pagination,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "type": "rest",  # Add the type field that the test expects
            "parallel_requests": self.enable_parallel_requests,
            "max_parallel_requests": self.max_parallel_endpoints,
        }

        # Store config for backward compatibility and test expectations
        self.config = rest_config

        super().configure(rest_config)

    def _get_optimized_session(self) -> requests.Session:
        """Get optimized session with connection pooling and credential caching.

        Zen: "Simple is better than complex" - reuse connections efficiently.
        """
        current_time = time.time()

        # Return cached session if still valid
        if (
            self._cached_session is not None
            and current_time - self._session_cache_time < self._credential_cache_ttl
        ):
            # Update backward compatibility attribute
            self.session = self._cached_session
            return self._cached_session

        # Create new optimized session
        session = requests.Session()

        if self.enable_connection_pooling:
            # Configure retry strategy
            retry_strategy = Retry(
                total=self.max_retries,
                backoff_factor=self.retry_delay,
                status_forcelist=[
                    429,
                    500,
                    502,
                    503,
                    504,
                ],  # Retry on these status codes
                allowed_methods=["GET", "POST", "PUT", "DELETE"],
            )

            # Create HTTP adapter with connection pooling
            adapter = HTTPAdapter(
                pool_connections=self.connection_pool_size,
                pool_maxsize=self.connection_pool_size,
                max_retries=retry_strategy,
            )

            # Mount adapter for both HTTP and HTTPS
            session.mount("http://", adapter)
            session.mount("https://", adapter)

        # Set default headers
        session.headers.update(
            {
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
                "User-Agent": "SQLFlow-Shopify-Connector/1.0",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",  # Enable compression
            }
        )

        # Set timeout
        session.timeout = self.timeout

        # Cache the session
        self._cached_session = session
        self._session_cache_time = current_time

        # Update backward compatibility attribute
        self.session = session

        return session

    def _batch_process_endpoints(
        self,
        endpoints: List[str],
        batch_size: int = MAX_BATCH_SIZE,
        columns: Optional[List[str]] = None,
    ) -> Iterator[DataChunk]:
        """Process multiple endpoints in batches with parallel processing.

        Zen: "Flat is better than nested" - clean batch processing.
        """
        if not self.enable_parallel_requests or len(endpoints) <= 1:
            # Sequential processing
            for endpoint in endpoints:
                yield from self.read(endpoint, columns=columns, batch_size=batch_size)
        else:
            # Parallel processing
            max_workers = min(len(endpoints), self.max_parallel_endpoints)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all endpoint reading tasks
                future_to_endpoint = {
                    executor.submit(
                        list,
                        self.read(endpoint, columns=columns, batch_size=batch_size),
                    ): endpoint
                    for endpoint in endpoints
                }

                # Collect results
                for future in future_to_endpoint:
                    endpoint = future_to_endpoint[future]
                    try:
                        chunks = future.result()
                        for chunk in chunks:
                            yield chunk
                    except Exception as e:
                        logger.error(f"Endpoint {endpoint} processing failed: {str(e)}")

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,  # Legacy support
        cursor_value: Optional[Any] = None,  # Legacy support
    ) -> Iterator[DataChunk]:
        """Read data from Shopify API endpoint with optimized request handling.

        Supports both new interface and legacy interface for backward compatibility.
        """
        # Handle backward compatibility
        if options is not None:
            # Legacy interface: options dict contains table_name
            object_name = options.get("table_name", object_name)
            columns = options.get("columns", columns)
            batch_size = options.get("batch_size", batch_size)
            cursor_value = options.get("cursor_value", cursor_value)

        if not object_name:
            raise ValueError(
                "ShopifySource: 'object_name' or 'table_name' must be specified"
            )

        if object_name not in self.ENDPOINTS:
            raise ValueError(f"Invalid object_name: {object_name}")

        # Extract cursor_value from filters if not provided directly
        if cursor_value is None and filters:
            cursor_value = filters.get("cursor_value")

        try:
            # Use optimized session with connection pooling
            session = self._get_optimized_session()

            # Use Shopify-specific pagination method with optimizations
            if self.pagination_config:
                yield from self._read_paginated_optimized(
                    session, object_name, columns, batch_size, cursor_value
                )
            else:
                # Single request without pagination
                url = urljoin(self.base_url, f"{object_name}.json")
                data_key = self.ENDPOINTS[object_name]

                params = self._build_initial_params(batch_size, cursor_value)
                response = self._make_request_optimized(session, url, params)
                data = response.json()

                items = data.get(data_key, [])
                if items:
                    chunk = self._process_data_chunk_optimized(items, columns)
                    yield chunk

        except Exception as e:
            logger.error(f"Failed to read data from Shopify API: {e}")
            raise

    def _read_paginated_optimized(
        self,
        session: requests.Session,
        object_name: str,
        columns: Optional[List[str]],
        batch_size: int,
        cursor_value: Optional[Any],
    ) -> Iterator[DataChunk]:
        """Read data with optimized pagination support and rate limiting."""
        url = urljoin(self.base_url, f"{object_name}.json")
        data_key = self.ENDPOINTS[object_name]

        params = self._build_initial_params(batch_size, cursor_value)
        page_info = None
        request_count = 0
        start_time = time.time()

        while True:
            # Add pagination parameters
            if page_info:
                params["page_info"] = page_info

            # Implement intelligent rate limiting
            if request_count > 0:
                self._apply_rate_limiting(request_count, start_time)

            response = self._make_request_optimized(session, url, params)
            data = response.json()

            items = data.get(data_key, [])
            if not items:
                break

            # Process and yield data chunk with optimizations
            chunk = self._process_data_chunk_optimized(items, columns)
            yield chunk

            # Check for next page and update pagination
            page_info = self._extract_next_page_info(response)
            if not page_info:
                break

            # Remove since_id for subsequent pages
            params.pop("since_id", None)
            request_count += 1

    def _apply_rate_limiting(self, request_count: int, start_time: float) -> None:
        """Apply intelligent rate limiting to avoid API quotas.

        Zen: "Explicit is better than implicit" - manage rate limits clearly.
        """
        # Shopify allows 2 requests per second by default
        # Be conservative and aim for 1.5 requests per second
        expected_duration = request_count / 1.5
        actual_duration = time.time() - start_time

        if actual_duration < expected_duration:
            sleep_time = expected_duration - actual_duration + self.rate_limit_delay
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

    def _make_request_optimized(
        self,
        session: requests.Session,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        raise_for_status: bool = True,
    ) -> requests.Response:
        """Make HTTP request with optimized error handling and retry logic."""
        for attempt in range(self.max_retries + 1):
            try:
                response = session.get(url, params=params)

                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    backoff_time = retry_after * (2**attempt)  # Exponential backoff
                    logger.warning(f"Rate limited, waiting {backoff_time} seconds")
                    time.sleep(backoff_time)
                    continue

                # Raise for HTTP errors if requested
                if raise_for_status:
                    response.raise_for_status()

                return response

            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries:
                    raise

                wait_time = self.retry_delay * (2**attempt)  # Exponential backoff
                logger.warning(
                    f"Request failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}"
                )
                time.sleep(wait_time)

        raise requests.exceptions.RequestException("Max retries exceeded")

    def _process_data_chunk_optimized(
        self, items: List[Dict], columns: Optional[List[str]]
    ) -> DataChunk:
        """Process raw items into a DataChunk with performance optimizations."""
        # Zen: "Readability counts" - clear data processing
        if not items:
            return DataChunk(data=pd.DataFrame())

        # Use PyArrow for better performance with large datasets
        if len(items) > 1000:
            try:
                # Convert to Arrow table first for better performance
                import pyarrow as pa

                table = pa.Table.from_pylist(items)

                # Convert to pandas with optimized types
                df = table.to_pandas(types_mapper=pd.ArrowDtype)

            except Exception as e:
                logger.debug(f"Arrow conversion failed, using pandas: {str(e)}")
                # Fallback to pandas
                df = pd.json_normalize(items)
        else:
            # Use pandas for smaller datasets
            df = pd.json_normalize(items)

        # Filter columns if specified
        if columns:
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        return DataChunk(data=df)

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the Shopify store."""
        if self.state != ConnectorState.CONFIGURED:
            return ConnectionTestResult(
                success=False, message="Connector not configured"
            )

        try:
            session = self._get_optimized_session()

            # Test connection by fetching shop info
            url = urljoin(self.base_url, "shop.json")
            response = self._make_request_optimized(
                session, url, raise_for_status=False
            )

            if response.status_code == 200:
                shop_data = response.json().get("shop", {})
                shop_name = shop_data.get("name", "Unknown")
                return ConnectionTestResult(
                    success=True,
                    message=f"Successfully connected to Shopify store: {shop_name}",
                )
            else:
                return ConnectionTestResult(
                    success=False,
                    message=f"HTTP {response.status_code}",
                )

        except requests.exceptions.Timeout:
            return ConnectionTestResult(
                success=False,
                message="Connection timeout",
            )
        except requests.exceptions.ConnectionError:
            return ConnectionTestResult(
                success=False,
                message="Connection error occurred",
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection test failed: {str(e)}",
            )

    def discover(self) -> List[str]:
        """Discover available Shopify data objects."""
        return list(self.ENDPOINTS.keys())

    def get_schema(self, object_name: Optional[str] = None) -> Schema:
        """Infer schema from Shopify API response with caching."""
        if not object_name or object_name not in self.ENDPOINTS:
            return Schema(pa.schema([]))

        try:
            session = self._get_optimized_session()

            # Get a small sample to infer schema
            url = urljoin(self.base_url, f"{object_name}.json")
            params = {"limit": 5}  # Small sample for schema inference

            response = self._make_request_optimized(session, url, params)
            data = response.json()

            # Extract data using the endpoint's data key
            data_key = self.ENDPOINTS[object_name]
            items = data.get(data_key, [])

            if not items:
                return Schema(pa.schema([]))

            # Convert to DataFrame to infer schema
            df = pd.json_normalize(items)

            # Convert to arrow schema
            arrow_table = pa.Table.from_pandas(df)
            return Schema(arrow_table.schema)

        except Exception as e:
            logger.error(f"Failed to infer schema for {object_name}: {e}")
            return Schema(pa.schema([]))

    def get_cursor_value(self, object_name: str, cursor_column: str) -> Optional[Any]:
        """Get the maximum cursor value for incremental loading."""
        try:
            session = self._get_optimized_session()

            # Get the most recent item by ID (Shopify uses ascending IDs)
            url = urljoin(self.base_url, f"{object_name}.json")
            params = {"limit": 1, "order": "id desc"}  # Get most recent by ID

            response = self._make_request_optimized(session, url, params)
            data = response.json()

            data_key = self.ENDPOINTS[object_name]
            items = data.get(data_key, [])

            if items and cursor_column in items[0]:
                return items[0][cursor_column]

            return None

        except Exception as e:
            logger.warning(
                f"Failed to get cursor value for {object_name}.{cursor_column}: {e}"
            )
            return None

    def _create_session(self) -> requests.Session:
        """Create and configure a requests session (legacy compatibility)."""
        return self._get_optimized_session()

    def _make_request(
        self,
        session: requests.Session,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Make HTTP request (legacy compatibility)."""
        return self._make_request_optimized(session, url, params)

    def _read_paginated(
        self,
        session: requests.Session,
        object_name: str,
        columns: Optional[List[str]],
        batch_size: int,
        cursor_value: Optional[Any],
    ) -> Iterator[DataChunk]:
        """Read data with pagination support (legacy compatibility)."""
        yield from self._read_paginated_optimized(
            session, object_name, columns, batch_size, cursor_value
        )

    def _build_initial_params(
        self, batch_size: int, cursor_value: Optional[Any]
    ) -> Dict[str, Any]:
        """Build initial request parameters."""
        params = {"limit": min(batch_size, MAX_BATCH_SIZE)}  # Shopify max limit is 250

        # Add cursor for incremental loading
        if cursor_value is not None:
            params["since_id"] = cursor_value

        return params

    def _process_data_chunk(
        self, items: List[Dict], columns: Optional[List[str]]
    ) -> DataChunk:
        """Process raw items into a DataChunk (legacy compatibility)."""
        return self._process_data_chunk_optimized(items, columns)

    def _extract_next_page_info(self, response: requests.Response) -> Optional[str]:
        """Extract page_info for next page from Link header."""
        link_header = response.headers.get("Link", "")
        if 'rel="next"' not in link_header:
            return None

        # Extract page_info from Link header
        for link in link_header.split(","):
            if 'rel="next"' in link:
                # Extract page_info parameter from URL
                next_url = link.split(";")[0].strip("<> ")
                if "page_info=" in next_url:
                    return next_url.split("page_info=")[1].split("&")[0]

        return None
