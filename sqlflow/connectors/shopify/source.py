"""Shopify Source Connector implementation.

This module provides a comprehensive Shopify connector for extracting data from
Shopify stores using the Admin API with authentication, pagination, and error handling.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import requests

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.rest.source import RestSource

logger = logging.getLogger(__name__)


class ShopifySource(RestSource):
    """
    Connector for reading data from Shopify.
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

        if config:
            # Configure directly with the Shopify config
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the Shopify connector with connection parameters."""
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
        self.max_retries = params.get("max_retries", 3)
        self.retry_delay = params.get("retry_delay", 1.0)
        self.rate_limit_delay = params.get("rate_limit_delay", 0.5)

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
            "page_size": 250,
        }
        rest_config = {
            "url": url,
            "auth": auth,
            "pagination": pagination,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "type": "rest",  # Add the type field that the test expects
        }

        # Store config for backward compatibility and test expectations
        self.config = rest_config

        super().configure(rest_config)

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,  # Legacy support
        cursor_value: Optional[Any] = None,  # Legacy support
    ) -> Iterator[DataChunk]:
        """Read data from Shopify API endpoint.

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
            session = self._create_session()

            # Use Shopify-specific pagination method
            if self.pagination_config:
                yield from self._read_paginated(
                    session, object_name, columns, batch_size, cursor_value
                )
            else:
                # Single request without pagination
                url = urljoin(self.base_url, f"{object_name}.json")
                data_key = self.ENDPOINTS[object_name]

                params = self._build_initial_params(batch_size, cursor_value)
                response = self._make_request(session, url, params)
                data = response.json()

                items = data.get(data_key, [])
                if items:
                    chunk = self._process_data_chunk(items, columns)
                    yield chunk

        except Exception as e:
            logger.error(f"Failed to read data from Shopify API: {e}")
            raise

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the Shopify store."""
        if self.state != ConnectorState.CONFIGURED:
            return ConnectionTestResult(
                success=False, message="Connector not configured"
            )

        try:
            session = self._create_session()

            # Test connection by fetching shop info
            url = urljoin(self.base_url, "shop.json")
            response = self._make_request(session, url)

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
                    message=f"Failed to connect: HTTP {response.status_code}",
                )

        except requests.exceptions.Timeout:
            return ConnectionTestResult(
                success=False,
                message=f"Connection timeout after {self.timeout} seconds",
            )
        except requests.exceptions.ConnectionError as e:
            return ConnectionTestResult(
                success=False, message=f"Connection failed: {str(e)}"
            )
        except requests.exceptions.HTTPError as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection test failed: HTTP {e.response.status_code}",
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False, message=f"Connection test failed: {str(e)}"
            )

    def discover(self) -> List[str]:
        """Discover available Shopify data objects."""
        return list(self.ENDPOINTS.keys())

    def get_schema(self, object_name: Optional[str] = None) -> Schema:
        """Infer schema from Shopify API response."""
        if not object_name or object_name not in self.ENDPOINTS:
            return Schema(pa.schema([]))

        try:
            session = self._create_session()

            # Get a small sample to infer schema
            url = urljoin(self.base_url, f"{object_name}.json")
            params = {"limit": 5}  # Small sample for schema inference

            response = self._make_request(session, url, params)
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
            session = self._create_session()

            # Get the most recent item by ID (Shopify uses ascending IDs)
            url = urljoin(self.base_url, f"{object_name}.json")
            params = {"limit": 1, "order": "id desc"}  # Get most recent by ID

            response = self._make_request(session, url, params)
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
        """Create and configure a requests session."""
        if self.session is None:
            self.session = requests.Session()

            # Set default headers
            self.session.headers.update(
                {
                    "X-Shopify-Access-Token": self.config.get("access_token"),
                    "Content-Type": "application/json",
                    "User-Agent": "SQLFlow-Shopify-Connector/1.0",
                }
            )

            # Set timeout
            self.session.timeout = self.timeout

        return self.session

    def _make_request(
        self,
        session: requests.Session,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Make HTTP request with retry logic and rate limiting."""
        for attempt in range(self.max_retries + 1):
            try:
                response = session.get(url, params=params)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    time.sleep(retry_after)
                    continue

                # Raise for other HTTP errors
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

    def _read_paginated(
        self,
        session: requests.Session,
        object_name: str,
        columns: Optional[List[str]],
        batch_size: int,
        cursor_value: Optional[Any],
    ) -> Iterator[DataChunk]:
        """Read data with pagination support."""
        url = urljoin(self.base_url, f"{object_name}.json")
        data_key = self.ENDPOINTS[object_name]

        params = self._build_initial_params(batch_size, cursor_value)
        page_info = None

        while True:
            # Add pagination parameters
            if page_info:
                params["page_info"] = page_info

            response = self._make_request(session, url, params)
            data = response.json()

            items = data.get(data_key, [])
            if not items:
                break

            # Process and yield data chunk
            chunk = self._process_data_chunk(items, columns)
            yield chunk

            # Check for next page and update pagination
            page_info = self._extract_next_page_info(response)
            if not page_info:
                break

            # Remove since_id for subsequent pages
            params.pop("since_id", None)

    def _build_initial_params(
        self, batch_size: int, cursor_value: Optional[Any]
    ) -> Dict[str, Any]:
        """Build initial request parameters."""
        params = {"limit": min(batch_size, 250)}  # Shopify max limit is 250

        # Add cursor for incremental loading
        if cursor_value is not None:
            params["since_id"] = cursor_value

        return params

    def _process_data_chunk(
        self, items: List[Dict], columns: Optional[List[str]]
    ) -> DataChunk:
        """Process raw items into a DataChunk with optional column filtering."""
        # Convert to DataFrame
        df = pd.json_normalize(items)

        # Filter columns if specified
        if columns:
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        return DataChunk(data=df)

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
