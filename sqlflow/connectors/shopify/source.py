"""Shopify Source Connector implementation.

This module provides a comprehensive Shopify connector for extracting data from
Shopify stores using the Admin API with authentication, pagination, and error handling.
"""

import time
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import requests

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ShopifySource(Connector):
    """
    Enhanced Shopify connector for extracting data from Shopify Admin API.

    Supports multiple authentication methods, pagination, rate limiting,
    and comprehensive error handling for production use.
    """

    # Shopify API endpoints and their data keys
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

    def __init__(self):
        super().__init__()
        self.shop_domain: str = ""
        self.access_token: str = ""
        self.api_version: str = "2023-10"
        self.base_url: str = ""
        self.timeout: int = 30
        self.max_retries: int = 3
        self.retry_delay: float = 1.0
        self.rate_limit_delay: float = 0.5  # Shopify rate limiting
        self.session: Optional[requests.Session] = None

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the Shopify connector with connection parameters."""
        self.shop_domain = params.get("shop_domain", "")
        if not self.shop_domain:
            raise ValueError("Shopify connector requires 'shop_domain' parameter")

        # Normalize shop domain
        if not self.shop_domain.endswith(".myshopify.com"):
            if "." not in self.shop_domain:
                self.shop_domain = f"{self.shop_domain}.myshopify.com"

        self.access_token = params.get("access_token", "")
        if not self.access_token:
            raise ValueError("Shopify connector requires 'access_token' parameter")

        self.api_version = params.get("api_version", "2023-10")
        self.timeout = params.get("timeout", 30)
        self.max_retries = params.get("max_retries", 3)
        self.retry_delay = params.get("retry_delay", 1.0)
        self.rate_limit_delay = params.get("rate_limit_delay", 0.5)

        # Construct base URL
        self.base_url = f"https://{self.shop_domain}/admin/api/{self.api_version}/"

        self.state = ConnectorState.CONFIGURED

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

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 250,  # Shopify default limit
        cursor_value: Optional[Any] = None,
    ) -> Iterator[DataChunk]:
        """Read data from Shopify API endpoint."""
        if not object_name or object_name not in self.ENDPOINTS:
            raise ValueError(
                f"Invalid object_name: {object_name}. Must be one of {list(self.ENDPOINTS.keys())}"
            )

        try:
            session = self._create_session()
            yield from self._read_paginated(
                session, object_name, columns, batch_size, cursor_value
            )

        except Exception as e:
            logger.error(f"Failed to read data from Shopify {object_name}: {e}")
            raise

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
                    "X-Shopify-Access-Token": self.access_token,
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
