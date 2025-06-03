"""Shopify connector for SQLFlow with SME-optimized e-commerce analytics.

This module implements a production-ready Shopify connector following the
implementation plan in docs/developer/technical/implementation/shopify_connector_implementation_plan.md
"""

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import requests

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    ParameterError,
    ParameterValidator,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector
from sqlflow.connectors.resilience import (
    ResilienceConfig,
    ResilienceManager,
    resilient_operation,
)
from sqlflow.core.errors import ConnectorError
from sqlflow.logging import get_logger

logger = get_logger(__name__)


# Industry-standard parameter schema following Airbyte/Fivetran conventions
SHOPIFY_PARAMETER_SCHEMA = {
    "properties": {
        # Required parameters
        "shop_domain": {
            "type": "string",
            "description": "Shopify shop domain (e.g., 'mystore.myshopify.com')",
            "pattern": r"^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]\.myshopify\.com$",
        },
        "access_token": {
            "type": "string",
            "description": "Shopify private app access token",
            "minLength": 20,
        },
        # Standard sync parameters (Airbyte/Fivetran compatible)
        "sync_mode": {
            "type": "string",
            "enum": ["full_refresh", "incremental"],
            "default": "incremental",
            "description": "Synchronization mode",
        },
        "cursor_field": {
            "type": "string",
            "default": "updated_at",
            "description": "Field to use for incremental loading",
        },
        "lookback_window": {
            "type": "string",
            "default": "P7D",
            "description": "ISO 8601 duration for lookback buffer",
        },
        # SME-specific parameters
        "flatten_line_items": {
            "type": "boolean",
            "default": True,
            "description": "Flatten order line items into separate rows",
        },
        "financial_status_filter": {
            "type": "array",
            "items": {
                "enum": [
                    "authorized",
                    "pending",
                    "paid",
                    "partially_paid",
                    "refunded",
                    "voided",
                    "partially_refunded",
                ]
            },
            "default": ["paid", "pending", "authorized"],
            "description": "Filter orders by financial status",
        },
        "include_fulfillments": {
            "type": "boolean",
            "default": True,
            "description": "Include fulfillment data in orders",
        },
        "include_refunds": {
            "type": "boolean",
            "default": True,
            "description": "Include refund data in orders",
        },
    },
    "required": ["shop_domain", "access_token"],
}


class ShopifyParameterValidator(ParameterValidator):
    """Shopify-specific parameter validator with industry-standard compatibility."""

    def __init__(self):
        super().__init__("SHOPIFY")

    def _get_required_params(self) -> List[str]:
        """Get required parameters for Shopify connector."""
        return ["shop_domain", "access_token"]

    def _get_optional_params(self) -> Dict[str, Any]:
        """Get optional parameters with SME-optimized defaults."""
        return {
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "lookback_window": "P7D",
            "flatten_line_items": True,
            "financial_status_filter": ["paid", "pending", "authorized"],
            "include_fulfillments": True,
            "include_refunds": True,
            "batch_size": 250,  # Shopify API limit
            "timeout_seconds": 300,
            "max_retries": 3,
        }

    def validate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize Shopify connector parameters."""
        logger.info("Validating Shopify connector parameters")

        # Start with default parameters
        validated = self._get_default_params()

        # Update with provided parameters
        validated.update(params)

        # Validate required parameters
        required_params = self._get_required_params()
        for param in required_params:
            if param not in validated or validated[param] is None:
                raise ParameterError(
                    f"Required parameter '{param}' is missing", "SHOPIFY"
                )

        # Validate shop domain format
        self._validate_shop_domain(validated["shop_domain"])

        # Validate access token
        self._validate_access_token(validated["access_token"])

        # Validate all parameters and their relationships
        self._validate_all_params(validated)

        logger.info("Shopify connector parameters validated successfully")
        return validated

    def _validate_shop_domain(self, domain: str) -> None:
        """Validate shop domain format and security."""
        # Remove protocol if present
        domain = domain.replace("https://", "").replace("http://", "")

        # Add .myshopify.com if not present
        if not domain.endswith(".myshopify.com"):
            domain = f"{domain}.myshopify.com"

        # Validate domain format against injection attacks
        pattern = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]\.myshopify\.com$")
        if not pattern.match(domain):
            raise ParameterError(
                f"Invalid shop domain format: {domain}. Must be format: 'storename.myshopify.com'",
                "SHOPIFY",
            )

        # Prevent domain injection attacks
        if ".." in domain or domain.count(".") != 2:
            raise ParameterError(
                "Potential domain injection detected in shop_domain", "SHOPIFY"
            )

    def _validate_access_token(self, token: str) -> None:
        """Validate access token format and basic security."""
        if not token or len(token) < 20:
            raise ParameterError(
                "Access token too short or empty. Shopify tokens must be at least 20 characters",
                "SHOPIFY",
            )

        # Check for obvious test tokens or placeholder values (but allow realistic test tokens)
        dangerous_patterns = [
            "your-token",
            "placeholder",
            "example_token",
            "demo_only",
            "replace_me",
        ]
        if any(pattern in token.lower() for pattern in dangerous_patterns):
            raise ParameterError(
                "Placeholder token detected - use real Shopify access token", "SHOPIFY"
            )

    def _validate_lookback_window(self, window: str) -> None:
        """Validate ISO 8601 duration format for lookback window."""
        if not window.startswith("P"):
            raise ParameterError(
                f"Invalid lookback window format: {window}. Must be ISO 8601 duration (e.g., 'P7D')",
                "SHOPIFY",
            )

    def _validate_sync_mode(self, sync_mode: str) -> None:
        """Validate sync mode parameter."""
        valid_modes = ["full_refresh", "incremental"]
        if sync_mode not in valid_modes:
            raise ParameterError(
                f"Invalid sync_mode '{sync_mode}'. Must be one of: {valid_modes}",
                "SHOPIFY",
            )

    def _validate_all_params(self, validated: Dict[str, Any]) -> None:
        """Validate all parameter relationships and business rules."""
        # Validate sync mode
        if "sync_mode" in validated:
            self._validate_sync_mode(validated["sync_mode"])

        # Validate cursor field for incremental mode
        if validated.get("sync_mode") == "incremental" and not validated.get(
            "cursor_field"
        ):
            raise ParameterError(
                "cursor_field is required for incremental sync mode", "SHOPIFY"
            )

    def _get_default_params(self) -> Dict[str, Any]:
        """Get default parameter values."""
        return {
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "flatten_line_items": True,
            "include_fulfillments": True,
            "lookback_window": "P7D",
            "api_version": "2023-10",
        }


class ShopifyAPIClient:
    """Rate-limited Shopify API client with authentication and error handling."""

    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = self._normalize_domain(shop_domain)
        self.access_token = access_token
        self.api_version = "2024-01"  # Use stable API version
        self.base_url = f"https://{self.shop_domain}/admin/api/{self.api_version}/"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json",
                "User-Agent": "SQLFlow-Shopify-Connector/1.0",
            }
        )

    def _normalize_domain(self, domain: str) -> str:
        """Normalize shop domain to standard format."""
        domain = domain.replace("https://", "").replace("http://", "")
        if not domain.endswith(".myshopify.com"):
            domain = f"{domain}.myshopify.com"
        return domain

    @resilient_operation()
    def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make authenticated GET request to Shopify API."""
        url = urljoin(self.base_url, endpoint)

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                raise ConnectorError(
                    "SHOPIFY", "Authentication failed - invalid access token"
                )
            elif response.status_code == 403:
                raise ConnectorError(
                    "SHOPIFY", "Access forbidden - insufficient permissions"
                )
            elif response.status_code == 404:
                raise ConnectorError("SHOPIFY", f"Endpoint not found: {endpoint}")
            elif response.status_code == 429:
                raise ConnectorError(
                    "SHOPIFY", "Rate limit exceeded - please retry later"
                )
            else:
                raise ConnectorError(
                    "SHOPIFY", f"API error {response.status_code}: {str(e)}"
                )
        except requests.exceptions.RequestException as e:
            raise ConnectorError("SHOPIFY", f"Request failed: {str(e)}")

    def test_connection(self) -> ConnectionTestResult:
        """Test connection to Shopify API using shop.json endpoint."""
        try:
            shop_data = self.get("shop.json")
            shop_info = shop_data.get("shop", {})
            shop_name = shop_info.get("name", "Unknown Shop")

            return ConnectionTestResult(
                success=True,
                message=f"Successfully connected to Shopify store: {shop_name}",
            )
        except ConnectorError as e:
            return ConnectionTestResult(success=False, message=str(e))
        except Exception as e:
            return ConnectionTestResult(
                success=False, message=f"Connection test failed: {str(e)}"
            )


@register_connector("SHOPIFY")
class ShopifyConnector(Connector):
    """Shopify connector for SQLFlow with SME-optimized e-commerce analytics."""

    def __init__(self):
        """Initialize Shopify connector with automatic resilience patterns."""
        super().__init__()
        self.name = "SHOPIFY"

        # Initialize parameter validator
        self._parameter_validator = ShopifyParameterValidator()

        # Initialize with resilience patterns
        self.resilience_manager = ResilienceManager(
            config=ResilienceConfig(), name="SHOPIFY"
        )

        # Apply Shopify-specific resilience configuration
        # Note: API_RESILIENCE_CONFIG is a predefined config we can use

        # Initialize client holder
        self.shopify_client: Optional[ShopifyAPIClient] = None

        logger.info("ShopifyConnector initialized with automatic resilience patterns")

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the Shopify connector with validated parameters."""
        try:
            # Validate parameters
            validated_params = self._parameter_validator.validate(params)

            # Store parameters
            self.params = validated_params

            # Initialize Shopify API client
            self._initialize_shopify_client()

            # Update connector state
            self.state = ConnectorState.CONFIGURED

            logger.info(
                f"Shopify connector configured for shop '{validated_params['shop_domain']}' "
                f"with sync_mode '{validated_params['sync_mode']}'"
            )

        except Exception as e:
            self.state = ConnectorState.ERROR
            logger.error(f"Failed to configure Shopify connector: {str(e)}")
            raise

    def _initialize_shopify_client(self) -> None:
        """Initialize the Shopify API client."""
        self.shopify_client = ShopifyAPIClient(
            shop_domain=self.params["shop_domain"],
            access_token=self.params["access_token"],
        )
        logger.info("Shopify API client initialized successfully")

    def test_connection(self) -> ConnectionTestResult:
        """Test connection to Shopify API."""
        if self.state != ConnectorState.CONFIGURED:
            return ConnectionTestResult(
                success=False, message="Connector not configured"
            )

        try:
            # Test connection by fetching shop information
            shop_url = (
                f"https://{self.params['shop_domain']}/admin/api/2023-10/shop.json"
            )
            headers = {
                "X-Shopify-Access-Token": self.params["access_token"],
                "Content-Type": "application/json",
            }

            # Use the resilience manager directly with requests
            response = requests.get(shop_url, headers=headers, timeout=30)
            response.raise_for_status()

            shop_data = response.json()
            shop_name = shop_data.get("shop", {}).get("name", "Unknown")

            return ConnectionTestResult(
                success=True,
                message=f"Successfully connected to Shopify store: {shop_name}",
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                return ConnectionTestResult(
                    success=False,
                    message="Authentication failed - invalid access token",
                )
            elif e.response.status_code == 403:
                return ConnectionTestResult(
                    success=False, message="Access forbidden - insufficient permissions"
                )
            else:
                return ConnectionTestResult(
                    success=False,
                    message=f"HTTP error {e.response.status_code}: {str(e)}",
                )
        except Exception as e:
            return ConnectionTestResult(
                success=False, message=f"Connection failed: {str(e)}"
            )

    def discover(self) -> List[str]:
        """Discover available Shopify objects for SME analytics."""
        self.validate_state(ConnectorState.CONFIGURED)

        # Return SME-focused Shopify objects in priority order
        return [
            "orders",  # Tier 1: Core business metrics
            "customers",  # Tier 2: Customer analytics
            "products",  # Tier 3: Product performance
            "collections",  # Additional: Product organization
            "transactions",  # Additional: Payment details
        ]

    def supports_incremental(self) -> bool:
        """Return True as Shopify connector supports incremental loading."""
        return True

    def get_schema(self, stream: str) -> Optional[Schema]:
        """Get schema for the specified Shopify object."""
        self.validate_state(ConnectorState.CONFIGURED)

        if stream == "orders":
            # SME-optimized flattened orders schema
            if self.params.get("flatten_line_items", True):
                return Schema(
                    pa.schema(
                        [
                            # Order identifiers
                            pa.field("order_id", pa.int64()),
                            pa.field("order_number", pa.string()),
                            pa.field("order_name", pa.string()),
                            # Customer information
                            pa.field("customer_id", pa.int64()),
                            pa.field("customer_email", pa.string()),
                            pa.field("customer_first_name", pa.string()),
                            pa.field("customer_last_name", pa.string()),
                            # Financial data (enhanced for SME requirements)
                            pa.field(
                                "total_price", pa.string()
                            ),  # Keep as string for precision
                            pa.field("subtotal_price", pa.string()),
                            pa.field("total_tax", pa.string()),
                            pa.field("total_discounts", pa.string()),
                            pa.field(
                                "total_refunded", pa.string()
                            ),  # New: refund tracking
                            pa.field("currency", pa.string()),
                            # Order status
                            pa.field("financial_status", pa.string()),
                            pa.field("fulfillment_status", pa.string()),
                            pa.field("cancelled_at", pa.timestamp("s")),
                            # Timestamps
                            pa.field("created_at", pa.timestamp("s")),
                            pa.field("updated_at", pa.timestamp("s")),
                            pa.field("processed_at", pa.timestamp("s")),
                            # Geographic data (enhanced for SME regional analysis)
                            pa.field("billing_country", pa.string()),
                            pa.field("billing_province", pa.string()),
                            pa.field("billing_city", pa.string()),
                            pa.field("billing_zip", pa.string()),
                            pa.field("shipping_country", pa.string()),
                            pa.field("shipping_province", pa.string()),
                            pa.field("shipping_city", pa.string()),
                            pa.field("shipping_zip", pa.string()),
                            # Fulfillment tracking (SME operational metrics)
                            pa.field("tracking_company", pa.string()),
                            pa.field("tracking_number", pa.string()),
                            pa.field("tracking_url", pa.string()),
                            pa.field("fulfillment_created_at", pa.timestamp("s")),
                            pa.field("fulfillment_updated_at", pa.timestamp("s")),
                            # Line item details (flattened for SME analytics)
                            pa.field("line_item_id", pa.int64()),
                            pa.field("product_id", pa.int64()),
                            pa.field("variant_id", pa.int64()),
                            pa.field("sku", pa.string()),
                            pa.field("product_title", pa.string()),
                            pa.field("variant_title", pa.string()),
                            pa.field("vendor", pa.string()),
                            pa.field("quantity", pa.int64()),
                            pa.field("line_item_price", pa.string()),
                            pa.field("line_total", pa.string()),
                            # Enhanced line item analytics for SME
                            pa.field("line_item_grams", pa.int64()),
                            pa.field("line_item_requires_shipping", pa.bool_()),
                            pa.field("line_item_taxable", pa.bool_()),
                            pa.field("line_item_fulfillment_service", pa.string()),
                        ]
                    )
                )
            else:
                # Non-flattened orders schema
                return Schema(
                    pa.schema(
                        [
                            pa.field("id", pa.int64()),
                            pa.field("order_number", pa.string()),
                            pa.field("email", pa.string()),
                            pa.field("created_at", pa.timestamp("s")),
                            pa.field("updated_at", pa.timestamp("s")),
                            pa.field("total_price", pa.string()),
                            pa.field("currency", pa.string()),
                            pa.field("financial_status", pa.string()),
                            pa.field("fulfillment_status", pa.string()),
                            pa.field("line_items", pa.string()),  # JSON array as string
                        ]
                    )
                )

        elif stream == "customers":
            return Schema(
                pa.schema(
                    [
                        pa.field("id", pa.int64()),
                        pa.field("email", pa.string()),
                        pa.field("first_name", pa.string()),
                        pa.field("last_name", pa.string()),
                        pa.field("orders_count", pa.int32()),
                        pa.field("total_spent", pa.string()),
                        pa.field("created_at", pa.timestamp("s")),
                        pa.field("updated_at", pa.timestamp("s")),
                        pa.field("state", pa.string()),
                    ]
                )
            )

        elif stream == "products":
            return Schema(
                pa.schema(
                    [
                        pa.field("id", pa.int64()),
                        pa.field("title", pa.string()),
                        pa.field("vendor", pa.string()),
                        pa.field("product_type", pa.string()),
                        pa.field("handle", pa.string()),
                        pa.field("status", pa.string()),
                        pa.field("created_at", pa.timestamp("s")),
                        pa.field("updated_at", pa.timestamp("s")),
                        pa.field("published_at", pa.timestamp("s")),
                    ]
                )
            )

        return None

    def read(self, stream: str) -> Iterator[DataChunk]:
        """Read data from Shopify object in full refresh mode."""
        self.validate_state(ConnectorState.CONFIGURED)

        if stream == "orders":
            yield from self._read_orders(sync_mode="full_refresh")
        elif stream == "customers":
            yield from self._read_customers(sync_mode="full_refresh")
        elif stream == "products":
            yield from self._read_products(sync_mode="full_refresh")
        else:
            logger.warning(f"Unknown stream: {stream}, returning empty data")
            yield DataChunk(pd.DataFrame())

    def read_incremental(
        self, stream: str, cursor_field: str, cursor_value: Optional[Any] = None
    ) -> Iterator[DataChunk]:
        """Read data from Shopify object in incremental mode."""
        self.validate_state(ConnectorState.CONFIGURED)

        # Apply lookback window if cursor_value provided
        if cursor_value and isinstance(cursor_value, str):
            try:
                cursor_datetime = datetime.fromisoformat(
                    cursor_value.replace("Z", "+00:00")
                )
                adjusted_cursor = self._apply_lookback_window(cursor_datetime)
                adjusted_cursor_str = adjusted_cursor.isoformat() + "Z"

                logger.info(
                    f"Incremental loading from {cursor_value} adjusted to {adjusted_cursor_str} with lookback"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to apply lookback window: {e}, using original cursor"
                )
                adjusted_cursor_str = cursor_value
        else:
            adjusted_cursor_str = cursor_value

        if stream == "orders":
            yield from self._read_orders(
                sync_mode="incremental", cursor_value=adjusted_cursor_str
            )
        elif stream == "customers":
            yield from self._read_customers(
                sync_mode="incremental", cursor_value=adjusted_cursor_str
            )
        elif stream == "products":
            yield from self._read_products(
                sync_mode="incremental", cursor_value=adjusted_cursor_str
            )
        else:
            logger.warning(f"Unknown stream: {stream}, returning empty data")
            yield DataChunk(pd.DataFrame())

    def _read_orders(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Iterator[DataChunk]:
        """Read orders data from Shopify with SME-optimized processing."""
        try:
            yield from self._paginate_orders(sync_mode, cursor_value)
        except Exception as e:
            logger.error(f"Error reading orders: {str(e)}")
            # Return empty chunk on error for graceful handling
            yield DataChunk(pd.DataFrame())

    def _paginate_orders(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Iterator[DataChunk]:
        """Handle pagination through orders with resilient API calls."""
        api_params = self._build_orders_api_params(sync_mode, cursor_value)
        batch_size = self.params.get("batch_size", 250)  # Shopify max
        orders_processed = 0

        while True:
            logger.info(f"Fetching orders batch, processed so far: {orders_processed}")

            chunk = self._fetch_orders_batch(api_params)
            if chunk is None:
                break

            orders = chunk.get("orders", [])

            if not orders:
                logger.info("No more orders to process")
                break

            # Process and yield data
            processed_orders = self._process_orders_batch(orders)
            if processed_orders:
                df = pd.DataFrame(processed_orders)
                df = self._convert_order_data_types(df)
                orders_processed += len(processed_orders)
                logger.info(
                    f"Processed {len(processed_orders)} orders, total: {orders_processed}"
                )
                yield DataChunk(df)

            # Check for more pages and update pagination
            if not self._should_continue_pagination(orders, batch_size, api_params):
                break

    def _fetch_orders_batch(
        self, api_params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single batch of orders with error handling."""
        try:
            return self._make_shopify_api_call("orders.json", api_params)
        except Exception as e:
            if self._should_retry_on_error(e):
                time.sleep(1)
                return None  # Signal to continue pagination loop
            else:
                raise  # Re-raise non-retryable errors

    def _should_retry_on_error(self, error: Exception) -> bool:
        """Determine if error should trigger a retry."""
        error_str = str(error).lower()
        return "rate" in error_str or "429" in error_str

    def _should_continue_pagination(
        self, orders: List[Dict], batch_size: int, api_params: Dict[str, Any]
    ) -> bool:
        """Determine if pagination should continue and update parameters."""
        if len(orders) < batch_size:
            return False

        if orders:
            last_order = orders[-1]
            api_params["since_id"] = last_order.get("id")

        return True

    def _read_customers(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Iterator[DataChunk]:
        """Read customers data from Shopify."""
        try:
            api_params = self._build_customers_api_params(sync_mode, cursor_value)

            response = self._make_shopify_api_call("customers.json", api_params)
            customers = response.get("customers", [])

            if customers:
                processed_customers = self._process_customers_batch(customers)
                df = pd.DataFrame(processed_customers)
                df = self._convert_customer_data_types(df)
                yield DataChunk(df)
            else:
                yield DataChunk(pd.DataFrame())

        except Exception as e:
            logger.error(f"Error reading customers: {str(e)}")
            yield DataChunk(pd.DataFrame())

    def _read_products(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Iterator[DataChunk]:
        """Read products data from Shopify."""
        try:
            api_params = self._build_products_api_params(sync_mode, cursor_value)

            response = self._make_shopify_api_call("products.json", api_params)
            products = response.get("products", [])

            if products:
                processed_products = self._process_products_batch(products)
                df = pd.DataFrame(processed_products)
                df = self._convert_product_data_types(df)
                yield DataChunk(df)
            else:
                yield DataChunk(pd.DataFrame())

        except Exception as e:
            logger.error(f"Error reading products: {str(e)}")
            yield DataChunk(pd.DataFrame())

    def _make_shopify_api_call(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make resilient API call to Shopify."""
        url = f"https://{self.params['shop_domain']}/admin/api/2023-10/{endpoint}"
        headers = {
            "X-Shopify-Access-Token": self.params["access_token"],
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                raise ConnectorError(
                    "SHOPIFY", "Rate limit exceeded - please retry later"
                )
            elif e.response.status_code == 401:
                raise ConnectorError(
                    "SHOPIFY", "Authentication failed - invalid access token"
                )
            elif e.response.status_code == 403:
                raise ConnectorError(
                    "SHOPIFY", "Access forbidden - insufficient permissions"
                )
            else:
                raise ConnectorError(
                    "SHOPIFY", f"API error {e.response.status_code}: {str(e)}"
                )
        except requests.exceptions.RequestException as e:
            raise ConnectorError("SHOPIFY", f"Request failed: {str(e)}")

    def _build_orders_api_params(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build API parameters for orders endpoint."""
        params = {
            "limit": self.params.get("batch_size", 250),
            "status": "any",  # Include all statuses initially
        }

        # Add financial status filtering
        financial_filter = self.params.get("financial_status_filter")
        if financial_filter:
            if isinstance(financial_filter, list):
                params["financial_status"] = ",".join(financial_filter)
            else:
                params["financial_status"] = financial_filter

        # Add incremental filtering
        if sync_mode == "incremental" and cursor_value:
            params["updated_at_min"] = cursor_value

        # Add fulfillment status if specified
        fulfillment_filter = self.params.get("fulfillment_status_filter")
        if fulfillment_filter:
            if isinstance(fulfillment_filter, list):
                params["fulfillment_status"] = ",".join(fulfillment_filter)
            else:
                params["fulfillment_status"] = fulfillment_filter

        # Include fulfillments and refunds for SME analytics if enabled
        if self.params.get("include_fulfillments", True):
            # Note: We'll fetch fulfillments via separate API calls for each order
            # to avoid hitting the fulfillments_limit in the orders endpoint
            pass

        if self.params.get("include_refunds", True):
            # Note: We'll fetch refunds via separate API calls for each order
            # to get detailed refund information
            pass

        return params

    def _build_customers_api_params(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build API parameters for customers endpoint."""
        params = {
            "limit": self.params.get("batch_size", 250),
        }

        if sync_mode == "incremental" and cursor_value:
            params["updated_at_min"] = cursor_value

        return params

    def _build_products_api_params(
        self, sync_mode: str, cursor_value: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build API parameters for products endpoint."""
        params = {
            "limit": self.params.get("batch_size", 250),
        }

        if sync_mode == "incremental" and cursor_value:
            params["updated_at_min"] = cursor_value

        return params

    def _flatten_order_line_items(
        self, order_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Flatten order line items into separate rows (placeholder for testing)."""
        if not self.params.get("flatten_line_items", True):
            return [order_data]

        line_items = order_data.get("line_items", [])
        if not line_items:
            return [order_data]

        flattened_orders = []
        for line_item in line_items:
            flattened_order = order_data.copy()
            # Remove line_items array and add individual line item data
            flattened_order.pop("line_items", None)
            flattened_order["line_item_id"] = line_item.get("id")
            flattened_order["product_id"] = line_item.get("product_id")
            flattened_order["variant_id"] = line_item.get("variant_id")
            flattened_order["quantity"] = line_item.get("quantity")
            flattened_order["line_item_price"] = line_item.get("price")
            # Keep order-level ID
            flattened_order["order_id"] = order_data.get("id")
            flattened_orders.append(flattened_order)

        return flattened_orders

    def _build_api_filters(self) -> Dict[str, Any]:
        """Build API filters from connector parameters."""
        filters = {}

        # Financial status filtering
        if (
            "financial_status_filter" in self.params
            and self.params["financial_status_filter"]
        ):
            # Shopify API expects comma-separated string for multiple values
            status_list = self.params["financial_status_filter"]
            if isinstance(status_list, list):
                filters["financial_status"] = ",".join(status_list)
            else:
                filters["financial_status"] = status_list

        # Fulfillment status filtering
        if (
            "fulfillment_status_filter" in self.params
            and self.params["fulfillment_status_filter"]
        ):
            status_list = self.params["fulfillment_status_filter"]
            if isinstance(status_list, list):
                filters["fulfillment_status"] = ",".join(status_list)
            else:
                filters["fulfillment_status"] = status_list

        return filters

    def _apply_lookback_window(self, cursor_value: datetime) -> datetime:
        """Apply lookback window to cursor value."""
        if not self.params:
            return cursor_value

        lookback_window = self.params.get("lookback_window", "P7D")

        # Simple parsing for testing - supports format like P7D (7 days)
        if lookback_window.startswith("P") and lookback_window.endswith("D"):
            days = int(lookback_window[1:-1])
            return cursor_value - timedelta(days=days)

        return cursor_value

    def _process_orders_batch(
        self, orders: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process orders batch with SME-optimized transformations."""
        processed_orders = []

        for order in orders:
            if self.params.get("flatten_line_items", True):
                # Flatten line items into separate rows (SME preferred format)
                flattened_orders = self._flatten_order_with_line_items(order)
                processed_orders.extend(flattened_orders)
            else:
                # Keep line items as JSON (enterprise format)
                processed_order = self._transform_order_data(order)
                processed_orders.append(processed_order)

        return processed_orders

    def _flatten_order_with_line_items(
        self, order: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Flatten order with line items into separate rows for SME analytics."""
        line_items = order.get("line_items", [])

        # If no line items, create one row with order data only
        if not line_items:
            return [self._create_flattened_order_row(order, None)]

        # Create one row per line item
        flattened_rows = []
        for line_item in line_items:
            row = self._create_flattened_order_row(order, line_item)
            flattened_rows.append(row)

        return flattened_rows

    def _create_flattened_order_row(
        self, order: Dict[str, Any], line_item: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create a single flattened row with order and line item data."""
        # Extract customer data
        customer = order.get("customer") or {}
        billing_address = order.get("billing_address") or {}
        shipping_address = order.get("shipping_address") or {}

        # Extract fulfillment data for SME analytics
        fulfillments = order.get("fulfillments", [])
        fulfillment_data = fulfillments[0] if fulfillments else {}

        # Extract refund data for financial accuracy
        refunds = order.get("refunds", [])
        total_refunded = sum(float(refund.get("amount", "0")) for refund in refunds)

        # Base order data
        row = {
            # Order identifiers
            "order_id": order.get("id"),
            "order_number": order.get("order_number"),
            "order_name": order.get("name"),
            # Customer information
            "customer_id": customer.get("id"),
            "customer_email": customer.get("email") or order.get("email"),
            "customer_first_name": customer.get("first_name"),
            "customer_last_name": customer.get("last_name"),
            # Financial data (enhanced for SME requirements)
            "total_price": order.get("total_price"),
            "subtotal_price": order.get("subtotal_price"),
            "total_tax": order.get("total_tax"),
            "total_discounts": order.get("total_discounts"),
            "total_refunded": str(total_refunded),
            "currency": order.get("currency"),
            # Order status
            "financial_status": order.get("financial_status"),
            "fulfillment_status": order.get("fulfillment_status"),
            "cancelled_at": order.get("cancelled_at"),
            # Timestamps
            "created_at": order.get("created_at"),
            "updated_at": order.get("updated_at"),
            "processed_at": order.get("processed_at"),
            # Geographic data (enhanced for SME regional analysis)
            "billing_country": billing_address.get("country"),
            "billing_province": billing_address.get("province"),
            "billing_city": billing_address.get("city"),
            "billing_zip": billing_address.get("zip"),
            "shipping_country": shipping_address.get("country"),
            "shipping_province": shipping_address.get("province"),
            "shipping_city": shipping_address.get("city"),
            "shipping_zip": shipping_address.get("zip"),
            # Fulfillment tracking (SME operational metrics)
            "tracking_company": fulfillment_data.get("tracking_company"),
            "tracking_number": fulfillment_data.get("tracking_number"),
            "tracking_url": fulfillment_data.get("tracking_url"),
            "fulfillment_created_at": fulfillment_data.get("created_at"),
            "fulfillment_updated_at": fulfillment_data.get("updated_at"),
        }

        # Add line item data if present
        if line_item:
            row.update(
                {
                    "line_item_id": line_item.get("id"),
                    "product_id": line_item.get("product_id"),
                    "variant_id": line_item.get("variant_id"),
                    "sku": line_item.get("sku"),
                    "product_title": line_item.get("title"),
                    "variant_title": line_item.get("variant_title"),
                    "vendor": line_item.get("vendor"),
                    "quantity": line_item.get("quantity"),
                    "line_item_price": line_item.get("price"),
                    "line_total": str(
                        float(line_item.get("price", "0"))
                        * int(line_item.get("quantity", 0))
                    ),
                    # Enhanced line item analytics for SME
                    "line_item_grams": line_item.get("grams"),
                    "line_item_requires_shipping": line_item.get("requires_shipping"),
                    "line_item_taxable": line_item.get("taxable"),
                    "line_item_fulfillment_service": line_item.get(
                        "fulfillment_service"
                    ),
                }
            )
        else:
            # Fill line item fields with None for orders without line items
            row.update(
                {
                    "line_item_id": None,
                    "product_id": None,
                    "variant_id": None,
                    "sku": None,
                    "product_title": None,
                    "variant_title": None,
                    "vendor": None,
                    "quantity": None,
                    "line_item_price": None,
                    "line_total": None,
                    "line_item_grams": None,
                    "line_item_requires_shipping": None,
                    "line_item_taxable": None,
                    "line_item_fulfillment_service": None,
                }
            )

        return row

    def _transform_order_data(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Transform order data for non-flattened format."""
        customer = order.get("customer") or {}

        return {
            "id": order.get("id"),
            "order_number": order.get("order_number"),
            "email": customer.get("email") or order.get("email"),
            "created_at": order.get("created_at"),
            "updated_at": order.get("updated_at"),
            "total_price": order.get("total_price"),
            "currency": order.get("currency"),
            "financial_status": order.get("financial_status"),
            "fulfillment_status": order.get("fulfillment_status"),
            "line_items": json.dumps(order.get("line_items", [])),
        }

    def _process_customers_batch(
        self, customers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process customers batch."""
        return [self._transform_customer_data(customer) for customer in customers]

    def _transform_customer_data(self, customer: Dict[str, Any]) -> Dict[str, Any]:
        """Transform customer data."""
        return {
            "id": customer.get("id"),
            "email": customer.get("email"),
            "first_name": customer.get("first_name"),
            "last_name": customer.get("last_name"),
            "orders_count": customer.get("orders_count"),
            "total_spent": customer.get("total_spent"),
            "created_at": customer.get("created_at"),
            "updated_at": customer.get("updated_at"),
            "state": customer.get("state"),
        }

    def _process_products_batch(
        self, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process products batch."""
        return [self._transform_product_data(product) for product in products]

    def _transform_product_data(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Transform product data."""
        return {
            "id": product.get("id"),
            "title": product.get("title"),
            "vendor": product.get("vendor"),
            "product_type": product.get("product_type"),
            "handle": product.get("handle"),
            "status": product.get("status"),
            "created_at": product.get("created_at"),
            "updated_at": product.get("updated_at"),
            "published_at": product.get("published_at"),
        }

    def _convert_order_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert order data types for optimal storage and analysis."""
        if df.empty:
            return df

        # Convert timestamps
        timestamp_cols = [
            "created_at",
            "updated_at",
            "processed_at",
            "cancelled_at",
            "fulfillment_created_at",
            "fulfillment_updated_at",
        ]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # Convert numeric columns
        numeric_cols = [
            "order_id",
            "customer_id",
            "line_item_id",
            "product_id",
            "variant_id",
            "quantity",
            "line_item_grams",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert boolean columns
        boolean_cols = [
            "line_item_requires_shipping",
            "line_item_taxable",
        ]
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool, errors="ignore")

        return df

    def _convert_customer_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert customer data types."""
        if df.empty:
            return df

        # Convert timestamps
        timestamp_cols = ["created_at", "updated_at"]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # Convert numeric columns
        numeric_cols = ["id", "orders_count"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _convert_product_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert product data types."""
        if df.empty:
            return df

        # Convert timestamps
        timestamp_cols = ["created_at", "updated_at", "published_at"]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # Convert numeric columns
        numeric_cols = ["id"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _process_datetime_cursor(self, max_value, cursor_field: str) -> Optional[str]:
        """Process datetime cursor value for Shopify API compatibility."""
        if pd.notna(max_value):
            # Handle pandas Timestamp objects
            if hasattr(max_value, "to_pydatetime"):
                # Convert pandas Timestamp to datetime
                dt_value = max_value.to_pydatetime()
            else:
                dt_value = max_value

            # Ensure timezone-aware datetime for Shopify API
            if dt_value.tzinfo is None:
                # Localize to UTC if no timezone info
                dt_value = dt_value.replace(tzinfo=timezone.utc)
            else:
                # Convert to UTC if different timezone
                dt_value = dt_value.astimezone(timezone.utc)

            # Return in Shopify API format (ISO 8601 with Z suffix)
            iso_timestamp = dt_value.isoformat().replace("+00:00", "Z")
            logger.debug(f"Extracted cursor value (timestamp): {iso_timestamp}")
            return iso_timestamp
        return None

    def _process_string_cursor(
        self, max_value: str, cursor_field: str
    ) -> Optional[str]:
        """Process string cursor value, validating and normalizing timestamp format."""
        try:
            # Parse the timestamp and ensure UTC timezone
            parsed_dt = datetime.fromisoformat(max_value.replace("Z", "+00:00"))
            # Convert to UTC and format for Shopify API
            if parsed_dt.tzinfo is None:
                # Assume UTC if no timezone info
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            else:
                # Convert to UTC
                parsed_dt = parsed_dt.astimezone(timezone.utc)
            iso_timestamp = parsed_dt.isoformat().replace("+00:00", "Z")
            logger.debug(f"Extracted cursor value (parsed string): {iso_timestamp}")
            return iso_timestamp
        except (ValueError, AttributeError) as e:
            logger.warning(
                f"Could not parse cursor value as datetime: {max_value}, error: {e}"
            )
            return max_value

    def get_cursor_value(self, chunk: DataChunk, cursor_field: str) -> Optional[Any]:
        """Extract cursor value from data chunk for watermark management.

        Enhanced for Phase 1 Day 3 - Incremental Loading Integration:
        - Proper timezone handling for Shopify API compatibility
        - Robust timestamp parsing and normalization
        - SME-friendly error handling and logging
        """
        if chunk.pandas_df.empty:
            logger.debug("Empty data chunk, no cursor value to extract")
            return None

        if cursor_field not in chunk.pandas_df.columns:
            logger.warning(
                f"Cursor field '{cursor_field}' not found in data chunk columns: {list(chunk.pandas_df.columns)}"
            )
            return None

        # Return the maximum value for cursor field (for incremental loading)
        try:
            max_value = chunk.pandas_df[cursor_field].max()

            if pd.isna(max_value):
                logger.debug(f"All values in cursor field '{cursor_field}' are null")
                return None

            # Convert timestamp to ISO format for Shopify API compatibility
            if pd.api.types.is_datetime64_any_dtype(chunk.pandas_df[cursor_field]):
                return self._process_datetime_cursor(max_value, cursor_field)

            # For string timestamps, validate and normalize format
            if isinstance(max_value, str):
                return self._process_string_cursor(max_value, cursor_field)

            # For other data types, return as-is
            logger.debug(f"Extracted cursor value (raw): {max_value}")
            return max_value

        except Exception as e:
            logger.error(
                f"Error extracting cursor value from field '{cursor_field}': {e}"
            )
            return None

    def close(self) -> None:
        """Clean up connector resources."""
        self.shopify_client = None
        logger.info("Shopify connector closed and resources cleaned up")

    def _enhance_order_with_fulfillments_and_refunds(
        self, order: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enhance order data with detailed fulfillments and refunds for SME analytics."""
        order_id = order.get("id")
        if not order_id:
            return order

        enhanced_order = order.copy()

        try:
            # Fetch detailed fulfillments if enabled
            if self.params.get("include_fulfillments", True):
                fulfillments = self._fetch_order_fulfillments(order_id)
                enhanced_order["fulfillments"] = fulfillments

            # Fetch detailed refunds if enabled
            if self.params.get("include_refunds", True):
                refunds = self._fetch_order_refunds(order_id)
                enhanced_order["refunds"] = refunds

        except Exception as e:
            logger.warning(
                f"Failed to enhance order {order_id} with fulfillments/refunds: {e}"
            )
            # Continue with original order data if enhancement fails

        return enhanced_order

    def _fetch_order_fulfillments(self, order_id: int) -> List[Dict[str, Any]]:
        """Fetch detailed fulfillment data for an order."""
        try:
            endpoint = f"orders/{order_id}/fulfillments.json"
            response = self._make_shopify_api_call(endpoint)
            return response.get("fulfillments", [])
        except Exception as e:
            logger.warning(f"Failed to fetch fulfillments for order {order_id}: {e}")
            return []

    def _fetch_order_refunds(self, order_id: int) -> List[Dict[str, Any]]:
        """Fetch detailed refund data for an order."""
        try:
            endpoint = f"orders/{order_id}/refunds.json"
            response = self._make_shopify_api_call(endpoint)
            return response.get("refunds", [])
        except Exception as e:
            logger.warning(f"Failed to fetch refunds for order {order_id}: {e}")
            return []
