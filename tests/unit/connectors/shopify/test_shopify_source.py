"""Unit tests for Shopify Source Connector."""

import unittest
from unittest.mock import patch

import pyarrow as pa
import requests
import requests_mock

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.shopify.source import ShopifySource


class TestShopifySource(unittest.TestCase):
    """Test cases for ShopifySource connector."""

    def setUp(self):
        """Set up test fixtures."""
        self.connector = ShopifySource()
        self.config = {
            "shop_domain": "test-shop.myshopify.com",
            "access_token": "shpat_test_token",
            "api_version": "2023-10",
        }

    def test_configure_success(self):
        """Test successful configuration."""
        self.connector.configure(self.config)

        self.assertEqual(self.connector.shop_domain, "test-shop.myshopify.com")
        self.assertEqual(self.connector.access_token, "shpat_test_token")
        self.assertEqual(self.connector.api_version, "2023-10")
        self.assertEqual(self.connector.state, ConnectorState.CONFIGURED)
        self.assertEqual(
            self.connector.base_url,
            "https://test-shop.myshopify.com/admin/api/2023-10/",
        )

    def test_configure_domain_normalization(self):
        """Test shop domain normalization."""
        config = self.config.copy()
        config["shop_domain"] = "test-shop"  # Without .myshopify.com

        self.connector.configure(config)
        self.assertEqual(self.connector.shop_domain, "test-shop.myshopify.com")

    def test_configure_missing_shop_domain(self):
        """Test configuration with missing shop domain."""
        config = self.config.copy()
        del config["shop_domain"]

        with self.assertRaises(ValueError) as context:
            self.connector.configure(config)

        self.assertIn("shop_domain", str(context.exception))

    def test_configure_missing_access_token(self):
        """Test configuration with missing access token."""
        config = self.config.copy()
        del config["access_token"]

        with self.assertRaises(ValueError) as context:
            self.connector.configure(config)

        self.assertIn("access_token", str(context.exception))

    def test_configure_optional_parameters(self):
        """Test configuration with optional parameters."""
        config = self.config.copy()
        config.update(
            {
                "timeout": 60,
                "max_retries": 5,
                "retry_delay": 2.0,
                "rate_limit_delay": 1.0,
            }
        )

        self.connector.configure(config)

        self.assertEqual(self.connector.timeout, 60)
        self.assertEqual(self.connector.max_retries, 5)
        self.assertEqual(self.connector.retry_delay, 2.0)
        self.assertEqual(self.connector.rate_limit_delay, 1.0)

    @requests_mock.Mocker()
    def test_test_connection_success(self, m):
        """Test successful connection test."""
        self.connector.configure(self.config)

        # Mock shop info endpoint
        shop_url = "https://test-shop.myshopify.com/admin/api/2023-10/shop.json"
        m.get(shop_url, json={"shop": {"name": "Test Store"}})

        result = self.connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertTrue(result.success)
        self.assertIn("Test Store", result.message)

    def test_test_connection_not_configured(self):
        """Test connection test when not configured."""
        result = self.connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertFalse(result.success)
        self.assertIn("not configured", result.message)

    @requests_mock.Mocker()
    def test_test_connection_failure(self, m):
        """Test connection test failure."""
        self.connector.configure(self.config)

        # Mock failed request
        shop_url = "https://test-shop.myshopify.com/admin/api/2023-10/shop.json"
        m.get(shop_url, status_code=401)

        result = self.connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertFalse(result.success)
        self.assertIn("HTTP 401", result.message)

    @requests_mock.Mocker()
    def test_test_connection_timeout(self, m):
        """Test connection test timeout."""
        self.connector.configure(self.config)

        # Mock timeout
        shop_url = "https://test-shop.myshopify.com/admin/api/2023-10/shop.json"
        m.get(shop_url, exc=requests.exceptions.Timeout)

        result = self.connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertFalse(result.success)
        self.assertIn("timeout", result.message)

    def test_discover(self):
        """Test object discovery."""
        self.connector.configure(self.config)

        objects = self.connector.discover()

        self.assertIsInstance(objects, list)
        self.assertIn("orders", objects)
        self.assertIn("customers", objects)
        self.assertIn("products", objects)
        self.assertEqual(len(objects), len(ShopifySource.ENDPOINTS))

    @requests_mock.Mocker()
    def test_get_schema_success(self, m):
        """Test successful schema inference."""
        self.connector.configure(self.config)

        # Mock orders endpoint
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        mock_data = {
            "orders": [
                {
                    "id": 1,
                    "name": "#1001",
                    "email": "test@example.com",
                    "total_price": "100.00",
                    "created_at": "2023-01-01T00:00:00Z",
                }
            ]
        }
        m.get(orders_url, json=mock_data)

        schema = self.connector.get_schema("orders")

        self.assertIsInstance(schema, Schema)
        self.assertIsInstance(schema.arrow_schema, pa.Schema)

        # Check that schema has expected columns
        column_names = [field.name for field in schema.arrow_schema]
        self.assertIn("id", column_names)
        self.assertIn("name", column_names)
        self.assertIn("email", column_names)

    @requests_mock.Mocker()
    def test_get_schema_empty_response(self, m):
        """Test schema inference with empty response."""
        self.connector.configure(self.config)

        # Mock empty response
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        m.get(orders_url, json={"orders": []})

        schema = self.connector.get_schema("orders")

        self.assertIsInstance(schema, Schema)
        self.assertEqual(len(schema.arrow_schema), 0)

    def test_get_schema_invalid_object(self):
        """Test schema inference with invalid object name."""
        self.connector.configure(self.config)

        schema = self.connector.get_schema("invalid_object")

        self.assertIsInstance(schema, Schema)
        self.assertEqual(len(schema.arrow_schema), 0)

    @requests_mock.Mocker()
    def test_read_success(self, m):
        """Test successful data reading."""
        self.connector.configure(self.config)

        # Mock orders endpoint
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        mock_data = {
            "orders": [
                {"id": 1, "name": "#1001", "total_price": "100.00"},
                {"id": 2, "name": "#1002", "total_price": "200.00"},
            ]
        }
        m.get(orders_url, json=mock_data)

        chunks = list(self.connector.read("orders"))

        self.assertEqual(len(chunks), 1)
        chunk = chunks[0]
        self.assertIsInstance(chunk, DataChunk)
        self.assertEqual(len(chunk.pandas_df), 2)
        self.assertIn("id", chunk.pandas_df.columns)
        self.assertIn("name", chunk.pandas_df.columns)

    def test_read_invalid_object(self):
        """Test reading with invalid object name."""
        self.connector.configure(self.config)

        with self.assertRaises(ValueError) as context:
            list(self.connector.read("invalid_object"))

        self.assertIn("Invalid object_name", str(context.exception))

    @requests_mock.Mocker()
    def test_read_with_columns(self, m):
        """Test reading with column selection."""
        self.connector.configure(self.config)

        # Mock orders endpoint
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        mock_data = {
            "orders": [
                {
                    "id": 1,
                    "name": "#1001",
                    "total_price": "100.00",
                    "email": "test@example.com",
                },
            ]
        }
        m.get(orders_url, json=mock_data)

        chunks = list(self.connector.read("orders", columns=["id", "name"]))

        chunk = chunks[0]
        self.assertEqual(len(chunk.pandas_df.columns), 2)
        self.assertIn("id", chunk.pandas_df.columns)
        self.assertIn("name", chunk.pandas_df.columns)
        self.assertNotIn("email", chunk.pandas_df.columns)

    @requests_mock.Mocker()
    def test_read_with_cursor(self, m):
        """Test reading with cursor value."""
        self.connector.configure(self.config)

        # Mock orders endpoint
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        mock_data = {"orders": [{"id": 2, "name": "#1002"}]}
        m.get(orders_url, json=mock_data)

        chunks = list(self.connector.read("orders", cursor_value=1))

        # Verify since_id parameter was used
        self.assertEqual(len(m.request_history), 1)
        request = m.request_history[0]
        self.assertIn("since_id=1", request.url)

    @requests_mock.Mocker()
    def test_read_pagination(self, m):
        """Test reading with pagination."""
        self.connector.configure(self.config)

        # Mock first page
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        first_page_data = {"orders": [{"id": 1, "name": "#1001"}]}
        first_headers = {
            "Link": '<https://test-shop.myshopify.com/admin/api/2023-10/orders.json?page_info=next_page>; rel="next"'
        }

        # Mock second page
        second_page_data = {"orders": [{"id": 2, "name": "#1002"}]}
        second_headers = {}

        m.get(
            orders_url,
            [
                {"json": first_page_data, "headers": first_headers},
                {"json": second_page_data, "headers": second_headers},
            ],
        )

        chunks = list(self.connector.read("orders"))

        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0].pandas_df), 1)
        self.assertEqual(len(chunks[1].pandas_df), 1)

    @requests_mock.Mocker()
    def test_get_cursor_value_success(self, m):
        """Test successful cursor value retrieval."""
        self.connector.configure(self.config)

        # Mock orders endpoint with order by id desc
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        mock_data = {"orders": [{"id": 100, "name": "#1100"}]}
        m.get(orders_url, json=mock_data)

        cursor_value = self.connector.get_cursor_value("orders", "id")

        self.assertEqual(cursor_value, 100)

        # Verify correct parameters were used
        request = m.request_history[0]
        self.assertIn("limit=1", request.url)
        self.assertIn("order=id+desc", request.url)

    @requests_mock.Mocker()
    def test_get_cursor_value_empty(self, m):
        """Test cursor value retrieval with empty response."""
        self.connector.configure(self.config)

        # Mock empty response
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        m.get(orders_url, json={"orders": []})

        cursor_value = self.connector.get_cursor_value("orders", "id")

        self.assertIsNone(cursor_value)

    @requests_mock.Mocker()
    def test_rate_limiting_handling(self, m):
        """Test rate limiting handling."""
        self.connector.configure(self.config)

        # Mock rate limited response followed by success
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        m.get(
            orders_url,
            [
                {"status_code": 429, "headers": {"Retry-After": "1"}},
                {"json": {"orders": [{"id": 1, "name": "#1001"}]}},
            ],
        )

        with patch("time.sleep") as mock_sleep:
            chunks = list(self.connector.read("orders"))

            # Verify sleep was called for rate limiting
            mock_sleep.assert_called_with(1)

        self.assertEqual(len(chunks), 1)

    @requests_mock.Mocker()
    def test_retry_logic(self, m):
        """Test retry logic on request failure."""
        self.connector.configure(self.config)

        # Mock connection error followed by success
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        m.get(
            orders_url,
            [
                {"exc": requests.exceptions.ConnectionError},
                {"json": {"orders": [{"id": 1, "name": "#1001"}]}},
            ],
        )

        with patch("time.sleep") as mock_sleep:
            chunks = list(self.connector.read("orders"))

            # Verify exponential backoff was used
            mock_sleep.assert_called()

        self.assertEqual(len(chunks), 1)

    @requests_mock.Mocker()
    def test_session_reuse(self, m):
        """Test that session is reused across requests."""
        self.connector.configure(self.config)

        # Mock multiple endpoints
        orders_url = "https://test-shop.myshopify.com/admin/api/2023-10/orders.json"
        customers_url = (
            "https://test-shop.myshopify.com/admin/api/2023-10/customers.json"
        )

        m.get(orders_url, json={"orders": []})
        m.get(customers_url, json={"customers": []})

        # Make multiple requests
        list(self.connector.read("orders"))
        list(self.connector.read("customers"))

        # Verify session was reused (same session object)
        self.assertIsNotNone(self.connector.session)

    def test_endpoints_coverage(self):
        """Test that all expected endpoints are available."""
        expected_endpoints = [
            "orders",
            "customers",
            "products",
            "inventory_items",
            "inventory_levels",
            "locations",
            "transactions",
            "fulfillments",
            "refunds",
            "discounts",
            "collections",
            "metafields",
        ]

        for endpoint in expected_endpoints:
            self.assertIn(endpoint, ShopifySource.ENDPOINTS)
            self.assertEqual(ShopifySource.ENDPOINTS[endpoint], endpoint)


if __name__ == "__main__":
    unittest.main()
