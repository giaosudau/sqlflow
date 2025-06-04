"""Integration tests for Shopify connector with realistic API patterns.

This test suite implements Phase 1, Day 5: Real Shopify Testing requirements:
- Test with realistic test data structures
- Validate multiple store configurations (small, medium, large)
- Test data accuracy and completeness
- Validate error handling with real API patterns
"""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.shopify_connector import ShopifyConnector
from sqlflow.core.errors import ConnectorError

# Load test fixtures
TEST_FIXTURES_PATH = (
    Path(__file__).parent.parent.parent / "fixtures" / "shopify_test_data.json"
)


@pytest.fixture
def shopify_test_data():
    """Load Shopify test data fixtures."""
    if not TEST_FIXTURES_PATH.exists():
        pytest.skip(f"Test fixtures not found at {TEST_FIXTURES_PATH}")

    with open(TEST_FIXTURES_PATH, "r") as f:
        return json.load(f)


@pytest.fixture
def small_sme_store_data(shopify_test_data):
    """Get small SME store test data."""
    return shopify_test_data["shopify_test_stores"]["small_sme"]


@pytest.fixture
def medium_sme_store_data(shopify_test_data):
    """Get medium SME store test data."""
    return shopify_test_data["shopify_test_stores"]["medium_sme"]


@pytest.fixture
def edge_case_data(shopify_test_data):
    """Get edge case test scenarios."""
    return shopify_test_data["shopify_test_stores"]["edge_cases"]


@pytest.fixture
def api_error_scenarios(shopify_test_data):
    """Get API error test scenarios."""
    return shopify_test_data["api_error_scenarios"]


class TestShopifyIntegrationBasic:
    """Basic integration tests with realistic data patterns."""

    def setup_method(self):
        """Set up test environment."""
        self.connector = ShopifyConnector()
        self.valid_params = {
            "shop_domain": "test-store.myshopify.com",
            "access_token": "shpat_test1234567890abcdef1234567890abcdef",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "flatten_line_items": True,
        }

    @patch("requests.get")
    def test_connection_with_realistic_shop_response(
        self, mock_get, small_sme_store_data
    ):
        """Test connection using realistic shop data."""
        # Mock shop.json response with realistic data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"shop": small_sme_store_data["shop_info"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.valid_params)
        result = self.connector.test_connection()

        assert result.success is True
        assert small_sme_store_data["shop_info"]["name"] in result.message

        # Verify correct API endpoint was called
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert "shop.json" in call_url
        assert self.valid_params["shop_domain"] in call_url

    @patch("requests.get")
    def test_orders_extraction_small_sme_store(self, mock_get, small_sme_store_data):
        """Test orders extraction with small SME store data."""
        # Mock orders API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": small_sme_store_data["orders"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.valid_params)
        chunks = list(self.connector.read("orders"))

        assert len(chunks) == 1
        df = chunks[0].pandas_df

        # Verify flattened structure (2 orders with multiple line items)
        expected_rows = sum(
            len(order["line_items"]) for order in small_sme_store_data["orders"]
        )
        assert len(df) == expected_rows

        # Verify first order data
        first_order = small_sme_store_data["orders"][0]
        first_row = df[df["order_id"] == first_order["id"]].iloc[0]

        assert first_row["order_number"] == first_order["order_number"]
        assert first_row["customer_email"] == first_order["customer"]["email"]
        assert first_row["total_price"] == first_order["total_price"]
        assert first_row["billing_country"] == first_order["billing_address"]["country"]
        assert (
            first_row["tracking_company"]
            == first_order["fulfillments"][0]["tracking_company"]
        )

    @patch("requests.get")
    def test_orders_extraction_with_refunds(self, mock_get, small_sme_store_data):
        """Test orders extraction handling refunds correctly."""
        # Mock orders API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": small_sme_store_data["orders"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.valid_params)
        chunks = list(self.connector.read("orders"))
        df = chunks[0].pandas_df

        # Find order with refund (order 5002)
        refund_order = df[df["order_id"] == 5002].iloc[0]
        assert refund_order["total_refunded"] == "10.0"  # From test data refund

    @patch("requests.get")
    def test_customers_extraction(self, mock_get, small_sme_store_data):
        """Test customers extraction with realistic data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "customers": small_sme_store_data["customers"]
        }
        mock_get.return_value = mock_response

        self.connector.configure(self.valid_params)
        chunks = list(self.connector.read("customers"))
        df = chunks[0].pandas_df

        assert len(df) == len(small_sme_store_data["customers"])

        # Verify customer data structure
        first_customer = small_sme_store_data["customers"][0]
        first_row = df[df["id"] == first_customer["id"]].iloc[0]

        assert first_row["email"] == first_customer["email"]
        assert first_row["first_name"] == first_customer["first_name"]
        assert first_row["orders_count"] == first_customer["orders_count"]
        assert first_row["total_spent"] == first_customer["total_spent"]

    @patch("requests.get")
    def test_products_extraction(self, mock_get, small_sme_store_data):
        """Test products extraction with realistic data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"products": small_sme_store_data["products"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.valid_params)
        chunks = list(self.connector.read("products"))
        df = chunks[0].pandas_df

        assert len(df) == len(small_sme_store_data["products"])

        # Verify product data structure
        first_product = small_sme_store_data["products"][0]
        first_row = df[df["id"] == first_product["id"]].iloc[0]

        assert first_row["title"] == first_product["title"]
        assert first_row["vendor"] == first_product["vendor"]
        assert first_row["product_type"] == first_product["product_type"]
        assert first_row["status"] == first_product["status"]


class TestShopifyStoreConfigurations:
    """Test different store size configurations for performance validation."""

    def setup_method(self):
        """Set up test environment."""
        self.connector = ShopifyConnector()
        self.base_params = {
            "shop_domain": "test-store.myshopify.com",
            "access_token": "shpat_test1234567890abcdef1234567890abcdef",
            "sync_mode": "full_refresh",
            "flatten_line_items": True,
        }

    @patch("requests.get")
    def test_small_sme_store_performance(self, mock_get, small_sme_store_data):
        """Test small SME store (100-500 orders) performance."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": small_sme_store_data["orders"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)

        # Time the extraction
        import time

        start_time = time.time()
        chunks = list(self.connector.read("orders"))
        extraction_time = time.time() - start_time

        # Small store should process quickly
        assert extraction_time < 5.0  # Should complete in under 5 seconds
        assert len(chunks) == 1

        df = chunks[0].pandas_df
        assert not df.empty

        # Verify memory usage is reasonable for small datasets
        memory_usage = df.memory_usage(deep=True).sum()
        assert memory_usage < 1024 * 1024  # Less than 1MB for small dataset

    @patch("requests.get")
    def test_medium_sme_store_handling(self, mock_get, medium_sme_store_data):
        """Test medium SME store (1k-5k orders) handling."""
        # Simulate larger dataset by repeating sample orders with unique IDs
        sample_orders = medium_sme_store_data["sample_orders"]
        large_order_set = []

        # Create 100 unique orders from template
        for i in range(100):
            order = json.loads(json.dumps(sample_orders[0]))  # Deep copy

            # Replace template placeholders with unique values
            order["id"] = 6000 + i
            order["order_number"] = f"200{i:03d}"
            order["name"] = f"#200{i:03d}"
            order["customer"]["id"] = 8000 + i

            # Update line item IDs to be unique
            for j, line_item in enumerate(order["line_items"]):
                line_item["id"] = 9000 + (i * 10) + j

            # Update fulfillment ID
            if order["fulfillments"]:
                order["fulfillments"][0]["id"] = 7000 + i

            large_order_set.append(order)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": large_order_set}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)
        chunks = list(self.connector.read("orders"))

        assert len(chunks) == 1
        df = chunks[0].pandas_df

        # Verify we can handle larger datasets
        assert len(df) >= 100  # At least 100 rows

        # Verify data integrity with larger datasets
        unique_orders = df["order_id"].nunique()
        assert unique_orders >= 100  # Multiple unique orders

    def test_incremental_loading_configuration(self):
        """Test incremental loading configuration for different store sizes."""
        # Small store - frequent incremental loads
        small_params = self.base_params.copy()
        small_params.update(
            {
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
                "lookback_window": "P1D",  # 1 day lookback for small stores
            }
        )

        self.connector.configure(small_params)
        assert self.connector.params["sync_mode"] == "incremental"
        assert self.connector.params["lookback_window"] == "P1D"

        # Large store - longer lookback window
        large_params = self.base_params.copy()
        large_params.update(
            {
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
                "lookback_window": "P7D",  # 7 day lookback for large stores
                "batch_size": 100,  # Smaller batches for large stores
            }
        )

        self.connector.configure(large_params)
        assert self.connector.params["lookback_window"] == "P7D"
        assert self.connector.params["batch_size"] == 100


class TestShopifyEdgeCases:
    """Test edge cases and error scenarios."""

    def setup_method(self):
        """Set up test environment."""
        self.connector = ShopifyConnector()
        self.base_params = {
            "shop_domain": "test-store.myshopify.com",
            "access_token": "shpat_test1234567890abcdef1234567890abcdef",
            "sync_mode": "full_refresh",
            "flatten_line_items": True,
        }

    @patch("requests.get")
    def test_order_without_line_items(self, mock_get, edge_case_data):
        """Test handling orders without line items."""
        edge_order = next(
            scenario["order"]
            for scenario in edge_case_data["test_scenarios"]
            if scenario["scenario"] == "order_without_line_items"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": [edge_order]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)
        chunks = list(self.connector.read("orders"))
        df = chunks[0].pandas_df

        # Should create one row with null line item data
        assert len(df) == 1
        row = df.iloc[0]
        assert row["order_id"] == edge_order["id"]
        assert pd.isna(row["line_item_id"])
        assert pd.isna(row["product_id"])

    @patch("requests.get")
    def test_order_with_missing_customer(self, mock_get, edge_case_data):
        """Test handling orders with missing customer data."""
        edge_order = next(
            scenario["order"]
            for scenario in edge_case_data["test_scenarios"]
            if scenario["scenario"] == "order_with_missing_customer"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": [edge_order]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)
        chunks = list(self.connector.read("orders"))
        df = chunks[0].pandas_df

        # Should handle missing customer gracefully
        assert len(df) == 1
        row = df.iloc[0]
        assert row["order_id"] == edge_order["id"]
        assert pd.isna(row["customer_id"])
        assert pd.isna(row["customer_email"])

    @patch("requests.get")
    def test_cancelled_order_with_refund(self, mock_get, edge_case_data):
        """Test handling cancelled orders with refunds."""
        edge_order = next(
            scenario["order"]
            for scenario in edge_case_data["test_scenarios"]
            if scenario["scenario"] == "cancelled_order_with_refund"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": [edge_order]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)
        chunks = list(self.connector.read("orders"))
        df = chunks[0].pandas_df

        # Should handle cancelled order with refunds
        assert len(df) == 1
        row = df.iloc[0]
        assert row["order_id"] == edge_order["id"]
        assert row["financial_status"] == "refunded"
        assert row["total_refunded"] == "100.0"  # From test data
        assert not pd.isna(row["cancelled_at"])

    @patch("requests.get")
    def test_api_rate_limit_handling(self, mock_get, api_error_scenarios):
        """Test handling of API rate limit errors."""
        rate_limit_response = api_error_scenarios["rate_limit_response"]

        mock_response = Mock()
        mock_response.status_code = rate_limit_response["status_code"]
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)

        with pytest.raises(ConnectorError) as exc_info:
            list(self.connector.read("orders"))

        assert "rate limit" in str(exc_info.value).lower()

    @patch("requests.get")
    def test_api_authentication_error(self, mock_get, api_error_scenarios):
        """Test handling of authentication errors."""
        auth_error_response = api_error_scenarios["unauthorized_response"]

        mock_response = Mock()
        mock_response.status_code = auth_error_response["status_code"]
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)

        with pytest.raises(ConnectorError) as exc_info:
            list(self.connector.read("orders"))

        assert "authentication" in str(exc_info.value).lower()

    @patch("requests.get")
    def test_api_server_error_handling(self, mock_get, api_error_scenarios):
        """Test handling of server errors."""
        server_error_response = api_error_scenarios["server_error_response"]

        mock_response = Mock()
        mock_response.status_code = server_error_response["status_code"]
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)

        with pytest.raises(ConnectorError) as exc_info:
            list(self.connector.read("orders"))

        assert "500" in str(exc_info.value)

    def test_schema_change_detection(self, small_sme_store_data):
        """Test schema change detection with modified data."""
        self.connector.configure(self.base_params)

        # Create test data with missing and extra fields
        test_data = pd.DataFrame(
            {
                "order_id": [1, 2],
                "order_number": ["1001", "1002"],
                # Missing: customer_email, total_price, currency
                "extra_field": ["extra1", "extra2"],  # New field
                "another_new_field": ["new1", "new2"],  # Another new field
            }
        )

        # Test schema change detection
        changes = self.connector._detect_schema_changes("orders", test_data)

        # Should detect missing and new fields
        assert len(changes) > 0
        changes_str = " ".join(changes)
        assert "Missing fields:" in changes_str
        assert "New fields:" in changes_str

    def test_schema_change_handling(self, small_sme_store_data):
        """Test schema change handling and adaptation."""
        self.connector.configure(self.base_params)

        # Create test data with missing fields
        test_data = pd.DataFrame(
            {
                "order_id": [1, 2],
                "order_number": ["1001", "1002"],
                # Missing expected fields that should be added
            }
        )

        # Detect changes
        changes = self.connector._detect_schema_changes("orders", test_data)

        # Handle changes
        adapted_data = self.connector._handle_schema_changes(
            "orders", changes, test_data
        )

        # Should have more columns after adaptation
        assert len(adapted_data.columns) >= len(test_data.columns)

        # Original data should be preserved
        assert "order_id" in adapted_data.columns
        assert "order_number" in adapted_data.columns

    @patch("time.sleep")
    def test_shop_maintenance_detection(self, mock_sleep):
        """Test detection and handling of shop maintenance scenarios."""
        self.connector.configure(self.base_params)

        # Test various maintenance error messages
        maintenance_errors = [
            "Shop is under maintenance",
            "Service temporarily unavailable",
            "503 Service Unavailable",
            "502 Bad Gateway",
            "Store is closed for maintenance",
        ]

        for error_msg in maintenance_errors:
            error = Exception(error_msg)
            result = self.connector._handle_shop_maintenance(error)

            # Should detect maintenance and return True
            assert result is True

        # Verify sleep was called for maintenance scenarios
        assert mock_sleep.call_count == len(maintenance_errors)

    @patch("time.sleep")
    def test_non_maintenance_error_handling(self, mock_sleep):
        """Test that non-maintenance errors are not treated as maintenance."""
        self.connector.configure(self.base_params)

        # Test non-maintenance error messages
        non_maintenance_errors = [
            "Invalid API key",
            "Rate limit exceeded",
            "Network timeout",
            "Connection refused",
        ]

        for error_msg in non_maintenance_errors:
            error = Exception(error_msg)
            result = self.connector._handle_shop_maintenance(error)

            # Should not detect maintenance and return False
            assert result is False

        # Sleep should not be called for non-maintenance errors
        assert mock_sleep.call_count == 0


class TestShopifyDataAccuracy:
    """Test data accuracy and completeness validation."""

    def setup_method(self):
        """Set up test environment."""
        self.connector = ShopifyConnector()
        self.base_params = {
            "shop_domain": "test-store.myshopify.com",
            "access_token": "shpat_test1234567890abcdef1234567890abcdef",
            "sync_mode": "full_refresh",
            "flatten_line_items": True,
        }

    @patch("requests.get")
    def test_financial_data_accuracy(self, mock_get, small_sme_store_data):
        """Test accuracy of financial calculations."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": small_sme_store_data["orders"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)
        chunks = list(self.connector.read("orders"))
        df = chunks[0].pandas_df

        # Verify financial accuracy for first order
        first_order = small_sme_store_data["orders"][0]
        order_rows = df[df["order_id"] == first_order["id"]]

        # All rows for same order should have same financial totals
        assert all(order_rows["total_price"] == first_order["total_price"])
        assert all(order_rows["subtotal_price"] == first_order["subtotal_price"])
        assert all(order_rows["total_tax"] == first_order["total_tax"])

        # Line item calculations should be accurate
        for _, row in order_rows.iterrows():
            line_item_total = float(row["line_item_price"]) * int(row["quantity"])
            assert abs(float(row["line_total"]) - line_item_total) < 0.01

    @patch("requests.get")
    def test_data_type_consistency(self, mock_get, small_sme_store_data):
        """Test data type consistency after processing."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": small_sme_store_data["orders"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)
        chunks = list(self.connector.read("orders"))
        df = chunks[0].pandas_df

        # Verify timestamp columns are properly converted
        timestamp_columns = ["created_at", "updated_at", "fulfillment_created_at"]
        for col in timestamp_columns:
            if col in df.columns:
                assert pd.api.types.is_datetime64_any_dtype(df[col])

        # Verify numeric columns are properly converted
        numeric_columns = ["order_id", "customer_id", "line_item_id", "quantity"]
        for col in numeric_columns:
            if col in df.columns and not df[col].isna().all():
                assert pd.api.types.is_numeric_dtype(df[col])

        # Verify boolean columns are properly converted
        boolean_columns = ["line_item_requires_shipping", "line_item_taxable"]
        for col in boolean_columns:
            if col in df.columns and not df[col].isna().all():
                assert df[col].dtype == bool

    @patch("requests.get")
    def test_schema_completeness(self, mock_get, small_sme_store_data):
        """Test that all expected schema fields are present."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": small_sme_store_data["orders"]}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)

        # Test schema discovery
        schema = self.connector.get_schema("orders")
        field_names = [field.name for field in schema.arrow_schema]

        # Verify all expected SME analytics fields are present
        expected_fields = [
            "order_id",
            "order_number",
            "customer_email",
            "total_price",
            "currency",
            "financial_status",
            "line_item_id",
            "product_id",
            "sku",
            "billing_country",
            "shipping_country",
            "tracking_company",
            "tracking_number",
            "total_refunded",
        ]

        for field in expected_fields:
            assert field in field_names, f"Missing field: {field}"

    def test_incremental_cursor_extraction(self):
        """Test cursor value extraction for incremental loading."""
        # Create test data chunk
        test_data = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "updated_at": [
                    "2024-01-01T10:00:00Z",
                    "2024-01-02T15:30:00Z",
                    "2024-01-01T20:00:00Z",
                ],
            }
        )

        # Convert to proper timestamp format
        test_data["updated_at"] = pd.to_datetime(test_data["updated_at"])
        chunk = DataChunk(test_data)

        self.connector.configure(self.base_params)
        cursor_value = self.connector.get_cursor_value(chunk, "updated_at")

        # Should return the latest timestamp in ISO format
        assert cursor_value is not None
        assert "2024-01-02T15:30:00" in cursor_value
        assert cursor_value.endswith("Z")


class TestShopifyRealAPIPatterns:
    """Test realistic API interaction patterns."""

    def setup_method(self):
        """Set up test environment."""
        self.connector = ShopifyConnector()
        self.base_params = {
            "shop_domain": "test-store.myshopify.com",
            "access_token": "shpat_test1234567890abcdef1234567890abcdef",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "flatten_line_items": True,
        }

    @patch("requests.get")
    def test_api_parameter_construction(self, mock_get):
        """Test API parameter construction for different scenarios."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orders": []}
        mock_get.return_value = mock_response

        self.connector.configure(self.base_params)

        # Test incremental read
        cursor_value = "2024-01-01T00:00:00Z"
        list(self.connector.read_incremental("orders", "updated_at", cursor_value))

        # Verify API was called with correct parameters
        assert mock_get.called
        call_args = mock_get.call_args

        # Verify URL construction
        url = call_args[0][0]
        assert "orders.json" in url
        assert self.base_params["shop_domain"] in url

        # Verify headers
        headers = call_args[1]["headers"]
        assert "X-Shopify-Access-Token" in headers
        assert headers["X-Shopify-Access-Token"] == self.base_params["access_token"]

    @patch("requests.get")
    def test_pagination_handling(self, mock_get, small_sme_store_data):
        """Test pagination with multiple API calls."""
        # Simulate pagination by returning different data on subsequent calls
        orders = small_sme_store_data["orders"]

        # First call - return first order
        first_response = Mock()
        first_response.status_code = 200
        first_response.json.return_value = {"orders": [orders[0]]}

        # Second call - return second order (simulating pagination)
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {"orders": [orders[1]]}

        # Third call - empty response (end of pagination)
        empty_response = Mock()
        empty_response.status_code = 200
        empty_response.json.return_value = {"orders": []}

        mock_get.side_effect = [first_response, second_response, empty_response]

        # Configure with smaller batch size to trigger pagination
        params = self.base_params.copy()
        params["batch_size"] = 1
        self.connector.configure(params)

        chunks = list(self.connector.read("orders"))

        # Should receive data from both API calls
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        assert total_rows > 0

        # Verify multiple API calls were made
        assert mock_get.call_count >= 2

    def test_lookback_window_application(self):
        """Test lookback window calculation for incremental loading."""
        from datetime import datetime, timedelta

        self.connector.configure(self.base_params)

        # Test with 7-day lookback
        original_date = datetime(2024, 1, 8, 12, 0, 0)
        adjusted_date = self.connector._apply_lookback_window(original_date)

        # Should subtract 7 days
        expected_date = original_date - timedelta(days=7)
        assert adjusted_date == expected_date
