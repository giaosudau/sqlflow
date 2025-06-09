import pandas as pd
import pytest
import requests_mock

from sqlflow.connectors.shopify.source import ShopifySource


@pytest.fixture
def shopify_config():
    """Provides a basic Shopify config."""
    return {
        "shop_domain": "test-shop",
        "api_version": "2024-01",
        "access_token": "test_token",
    }


def test_shopify_source_initialization(shopify_config):
    """Test that the ShopifySource correctly constructs the REST API call."""
    source = ShopifySource(config=shopify_config)
    assert "rest" in source.config["type"]
    assert "test-shop" in source.config["url"]
    assert source.config["auth"]["key_name"] == "X-Shopify-Access-Token"


def test_shopify_read_orders(shopify_config):
    """Test reading orders from the Shopify source."""
    with requests_mock.Mocker() as m:
        # 1. Setup Mock
        api_url = "https://test-shop.myshopify.com/admin/api/2024-01/orders.json"
        mock_response = {
            "orders": [
                {"id": 1, "name": "#1001", "total_price": "100.00"},
                {"id": 2, "name": "#1002", "total_price": "200.00"},
            ]
        }
        m.get(api_url, json=mock_response)

        # 2. Configure and Read
        source = ShopifySource(config=shopify_config)
        read_df = next(source.read(options={"table_name": "orders"})).to_pandas()

    # 3. Verify
    expected_df = pd.json_normalize(mock_response["orders"])
    pd.testing.assert_frame_equal(read_df, expected_df)
