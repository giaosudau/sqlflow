import unittest

import pandas as pd
import requests_mock

from sqlflow.connectors.shopify.source import ShopifySource


class TestShopifySource(unittest.TestCase):
    def setUp(self):
        self.config = {
            "shop_domain": "test-shop.myshopify.com",
            "access_token": "test-token",
            "endpoint": "products",
        }

    def test_read_success(self):
        """Test successful read from the Shopify API."""
        url = "https://test-shop.myshopify.com/admin/api/2023-10/products.json"
        with requests_mock.Mocker() as m:
            m.get(
                url,
                json={"products": [{"id": 1, "title": "Test Product"}]},
                headers={"X-Shopify-Access-Token": "test-token"},
            )
            connector = ShopifySource(config=self.config)
            df = connector.read()
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(df.shape, (1, 2))

    def test_missing_config(self):
        """Test that an error is raised if config is missing."""
        with self.assertRaises(ValueError):
            ShopifySource(config={})


if __name__ == "__main__":
    unittest.main()
