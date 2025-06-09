import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

from sqlflow.connectors.base.source_connector import SourceConnector

logger = logging.getLogger(__name__)


class ShopifySource(SourceConnector):
    """
    Connector for reading data from Shopify.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.shop_domain = self.config.get("shop_domain")
        self.access_token = self.config.get("access_token")
        self.endpoint = self.config.get("endpoint")

        if not all([self.shop_domain, self.access_token, self.endpoint]):
            raise ValueError(
                "ShopifySource: 'shop_domain', 'access_token', and 'endpoint' are required"
            )

        self.base_url = f"https://{self.shop_domain}/admin/api/2023-10/"

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the Shopify API.
        """
        url = urljoin(self.base_url, f"{self.endpoint}.json")
        headers = {"X-Shopify-Access-Token": self.access_token}

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # The data is nested under a key that matches the endpoint name
        data_key = self.endpoint
        return pd.json_normalize(data[data_key])
