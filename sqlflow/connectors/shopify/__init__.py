from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.shopify.source import ShopifySource

source_registry.register("shopify", ShopifySource)

__all__ = [
    "ShopifySource",
]
