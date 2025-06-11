from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.shopify.source import ShopifySource

# Register with old registries for backward compatibility
source_registry.register("shopify", ShopifySource)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "shopify",
    ShopifySource,
    default_config={"api_version": "2023-10", "timeout": 30},
    required_params=["shop_domain", "access_token"],
    optional_params={
        "api_version": "2023-10",
        "timeout": 30,
        "max_retries": 3,
        "retry_delay": 1.0,
    },
    description="Shopify source connector",
)

__all__ = [
    "ShopifySource",
]
