# Shopify Source Connector

The Shopify Source connector reads data from the Shopify Admin API. It is a specialized version of the REST connector, pre-configured for Shopify's endpoints and pagination scheme.

## ✅ Features

- **Shopify API Integration**: Natively connects to the Shopify Admin API.
- **Pre-defined Endpoints**: Discovers and reads from a list of common Shopify objects like `products`, `orders`, and `customers`.
- **Automatic Pagination**: Automatically handles Shopify's cursor-based pagination using the `Link` header.
- **Secure Authentication**: Uses Shopify's standard API access token.
- **Schema Discovery**: Infers the schema for each Shopify object.

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"shopify"`. | ✅ | `"shopify"` |
| `shop_name` | `string` | Your Shopify store name (the part before `.myshopify.com`). | ✅ | `"my-awesome-store"`|
| `access_token`| `string` | Your Shopify Admin API access token. Use a variable. | ✅ | `"${SHOPIFY_API_TOKEN}"` |
| `api_version`| `string` | The Shopify API version to use. | `2024-04` (default) | `"2023-10"` |

## 💡 Discoverable Objects
When you use the `discover` command or `LOAD` from this source, the following Shopify objects are available to be read as tables:

- `collects`
- `custom_collections`
- `customers`
- `metafields`
- `orders`
- `products`
- `smart_collections`

## Example

This example defines a source that connects to a Shopify store and then loads all products into a table named `shopify_products`.

```sql
-- 1. Define the connection to your Shopify store
SOURCE my_shopify_store TYPE SHOPIFY PARAMS {
    "shop_name": "my-awesome-store",
    "access_token": "${SHOPIFY_API_TOKEN}"
};

-- 2. Load the 'products' object from that source
LOAD shopify_products FROM my_shopify_store OPTIONS {
    "object_name": "products"
};
```

## 📈 Incremental Loading

This connector supports incremental loading for objects that can be filtered by `updated_at` or `created_at` timestamps (e.g., `orders`, `products`). Shopify's API is designed for this pattern.

### Configuration
- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: Set to `"updated_at"` (most common) or `"created_at"`.

```yaml
# In a profile (e.g., profiles/dev.yml)
sources:
  shopify_incremental_orders:
    type: "shopify"
    shop_name: "my-awesome-store"
    access_token: "${SHOPIFY_API_TOKEN}"
    sync_mode: "incremental"
    cursor_field: "updated_at"
```

### Behavior
When running in incremental mode, the connector adds a `updated_at_min` (or `created_at_min`) parameter to the API request, using the last saved watermark value to fetch only new or updated records.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported 