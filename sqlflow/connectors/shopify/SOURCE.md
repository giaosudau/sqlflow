# Shopify Source

The Shopify Source connector allows you to extract data from your Shopify store using the Admin API. For a general overview, see the [main README](./README.md).

## Configuration

The source is configured in your `profiles.yml` file.

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | `string` | Must be `"shopify"`. |
| `shop_domain` | `string` | Your shop's `.myshopify.com` domain (e.g., `your-store.myshopify.com` or just `your-store`). |
| `access_token`| `string` | The Admin API access token from your private app. |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_version`|`string`|`"2023-10"`| The Shopify API version to use. |
| `timeout`| `integer`| `30` | Request timeout in seconds. |
| `max_retries`| `integer`| `3` | Maximum number of retries on failed requests. |
| `retry_delay`|`float`|`1.0`| Delay in seconds between retries. |

### Example Profile Configuration
```yaml
# profiles/dev.yml
sources:
  my_shopify_store:
    type: shopify
    shop_domain: "your-store.myshopify.com"
    access_token: "shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    api_version: "2024-01"
```

## Reading Data

You must specify the data object you want to read from Shopify. This is provided as the `table_name` option in your `READ` or `FROM` statement.

### Read Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `table_name` | `string` | ✅ | The name of the Shopify data object to read (e.g., `orders`, `products`). |

### Available Data Objects

The connector supports all major Shopify API objects, including:
- `orders`
- `customers`
- `products`
- `inventory_items`
- `inventory_levels`
- `locations`
- `transactions`
- `fulfillments`
- `refunds`
- `discounts`
- `collections`
- `metafields`

### Usage Example
```sql
-- pipelines/extract_orders.sql
READ 'my_shopify_store' OPTIONS (
  table_name = 'orders'
);
```

This will fetch all orders from your Shopify store. You can then apply transformations or write the data to a destination.

```sql
-- pipelines/process_customers.sql
FROM READ 'my_shopify_store' OPTIONS (table_name = 'customers')
SELECT
  id,
  email,
  first_name,
  last_name,
  orders_count
WHERE
  orders_count > 5
WRITE 'my_data_warehouse' OPTIONS (table_name = 'loyal_customers');
```

## 📈 Incremental Loading

This connector has built-in support for efficient, `since_id`-based incremental loading.

### Configuration
To enable incremental loading, you must configure the source for it in your `profiles.yml`.

- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: For Shopify, this should always be `"id"`.

```yaml
# profiles/dev.yml
sources:
  my_shopify_incremental:
    type: shopify
    shop_domain: "your-demo-store"
    access_token: "shpat_xxxxxxxxxxxxx"
    sync_mode: "incremental"
    cursor_field: "id"
```

### Behavior
When an incremental pipeline runs, the connector automatically passes a `since_id=<last_id>` parameter to the Shopify API. This ensures that only records created since the last successful run are fetched, which is highly efficient and avoids re-ingesting data.

## Rate Limiting
The underlying REST connector automatically handles Shopify's API rate limits. It will:
- Respect the standard API rate limits.
- Pause and retry when it receives a `429 Too Many Requests` response, using the `Retry-After` header if provided.
- Use exponential backoff for other transient connection errors. 