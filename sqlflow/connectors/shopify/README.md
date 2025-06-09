# Shopify Source Connector

The Shopify Source Connector enables you to extract data from Shopify stores using the Admin API. It supports multiple data objects, pagination, rate limiting, and incremental loading.

## Configuration

### Basic Configuration

```yaml
sources:
  shopify_store:
    type: shopify
    shop_domain: "your-store.myshopify.com"  # or just "your-store"
    access_token: "your_private_app_access_token"
```

### Advanced Configuration

```yaml
sources:
  shopify_store:
    type: shopify
    shop_domain: "your-store.myshopify.com"
    access_token: "your_private_app_access_token"
    api_version: "2023-10"  # Optional, defaults to 2023-10
    timeout: 30  # Optional, defaults to 30 seconds
    max_retries: 3  # Optional, defaults to 3
    retry_delay: 1.0  # Optional, defaults to 1.0 seconds
    rate_limit_delay: 0.5  # Optional, defaults to 0.5 seconds
```

## Authentication Setup

### 1. Create a Private App

1. Go to your Shopify admin panel
2. Navigate to **Settings** → **Apps and sales channels**
3. Click **Develop apps for your store**
4. Click **Create an app**
5. Give your app a name (e.g., "SQLFlow Data Connector")

### 2. Configure API Scopes

In your private app, configure the following scopes based on your data needs:

**Read Access Required:**
- `read_orders` - For orders data
- `read_customers` - For customers data
- `read_products` - For products data
- `read_inventory` - For inventory items and levels
- `read_locations` - For locations data
- `read_fulfillments` - For fulfillments data
- `read_discounts` - For discounts data
- `read_content` - For collections and metafields

### 3. Install the App and Get Access Token

1. Click **Install app**
2. Copy the **Admin API access token**
3. Use this token in your SQLFlow configuration

## Available Data Objects

The connector supports the following Shopify data objects:

| Object | Description | Incremental Support |
|--------|-------------|-------------------|
| `orders` | Customer orders | ✅ |
| `customers` | Customer information | ✅ |
| `products` | Product catalog | ✅ |
| `inventory_items` | Inventory item details | ✅ |
| `inventory_levels` | Stock levels by location | ✅ |
| `locations` | Store locations | ✅ |
| `transactions` | Payment transactions | ✅ |
| `fulfillments` | Order fulfillments | ✅ |
| `refunds` | Order refunds | ✅ |
| `discounts` | Discount codes | ✅ |
| `collections` | Product collections | ✅ |
| `metafields` | Custom metadata | ✅ |

## Usage Examples

### Basic Data Loading

```yaml
# profile.yml
sources:
  my_shopify:
    type: shopify
    shop_domain: "demo-store"
    access_token: "shpat_xxxxxxxxxxxxx"

# Load orders
load:
  - source: my_shopify
    table: orders
    target_table: shopify_orders
```

### Column Selection

```yaml
# Load specific columns only
load:
  - source: my_shopify
    table: orders
    target_table: shopify_orders
    columns:
      - id
      - name
      - email
      - total_price
      - created_at
      - updated_at
```

### Incremental Loading

```yaml
# Load only new orders since last run
load:
  - source: my_shopify
    table: orders
    target_table: shopify_orders
    mode: incremental
    cursor_field: updated_at
```

### Pagination Control

```yaml
# Control batch size for large datasets
load:
  - source: my_shopify
    table: orders
    target_table: shopify_orders
    batch_size: 100  # Smaller batches for memory efficiency
```

## Rate Limiting

The connector automatically handles Shopify's rate limiting:

- **API Rate Limits**: Respects Shopify's standard API rate limits
- **429 Responses**: Automatically waits for the time specified in `Retry-After` headers
- **Exponential Backoff**: Uses exponential backoff for connection errors
- **Configurable Delays**: Customize retry delays and rate limiting behavior

```yaml
sources:
  shopify_store:
    type: shopify
    shop_domain: "your-store"
    access_token: "your_token"
    max_retries: 5  # Increase for unreliable connections
    retry_delay: 2.0  # Longer delays between retries
    rate_limit_delay: 1.0  # Longer delays for rate limiting
```

## Data Schema

Each Shopify object returns data as provided by the API. Common fields include:

### Orders
- `id` - Unique order identifier
- `name` - Order number (e.g., "#1001")
- `email` - Customer email
- `total_price` - Order total
- `created_at` - Order creation timestamp
- `updated_at` - Last update timestamp
- `customer` - Nested customer data
- `line_items` - Array of ordered items

### Customers
- `id` - Unique customer identifier
- `email` - Customer email
- `first_name` - Customer first name
- `last_name` - Customer last name
- `created_at` - Account creation timestamp
- `updated_at` - Last update timestamp
- `addresses` - Array of customer addresses

### Products
- `id` - Unique product identifier
- `title` - Product title
- `handle` - URL handle
- `product_type` - Product category
- `vendor` - Product vendor
- `created_at` - Product creation timestamp
- `updated_at` - Last update timestamp
- `variants` - Array of product variants

## Error Handling

The connector includes comprehensive error handling:

### Connection Errors
```
Connection failed: [Errno 8] nodename nor servname provided
```
**Solution**: Check internet connectivity and shop domain

### Authentication Errors
```
HTTP 401: Unauthorized
```
**Solution**: Verify access token and API permissions

### Rate Limiting
```
Rate limited, waiting 2 seconds
```
**Solution**: This is handled automatically, no action needed

### Invalid Object Names
```
Invalid object_name: invalid_table. Must be one of [orders, customers, ...]
```
**Solution**: Use valid object names from the supported list

## Performance Tips

### 1. Use Column Selection
Only load columns you need to reduce data transfer:

```yaml
load:
  - source: my_shopify
    table: orders
    columns: [id, name, total_price, created_at]
    target_table: shopify_orders
```

### 2. Implement Incremental Loading
Use incremental loading for large datasets:

```yaml
load:
  - source: my_shopify
    table: orders
    mode: incremental
    cursor_field: updated_at
    target_table: shopify_orders
```

### 3. Optimize Batch Sizes
Adjust batch sizes based on your data and memory:

```yaml
# For large objects with many fields
load:
  - source: my_shopify
    table: orders
    batch_size: 50
    target_table: shopify_orders

# For simple objects
load:
  - source: my_shopify
    table: locations
    batch_size: 250
    target_table: shopify_locations
```

### 4. Monitor API Usage
- Track your API call limit in Shopify Admin
- Use larger batch sizes to reduce API calls
- Consider running extracts during off-peak hours

## Troubleshooting

### Common Issues

**Issue**: `shop_domain` parameter required
```
ValueError: Shopify connector requires 'shop_domain' parameter
```
**Solution**: Add `shop_domain` to your connector configuration

**Issue**: `access_token` parameter required
```
ValueError: Shopify connector requires 'access_token' parameter
```
**Solution**: Add `access_token` to your connector configuration

**Issue**: Connection timeout
```
Connection timeout after 30 seconds
```
**Solution**: Increase timeout value or check network connectivity

**Issue**: Empty results
```
No data returned from Shopify API
```
**Solutions**:
- Verify the object name is correct
- Check if your store has data for the requested object
- Verify API permissions include read access for the object

### Debug Mode

Enable debug logging to troubleshoot issues:

```python
import logging
logging.getLogger('sqlflow.connectors.shopify').setLevel(logging.DEBUG)
```

### API Limits

Shopify has the following limits:
- **Standard**: 2 calls per second
- **Shopify Plus**: 4 calls per second
- **Burst**: Up to 40 calls in short bursts

The connector automatically handles these limits with built-in rate limiting and retry logic.

## Security Considerations

1. **Token Security**: Store access tokens securely, never commit them to version control
2. **Minimal Permissions**: Only grant the API scopes you actually need
3. **Token Rotation**: Regularly rotate access tokens
4. **Network Security**: Use HTTPS and secure network connections
5. **Audit Logs**: Monitor API access in your Shopify admin panel

## Support

For issues specific to the Shopify connector:
1. Check the troubleshooting section above
2. Verify your Shopify API permissions
3. Test connection using the `test_connection()` method
4. Check Shopify's API documentation for object-specific requirements

For general SQLFlow support, refer to the main documentation. 