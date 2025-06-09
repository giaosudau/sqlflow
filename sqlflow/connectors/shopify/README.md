# Shopify Connector

The Shopify connector allows you to use your Shopify store's data as a source in your SQLFlow pipelines. It's a convenient way to pull in data from your store to use in your data warehouse or other systems.

## ✅ Features

- **Read Support**: Read data from any Shopify store you have access to.
- **Multiple Data Objects**: Supports a wide range of Shopify data objects, including orders, customers, products, and more.
- **Incremental Loading**: Supports incrementally processing new rows from a sheet.
- **Connection Testing**: Verifies credentials and access to the specified store.

## ⚙️ Setup & Prerequisites

To use this connector, you need a Shopify private app with the Admin API enabled.

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

## 📖 Documentation

For detailed information on how to configure the Shopify source, see the full source documentation.

**[Shopify Source Documentation →](./SOURCE.md)**

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

To load data, you define the source in your profile and then reference it in your pipeline.

```yaml
# profiles/dev.yml
sources:
  my_shopify:
    type: shopify
    shop_domain: "your-demo-store"
    access_token: "shpat_xxxxxxxxxxxxx"
```

```sql
-- pipelines/extract_orders.sql
-- This query loads all orders into the 'shopify_orders' table in the data warehouse.
FROM source(my_shopify, table => 'orders')
SELECT * 
TO TABLE shopify_orders;
```

### Column Selection

You can select specific columns within your SQL query to optimize performance.

```sql
-- pipelines/extract_orders.sql
FROM source(my_shopify, table => 'orders')
SELECT 
    id,
    name,
    email,
    total_price,
    created_at,
    updated_at
TO TABLE shopify_orders;
```

## 📈 Incremental Loading

This connector supports `since_id` based incremental loading for all major objects, which is highly efficient.

### Configuration

To enable incremental loading, you need to specify the `sync_mode` and `cursor_field` in your source configuration. For Shopify, the `cursor_field` should typically be `id`.

- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: Set to `"id"`.

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

When an incremental pipeline runs, SQLFlow passes a `since_id` parameter to the Shopify API, ensuring that only records created after the last run are fetched. This avoids re-ingesting the same data and is very efficient.

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
- `created_at`