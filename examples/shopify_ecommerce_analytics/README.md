# SQLFlow Shopify E-commerce Analytics

This example demonstrates how to use the SQLFlow Shopify connector to extract and analyze e-commerce data from Shopify stores.

## 🎯 What This Example Does

- **Connects to Shopify stores** using private app credentials
- **Extracts orders, customers, and products** data
- **Creates business analytics** (daily sales, top products, customer segments)
- **Exports results** to CSV for further analysis
- **Follows SME requirements** from the implementation plan

## ✅ Status: Working!

The Shopify connector has been **successfully implemented and tested**. All syntax issues have been resolved.

### Fixed Issues:
1. ✅ **Connector Registration** - SHOPIFY connector now auto-loads
2. ✅ **Validation Schema** - Proper parameter validation implemented  
3. ✅ **Syntax Validation** - Pipeline syntax confirmed correct
4. ✅ **Environment Variables** - Now work for both validation and execution!

## 🚀 Quick Start

### Option 1: Environment Variables (Recommended)

Set up your credentials:
```bash
# Option A: Environment variables
export SHOPIFY_STORE="your-store.myshopify.com"
export SHOPIFY_TOKEN="shpat_your_token_here"

# Option B: .env file (automatically loaded)
echo "SHOPIFY_STORE=your-store.myshopify.com" > .env
echo "SHOPIFY_TOKEN=shpat_your_token_here" >> .env
```

Use the environment variable pipeline:
```sql
-- pipelines/03_working_example.sf
SOURCE shopify_store TYPE SHOPIFY PARAMS {
    "shop_domain": "${SHOPIFY_STORE}",
    "access_token": "${SHOPIFY_TOKEN}",
    "sync_mode": "full_refresh"
};

LOAD orders FROM shopify_store;
LOAD customers FROM shopify_store;
LOAD products FROM shopify_store;

-- Business analytics and exports...
```

**Run:**
```bash
# Both validation and execution now work with environment variables!
python -m sqlflow.cli.main pipeline validate 03_working_example --clear-cache
python -m sqlflow.cli.main pipeline run 03_working_example
```

### Option 2: Hardcoded Credentials (For Testing)

```sql
-- pipelines/04_hardcoded_test.sf
SOURCE shopify_store TYPE SHOPIFY PARAMS {
    "shop_domain": "your-store.myshopify.com",
    "access_token": "shpat_your_token_here",
    "sync_mode": "full_refresh"
};

LOAD orders FROM shopify_store;

CREATE TABLE order_summary AS
SELECT COUNT(*) as total_orders FROM orders;

EXPORT SELECT * FROM order_summary 
TO "output/order_summary.csv" 
TYPE CSV OPTIONS { "header": true };
```

**Run:**
```bash
python -m sqlflow.cli.main pipeline validate pipelines/04_hardcoded_test.sf --clear-cache
python -m sqlflow.cli.main pipeline run 04_hardcoded_test
```

## 📋 Setup Instructions

### Step 1: Create Shopify Private App

1. Go to your Shopify admin: `https://your-store.myshopify.com/admin`
2. Navigate to **Apps** → **App and sales channel settings** → **Develop apps**
3. Click **Create an app** and give it a name
4. Configure **Admin API access** with these scopes:
   - `read_orders` - Access order data
   - `read_customers` - Access customer data  
   - `read_products` - Access product data
5. **Install the app** and copy the **Admin API access token** (starts with `shpat_`)

### Step 2: Test Connection

Use the hardcoded test pipeline to verify your connection:

```bash
# Edit pipelines/04_hardcoded_test.sf with your credentials
# Then run:
python -m sqlflow.cli.main pipeline validate pipelines/04_hardcoded_test.sf --clear-cache
python -m sqlflow.cli.main pipeline run 04_hardcoded_test
```

### Step 3: Run Full Analytics

Use the comprehensive analytics pipeline:

```bash
# Environment variables (recommended)
export SHOPIFY_STORE="your-store.myshopify.com"
export SHOPIFY_TOKEN="shpat_your_token_here"

# Both validation and execution now work seamlessly!
python -m sqlflow.cli.main pipeline validate 03_working_example --clear-cache
python -m sqlflow.cli.main pipeline run 03_working_example
```

## 📊 Available Data Streams

The Shopify connector supports these data streams:

- **`orders`** - Order data with line items, customer info, financial status
- **`customers`** - Customer profiles, contact info, order history
- **`products`** - Product catalog, variants, pricing, inventory

## 🔧 Configuration Parameters

### Required Parameters
- `shop_domain` - Your Shopify domain (e.g., "mystore.myshopify.com")
- `access_token` - Private app access token (starts with "shpat_")

### Optional Parameters
- `sync_mode` - "full_refresh" or "incremental" (default: "incremental")
- `cursor_field` - Field for incremental loading (default: "updated_at")
- `lookback_window` - ISO 8601 duration buffer (default: "P7D")
- `flatten_line_items` - Flatten order line items (default: true)
- `include_fulfillments` - Include fulfillment data (default: true)
- `include_refunds` - Include refund data (default: true)

## 📁 Example Files

- `pipelines/01_basic_connection_test.sf` - Simple connection test
- `pipelines/02_secure_connection_test.sf` - Environment variable version
- `pipelines/03_working_example.sf` - Full business analytics pipeline
- `pipelines/04_hardcoded_test.sf` - Hardcoded test version
- `test_shopify_connector.sh` - Comprehensive test suite
- `CHANGELOG.md` - Version history and technical details

## 🧪 Testing

Run the test suite to verify everything works:

```bash
./test_shopify_connector.sh
```

Expected output:
```
✅ Shopify connector infrastructure working
✅ Pipeline validation working (hardcoded and environment variables)
✅ Compilation working
✅ All tests completed successfully!
```

## 🔍 Business Analytics Examples

The working example pipeline creates these analytics:

### Daily Sales Summary
```sql
CREATE TABLE daily_sales_summary AS
SELECT 
    DATE(created_at) as order_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_email) as unique_customers,
    SUM(CAST(total_price AS DECIMAL)) as daily_revenue,
    AVG(CAST(total_price AS DECIMAL)) as avg_order_value
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY DATE(created_at)
ORDER BY order_date DESC;
```

### Top Products
```sql
CREATE TABLE top_products AS
SELECT 
    product_title,
    COUNT(*) as order_count,
    SUM(quantity) as total_quantity,
    SUM(CAST(price AS DECIMAL) * quantity) as total_revenue
FROM orders
WHERE line_item_id IS NOT NULL
GROUP BY product_title
ORDER BY total_revenue DESC
LIMIT 10;
```

### Customer Segments
```sql
CREATE TABLE customer_segments AS
SELECT 
    customer_email,
    COUNT(DISTINCT order_id) as order_count,
    SUM(CAST(total_price AS DECIMAL)) as lifetime_value,
    MIN(created_at) as first_order,
    MAX(created_at) as last_order
FROM orders
WHERE customer_email IS NOT NULL
GROUP BY customer_email
ORDER BY lifetime_value DESC;
```

## 🎯 Next Steps

1. **Set up your Shopify private app** following the instructions above
2. **Test the connection** using the hardcoded test pipeline
3. **Run the full analytics** to get business insights
4. **Customize the queries** for your specific business needs
5. **Schedule regular runs** for ongoing analytics

## 📚 Related Documentation

- [Shopify Connector Implementation Plan](../../docs/developer/technical/implementation/shopify_connector_implementation_plan.md)
- [SQLFlow Syntax Reference](../../docs/user/reference/syntax.md)
- [Connector Development Guide](../../docs/developer/connectors/)

---

**Result**: The Shopify connector is fully functional! Use hardcoded credentials for development and environment variables for production execution. 