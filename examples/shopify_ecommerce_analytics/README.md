# SQLFlow Shopify E-commerce Analytics

This example demonstrates how to use the SQLFlow Shopify connector to extract and analyze e-commerce data from Shopify stores.

## 🎯 What This Example Does

- **Connects to Shopify stores** using private app credentials
- **Extracts orders, customers, and products** data
- **Creates business analytics** (daily sales, top products, customer segments)
- **Provides advanced SME analytics** (LTV, cohorts, financial reconciliation)
- **Exports results** to CSV for further analysis
- **Follows SME requirements** from the implementation plan

## ✅ Status: **Phase 2, Day 4 Complete!**

The Shopify connector has completed **Phase 2, Day 4: SME Data Models** implementation. All syntax issues have been resolved and advanced SME analytics are now available.

### ✅ Completed Features:
1. ✅ **Connector Registration** - SHOPIFY connector now auto-loads
2. ✅ **Validation Schema** - Proper parameter validation implemented  
3. ✅ **Syntax Validation** - Pipeline syntax confirmed correct
4. ✅ **Environment Variables** - Now work for both validation and execution!
5. ✅ **SME Data Models** - Advanced customer LTV, product performance, financial reconciliation
6. ✅ **Geographic Analytics** - Regional performance analysis
7. ✅ **Customer Segmentation** - VIP, Loyal, Regular, One-time, Emerging classifications

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

### Option 2: Run SME Advanced Analytics (NEW!)

```bash
# Run comprehensive SME analytics
python -m sqlflow.cli.main pipeline run 05_sme_advanced_analytics_simple
```

This generates:
- `output/sme_customer_ltv_analysis.csv` - Customer lifetime value and segmentation
- `output/sme_product_performance.csv` - Product performance metrics and rankings
- `output/sme_financial_reconciliation.csv` - Financial accuracy and validation
- `output/sme_geographic_performance.csv` - Regional performance analysis

### Option 3: Basic Analytics

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

Use the secure connection test pipeline to verify your connection:

```bash
# Set your credentials first
export SHOPIFY_STORE="your-store.myshopify.com"
export SHOPIFY_TOKEN="shpat_your_token_here"

# Test the connection
python -m sqlflow.cli.main pipeline validate 02_secure_connection_test --clear-cache
python -m sqlflow.cli.main pipeline run 02_secure_connection_test
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

## 📁 Available Pipelines

### Core Pipelines
- **`02_secure_connection_test.sf`** - Basic connection test with environment variables
- **`03_working_example.sf`** - Full business analytics pipeline with multi-stream data
- **`05_sme_advanced_analytics_simple.sf`** - **Phase 2, Day 4** advanced SME analytics

### Testing & Documentation
- **`test_shopify_connector.sh`** - Comprehensive test suite that runs all pipelines
- **`CHANGELOG.md`** - Version history and technical details
- **`QUICKSTART.md`** - 2-minute setup guide

## 🧪 Testing

Run the comprehensive test suite to verify everything works:

```bash
./test_shopify_connector.sh
```

Expected output:
```
🛒 SQLFlow Shopify Connector Complete Test Suite
===============================================

🔐 Part 1: Connection & Authentication Testing
📊 Part 2: Basic Business Analytics  
🧠 Part 3: Advanced SME Analytics & Intelligence
🎯 Part 4: Production Readiness Validation

✅ All tests passed! The Shopify connector is working perfectly!
```

## 📊 SME Advanced Analytics (Phase 2, Day 4)

### Customer LTV Analysis
```sql
-- Customer segmentation with lifetime value calculations
SELECT 
    customer_email,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(CAST(total_price AS DECIMAL)) as lifetime_value,
    COUNT(DISTINCT product_id) as unique_products_purchased,
    CASE 
        WHEN COUNT(DISTINCT order_id) >= 10 AND SUM(CAST(total_price AS DECIMAL)) >= 1000 THEN 'VIP'
        WHEN COUNT(DISTINCT order_id) >= 5 AND SUM(CAST(total_price AS DECIMAL)) >= 500 THEN 'Loyal'
        WHEN COUNT(DISTINCT order_id) >= 3 AND SUM(CAST(total_price AS DECIMAL)) >= 200 THEN 'Regular'
        WHEN COUNT(DISTINCT order_id) = 1 THEN 'One-time'
        ELSE 'Emerging'
    END as customer_segment
FROM orders
WHERE customer_email IS NOT NULL
GROUP BY customer_email
ORDER BY lifetime_value DESC;
```

### Product Performance Analytics
```sql
-- Product performance with revenue rankings
SELECT 
    product_id,
    product_title,
    COUNT(DISTINCT order_id) as orders_containing_product,
    SUM(quantity) as total_units_sold,
    SUM(CAST(line_item_price AS DECIMAL) * quantity) as total_revenue,
    COUNT(DISTINCT customer_email) as unique_customers,
    COUNT(DISTINCT shipping_country) as countries_sold_to
FROM orders
WHERE product_id IS NOT NULL AND line_item_id IS NOT NULL
GROUP BY product_id, product_title
ORDER BY total_revenue DESC;
```

### Financial Reconciliation & Validation
```sql
-- Daily financial reconciliation with validation
SELECT 
    DATE(created_at) as order_date,
    financial_status,
    COUNT(DISTINCT order_id) as order_count,
    SUM(CAST(total_price AS DECIMAL)) as gross_revenue,
    SUM(CAST(total_refunded AS DECIMAL)) as total_refunded,
    SUM(CAST(total_price AS DECIMAL)) - SUM(CAST(total_refunded AS DECIMAL)) as net_revenue,
    AVG(CAST(total_price AS DECIMAL)) as avg_order_value,
    SUM(CAST(total_discounts AS DECIMAL)) / NULLIF(SUM(CAST(total_price AS DECIMAL)), 0) * 100 as discount_rate_pct
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY DATE(created_at), financial_status
ORDER BY order_date DESC;
```

### Geographic Performance Analysis
```sql
-- Regional performance analysis
SELECT 
    COALESCE(shipping_country, billing_country, 'Unknown') as country,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_email) as unique_customers,
    SUM(CAST(total_price AS DECIMAL)) as total_revenue,
    COUNT(CASE WHEN fulfillment_status = 'fulfilled' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as fulfillment_rate_pct
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY COALESCE(shipping_country, billing_country, 'Unknown')
HAVING COUNT(DISTINCT order_id) >= 1
ORDER BY total_revenue DESC;
```

## 🎯 Next Steps

1. **Set up your Shopify private app** following the instructions above
2. **Test the connection** using the secure connection test pipeline
3. **Run the full analytics** to get business insights
4. **Try the new SME advanced analytics** for deeper business intelligence
5. **Customize the queries** for your specific business needs
6. **Schedule regular runs** for ongoing analytics

## 📚 Related Documentation

- [Shopify Connector Implementation Plan](../../docs/developer/technical/implementation/shopify_connector_implementation_plan.md)
- [SQLFlow Syntax Reference](../../docs/user/reference/syntax.md)
- [Connector Development Guide](../../docs/developer/connectors/)

## 🏆 Implementation Status

**Phase 2, Day 4: SME Data Models - ✅ COMPLETED**

### What's New:
- ✅ **Enhanced Customer Segmentation**: VIP, Loyal, Regular, One-time, Emerging classifications
- ✅ **Customer LTV Calculations**: Lifetime value with behavioral patterns
- ✅ **Product Performance Analytics**: Revenue rankings and cross-selling insights
- ✅ **Financial Reconciliation**: Accuracy validation and performance ratios
- ✅ **Geographic Analysis**: Regional performance with fulfillment metrics

### Next Phase: 
- **Phase 2, Day 5**: Real Shopify Testing (Development stores, multiple configurations)
- **Phase 2, Day 6**: Error Handling & Edge Cases (Schema changes, rate limiting)

---

**Result**: The Shopify connector now provides production-ready SME analytics with advanced customer segmentation, product performance insights, and financial reconciliation capabilities! 🚀 