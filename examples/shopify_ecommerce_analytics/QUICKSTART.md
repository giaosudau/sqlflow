# Shopify QuickStart (2 Minutes)

## 1. Get Your Shopify Credentials

### Option A: Use Your Existing Store
1. Go to your Shopify admin: `https://YOURSTORE.myshopify.com/admin`
2. Apps → App and sales channel settings → Develop apps
3. Create app → Configure Admin API scopes:
   - ✅ `read_orders`
   - ✅ `read_customers` 
   - ✅ `read_products`
4. Save → Create app → Copy the **Admin API access token**

### Option B: Free Development Store
1. Go to [Shopify Partners](https://partners.shopify.com/) (free)
2. Create development store
3. Follow Option A steps

## 2. Test Connection (30 seconds)

```bash
# Set your credentials
export SHOPIFY_STORE="mystore.myshopify.com"  # Replace with your store
export SHOPIFY_TOKEN="shpat_abc123..."        # Replace with your token

# Navigate to the example and run comprehensive test suite
cd examples/shopify_ecommerce_analytics
./test_shopify_connector.sh
```

**Expected Output:**
```
🛒 SQLFlow Shopify Connector Complete Test Suite
===============================================

🔐 Part 1: Connection & Authentication Testing
📊 Part 2: Basic Business Analytics  
🧠 Part 3: Advanced SME Analytics & Intelligence
🎯 Part 4: Production Readiness Validation

✅ All tests passed! The Shopify connector is working perfectly!
```

## 3. Check Your Data

Look in the `output/` folder:
- `connection_status.csv` - What data was found
- `sample_orders.csv` - Your actual order data
- `sample_customers.csv` - Your customer data
- `sample_products.csv` - Your product data
- `sme_customer_ltv_analysis.csv` - Customer lifetime value analysis
- `sme_product_performance.csv` - Product performance insights
- `sme_financial_reconciliation.csv` - Financial accuracy validation
- `sme_geographic_performance.csv` - Regional performance analysis

## 4. Common Issues

**"Authentication failed"**: Check your token starts with `shpat_`  
**"Shop domain not found"**: Use format `mystore.myshopify.com` (no https://)  
**"No data found"**: Your store might be empty - create a test order

## 5. What's Next?

✅ **Basic connection working?** Try the individual pipelines  
✅ **Ready for analytics?** Check the main [README.md](README.md)  
✅ **Want incremental loading?** Change `sync_mode` to `"incremental"`

**That's it! You're connected to Shopify and running advanced SME analytics in under 2 minutes.** 