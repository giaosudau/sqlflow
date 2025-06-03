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

# Navigate to the example and test
cd examples/shopify_ecommerce_analytics
./run_shopify_test.sh
```

**Expected Output:**
```
🛍️  Shopify E-commerce Analytics Test
======================================
✅ Found Shopify credentials for: mystore.myshopify.com

🔗 Testing Shopify connection...
🎉 Success! Connection test completed.

Data found:
data_type,records_found
orders,3
customers,2
products,5
```

## 3. Check Your Data

Look in the `output/` folder:
- `connection_status.csv` - What data was found
- `sample_orders.csv` - Your actual order data
- `sample_customers.csv` - Your customer data
- `sample_products.csv` - Your product data

## 4. Common Issues

**"Authentication failed"**: Check your token starts with `shpat_`  
**"Shop domain not found"**: Use format `mystore.myshopify.com` (no https://)  
**"No data found"**: Your store might be empty - create a test order

## 5. What's Next?

✅ **Basic connection working?** Try the other pipelines  
✅ **Ready for analytics?** Check the main [README.md](README.md)  
✅ **Want incremental loading?** Change `sync_mode` to `"incremental"`

**That's it! You're connected to Shopify in under 2 minutes.** 