#!/bin/bash

# Simple Shopify Connection Test Script
# Usage: ./run_shopify_test.sh

echo "🛍️  Shopify E-commerce Analytics Test"
echo "======================================"

# Check if environment variables are set
if [ -z "$SHOPIFY_STORE" ] || [ -z "$SHOPIFY_TOKEN" ]; then
    echo "❌ Missing Shopify credentials!"
    echo ""
    echo "Please set these environment variables:"
    echo "export SHOPIFY_STORE=\"mystore.myshopify.com\""
    echo "export SHOPIFY_TOKEN=\"shpat_your_token_here\""
    echo ""
    echo "Or edit pipelines/01_basic_connection_test.sf directly"
    exit 1
fi

echo "✅ Found Shopify credentials for: $SHOPIFY_STORE"
echo ""

# Create output directory
mkdir -p output

# Run the secure connection test
echo "🔗 Testing Shopify connection..."
sqlflow pipeline run 02_secure_connection_test --profile dev

# Check if it worked
if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Success! Connection test completed."
    echo ""
    echo "📁 Check these files for your data:"
    echo "   output/connection_status.csv  - Shows what data was found"
    echo "   output/sample_orders.csv      - Sample order data"
    echo "   output/sample_customers.csv   - Sample customer data"
    echo "   output/sample_products.csv    - Sample product data"
    echo ""
    echo "🔍 Quick preview:"
    if [ -f "output/connection_status.csv" ]; then
        echo "Data found:"
        cat output/connection_status.csv
    fi
else
    echo ""
    echo "❌ Connection test failed."
    echo ""
    echo "Common issues:"
    echo "1. Check your SHOPIFY_STORE format (should be: mystore.myshopify.com)"
    echo "2. Verify your SHOPIFY_TOKEN (should start with: shpat_)"
    echo "3. Make sure your Shopify app has read permissions"
    echo "4. Check if your store has any orders/customers/products"
fi 