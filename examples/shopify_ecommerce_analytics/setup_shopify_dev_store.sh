#!/bin/bash

# SQLFlow Shopify Development Store Setup Guide
# This script helps users set up Shopify Partner development stores for testing

set -e

echo "🏪 SQLFlow Shopify Development Store Setup Guide"
echo "================================================"
echo ""

# Check if running in interactive mode
if [ ! -t 0 ]; then
    echo "This script requires interactive mode. Please run directly in terminal."
    exit 1
fi

echo "This script will guide you through setting up Shopify development stores"
echo "for testing the SQLFlow Shopify connector with real data."
echo ""

# Step 1: Partner Account Setup
echo "📋 Step 1: Shopify Partner Account Setup"
echo "----------------------------------------"
echo ""
echo "1. Go to: https://partners.shopify.com/"
echo "2. Create a free Partner account (if you don't have one)"
echo "3. Complete account verification"
echo ""

read -p "✅ Do you have a Shopify Partner account? (y/n): " has_partner_account

if [ "$has_partner_account" != "y" ] && [ "$has_partner_account" != "Y" ]; then
    echo ""
    echo "Please create a Shopify Partner account first:"
    echo "🔗 https://partners.shopify.com/"
    echo ""
    echo "Once created, run this script again."
    exit 0
fi

# Step 2: Development Store Creation
echo ""
echo "📋 Step 2: Create Development Stores"
echo "------------------------------------"
echo ""
echo "Recommended development stores for testing:"
echo ""
echo "1. 🏪 Small SME Store (sqlflow-test-small)"
echo "   - Purpose: Basic testing with 10-50 test orders"
echo "   - Use case: Individual developer testing"
echo ""
echo "2. 🏢 Medium SME Store (sqlflow-test-medium)"  
echo "   - Purpose: Performance testing with 100-500 test orders"
echo "   - Use case: Team integration testing"
echo ""
echo "3. 🏭 Large Store (sqlflow-test-large)"
echo "   - Purpose: Stress testing with 1000+ test orders"
echo "   - Use case: Production readiness validation"
echo ""

read -p "How many development stores do you want to create? (1-3): " store_count

# Validate input
if [[ ! "$store_count" =~ ^[1-3]$ ]]; then
    echo "❌ Invalid input. Please enter 1, 2, or 3."
    exit 1
fi

echo ""
echo "📋 Step 3: Store Creation Instructions"
echo "--------------------------------------"

store_names=("sqlflow-test-small" "sqlflow-test-medium" "sqlflow-test-large")
store_descriptions=("Small SME testing" "Medium SME performance testing" "Large scale stress testing")

for i in $(seq 1 $store_count); do
    store_name=${store_names[$((i-1))]}
    store_desc=${store_descriptions[$((i-1))]}
    
    echo ""
    echo "🏪 Creating Store $i: $store_name"
    echo "Description: $store_desc"
    echo ""
    echo "Manual steps (in Shopify Partners dashboard):"
    echo "1. Navigate to: Stores → Create development store"
    echo "2. Choose 'Development store'"
    echo "3. Store name: $store_name"
    echo "4. Store purpose: Development testing"
    echo "5. Click 'Create development store'"
    echo ""
    
    read -p "✅ Have you created the '$store_name' store? (y/n): " store_created
    
    if [ "$store_created" != "y" ] && [ "$store_created" != "Y" ]; then
        echo "⚠️  Please create the store before continuing."
        echo "You can run this script again to continue setup."
        exit 0
    fi
done

# Step 4: Private App Configuration
echo ""
echo "📋 Step 4: Configure Private Apps"
echo "---------------------------------"
echo ""
echo "For each development store, you need to create a private app:"
echo ""

for i in $(seq 1 $store_count); do
    store_name=${store_names[$((i-1))]}
    
    echo "🔧 Configuring private app for: $store_name"
    echo ""
    echo "Manual steps:"
    echo "1. Go to your '$store_name' admin: https://$store_name.myshopify.com/admin"
    echo "2. Navigate to: Apps → App and sales channel settings → Develop apps"
    echo "3. Click 'Create an app'"
    echo "4. App name: 'SQLFlow Connector'"
    echo "5. App developer: Your Partner account"
    echo ""
    echo "6. Configure Admin API access scopes:"
    echo "   ✅ read_orders (Access order data)"
    echo "   ✅ read_customers (Access customer data)"
    echo "   ✅ read_products (Access product data)"
    echo ""
    echo "7. Click 'Save'"
    echo "8. Click 'Install app'"
    echo "9. Copy the 'Admin API access token' (starts with shpat_)"
    echo ""
    
    read -p "✅ Have you configured the private app for '$store_name'? (y/n): " app_configured
    
    if [ "$app_configured" = "y" ] || [ "$app_configured" = "Y" ]; then
        echo ""
        read -p "📋 Enter the access token for '$store_name' (shpat_...): " access_token
        
        # Validate token format
        if [[ ! "$access_token" =~ ^shpat_ ]]; then
            echo "⚠️  Warning: Token should start with 'shpat_'"
            echo "Please double-check the token format."
        fi
        
        # Store credentials in .env format
        echo ""
        echo "# $store_name credentials" >> .env.shopify-dev
        echo "${store_name^^}_DOMAIN=$store_name.myshopify.com" >> .env.shopify-dev
        echo "${store_name^^}_TOKEN=$access_token" >> .env.shopify-dev
        echo "" >> .env.shopify-dev
        
        echo "✅ Credentials saved to .env.shopify-dev"
    else
        echo "⚠️  Private app not configured for '$store_name'"
        echo "You can complete this later and update credentials manually."
    fi
    echo ""
done

# Step 5: Test Data Generation
echo "📋 Step 5: Generate Test Data"
echo "-----------------------------"
echo ""
echo "To test the connector effectively, add some test data to your stores:"
echo ""
echo "📦 Recommended test data:"
echo "1. Products: Add 5-20 products with variants"
echo "2. Customers: Create 5-10 test customers"  
echo "3. Orders: Create 10-50 test orders with line items"
echo ""
echo "💡 Quick test data creation:"
echo "1. Use Shopify's 'Sample data' feature in dev stores"
echo "2. Or manually create a few orders to test real scenarios"
echo ""

read -p "✅ Would you like instructions for adding sample data? (y/n): " want_sample_data

if [ "$want_sample_data" = "y" ] || [ "$want_sample_data" = "Y" ]; then
    echo ""
    echo "📦 Sample Data Instructions:"
    echo ""
    echo "For each development store:"
    echo "1. Go to store admin → Settings → Plan and permissions"
    echo "2. Look for 'Sample data' section"
    echo "3. Click 'Add sample products' and 'Add sample orders'"
    echo ""
    echo "Or create manual test data:"
    echo "1. Products: Add a few products with different variants"
    echo "2. Customers: Create test customers with different addresses"
    echo "3. Orders: Create orders with different statuses (paid, pending, refunded)"
    echo "4. Include some international orders for geographic testing"
    echo ""
fi

# Step 6: Connection Testing
echo ""
echo "📋 Step 6: Test SQLFlow Connection"
echo "----------------------------------"
echo ""

if [ -f ".env.shopify-dev" ]; then
    echo "✅ Credentials file created: .env.shopify-dev"
    echo ""
    echo "📝 Example test commands:"
    echo ""
    
    for i in $(seq 1 $store_count); do
        store_name=${store_names[$((i-1))]}
        store_env_name=${store_name^^}
        
        echo "# Test $store_name:"
        echo "export SHOPIFY_STORE=\$${store_env_name}_DOMAIN"
        echo "export SHOPIFY_TOKEN=\$${store_env_name}_TOKEN"
        echo "cd examples/shopify_ecommerce_analytics"
        echo "./test_shopify_connector.sh"
        echo ""
    done
    
    echo "📋 To use your credentials:"
    echo "source .env.shopify-dev"
    echo "export SHOPIFY_STORE=\$SQLFLOW_TEST_SMALL_DOMAIN"
    echo "export SHOPIFY_TOKEN=\$SQLFLOW_TEST_SMALL_TOKEN"
    echo "cd examples/shopify_ecommerce_analytics"
    echo "./test_shopify_connector.sh"
else
    echo "⚠️  No credentials file created."
    echo "You'll need to set environment variables manually:"
    echo ""
    echo "export SHOPIFY_STORE=\"your-store.myshopify.com\""
    echo "export SHOPIFY_TOKEN=\"shpat_your_token_here\""
fi

echo ""
echo "🎉 Development Store Setup Complete!"
echo "===================================="
echo ""
echo "✅ Next steps:"
echo "1. Load credentials: source .env.shopify-dev (if created)"
echo "2. Set test store: export SHOPIFY_STORE=\$SQLFLOW_TEST_SMALL_DOMAIN"
echo "3. Set test token: export SHOPIFY_TOKEN=\$SQLFLOW_TEST_SMALL_TOKEN"
echo "4. Test connection: cd examples/shopify_ecommerce_analytics && ./test_shopify_connector.sh"
echo "5. Run analytics: python -m sqlflow.cli.main pipeline run 03_working_example"
echo ""
echo "📚 Documentation:"
echo "- Setup guide: examples/shopify_ecommerce_analytics/QUICKSTART.md"
echo "- Full examples: examples/shopify_ecommerce_analytics/README.md"
echo "- Test fixtures: tests/fixtures/shopify_test_data.json"
echo ""
echo "💡 Troubleshooting:"
echo "- Verify token starts with 'shpat_'"
echo "- Check domain format: 'store-name.myshopify.com'"
echo "- Ensure app has correct API scopes"
echo ""
echo "Happy testing! 🚀" 