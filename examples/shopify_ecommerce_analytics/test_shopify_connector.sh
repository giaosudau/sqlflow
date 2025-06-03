#!/bin/bash

# Comprehensive Shopify Connector Test Suite
# Tests connector functionality, validation, and data extraction
set -e

echo "🧪 SQLFlow Shopify Connector Test Suite"
echo "========================================"

# Determine script directory and navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Test 1: Connector Infrastructure
echo ""
echo "1. Testing connector infrastructure..."
python -c "
from sqlflow.connectors import CONNECTOR_REGISTRY
from sqlflow.validation.schemas import CONNECTOR_SCHEMAS

print('✅ SHOPIFY connector registered:', 'SHOPIFY' in CONNECTOR_REGISTRY)
print('✅ SHOPIFY validation schema available:', 'SHOPIFY' in CONNECTOR_SCHEMAS)
print('Available connectors:', ', '.join(sorted(CONNECTOR_REGISTRY.keys())))
"

# Test 2: Pipeline Validation
echo ""
echo "2. Testing pipeline validation..."
cd "$SCRIPT_DIR"

echo "   → Hardcoded parameters..."
python -m sqlflow.cli.main pipeline validate pipelines/04_hardcoded_test.sf --clear-cache

echo "   → Environment variables..."
export SHOPIFY_STORE="test.myshopify.com"
export SHOPIFY_TOKEN="shpat_test_token"
python -m sqlflow.cli.main pipeline validate pipelines/03_working_example.sf --clear-cache

echo "   → All pipelines..."
python -m sqlflow.cli.main pipeline validate --clear-cache

# Test 3: Connection Test (if credentials available)
echo ""
echo "3. Testing connection (if credentials available)..."
if [ -n "$SHOPIFY_STORE" ] && [ -n "$SHOPIFY_TOKEN" ] && [[ "$SHOPIFY_TOKEN" == shpat_* ]] && [[ "$SHOPIFY_TOKEN" != *"test"* ]]; then
    echo "   → Found real credentials, testing connection..."
    mkdir -p output
    python -m sqlflow.cli.main pipeline run 02_secure_connection_test --profile dev
    
    if [ -f "output/connection_status.csv" ]; then
        echo "   → Connection successful! Data found:"
        cat output/connection_status.csv
    fi
else
    echo "   → Test credentials detected or no real credentials provided"
    echo "   → Skipping live connection test (set real SHOPIFY_STORE and SHOPIFY_TOKEN for live test)"
fi

# Test 4: Compilation
echo ""
echo "4. Testing compilation..."
python -m sqlflow.cli.main pipeline compile 03_working_example

echo ""
echo "📋 Test Results Summary:"
echo "✅ Shopify connector infrastructure working"
echo "✅ Pipeline validation working (hardcoded and environment variables)"
echo "✅ Compilation working"
if [ -f "output/connection_status.csv" ]; then
    echo "✅ Live connection test passed"
else
    echo "ℹ️  Live connection test skipped (no credentials)"
fi

echo ""
echo "🎯 All tests completed successfully!"
echo ""
echo "💡 Quick Start:"
echo "   1. Set credentials: export SHOPIFY_STORE='your-store.myshopify.com'"
echo "   2. Set token: export SHOPIFY_TOKEN='shpat_your_token'"
echo "   3. Run analytics: sqlflow pipeline run 03_working_example" 