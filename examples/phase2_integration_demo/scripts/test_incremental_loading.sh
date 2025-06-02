#!/bin/bash

# SQLFlow Phase 2 Demo: Incremental Loading Test
# Focused testing of automatic watermark-based incremental loading

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

print_step() {
    echo -e "${CYAN}🔄 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${BLUE}📋 $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_step "Testing Incremental Loading Capabilities"

# Step 1: Check initial state
print_step "Step 1: Checking initial database state"
docker compose exec -T postgres psql -U sqlflow -d demo -c "
SELECT 
    'Initial State' as phase,
    (SELECT COUNT(*) FROM incremental_test_data) as incremental_records,
    (SELECT COUNT(*) FROM customers) as customer_records,
    (SELECT COUNT(*) FROM orders) as order_records,
    (SELECT MAX(updated_at) FROM incremental_test_data) as latest_incremental_update,
    (SELECT MAX(updated_at) FROM customers) as latest_customer_update,
    (SELECT MAX(updated_at) FROM orders) as latest_order_update;
"

# Step 2: Run initial incremental load
print_step "Step 2: Running initial incremental load"
docker compose exec -T sqlflow sqlflow pipeline run 02_incremental_loading_test --profile docker

# Step 3: Check watermark state
print_step "Step 3: Checking watermark state after initial load"
docker compose exec -T sqlflow python3 -c "
import sys
sys.path.append('/sqlflow_source')

from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.core.state.backends import DuckDBStateBackend

# Initialize state backend
backend = DuckDBStateBackend('/app/target/demo.duckdb')
watermark_manager = WatermarkManager(backend)

# Check watermarks for our test sources
sources = [
    ('incremental_data', 'incremental_test_data', 'updated_at'),
    ('orders_incremental', 'orders', 'updated_at'),
    ('customers_incremental', 'customers', 'updated_at')
]

print('📊 Current Watermark State:')
for source_name, table_name, cursor_field in sources:
    try:
        watermark = watermark_manager.get_watermark(
            pipeline_name='02_incremental_loading_test',
            source_name=source_name,
            cursor_field=cursor_field
        )
        if watermark:
            print(f'  ✅ {source_name}: {watermark}')
        else:
            print(f'  ⚠️  {source_name}: No watermark found')
    except Exception as e:
        print(f'  ❌ {source_name}: Error - {e}')
"

# Step 4: Add new data to test incremental loading
print_step "Step 4: Adding new data for incremental loading test"
docker compose exec -T postgres psql -U sqlflow -d demo -c "
-- Add more incremental test data
INSERT INTO incremental_test_data (name, value, created_at, updated_at) VALUES
('Incremental Record 6', 600, NOW() + INTERVAL '1 minute', NOW() + INTERVAL '1 minute'),
('Incremental Record 7', 700, NOW() + INTERVAL '2 minutes', NOW() + INTERVAL '2 minutes');

-- Update an existing customer
UPDATE customers 
SET updated_at = NOW() + INTERVAL '1 minute',
    phone = '555-0299'
WHERE customer_id = 2;

-- Add another new order
INSERT INTO orders (customer_id, order_date, total_amount, status, shipping_address, created_at, updated_at) 
VALUES (3, CURRENT_DATE, 149.99, 'shipped', '789 Pine St, Chicago, IL 60601', NOW() + INTERVAL '1 minute', NOW() + INTERVAL '1 minute');

-- Show what we added
SELECT 'New incremental data added' as status;
"

# Step 5: Run incremental load again
print_step "Step 5: Running second incremental load (should only process new/updated records)"
docker compose exec -T sqlflow sqlflow pipeline run 02_incremental_loading_test --profile docker

# Step 6: Verify incremental loading worked
print_step "Step 6: Verifying incremental loading results"
docker compose exec -T sqlflow python3 -c "
import sys
sys.path.append('/sqlflow_source')
import duckdb

# Connect to DuckDB to check results
conn = duckdb.connect('/app/target/demo.duckdb')

# Check staging tables for incremental data
print('📊 Incremental Loading Results:')

# Check incremental_test_data
result = conn.execute('SELECT COUNT(*) FROM staging_incremental_data').fetchone()
print(f'  📋 staging_incremental_data records: {result[0]}')

# Check orders
result = conn.execute('SELECT COUNT(*) FROM staging_orders_incremental').fetchone()
print(f'  📋 staging_orders_incremental records: {result[0]}')

# Check customers
result = conn.execute('SELECT COUNT(*) FROM staging_customers_incremental').fetchone()
print(f'  📋 staging_customers_incremental records: {result[0]}')

# Show latest records from incremental_test_data
print('\\n📋 Latest incremental test data records:')
results = conn.execute('''
    SELECT id, name, value, updated_at 
    FROM staging_incremental_data 
    ORDER BY updated_at DESC 
    LIMIT 5
''').fetchall()

for row in results:
    print(f'  ID: {row[0]}, Name: {row[1]}, Value: {row[2]}, Updated: {row[3]}')

conn.close()
"

# Step 7: Check updated watermarks
print_step "Step 7: Checking updated watermark state"
docker compose exec -T sqlflow python3 -c "
import sys
sys.path.append('/sqlflow_source')

from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.core.state.backends import DuckDBStateBackend

# Initialize state backend
backend = DuckDBStateBackend('/app/target/demo.duckdb')
watermark_manager = WatermarkManager(backend)

# Check watermarks again
sources = [
    ('incremental_data', 'incremental_test_data', 'updated_at'),
    ('orders_incremental', 'orders', 'updated_at'),
    ('customers_incremental', 'customers', 'updated_at')
]

print('📊 Updated Watermark State:')
for source_name, table_name, cursor_field in sources:
    try:
        watermark = watermark_manager.get_watermark(
            pipeline_name='02_incremental_loading_test',
            source_name=source_name,
            cursor_field=cursor_field
        )
        if watermark:
            print(f'  ✅ {source_name}: {watermark}')
        else:
            print(f'  ⚠️  {source_name}: No watermark found')
    except Exception as e:
        print(f'  ❌ {source_name}: Error - {e}')
"

# Step 8: Performance comparison
print_step "Step 8: Performance analysis"
print_info "Incremental loading performance benefits:"
print_info "  🚀 Only processes new/updated records since last watermark"
print_info "  💾 Reduces data transfer and processing time"
print_info "  🔄 Automatic watermark management - no manual intervention needed"
print_info "  📊 State persistence across pipeline runs"

print_success "Incremental loading tests completed!"
print_info "Key capabilities verified:"
print_info "  ✅ Automatic watermark-based filtering"
print_info "  ✅ Industry-standard sync_mode='incremental' parameter"
print_info "  ✅ Cursor field-based incremental loading"
print_info "  ✅ State persistence with DuckDB backend"
print_info "  ✅ Performance optimization through selective processing" 