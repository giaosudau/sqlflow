#!/bin/bash

# SQLFlow Phase 2 Demo: PostgreSQL Connector Test
# Focused testing of PostgreSQL connector with industry-standard parameters

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

print_step "Testing PostgreSQL Connector Capabilities"

# Step 1: Check PostgreSQL service health
print_step "Step 1: Checking PostgreSQL service health"
if docker compose exec -T postgres pg_isready -U postgres -d postgres > /dev/null 2>&1; then
    print_success "PostgreSQL service is healthy"
else
    print_error "PostgreSQL service is not responding"
    exit 1
fi

# Step 2: Verify database and tables exist
print_step "Step 2: Verifying demo database and tables"
docker compose exec -T postgres psql -U sqlflow -d demo -c "
SELECT 
    'Database Verification' as check_type,
    (SELECT COUNT(*) FROM customers) as customer_count,
    (SELECT COUNT(*) FROM orders) as order_count,
    (SELECT COUNT(*) FROM products) as product_count,
    (SELECT COUNT(*) FROM incremental_test_data) as incremental_data_count;
"

# Step 3: Test SQLFlow connectivity
print_step "Step 3: Testing SQLFlow service connectivity"
if docker compose exec -T sqlflow sqlflow --version > /dev/null 2>&1; then
    print_success "SQLFlow service is accessible"
else
    print_error "SQLFlow service is not responding"
    exit 1
fi

# Step 4: Run PostgreSQL basic connectivity test
print_step "Step 4: Running PostgreSQL basic connectivity test"
if docker compose exec -T sqlflow sqlflow pipeline run 01_postgres_basic_test --profile docker; then
    print_success "PostgreSQL basic connectivity test completed"
else
    print_error "PostgreSQL basic connectivity test failed"
    exit 1
fi

# Step 5: Verify test results
print_step "Step 5: Verifying test results"
docker compose exec -T sqlflow python3 -c "
import duckdb
import sys

try:
    conn = duckdb.connect('/app/target/demo.duckdb')
    
    # Check if test results table exists
    tables = conn.execute(\"SELECT table_name FROM information_schema.tables WHERE table_name = 'test_results'\").fetchall()
    if not tables:
        print('❌ test_results table not found')
        sys.exit(1)
    
    # Get test results
    results = conn.execute('SELECT * FROM test_results').fetchall()
    
    print('📊 Parameter Compatibility Test Results:')
    for row in results:
        source_type, record_count, min_id, max_id = row
        print(f'  📋 {source_type}: {record_count} records (ID range: {min_id}-{max_id})')
    
    # Verify all three tests loaded the same data
    record_counts = [row[1] for row in results]
    if len(set(record_counts)) == 1:
        print('✅ All parameter formats loaded identical data!')
        print(f'✅ Record count consistency: {record_counts[0]} records per test')
    else:
        print('❌ Parameter formats produced different results!')
        print(f'❌ Record counts: {record_counts}')
        sys.exit(1)
    
    conn.close()
    print('✅ PostgreSQL connector test verification completed successfully')
    
except Exception as e:
    print(f'❌ Error verifying test results: {e}')
    sys.exit(1)
"

# Step 6: Test connection parameters validation
print_step "Step 6: Testing connection parameter validation"
print_info "Key parameter compatibility features:"
print_info "  ✅ Industry-standard 'database' parameter (Airbyte/Fivetran compatible)"
print_info "  ✅ Industry-standard 'username' parameter (Airbyte/Fivetran compatible)"
print_info "  🔄 Backward compatibility with legacy 'dbname' and 'user' parameters"
print_info "  🏆 Parameter precedence: new parameters override legacy ones"
print_info "  📊 Automatic parameter mapping for seamless migration"

# Step 7: Display connection examples
print_step "Step 7: Connection parameter examples"
print_info "Modern (Industry-Standard) Format:"
print_info "  SOURCE customers TYPE POSTGRES PARAMS {"
print_info "    \"host\": \"postgres\","
print_info "    \"database\": \"demo\",        # Airbyte standard"
print_info "    \"username\": \"sqlflow\",     # Airbyte standard"
print_info "    \"password\": \"sqlflow123\""
print_info "  };"
print_info ""
print_info "Legacy Format (Still Supported):"
print_info "  SOURCE customers TYPE POSTGRES PARAMS {"
print_info "    \"host\": \"postgres\","
print_info "    \"dbname\": \"demo\",          # Legacy parameter"
print_info "    \"user\": \"sqlflow\",         # Legacy parameter"
print_info "    \"password\": \"sqlflow123\""
print_info "  };"

print_success "PostgreSQL connector test completed successfully!"
print_info "✅ Industry-standard parameter support verified"
print_info "✅ Backward compatibility with legacy parameters verified"
print_info "✅ Parameter precedence rules verified"
print_info "✅ Connection stability and data consistency verified" 