#!/bin/bash

# SQLFlow Phase 2 Integration Demo - Main Test Runner
# Comprehensive testing of all Phase 2 connector implementations

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
RED='\033[0;31m'
PURPLE='\033[0;35m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_step() {
    echo -e "${CYAN}🔄 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${PURPLE}📋 $1${NC}"
}

# Function to run pipeline and check for success
run_pipeline_test() {
    local pipeline_name="$1"
    local test_description="$2"
    
    print_step "Test: $test_description"
    
    # Run the pipeline and capture output
    local output
    output=$(docker compose exec sqlflow sqlflow pipeline run "$pipeline_name" --profile docker 2>&1)
    local exit_code=$?
    
    # Check for success indicators in the output
    if echo "$output" | grep -q "✅ Pipeline completed successfully"; then
        print_success "$test_description passed"
        return 0
    elif echo "$output" | grep -q "❌ Pipeline failed"; then
        print_error "$test_description failed"
        echo "$output" | tail -5  # Show last 5 lines of error
        return 1
    else
        print_warning "$test_description completed with unclear status (exit code: $exit_code)"
        echo "$output" | tail -3  # Show last 3 lines
        return 1
    fi
}

# Function to check service health
check_service_health() {
    print_step "Checking service health..."
    
    # Check PostgreSQL
    if docker compose exec postgres psql -U sqlflow -d demo -c "SELECT 1;" > /dev/null 2>&1; then
        print_success "PostgreSQL is healthy"
    else
        print_error "PostgreSQL is not responding"
        return 1
    fi
    
    # Check MinIO
    if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        print_success "MinIO is healthy"
    else
        print_error "MinIO is not responding"
        return 1
    fi
    
    # Check SQLFlow service
    if docker compose exec sqlflow python3 -c "import sqlflow; print('SQLFlow OK')" > /dev/null 2>&1; then
        print_success "SQLFlow service is healthy"
    else
        print_error "SQLFlow service is not responding"
        return 1
    fi
    
    print_success "All services are healthy!"
    return 0
}

# Main execution
print_header "🚀 SQLFlow Phase 2 Integration Demo"

print_info "Testing Phase 2 implementations:"
print_info "  ✅ Task 2.0: Complete Incremental Loading Integration"
print_info "  ✅ Task 2.1: Connector Interface Standardization"
print_info "  ✅ Task 2.2: Enhanced PostgreSQL Connector"
print_info "  ✅ Enhanced S3 Connector with cost management"
echo

# Check service health first
if ! check_service_health; then
    print_error "Service health check failed. Aborting tests."
    exit 1
fi

echo
print_header "🧪 Running Test Suite"

# Test 1: PostgreSQL Basic Connectivity
if run_pipeline_test "01_postgres_basic_test" "PostgreSQL Basic Connectivity & Parameter Compatibility"; then
    print_info "Verifying PostgreSQL connection results..."
    if [ -f "output/postgres_connection_test_results.csv" ]; then
        print_success "PostgreSQL results file created successfully"
        echo "Results preview:"
        head -5 "output/postgres_connection_test_results.csv" 2>/dev/null || echo "  (No data to preview)"
    else
        print_warning "PostgreSQL results file not found"
    fi
else
    print_error "PostgreSQL basic test failed - skipping verification"
fi

echo

# Test 2: Incremental Loading
if run_pipeline_test "02_incremental_loading_test" "Incremental Loading with Watermarks"; then
    print_info "Verifying incremental loading results..."
    if [ -f "output/incremental_loading_results.csv" ]; then
        print_success "Incremental loading results file created successfully"
        echo "Results preview:"
        head -5 "output/incremental_loading_results.csv" 2>/dev/null || echo "  (No data to preview)"
    else
        print_warning "Incremental loading results file not found"
    fi
else
    print_error "Incremental loading test failed - skipping verification"
fi

echo

# Test 3: S3 Connector
if run_pipeline_test "03_s3_connector_test" "S3 Connector with Multi-Format Support"; then
    print_info "Verifying S3 export results..."
    if [ -f "output/s3_export_summary.csv" ]; then
        print_success "S3 export results file created successfully"
        echo "Results preview:"
        head -5 "output/s3_export_summary.csv" 2>/dev/null || echo "  (No data to preview)"
    else
        print_warning "S3 export results file not found"
    fi
else
    print_error "S3 connector test failed - skipping verification"
fi

echo

# Test 4: Multi-Connector Workflow
if run_pipeline_test "04_multi_connector_workflow" "Complete Multi-Connector Workflow"; then
    print_info "Verifying multi-connector workflow results..."
    if [ -f "output/workflow_summary.csv" ]; then
        print_success "Multi-connector workflow completed successfully"
        echo "Workflow summary:"
        cat "output/workflow_summary.csv" 2>/dev/null || echo "  (No summary to display)"
    else
        print_warning "Workflow summary file not found"
    fi
else
    print_error "Multi-connector workflow test failed - skipping verification"
fi

echo
print_header "📊 Demo Summary"

# Count successful outputs
successful_tests=0
total_tests=4

if [ -f "output/postgres_connection_test_results.csv" ]; then
    ((successful_tests++))
fi

if [ -f "output/incremental_loading_results.csv" ]; then
    ((successful_tests++))
fi

if [ -f "output/s3_export_summary.csv" ]; then
    ((successful_tests++))
fi

if [ -f "output/workflow_summary.csv" ]; then
    ((successful_tests++))
fi

print_info "Test Results: $successful_tests/$total_tests tests completed successfully"

if [ $successful_tests -eq $total_tests ]; then
    print_success "🎉 All Phase 2 integration tests passed!"
    print_info "✨ SQLFlow Phase 2 implementation is working correctly"
elif [ $successful_tests -gt 0 ]; then
    print_warning "⚠️  Partial success: $successful_tests/$total_tests tests passed"
    print_info "🔧 Some components need attention"
else
    print_error "❌ All tests failed"
    print_info "🚨 Phase 2 implementation needs debugging"
fi

echo
print_info "📁 Output files are available in: ./output/"
print_info "🐳 Services are running and accessible:"
print_info "  - PostgreSQL: localhost:5432"
print_info "  - MinIO: http://localhost:9000"
print_info "  - pgAdmin: http://localhost:8080"

echo
print_header "🏁 Demo Complete" 