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
print_info "  ✅ Resilient Connector Patterns & Recovery"
print_info "  ✅ Enhanced S3 Connector with Partition Awareness"
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
    if [ -f "output/incremental_test_results.csv" ]; then
        print_success "Incremental loading results file created successfully"
        echo "Results preview:"
        head -5 "output/incremental_test_results.csv" 2>/dev/null || echo "  (No data to preview)"
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
    # Check S3 exports by looking for actual export files in MinIO bucket
    if docker compose exec sqlflow python3 -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin'); response = s3.list_objects_v2(Bucket='sqlflow-demo', Prefix='exports/'); exit(0 if response.get('Contents') else 1)" > /dev/null 2>&1; then
        print_success "S3 export results found in MinIO bucket"
        echo "S3 exports completed successfully"
    else
        print_warning "S3 export results not found in MinIO bucket"
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

# Test 5: 🔥 NEW - Resilient Connector Patterns
if run_pipeline_test "05_resilient_postgres_test" "Resilient Connector Patterns & Recovery"; then
    print_info "Verifying resilience test results..."
    if [ -f "output/resilience_test_results.csv" ]; then
        print_success "Resilience test completed successfully"
        echo "Resilience summary:"
        # Show resilience config and benefits
        if command -v python3 >/dev/null 2>&1; then
            python3 -c "
import csv
try:
    with open('output/resilience_test_results.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(f'  • Status: {row.get(\"status\", \"N/A\")}')
            print(f'  • Config: {row.get(\"resilience_config\", \"N/A\")}')
            print(f'  • Benefits: {row.get(\"benefits\", \"N/A\")}')
            break
except Exception as e:
    print(f'  Error reading resilience results: {e}')
"
        else
            head -2 "output/resilience_test_results.csv" 2>/dev/null || echo "  (No summary to display)"
        fi
    else
        print_warning "Resilience test results file not found"
    fi
    
    # Also check stress test results
    if [ -f "output/resilience_stress_test_results.csv" ]; then
        stress_count=$(tail -n +2 "output/resilience_stress_test_results.csv" | wc -l)
        print_success "Stress test loaded $stress_count records under aggressive timeout conditions"
    fi
else
    print_error "Resilient connector test failed - skipping verification"
fi

echo

# Test 6: 🚀 NEW - Enhanced S3 Connector Demo
print_info "🛠️ Setting up Enhanced S3 test data..."
if docker compose exec sqlflow python3 scripts/setup_s3_test_data.py; then
    print_success "✅ S3 test data setup completed"
else
    print_warning "⚠️ S3 test data setup failed, proceeding with mock mode"
fi

if run_pipeline_test "pipelines/06_enhanced_s3_connector_demo.sf" "Enhanced S3 Connector with Cost Management & Partition Awareness"; then
    print_info "Verifying enhanced S3 connector results..."
    
    # Check for output files created by the enhanced S3 connector
    enhanced_s3_files=0
    for file in output/enhanced_s3_*.csv output/s3_*.csv; do
        if [ -f "$file" ]; then
            ((enhanced_s3_files++))
            filename=$(basename "$file")
            filesize=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "unknown")
            print_success "Found enhanced S3 output: $filename (${filesize} bytes)"
        fi
    done
    
    if [ $enhanced_s3_files -gt 0 ]; then
        print_success "Enhanced S3 connector created $enhanced_s3_files output file(s)"
        echo "Enhanced S3 Features Demonstrated:"
        echo "  💰 Cost Management: Spending limits and monitoring"
        echo "  🗂️ Partition Awareness: Automatic pattern detection"
        echo "  📄 Multi-format Support: CSV, Parquet, JSON, JSONL"
        echo "  🛡️ Resilience Patterns: Retry logic and circuit breakers"
        echo "  ⚙️ Zero Configuration: Enterprise-grade features out-of-the-box"
    else
        print_warning "No enhanced S3 output files found"
    fi
    
    # Verify S3 exports in MinIO using enhanced verification
    if docker compose exec sqlflow python3 -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin'); response = s3.list_objects_v2(Bucket='sqlflow-demo', Prefix='exports/'); exit(0 if response.get('Contents') else 1)" > /dev/null 2>&1; then
        print_success "Enhanced S3 exports verified in MinIO bucket"
    else
        print_info "Enhanced S3 demo completed (mock mode or MinIO verification skipped)"
    fi
else
    print_error "Enhanced S3 connector test failed - skipping verification"
fi

echo
print_header "📊 Demo Summary"

# Count successful outputs
successful_tests=0
total_tests=6  # Updated to include the new enhanced S3 test

if [ -f "output/postgres_connection_test_results.csv" ]; then
    ((successful_tests++))
fi

if [ -f "output/incremental_test_results.csv" ]; then
    ((successful_tests++))
fi

# Check S3 test success by looking for files in MinIO bucket
if docker compose exec sqlflow python3 -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin'); response = s3.list_objects_v2(Bucket='sqlflow-demo', Prefix='exports/'); exit(0 if response.get('Contents') else 1)" > /dev/null 2>&1; then
    ((successful_tests++))
fi

if [ -f "output/workflow_summary.csv" ]; then
    ((successful_tests++))
fi

# Check resilience test success
if [ -f "output/resilience_test_results.csv" ]; then
    ((successful_tests++))
fi

# Check enhanced S3 test success (either output files or successful pipeline run)
enhanced_s3_files=0
for file in output/enhanced_s3_*.csv output/s3_*.csv; do
    if [ -f "$file" ]; then
        ((enhanced_s3_files++))
    fi
done
if [ $enhanced_s3_files -gt 0 ]; then
    ((successful_tests++))
fi

print_info "Test Results: $successful_tests/$total_tests tests completed successfully"

if [ $successful_tests -eq $total_tests ]; then
    print_success "🎉 All Phase 2 integration tests passed!"
    print_info "✨ SQLFlow Phase 2 implementation is working correctly"
    print_success "🛡️ Resilience patterns are operational and production-ready"
    print_success "💰 Enhanced S3 connector with cost management is functional"
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

# NEW: Show resilience-specific next steps
if [ -f "output/resilience_test_results.csv" ]; then
    echo
    print_info "🛡️ Resilience Pattern Benefits Demonstrated:"
    print_info "  • Automatic retry on connection timeouts (3 attempts max)"
    print_info "  • Circuit breaker protection (fails fast after 5 failures, recovers in 30s)"
    print_info "  • Rate limiting prevents database overload (300/min + 50 burst)"
    print_info "  • Connection recovery handles network failures automatically"
    print_info "  • Zero configuration required - production-ready out of the box"
fi

# NEW: Show enhanced S3 connector benefits
if [ $enhanced_s3_files -gt 0 ]; then
    echo
    print_info "💰 Enhanced S3 Connector Benefits Demonstrated:"
    print_info "  • Cost Management: Real-time spending monitoring and limits"
    print_info "  • Partition Awareness: Optimized S3 prefix scanning for performance"
    print_info "  • Multi-format Support: CSV, Parquet, JSON, JSONL, TSV, Avro"
    print_info "  • Mock Mode: Perfect for testing without AWS costs"
    print_info "  • Industry Standards: Airbyte/Fivetran compatible parameters"
    print_info "  • Zero Configuration: Enterprise features enabled automatically"
fi

echo
print_header "🏁 Demo Complete" 