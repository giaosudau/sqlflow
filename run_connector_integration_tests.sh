#!/bin/bash

# Integration Test Runner for Connector Engine Components
# Tests the refactored connector engine methods against real Docker services
# Focuses on small unit tests rather than full pipeline execution

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
VERBOSE=false
QUICK=false
COVERAGE=false
SPECIFIC_TEST=""
TEST_PATTERN=""

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker services are healthy
check_services() {
    print_status "Checking Docker services health..."
    
    # Change to phase2 demo directory where docker-compose.yml is located
    cd examples/phase2_integration_demo
    
    # Check if services are running
    if ! docker-compose ps | grep -q "Up"; then
        print_warning "Docker services not running. Starting services..."
        docker-compose up -d postgres minio redis
        
        # Wait for services to be healthy
        print_status "Waiting for services to be ready..."
        sleep 10
        
        # Check PostgreSQL
        for i in {1..30}; do
            if docker-compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
                print_status "PostgreSQL is ready"
                break
            fi
            if [ $i -eq 30 ]; then
                print_error "PostgreSQL failed to start"
                exit 1 
            fi
            sleep 2
        done
        
        # Check MinIO
        for i in {1..30}; do
            if curl -f http://localhost:9000/minio/health/live >/dev/null 2>&1; then
                print_status "MinIO is ready"
                break
            fi
            if [ $i -eq 30 ]; then
                print_error "MinIO failed to start"
                exit 1
            fi
            sleep 2
        done
        
        # Check Redis
        for i in {1..15}; do
            if docker-compose exec -T redis redis-cli ping | grep -q PONG; then
                print_status "Redis is ready"
                break
            fi
            if [ $i -eq 15 ]; then
                print_error "Redis failed to start"
                exit 1
            fi
            sleep 2
        done
        
    else
        print_status "Docker services are already running"
    fi
    
    # Return to root directory
    cd ../..
}

# Function to set environment variables for tests
set_test_environment() {
    print_status "Setting test environment variables..."
    
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT=5432
    export POSTGRES_DB=demo
    export POSTGRES_USER=sqlflow
    export POSTGRES_PASSWORD=sqlflow123
    
    export AWS_ACCESS_KEY_ID=minioadmin
    export AWS_SECRET_ACCESS_KEY=minioadmin
    export AWS_ENDPOINT_URL=http://localhost:9000
    export S3_BUCKET=sqlflow-demo
    
    export REDIS_HOST=localhost
    export REDIS_PORT=6379
    
    print_status "Environment variables set for integration tests"
}

# Function to run specific test categories
run_connector_engine_tests() {
    print_status "Running Connector Engine Integration Tests..."
    
    local pytest_args=""
    
    if [ "$VERBOSE" = true ]; then
        pytest_args="$pytest_args -v"
    fi
    
    if [ "$COVERAGE" = true ]; then
        pytest_args="$pytest_args --cov=sqlflow.core.executors.local_executor"
        pytest_args="$pytest_args --cov-report=term-missing"
        pytest_args="$pytest_args --cov-report=html:htmlcov"
    fi
    
    if [ -n "$SPECIFIC_TEST" ]; then
        pytest_args="$pytest_args -k $SPECIFIC_TEST"
    elif [ -n "$TEST_PATTERN" ]; then
        pytest_args="$pytest_args -k $TEST_PATTERN"
    fi
    
    # Add markers for external services
    pytest_args="$pytest_args -m external_services"
    
    # Run connector engine integration tests
    print_status "Testing Connector Engine Components..."
    python -m pytest tests/integration/core/test_connector_engine_integration.py $pytest_args
    
    print_status "Testing Parameter Validation and Translation..."
    python -m pytest tests/integration/core/test_connector_parameter_validation.py $pytest_args
    
    print_status "Connector engine integration tests completed"
}

# Function to run quick smoke tests
run_quick_tests() {
    print_status "Running quick smoke tests..."
    
    local pytest_args="-v --tb=short"
    
    if [ "$COVERAGE" = true ]; then
        pytest_args="$pytest_args --cov=sqlflow.core.executors.local_executor"
    fi
    
    # Run only fast tests
    pytest_args="$pytest_args -k 'not test_data_loading and not test_export'"
    pytest_args="$pytest_args -m external_services"
    
    python -m pytest tests/integration/core/test_connector_engine_integration.py::TestConnectorEngineRegistration $pytest_args
    python -m pytest tests/integration/core/test_connector_parameter_validation.py::TestParameterTranslation $pytest_args
    
    print_status "Quick smoke tests completed"
}

# Function to cleanup
cleanup() {
    if [ "$QUICK" = false ]; then
        print_status "Cleaning up test environment..."
        cd examples/phase2_integration_demo
        # Keep services running for development, just clean up test data
        print_status "Test services left running for development"
        cd ../..
    fi
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run connector engine integration tests against Docker services.
Tests the refactored connector engine methods with real PostgreSQL, MinIO/S3, and Redis.

OPTIONS:
    -v, --verbose          Enable verbose output
    -q, --quick           Run only quick smoke tests
    -c, --coverage        Generate coverage report
    -k, --test PATTERN    Run tests matching pattern
    -t, --specific TEST   Run specific test method
    -h, --help            Show this help message

EXAMPLES:
    $0                                    # Run all connector engine integration tests
    $0 -v -c                             # Run with verbose output and coverage
    $0 -q                                # Run quick smoke tests only
    $0 -k "postgres"                     # Run only PostgreSQL-related tests
    $0 -t "test_postgres_connector_registration_success"  # Run specific test

PREREQUISITES:
    - Docker and docker-compose installed
    - SQLFlow development environment set up
    - Port 5432 (PostgreSQL), 9000 (MinIO), 6379 (Redis) available

The tests focus on:
    - Connector registration and validation
    - Parameter translation (industry standard vs legacy)
    - Data loading and export operations
    - Error handling and propagation
    - Individual method functionality (not full pipelines)
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -q|--quick)
            QUICK=true
            shift
            ;;
        -c|--coverage)
            COVERAGE=true
            shift
            ;;
        -k|--test)
            TEST_PATTERN="$2"
            shift 2
            ;;
        -t|--specific)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_status "Starting Connector Engine Integration Tests"
    print_status "==========================================="
    
    # Check prerequisites
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "docker-compose is not installed or not in PATH"
        exit 1
    fi
    
    # Check if we're in the right directory
    if [ ! -f "pyproject.toml" ] || [ ! -d "sqlflow" ]; then
        print_error "Must run from SQLFlow root directory"
        exit 1
    fi
    
    # Setup
    check_services
    set_test_environment
    
    # Run tests
    if [ "$QUICK" = true ]; then
        run_quick_tests
    else
        run_connector_engine_tests
    fi
    
    # Cleanup
    cleanup
    
    print_status "Integration tests completed successfully!"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Run main function
main 