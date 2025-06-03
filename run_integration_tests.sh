#!/bin/bash

# SQLFlow Integration Test Runner
# Refactored to reuse shared CI utilities and avoid code duplication
# Usage: ./run_integration_tests.sh [options]

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Source shared CI utilities from phase2 demo
CI_UTILS_PATH="$PROJECT_ROOT/examples/phase2_integration_demo/scripts/ci_utils.sh"

if [[ ! -f "$CI_UTILS_PATH" ]]; then
    echo "❌ Error: ci_utils.sh not found at $CI_UTILS_PATH"
    echo "   Make sure you're running this from the SQLFlow project root"
    exit 1
fi

# Source the shared utilities early to make functions available
source "$CI_UTILS_PATH"

# Colors for output (reuse from ci_utils.sh)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Help function
show_help() {
    cat << EOF
SQLFlow Integration Test Runner

This script provides a convenient interface for running integration tests
that depend on external services (PostgreSQL, MinIO, Redis).

It leverages the shared CI utilities from examples/phase2_integration_demo/scripts/ci_utils.sh
to avoid code duplication and ensure consistency across testing environments.

Usage: $0 [OPTIONS]

OPTIONS:
    -h, --help          Show this help message
    -v, --verbose       Run tests with verbose output
    -k, --keyword       Run only tests matching keyword pattern
    -f, --file          Run specific test file
    --keep-services     Keep Docker services running after tests
    --no-cleanup        Skip cleanup on failure
    --coverage          Run with coverage reporting
    --parallel          Run tests in parallel
    --timeout SECONDS   Custom timeout for service startup (default: 120)
    --quick             Run quick smoke tests only

EXAMPLES:
    $0                                          # Run all integration tests
    $0 -v                                       # Run with verbose output
    $0 -k postgres                             # Run only PostgreSQL tests
    $0 -f test_postgres_resilience.py         # Run specific test file
    $0 --keep-services                         # Keep services running
    $0 --coverage                              # Run with coverage
    $0 --quick                                 # Quick smoke tests only

ENVIRONMENT:
    INTEGRATION_TESTS=true                     # Required to run integration tests
    PYTEST_WORKERS=auto                        # Number of parallel workers
    LOG_LEVEL=INFO                             # Logging level

SERVICES:
    This script automatically manages the following Docker services:
    - PostgreSQL (port 5432)
    - MinIO (ports 9000-9001) 
    - Redis (port 6379)

EXIT CODES:
    0    Success
    1    General error
    2    Service startup failure
    3    Test execution failure
    130  Interrupted by user
EOF
}

# Parse command line arguments
VERBOSE=""
KEYWORD=""
TEST_FILE=""
KEEP_SERVICES=false
NO_CLEANUP=false
COVERAGE=false
PARALLEL=false
QUICK=false
CUSTOM_TIMEOUT=""

# Special cleanup-only mode (check early before argument parsing)
if [[ "${1:-}" == "--cleanup-only" ]]; then
    print_info "Cleanup-only mode: Stopping Docker services"
    cd "$PROJECT_ROOT/examples/phase2_integration_demo"
    stop_docker_services
    exit 0
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -k|--keyword)
            KEYWORD="-k $2"
            shift 2
            ;;
        -f|--file)
            TEST_FILE="$2"
            shift 2
            ;;
        --keep-services)
            KEEP_SERVICES=true
            shift
            ;;
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        --coverage)
            COVERAGE=true
            shift
            ;;
        --parallel)
            PARALLEL=true
            shift
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --timeout)
            CUSTOM_TIMEOUT="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Enhanced test execution function
run_integration_tests() {
    print_step "Running integration tests..."
    
    # Set environment variables
    export INTEGRATION_TESTS=true
    export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
    
    # Build pytest command
    local pytest_cmd="python -m pytest"
    
    # Add coverage if requested
    if [[ "$COVERAGE" == "true" ]]; then
        pytest_cmd="$pytest_cmd --cov=sqlflow --cov-report=html:htmlcov --cov-report=term"
    fi
    
    # Add parallel execution if requested
    if [[ "$PARALLEL" == "true" ]]; then
        pytest_cmd="$pytest_cmd -n auto"
    fi
    
    # Add verbosity
    if [[ -n "$VERBOSE" ]]; then
        pytest_cmd="$pytest_cmd $VERBOSE"
    fi
    
    # Add keyword filter
    if [[ -n "$KEYWORD" ]]; then
        pytest_cmd="$pytest_cmd $KEYWORD"
    fi
    
    # Determine test target
    local test_target
    if [[ "$QUICK" == "true" ]]; then
        # Quick smoke tests - just test basic connectivity
        test_target="tests/integration/connectors/test_postgres_resilience.py::TestPostgresConnectorResilience::test_real_connection_resilience"
        pytest_cmd="$pytest_cmd -x"  # Stop on first failure for quick tests
    elif [[ -n "$TEST_FILE" ]]; then
        if [[ -f "tests/integration/connectors/$TEST_FILE" ]]; then
            test_target="tests/integration/connectors/$TEST_FILE"
        elif [[ -f "$TEST_FILE" ]]; then
            test_target="$TEST_FILE"
        else
            print_error "Test file not found: $TEST_FILE"
            return 1
        fi
    else
        # Run all external service tests
        test_target="tests/integration/connectors/"
        pytest_cmd="$pytest_cmd -m external_services"
    fi
    
    # Final pytest command
    pytest_cmd="$pytest_cmd $test_target"
    
    print_info "Running: $pytest_cmd"
    
    # Run tests
    local test_start_time=$(date +%s)
    
    if eval "$pytest_cmd"; then
        local test_end_time=$(date +%s)
        local test_duration=$((test_end_time - test_start_time))
        
        print_success "Integration tests passed! Duration: ${test_duration}s"
        
        if [[ "$COVERAGE" == "true" ]]; then
            print_info "Coverage report available at: htmlcov/index.html"
        fi
        
        return 0
    else
        local test_end_time=$(date +%s)
        local test_duration=$((test_end_time - test_start_time))
        
        print_error "Integration tests failed! Duration: ${test_duration}s"
        return 1
    fi
}

# Enhanced cleanup function
cleanup_services() {
    if [[ "$KEEP_SERVICES" == "true" ]]; then
        print_info "Keeping Docker services running (--keep-services flag)"
        print_info "Services running:"
        $DOCKER_COMPOSE_CMD ps 2>/dev/null || echo "Unable to list services"
        print_info "To stop services later, run: ./run_integration_tests.sh --cleanup-only"
        return
    fi
    
    print_step "Cleaning up Docker services..."
    
    # Change to the demo directory where docker-compose.yml is located
    cd "$PROJECT_ROOT/examples/phase2_integration_demo"
    
    # Use the cleanup function from ci_utils.sh
    stop_docker_services
}

# Signal handlers for graceful cleanup
trap 'print_warning "Script interrupted"; cleanup_services; exit 130' INT TERM

# Simplified integration test workflow (without demo)
run_integration_test_workflow() {
    local max_attempts="${1:-60}"
    
    print_info "Starting integration test workflow..."
    
    # Check prerequisites
    check_docker || return 1
    check_docker_compose || return 1
    
    # Start services
    start_docker_services || return 1
    
    # Wait for core services (skip SQLFlow service for integration tests)
    print_step "Waiting for core services to be ready..."
    
    # Wait for each service in parallel (background processes)
    wait_for_postgres $max_attempts &
    local postgres_pid=$!
    
    wait_for_minio $max_attempts &
    local minio_pid=$!
    
    wait_for_redis $max_attempts &
    local redis_pid=$!
    
    # Wait for all background processes
    local all_succeeded=true
    
    if ! wait $postgres_pid; then
        print_error "PostgreSQL failed to start"
        all_succeeded=false
    fi
    
    if ! wait $minio_pid; then
        print_error "MinIO failed to start"
        all_succeeded=false
    fi
    
    if ! wait $redis_pid; then
        print_error "Redis failed to start"
        all_succeeded=false
    fi
    
    if [ "$all_succeeded" = "true" ]; then
        print_success "All core services are ready!"
        return 0
    else
        print_error "Some services failed to start"
        print_info "Check service logs with: $DOCKER_COMPOSE_CMD logs"
        return 1
    fi
}

# Main execution function
main() {
    print_info "SQLFlow Integration Test Runner"
    print_info "Project root: $PROJECT_ROOT"
    print_info "Reusing shared utilities from: $CI_UTILS_PATH"
    
    # Change to demo directory for docker operations
    cd "$PROJECT_ROOT/examples/phase2_integration_demo"
    
    # Set custom timeout if provided
    local timeout_arg=""
    if [[ -n "$CUSTOM_TIMEOUT" ]]; then
        timeout_arg="$CUSTOM_TIMEOUT"
    else
        timeout_arg="120"  # Default timeout
    fi
    
    # Use the simplified integration test workflow
    local workflow_result=0
    if ! run_integration_test_workflow "$timeout_arg"; then
        workflow_result=1
    fi
    
    # Change back to project root for pytest
    cd "$PROJECT_ROOT"
    
    # Run integration tests if services started successfully
    local test_result=0
    if [[ $workflow_result -eq 0 ]]; then
        run_integration_tests || test_result=$?
    else
        print_error "Service startup failed, skipping tests"
        test_result=2
    fi
    
    # Cleanup (unless --no-cleanup is specified and tests failed)
    if [[ "$NO_CLEANUP" == "true" && $test_result -ne 0 ]]; then
        print_warning "Skipping cleanup due to test failure and --no-cleanup flag"
        print_info "Services are still running for debugging"
        print_info "To cleanup manually, run: ./run_integration_tests.sh --cleanup-only"
    else
        cd "$PROJECT_ROOT/examples/phase2_integration_demo"
        cleanup_services
    fi
    
    # Final status
    if [[ $test_result -eq 0 ]]; then
        print_success "Integration test run completed successfully!"
        if [[ "$COVERAGE" == "true" ]]; then
            print_info "Coverage report available at: htmlcov/index.html"
        fi
    else
        print_error "Integration test run failed!"
        case $test_result in
            2) print_error "Service startup failure" ;;
            3) print_error "Test execution failure" ;;
            *) print_error "Unknown error (exit code: $test_result)" ;;
        esac
    fi
    
    exit $test_result
}

# Run main function
main "$@" 