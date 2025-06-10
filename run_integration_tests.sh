#!/bin/bash

# SQLFlow Integration Test Runner
# Uses the refactored Python-based demo system for service management
# Usage: ./run_integration_tests.sh [options]

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Path to the new Python demo system
DEMO_RUNNER_PATH="$PROJECT_ROOT/examples/phase2_integration_demo/run_demo.py"

if [[ ! -f "$DEMO_RUNNER_PATH" ]]; then
    echo "❌ Error: run_demo.py not found at $DEMO_RUNNER_PATH"
    echo "   Make sure you're running this from the SQLFlow project root"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Simple logging functions
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_step() {
    echo -e "${BLUE}🔄${NC} $1"
}

# Help function
show_help() {
    cat << EOF
SQLFlow Integration Test Runner

This script provides a convenient interface for running integration tests
that depend on external services (PostgreSQL, MinIO, Redis).

It runs both the Phase 2 integration demo AND the pytest integration test suite,
providing comprehensive validation of SQLFlow functionality. Uses the refactored 
Python-based demo system (run_demo.py) for service management.

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
    $0                                          # Run Phase 2 demo + all integration tests
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
    python3 run_demo.py --stop
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
        print_info "To stop services later, run: ./run_integration_tests.sh --cleanup-only"
        return
    fi
    
    print_step "Cleaning up Docker services..."
    
    # Change to the demo directory and use Python demo system
    cd "$PROJECT_ROOT/examples/phase2_integration_demo"
    python3 run_demo.py --stop
}

# Signal handlers for graceful cleanup
trap 'print_warning "Script interrupted"; cleanup_services; exit 130' INT TERM

# Start services using Python demo system
start_services() {
    print_info "Starting services using Python demo system..."
    
    # Change to the demo directory
    cd "$PROJECT_ROOT/examples/phase2_integration_demo"
    
    # Use Python demo system to start services
    if python3 run_demo.py --start-only; then
        print_success "All services started successfully!"
        return 0
    else
        print_error "Failed to start services"
        return 1
    fi
}

# Main execution function
main() {
    print_info "SQLFlow Integration Test Runner"
    print_info "Project root: $PROJECT_ROOT"
    print_info "Using Python demo system: $DEMO_RUNNER_PATH"
    
    # Start services using Python demo system
    local workflow_result=0
    if ! start_services; then
        workflow_result=1
    fi
    
    # Change back to project root for pytest
    cd "$PROJECT_ROOT"
    
    # Run Phase 2 demo and integration tests if services started successfully
    local test_result=0
    local demo_result=0
    if [[ $workflow_result -eq 0 ]]; then
        # First run the Phase 2 integration demo
        print_step "Running Phase 2 Integration Demo..."
        cd "$PROJECT_ROOT/examples/phase2_integration_demo"
        if python3 run_demo.py --test-only; then
            print_success "Phase 2 integration demo passed!"
            demo_result=0
        else
            print_error "Phase 2 integration demo failed!"
            demo_result=1
        fi
        
        # Then run integration tests
        cd "$PROJECT_ROOT"
        run_integration_tests || test_result=$?
        
        # Combine results - fail if either demo or tests failed
        if [[ $demo_result -ne 0 && $test_result -eq 0 ]]; then
            test_result=$demo_result
        fi
    else
        print_error "Service startup failed, skipping tests"
        test_result=2
    fi
    
    # Cleanup (unless --no-cleanup is specified and tests failed)
    if [[ "$NO_CLEANUP" == "true" && ($test_result -ne 0 || $demo_result -ne 0) ]]; then
        print_warning "Skipping cleanup due to test failure and --no-cleanup flag"
        print_info "Services are still running for debugging"
        print_info "To cleanup manually, run: ./run_integration_tests.sh --cleanup-only"
    else
        cd "$PROJECT_ROOT/examples/phase2_integration_demo"
        cleanup_services
    fi
    
    # Final status
    if [[ $test_result -eq 0 && $demo_result -eq 0 ]]; then
        print_success "Integration test run completed successfully!"
        print_success "✅ Phase 2 demo validated all 6 pipeline implementations"
        print_success "✅ Integration test suite passed with external services"
        if [[ "$COVERAGE" == "true" ]]; then
            print_info "Coverage report available at: htmlcov/index.html"
        fi
    else
        print_error "Integration test run failed!"
        if [[ $demo_result -ne 0 ]]; then
            print_error "❌ Phase 2 demo failed - some pipelines did not complete successfully"
        fi
        if [[ $test_result -ne 0 ]]; then
            case $test_result in
                2) print_error "Service startup failure" ;;
                3) print_error "Test execution failure" ;;
                *) print_error "Unknown error (exit code: $test_result)" ;;
            esac
        fi
    fi
    
    # Return non-zero if either demo or tests failed
    local final_result=0
    if [[ $demo_result -ne 0 || $test_result -ne 0 ]]; then
        final_result=1
    fi
    
    exit $final_result
}

# Run main function
main "$@" 