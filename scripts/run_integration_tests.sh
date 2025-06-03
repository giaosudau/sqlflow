#!/bin/bash

# SQLFlow Integration Test Runner
# Spins up Docker services, runs integration tests, and cleans up
# Usage: ./scripts/run_integration_tests.sh [options]

set -e

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"
LOG_DIR="$PROJECT_ROOT/logs/integration_tests"
MAX_WAIT_TIME=120  # Maximum time to wait for services (seconds)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} ✅ $1"
}

log_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} ⚠️  $1"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} ❌ $1"
}

# Help function
show_help() {
    cat << EOF
SQLFlow Integration Test Runner

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

EXAMPLES:
    $0                                          # Run all integration tests
    $0 -v                                       # Run with verbose output
    $0 -k postgres                             # Run only PostgreSQL tests
    $0 -f test_postgres_resilience.py         # Run specific test file
    $0 --keep-services                         # Keep services running
    $0 --coverage                              # Run with coverage

ENVIRONMENT:
    INTEGRATION_TESTS=true                     # Required to run integration tests
    PYTEST_WORKERS=auto                        # Number of parallel workers
    LOG_LEVEL=INFO                             # Logging level
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
CUSTOM_TIMEOUT=""

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
        --timeout)
            CUSTOM_TIMEOUT="$2"
            MAX_WAIT_TIME="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Set timeout if provided
if [[ -n "$CUSTOM_TIMEOUT" ]]; then
    MAX_WAIT_TIME="$CUSTOM_TIMEOUT"
fi

# Create log directory
mkdir -p "$LOG_DIR"

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check if Docker is installed and running
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker is not running"
        exit 1
    fi
    
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "docker-compose is not installed"
        exit 1
    fi
    
    # Set docker compose command
    if command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    else
        DOCKER_COMPOSE_CMD="docker compose"
    fi
    
    # Check if docker-compose.yml exists
    if [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
        log_error "docker-compose.yml not found at $DOCKER_COMPOSE_FILE"
        exit 1
    fi
    
    # Check if Python virtual environment is activated
    if [[ -z "$VIRTUAL_ENV" ]] && [[ ! -f "$PROJECT_ROOT/.venv/bin/activate" ]]; then
        log_warning "No virtual environment detected. Make sure dependencies are installed."
    fi
    
    log_success "Prerequisites check passed"
}

# Start Docker services
start_services() {
    log "Starting Docker services..."
    
    cd "$PROJECT_ROOT"
    
    # Stop any existing services
    $DOCKER_COMPOSE_CMD down --remove-orphans > "$LOG_DIR/docker_down.log" 2>&1 || true
    
    # Start services
    log "Starting PostgreSQL, MinIO, and Redis services..."
    $DOCKER_COMPOSE_CMD up -d postgres minio redis > "$LOG_DIR/docker_up.log" 2>&1
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to start Docker services"
        cat "$LOG_DIR/docker_up.log"
        exit 1
    fi
    
    log_success "Docker services started"
}

# Wait for services to be ready
wait_for_services() {
    log "Waiting for services to be ready..."
    
    local start_time=$(date +%s)
    
    # Wait for PostgreSQL
    log "Waiting for PostgreSQL (localhost:5432)..."
    while ! nc -z localhost 5432 2>/dev/null; do
        local current_time=$(date +%s)
        local elapsed_time=$((current_time - start_time))
        
        if [[ $elapsed_time -gt $MAX_WAIT_TIME ]]; then
            log_error "Timeout waiting for PostgreSQL to start"
            show_service_logs
            exit 1
        fi
        
        sleep 2
    done
    log_success "PostgreSQL is ready"
    
    # Wait for MinIO
    log "Waiting for MinIO (localhost:9000)..."
    while ! nc -z localhost 9000 2>/dev/null; do
        local current_time=$(date +%s)
        local elapsed_time=$((current_time - start_time))
        
        if [[ $elapsed_time -gt $MAX_WAIT_TIME ]]; then
            log_error "Timeout waiting for MinIO to start"
            show_service_logs
            exit 1
        fi
        
        sleep 2
    done
    log_success "MinIO is ready"
    
    # Wait for Redis
    log "Waiting for Redis (localhost:6379)..."
    while ! nc -z localhost 6379 2>/dev/null; do
        local current_time=$(date +%s)
        local elapsed_time=$((current_time - start_time))
        
        if [[ $elapsed_time -gt $MAX_WAIT_TIME ]]; then
            log_error "Timeout waiting for Redis to start"
            show_service_logs
            exit 1
        fi
        
        sleep 2
    done
    log_success "Redis is ready"
    
    # Additional wait for services to fully initialize
    log "Waiting additional 10 seconds for services to fully initialize..."
    sleep 10
    
    # Test service connectivity
    test_service_connectivity
}

# Test service connectivity
test_service_connectivity() {
    log "Testing service connectivity..."
    
    # Test PostgreSQL
    if command -v psql &> /dev/null; then
        if PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT 1;" > /dev/null 2>&1; then
            log_success "PostgreSQL connection test passed"
        else
            log_warning "PostgreSQL connection test failed, but port is open"
        fi
    else
        log "psql not available for PostgreSQL connection test"
    fi
    
    # Test MinIO (HTTP health check)
    if command -v curl &> /dev/null; then
        if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            log_success "MinIO health check passed"
        else
            log_warning "MinIO health check failed, but port is open"
        fi
    else
        log "curl not available for MinIO health check"
    fi
    
    # Test Redis
    if command -v redis-cli &> /dev/null; then
        if redis-cli -h localhost -p 6379 ping | grep -q PONG; then
            log_success "Redis connection test passed"
        else
            log_warning "Redis connection test failed, but port is open"
        fi
    else
        log "redis-cli not available for Redis connection test"
    fi
}

# Show service logs for debugging
show_service_logs() {
    log_error "Service startup failed. Showing service logs:"
    
    echo -e "\n${YELLOW}=== PostgreSQL Logs ===${NC}"
    $DOCKER_COMPOSE_CMD logs postgres | tail -20
    
    echo -e "\n${YELLOW}=== MinIO Logs ===${NC}"
    $DOCKER_COMPOSE_CMD logs minio | tail -20
    
    echo -e "\n${YELLOW}=== Redis Logs ===${NC}"
    $DOCKER_COMPOSE_CMD logs redis | tail -20
}

# Run integration tests
run_tests() {
    log "Running integration tests..."
    
    cd "$PROJECT_ROOT"
    
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
    local test_target="tests/integration/connectors/"
    if [[ -n "$TEST_FILE" ]]; then
        if [[ -f "tests/integration/connectors/$TEST_FILE" ]]; then
            test_target="tests/integration/connectors/$TEST_FILE"
        elif [[ -f "$TEST_FILE" ]]; then
            test_target="$TEST_FILE"
        else
            log_error "Test file not found: $TEST_FILE"
            exit 1
        fi
    fi
    
    # Add external services marker
    pytest_cmd="$pytest_cmd -m external_services"
    
    # Final pytest command
    pytest_cmd="$pytest_cmd $test_target"
    
    log "Running: $pytest_cmd"
    
    # Run tests with output capture
    local test_start_time=$(date +%s)
    
    if eval "$pytest_cmd" > "$LOG_DIR/pytest_output.log" 2>&1; then
        local test_end_time=$(date +%s)
        local test_duration=$((test_end_time - test_start_time))
        
        log_success "Integration tests passed! Duration: ${test_duration}s"
        
        # Show test summary
        echo -e "\n${GREEN}=== Test Summary ===${NC}"
        tail -10 "$LOG_DIR/pytest_output.log"
        
        return 0
    else
        local test_end_time=$(date +%s)
        local test_duration=$((test_end_time - test_start_time))
        
        log_error "Integration tests failed! Duration: ${test_duration}s"
        
        # Show test failures
        echo -e "\n${RED}=== Test Failures ===${NC}"
        cat "$LOG_DIR/pytest_output.log"
        
        return 1
    fi
}

# Cleanup function
cleanup() {
    if [[ "$KEEP_SERVICES" == "true" ]]; then
        log "Keeping Docker services running (--keep-services flag)"
        log "Services running:"
        $DOCKER_COMPOSE_CMD ps
        log "To stop services later, run: $DOCKER_COMPOSE_CMD down"
        return
    fi
    
    log "Cleaning up Docker services..."
    cd "$PROJECT_ROOT"
    $DOCKER_COMPOSE_CMD down --remove-orphans > "$LOG_DIR/docker_cleanup.log" 2>&1
    
    if [[ $? -eq 0 ]]; then
        log_success "Cleanup completed"
    else
        log_warning "Cleanup completed with warnings"
    fi
}

# Signal handlers for graceful cleanup
trap 'log_error "Script interrupted"; cleanup; exit 130' INT TERM

# Main execution
main() {
    log "Starting SQLFlow Integration Test Runner"
    log "Project root: $PROJECT_ROOT"
    log "Log directory: $LOG_DIR"
    
    # Check prerequisites
    check_prerequisites
    
    # Start Docker services
    start_services
    
    # Wait for services to be ready
    wait_for_services
    
    # Run integration tests
    local test_result=0
    run_tests || test_result=$?
    
    # Cleanup (unless --no-cleanup is specified and tests failed)
    if [[ "$NO_CLEANUP" == "true" && $test_result -ne 0 ]]; then
        log_warning "Skipping cleanup due to test failure and --no-cleanup flag"
        log "Services are still running for debugging"
        log "To cleanup manually, run: $DOCKER_COMPOSE_CMD down"
    else
        cleanup
    fi
    
    # Final status
    if [[ $test_result -eq 0 ]]; then
        log_success "Integration test run completed successfully!"
        if [[ "$COVERAGE" == "true" ]]; then
            log "Coverage report available at: htmlcov/index.html"
        fi
    else
        log_error "Integration test run failed!"
        log "Check logs at: $LOG_DIR/"
    fi
    
    exit $test_result
}

# Run main function
main "$@" 