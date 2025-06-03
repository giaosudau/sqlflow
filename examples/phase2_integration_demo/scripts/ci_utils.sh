#!/bin/bash

# SQLFlow CI Utilities
# Shared functions for Docker service management and health checks
# Used by both CI workflows and quick_start.sh

set -e

# Colors for output (can be used in CI)
export RED='\033[0;31m'
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export CYAN='\033[0;36m'
export PURPLE='\033[0;35m'
export NC='\033[0m'

# Print functions with conditional color support
print_info() {
    if [ "${CI:-false}" = "true" ]; then
        echo "[INFO] $1"
    else
        echo -e "${BLUE}[INFO]${NC} $1"
    fi
}

print_success() {
    if [ "${CI:-false}" = "true" ]; then
        echo "[SUCCESS] $1"
    else
        echo -e "${GREEN}[SUCCESS]${NC} $1"
    fi
}

print_error() {
    if [ "${CI:-false}" = "true" ]; then
        echo "[ERROR] $1"
    else
        echo -e "${RED}[ERROR]${NC} $1"
    fi
}

print_warning() {
    if [ "${CI:-false}" = "true" ]; then
        echo "[WARNING] $1"
    else
        echo -e "${YELLOW}[WARNING]${NC} $1"
    fi
}

print_step() {
    if [ "${CI:-false}" = "true" ]; then
        echo "[STEP] $1"
    else
        echo -e "${CYAN}🔄 $1${NC}"
    fi
}

# Check if Docker is available
check_docker() {
    print_step "Checking Docker availability..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        return 1
    fi
    
    if ! docker --version &> /dev/null; then
        print_error "Docker is not running or not accessible."
        return 1
    fi
    
    print_success "Docker is available"
    return 0
}

# Check if Docker Compose is available
check_docker_compose() {
    print_step "Checking Docker Compose availability..."
    
    # Try docker compose (new syntax)
    if docker compose version &> /dev/null; then
        export DOCKER_COMPOSE_CMD="docker compose"
        print_success "Docker Compose (new syntax) is available"
        return 0
    fi
    
    # Try docker-compose (legacy syntax)
    if command -v docker-compose &> /dev/null; then
        export DOCKER_COMPOSE_CMD="docker-compose"
        print_success "Docker Compose (legacy syntax) is available"
        return 0
    fi
    
    print_error "Docker Compose is not available. Please install Docker Compose."
    return 1
}

# Start Docker services
start_docker_services() {
    print_step "Starting Docker services..."
    
    if [ -z "${DOCKER_COMPOSE_CMD:-}" ]; then
        check_docker_compose || return 1
    fi
    
    $DOCKER_COMPOSE_CMD up -d --build
    
    if [ $? -eq 0 ]; then
        print_success "Docker services started successfully"
        return 0
    else
        print_error "Failed to start Docker services"
        return 1
    fi
}

# Stop Docker services
stop_docker_services() {
    print_step "Stopping Docker services..."
    
    if [ -z "${DOCKER_COMPOSE_CMD:-}" ]; then
        check_docker_compose || return 1
    fi
    
    $DOCKER_COMPOSE_CMD down --volumes --remove-orphans
    
    if [ $? -eq 0 ]; then
        print_success "Docker services stopped successfully"
        return 0
    else
        print_warning "Some issues occurred while stopping services"
        return 1
    fi
}

# Wait for a specific service to be healthy
wait_for_service() {
    local service_name="$1"
    local health_check_cmd="$2"
    local max_attempts="${3:-60}"
    local sleep_interval="${4:-5}"
    
    print_step "Waiting for $service_name to be ready..."
    
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if eval "$health_check_cmd" &> /dev/null; then
            print_success "$service_name is ready!"
            return 0
        fi
        
        if [ "${CI:-false}" = "true" ]; then
            echo "Waiting for $service_name... ($attempt/$max_attempts)"
        else
            echo -ne "\r${CYAN}⏳ Waiting for $service_name... ($attempt/$max_attempts)${NC}"
        fi
        
        sleep $sleep_interval
        ((attempt++))
    done
    
    echo ""
    print_error "$service_name failed to become ready after $max_attempts attempts"
    return 1
}

# Wait for PostgreSQL to be ready
wait_for_postgres() {
    local max_attempts="${1:-60}"
    wait_for_service "PostgreSQL" \
        "$DOCKER_COMPOSE_CMD exec -T postgres pg_isready -U postgres -d postgres" \
        $max_attempts
}

# Wait for MinIO to be ready
wait_for_minio() {
    local max_attempts="${1:-60}"
    wait_for_service "MinIO" \
        "curl -f http://localhost:9000/minio/health/live" \
        $max_attempts
}

# Wait for SQLFlow service to be ready
wait_for_sqlflow() {
    local max_attempts="${1:-60}"
    wait_for_service "SQLFlow" \
        "$DOCKER_COMPOSE_CMD exec -T sqlflow sqlflow --version" \
        $max_attempts
}

# Wait for Redis to be ready
wait_for_redis() {
    local max_attempts="${1:-60}"
    wait_for_service "Redis" \
        "$DOCKER_COMPOSE_CMD exec -T redis redis-cli ping" \
        $max_attempts
}

# Wait for all core services to be ready
wait_for_all_services() {
    local max_attempts="${1:-60}"
    
    print_step "Waiting for all services to be ready..."
    
    # Wait for each service in parallel (background processes)
    wait_for_postgres $max_attempts &
    local postgres_pid=$!
    
    wait_for_minio $max_attempts &
    local minio_pid=$!
    
    wait_for_redis $max_attempts &
    local redis_pid=$!
    
    wait_for_sqlflow $max_attempts &
    local sqlflow_pid=$!
    
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
    
    if ! wait $sqlflow_pid; then
        print_error "SQLFlow failed to start"
        all_succeeded=false
    fi
    
    if [ "$all_succeeded" = "true" ]; then
        print_success "All services are ready!"
        return 0
    else
        print_error "Some services failed to start"
        print_info "Check service logs with: $DOCKER_COMPOSE_CMD logs"
        return 1
    fi
}

# Run the integration demo
run_integration_demo() {
    print_step "Running integration demo..."
    
    if [ -f "./scripts/run_integration_demo.sh" ]; then
        chmod +x ./scripts/run_integration_demo.sh
        echo "[DEBUG] About to execute integration demo script"
        if ./scripts/run_integration_demo.sh; then
            echo "[DEBUG] Integration demo script returned success (exit code 0)"
            print_success "Integration demo completed successfully!"
            return 0
        else
            local exit_code=$?
            echo "[DEBUG] Integration demo script failed with exit code: $exit_code"
            print_error "Integration demo failed"
            return 1
        fi
    else
        print_error "Integration demo script not found"
        return 1
    fi
}

# Check and display integration demo results
check_demo_results() {
    print_step "Checking integration demo results..."
    
    if [ -d "output" ]; then
        print_info "=== Integration Demo Results ==="
        ls -la output/ || print_warning "Could not list output directory"
        
        if [ -f "output/workflow_summary.csv" ]; then
            print_info "=== Workflow Summary ==="
            cat output/workflow_summary.csv
        fi
        
        if [ -f "output/resilience_test_results.csv" ]; then
            print_info "=== Resilience Test Results ==="
            cat output/resilience_test_results.csv
        fi
    else
        print_warning "No output directory found"
    fi
}

# Complete integration test workflow
run_complete_integration_test() {
    local max_attempts="${1:-60}"
    
    print_info "Starting complete integration test workflow..."
    
    # Check prerequisites
    check_docker || return 1
    check_docker_compose || return 1
    
    # Start services
    start_docker_services || return 1
    
    # Wait for services
    if ! wait_for_all_services $max_attempts; then
        print_error "Services failed to start, stopping..."
        stop_docker_services
        return 1
    fi
    
    # Run demo
    if ! run_integration_demo; then
        print_error "Demo failed, stopping services..."
        stop_docker_services
        return 1
    fi
    
    # Check results
    check_demo_results
    
    print_success "Complete integration test completed successfully!"
    return 0
}

# Cleanup function for trap
cleanup_on_exit() {
    print_warning "Received interrupt signal, cleaning up..."
    stop_docker_services
    exit 1
}

# Export functions for use in other scripts
export -f print_info print_success print_error print_warning print_step
export -f check_docker check_docker_compose
export -f start_docker_services stop_docker_services
export -f wait_for_service wait_for_postgres wait_for_minio wait_for_sqlflow wait_for_redis wait_for_all_services
export -f run_integration_demo check_demo_results run_complete_integration_test
export -f cleanup_on_exit 