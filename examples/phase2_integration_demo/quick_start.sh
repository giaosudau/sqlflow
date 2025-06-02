#!/bin/bash

# SQLFlow Phase 2 Integration Demo - Quick Start
# Get the demo running in under 3 minutes

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

# Check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if ports are available
    local ports=(5432 8080 9000 9001)
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            print_warning "Port $port is already in use. Please stop the service using this port."
            print_info "You can find what's using the port with: lsof -i :$port"
        fi
    done
    
    print_success "Prerequisites check completed"
}

# Start services
start_services() {
    print_step "Starting Docker services..."
    
    # Build and start services
    docker compose up -d --build
    
    print_success "Services started successfully"
    print_info "Services starting up in background..."
}

# Wait for services to be ready
wait_for_services() {
    print_step "Waiting for services to be ready..."
    
    local max_attempts=60
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        local all_healthy=true
        
        # Check PostgreSQL
        if ! docker compose exec -T postgres pg_isready -U postgres -d postgres > /dev/null 2>&1; then
            all_healthy=false
        fi
        
        # Check MinIO
        if ! curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            all_healthy=false
        fi
        
        # Check SQLFlow
        if ! docker compose exec -T sqlflow sqlflow --version > /dev/null 2>&1; then
            all_healthy=false
        fi
        
        if [ "$all_healthy" = true ]; then
            print_success "All services are ready!"
            return 0
        fi
        
        echo -ne "\r${CYAN}⏳ Waiting for services... ($attempt/$max_attempts)${NC}"
        sleep 5
        ((attempt++))
    done
    
    echo ""
    print_error "Services failed to become ready after $max_attempts attempts"
    print_info "You can check service logs with: docker compose logs"
    return 1
}

# Run demo
run_demo() {
    print_step "Running integration demo..."
    
    if ./scripts/run_integration_demo.sh; then
        print_success "Demo completed successfully!"
    else
        print_error "Demo failed. Check the logs above for details."
        return 1
    fi
}

# Display access information
show_access_info() {
    print_header "🌐 Access Information"
    
    echo -e "${CYAN}Web Interfaces:${NC}"
    echo -e "  • pgAdmin:       http://localhost:8080"
    echo -e "    Login:         admin@sqlflow.com / sqlflow123"
    echo -e "  • MinIO Console: http://localhost:9001"
    echo -e "    Login:         minioadmin / minioadmin"
    echo ""
    
    echo -e "${CYAN}Direct Access:${NC}"
    echo -e "  • PostgreSQL:    localhost:5432 (demo/sqlflow/sqlflow123)"
    echo -e "  • MinIO API:     localhost:9000"
    echo ""
    
    echo -e "${CYAN}Useful Commands:${NC}"
    echo -e "  • View logs:     docker compose logs -f"
    echo -e "  • Enter SQLFlow: docker compose exec sqlflow bash"
    echo -e "  • Stop demo:     docker compose down"
    echo -e "  • Restart:       docker compose restart"
}

# Main execution
main() {
    print_header "🚀 SQLFlow Phase 2 Integration Demo - Quick Start"
    
    print_info "This demo will:"
    print_info "  ✅ Start PostgreSQL, MinIO (S3), and pgAdmin services"
    print_info "  ✅ Test industry-standard connector parameters"
    print_info "  ✅ Demonstrate automatic incremental loading"
    print_info "  ✅ Validate multi-connector workflows"
    print_info "  ⏱️  Total time: ~3 minutes"
    echo ""
    
    # Step 1: Check prerequisites
    check_prerequisites
    
    # Step 2: Start services
    start_services
    
    # Step 3: Wait for services
    if ! wait_for_services; then
        print_error "Quick start failed: Services not ready"
        print_info "Try running 'docker compose logs' to see what went wrong"
        exit 1
    fi
    
    # Step 4: Run demo
    if ! run_demo; then
        print_error "Quick start failed: Demo execution failed"
        exit 1
    fi
    
    # Step 5: Show access info
    show_access_info
    
    print_header "🎉 Quick Start Complete!"
    print_success "SQLFlow Phase 2 demo is now running!"
    print_info "🔍 Explore the web interfaces above to see your data"
    print_info "📖 Check README.md for advanced usage and next steps"
    print_info "🛑 Run 'docker compose down' when you're done"
}

# Handle interruption
trap 'print_error "Quick start interrupted"; exit 1' INT TERM

# Run main function
main "$@" 