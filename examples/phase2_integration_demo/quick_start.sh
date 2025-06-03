#!/bin/bash

# SQLFlow Phase 2 Integration Demo - Quick Start
# Get the demo running in under 3 minutes

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source shared CI utilities
source "$SCRIPT_DIR/scripts/ci_utils.sh"

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

# Check prerequisites (port availability specific to quick_start)
check_port_availability() {
    print_step "Checking port availability..."
    
    local ports=(5432 8080 9000 9001)
    local ports_in_use=false
    
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            print_warning "Port $port is already in use. Please stop the service using this port."
            print_info "You can find what's using the port with: lsof -i :$port"
            ports_in_use=true
        fi
    done
    
    if [ "$ports_in_use" = "true" ]; then
        print_warning "Some ports are in use. Demo may fail if these are the required services."
        print_info "Continue? (y/N)"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            print_info "Exiting. Please stop the services using these ports and try again."
            exit 1
        fi
    fi
    
    print_success "Port availability check completed"
}

# Display access information
show_access_info() {
    print_step "Setting up access information..."
    
    echo ""
    if [ "${CI:-false}" = "true" ]; then
        echo "=== Access Information ==="
        echo "Web Interfaces:"
        echo "  • pgAdmin:       http://localhost:8080"
        echo "    Login:         admin@sqlflow.com / sqlflow123"
        echo "  • MinIO Console: http://localhost:9001"
        echo "    Login:         minioadmin / minioadmin"
        echo ""
        echo "Direct Access:"
        echo "  • PostgreSQL:    localhost:5432 (demo/sqlflow/sqlflow123)"
        echo "  • MinIO API:     localhost:9000"
        echo ""
        echo "Useful Commands:"
        echo "  • View logs:     docker compose logs -f"
        echo "  • Enter SQLFlow: docker compose exec sqlflow bash"
        echo "  • Stop demo:     docker compose down"
        echo "  • Restart:       docker compose restart"
    else
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
    fi
}

# Main execution
main() {
    if [ "${CI:-false}" != "true" ]; then
        echo -e "${BLUE}============================================${NC}"
        echo -e "${BLUE}🚀 SQLFlow Phase 2 Integration Demo - Quick Start${NC}"
        echo -e "${BLUE}============================================${NC}"
        
        echo -e "${PURPLE}📋 This demo will:${NC}"
        echo -e "${PURPLE}  ✅ Start PostgreSQL, MinIO (S3), and pgAdmin services${NC}"
        echo -e "${PURPLE}  ✅ Test industry-standard connector parameters${NC}"
        echo -e "${PURPLE}  ✅ Demonstrate automatic incremental loading${NC}"
        echo -e "${PURPLE}  ✅ Validate multi-connector workflows${NC}"
        echo -e "${PURPLE}  ⏱️  Total time: ~3 minutes${NC}"
        echo ""
    else
        print_info "SQLFlow Phase 2 Integration Demo - Quick Start"
        print_info "This demo will:"
        print_info "  ✅ Start PostgreSQL, MinIO (S3), and pgAdmin services"
        print_info "  ✅ Test industry-standard connector parameters"
        print_info "  ✅ Demonstrate automatic incremental loading"
        print_info "  ✅ Validate multi-connector workflows"
        print_info "  ⏱️  Total time: ~3 minutes"
    fi
    
    # Step 1: Check prerequisites (including port availability for local runs)
    if [ "${CI:-false}" != "true" ]; then
        check_port_availability
    fi
    
    # Step 2: Run complete integration test workflow
    if ! run_complete_integration_test 60; then
        print_error "Quick start failed"
        exit 1
    fi
    
    # Step 3: Show access info (only for local runs)
    if [ "${CI:-false}" != "true" ]; then
        show_access_info
        
        echo -e "${BLUE}============================================${NC}"
        echo -e "${BLUE}🎉 Quick Start Complete!${NC}"
        echo -e "${BLUE}============================================${NC}"
        print_success "SQLFlow Phase 2 demo is now running!"
        echo -e "${PURPLE}📋 🔍 Explore the web interfaces above to see your data${NC}"
        echo -e "${PURPLE}📋 📖 Check README.md for advanced usage and next steps${NC}"
        echo -e "${PURPLE}📋 🛑 Run 'docker compose down' when you're done${NC}"
    else
        print_info "🎉 Quick Start Complete!"
        print_success "SQLFlow Phase 2 demo is now running!"
        print_info "🔍 Explore the web interfaces to see your data"
        print_info "📖 Check README.md for advanced usage and next steps"
        print_info "🛑 Run 'docker compose down' when you're done"
    fi
}

# Handle interruption
trap cleanup_on_exit INT TERM

# Run main function
main "$@" 