#!/bin/bash

# SQLFlow Phase 2 Integration Demo - Quick Start
# Simple wrapper for the unified Python demo runner

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m'

# Check if Python 3 is available
check_python() {
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 is required but not found"
        echo "Please install Python 3 and try again"
        exit 1
    fi
}

# Check port availability
check_ports() {
    if [ "${CI:-false}" = "true" ]; then
        return 0  # Skip port check in CI
    fi
    
    local ports=(5432 8080 9000 9001)
    local ports_in_use=false
    
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo -e "${CYAN}⚠️ Port $port is already in use${NC}"
            ports_in_use=true
        fi
    done
    
    if [ "$ports_in_use" = "true" ]; then
        echo -e "${CYAN}Some ports are in use. Continue anyway? (y/N)${NC}"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "Exiting. Please stop the services using these ports and try again."
            exit 1
        fi
    fi
}

# Main execution
main() {
    if [ "${CI:-false}" != "true" ]; then
        echo -e "${BLUE}============================================${NC}"
        echo -e "${BLUE}🚀 SQLFlow Phase 2 Integration Demo${NC}"
        echo -e "${BLUE}============================================${NC}"
        echo -e "${PURPLE}📋 This demo will:${NC}"
        echo -e "${PURPLE}  ✅ Start PostgreSQL, MinIO (S3), and pgAdmin services${NC}"
        echo -e "${PURPLE}  ✅ Test all 6 Phase 2 pipeline implementations${NC}"
        echo -e "${PURPLE}  ✅ Validate resilience patterns and cost management${NC}"
        echo -e "${PURPLE}  ⏱️  Total time: ~3 minutes${NC}"
        echo ""
    fi
    
    # Basic checks
    check_python
    check_ports
    
    # Run the unified demo
    cd "$SCRIPT_DIR"
    echo -e "${CYAN}🚀 Starting unified demo runner...${NC}"
    
    if python3 run_demo.py; then
        if [ "${CI:-false}" != "true" ]; then
            echo -e "${GREEN}🎉 Demo completed successfully!${NC}"
            echo -e "${PURPLE}📖 Check README.md for advanced usage${NC}"
            echo -e "${PURPLE}🛑 Run 'python3 run_demo.py --stop' when done${NC}"
        fi
        exit 0
    else
        echo -e "${CYAN}❌ Demo failed. Check output above for details.${NC}"
        exit 1
    fi
}

# Handle interruption
trap 'echo -e "\n⚠️ Demo interrupted. Run: python3 run_demo.py --stop"; exit 1' INT TERM

# Run main function
main "$@" 