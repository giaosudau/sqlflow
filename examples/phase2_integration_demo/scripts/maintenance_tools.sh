#!/bin/bash

# SQLFlow Phase 2 Demo - Maintenance Tools
# Consolidated utility functions for demo maintenance

set -euo pipefail

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly NC='\033[0m'

show_help() {
    echo "SQLFlow Phase 2 Demo - Maintenance Tools"
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  fix-tables      Fix table conflicts (CREATE OR REPLACE TABLE)"
    echo "  fix-pgadmin     Reset pgAdmin permissions and restart"
    echo "  clean-database  Remove DuckDB database for fresh start"
    echo "  clean-output    Clean output directory"
    echo "  full-reset      Complete reset (database + output + restart)"
    echo "  check-health    Check all services health"
    echo "  help            Show this help message"
}

fix_table_conflicts() {
    echo -e "${CYAN}🔧 Fixing table conflicts in pipeline files...${NC}"
    
    local pipeline_files=(
        "pipelines/01_postgres_basic_test.sf"
        "pipelines/02_incremental_loading_test.sf"
        "pipelines/04_multi_connector_workflow.sf"
        "pipelines/05_resilient_postgres_test.sf"
        "pipelines/06_enhanced_s3_connector_demo.sf"
    )
    
    for file in "${pipeline_files[@]}"; do
        if [[ -f "$file" ]]; then
            echo -e "  📝 Checking $file..."
            
            # Only add CREATE OR REPLACE if not already present
            if grep -q "CREATE TABLE" "$file" && ! grep -q "CREATE OR REPLACE TABLE" "$file"; then
                sed -i '' 's/CREATE TABLE/CREATE OR REPLACE TABLE/g' "$file"
                echo -e "  ${GREEN}✅ Fixed $file${NC}"
            else
                echo -e "  ${GREEN}✅ $file already uses CREATE OR REPLACE TABLE${NC}"
            fi
        else
            echo -e "  ${YELLOW}⚠️ File not found: $file${NC}"
        fi
    done
    
    echo -e "${GREEN}✅ Table conflicts fix completed${NC}"
}

fix_pgadmin() {
    echo -e "${CYAN}🔧 Fixing pgAdmin permission issues...${NC}"
    
    echo -e "📋 Stopping pgAdmin container..."
    docker compose stop pgadmin
    
    echo -e "📋 Removing pgAdmin volume..."
    docker volume rm sqlflow_pgadmin_data 2>/dev/null || echo "Volume already removed or doesn't exist"
    
    echo -e "📋 Starting pgAdmin with fresh volume..."
    docker compose up -d pgadmin
    
    echo -e "${GREEN}✅ pgAdmin fix applied!${NC}"
    echo -e "${YELLOW}💡 Wait 30-60 seconds for pgAdmin to initialize${NC}"
    echo -e "${CYAN}🌐 Access pgAdmin at: http://localhost:8080${NC}"
    echo -e "${CYAN}🔑 Login: admin@sqlflow.com / sqlflow123${NC}"
}

clean_database() {
    echo -e "${CYAN}🗑️ Cleaning DuckDB database...${NC}"
    
    if [[ -f "target/demo.duckdb" ]]; then
        rm -f target/demo.duckdb*
        echo -e "${GREEN}✅ DuckDB database removed${NC}"
    else
        echo -e "${YELLOW}ℹ️ No DuckDB database found${NC}"
    fi
}

clean_output() {
    echo -e "${CYAN}🗑️ Cleaning output directory...${NC}"
    
    if [[ -d "output" ]]; then
        rm -f output/*.csv
        echo -e "${GREEN}✅ Output directory cleaned${NC}"
    else
        echo -e "${YELLOW}ℹ️ No output directory found${NC}"
    fi
}

check_health() {
    echo -e "${CYAN}🏥 Checking services health...${NC}"
    
    # Check PostgreSQL
    if docker compose exec -T postgres pg_isready -U postgres -d postgres > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL is healthy${NC}"
    else
        echo -e "${RED}❌ PostgreSQL is not responding${NC}"
    fi
    
    # Check MinIO
    if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo -e "${GREEN}✅ MinIO is healthy${NC}"
    else
        echo -e "${RED}❌ MinIO is not responding${NC}"
    fi
    
    # Check SQLFlow service
    if docker compose exec -T sqlflow python3 -c "import sqlflow; print('OK')" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ SQLFlow service is healthy${NC}"
    else
        echo -e "${RED}❌ SQLFlow service is not responding${NC}"
    fi
}

full_reset() {
    echo -e "${CYAN}🔄 Performing full reset...${NC}"
    
    echo -e "1️⃣ Stopping services..."
    docker compose down --volumes --remove-orphans
    
    echo -e "2️⃣ Cleaning database..."
    clean_database
    
    echo -e "3️⃣ Cleaning output..."
    clean_output
    
    echo -e "4️⃣ Starting services..."
    docker compose up -d
    
    echo -e "5️⃣ Waiting for services..."
    sleep 10
    check_health
    
    echo -e "${GREEN}✅ Full reset completed${NC}"
}

# Main command handling
case "${1:-help}" in
    fix-tables)
        fix_table_conflicts
        ;;
    fix-pgadmin)
        fix_pgadmin
        ;;
    clean-database)
        clean_database
        ;;
    clean-output)
        clean_output
        ;;
    full-reset)
        full_reset
        ;;
    check-health)
        check_health
        ;;
    help|*)
        show_help
        ;;
esac
