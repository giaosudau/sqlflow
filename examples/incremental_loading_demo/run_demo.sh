#!/bin/bash

# SQLFlow Phase 1 Demo Runner
# This script demonstrates the Phase 1 enhanced features:
# - Incremental loading with watermarks
# - Industry-standard parameters  
# - Enhanced debugging infrastructure
# - Error recovery mechanisms

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Demo configuration
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQLFLOW_ROOT="$(cd "$DEMO_DIR/../.." && pwd)"
OUTPUT_DIR="$DEMO_DIR/output"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   SQLFlow Phase 1 Features Demo${NC}"
echo -e "${BLUE}============================================${NC}"
echo
echo "🚀 Demonstrating Phase 1 enhanced features:"
echo "   • CREATE OR REPLACE TABLE support"
echo "   • Industry-standard parameters (sync_mode, primary_key, cursor_field)"
echo "   • Automatic watermark management"
echo "   • Enhanced debugging infrastructure"
echo

# Change to demo directory
cd "$DEMO_DIR"

# Clean previous runs
echo "🧹 Cleaning previous outputs..."
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Function to run pipeline and report status
run_pipeline() {
    local pipeline_name="$1"
    local pipeline_file="pipelines/${pipeline_name}.sf"
    
    echo -n "📦 Running $pipeline_name... "
    
    if sqlflow pipeline run "$pipeline_file" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ SUCCESS${NC}"
        return 0
    else
        echo -e "${RED}❌ FAILED${NC}"
        return 1
    fi
}

# Run all pipelines
echo "🔄 Executing pipelines:"
echo

TOTAL_PIPELINES=0
SUCCESSFUL_PIPELINES=0

# Pipeline 1: Basic incremental loading
TOTAL_PIPELINES=$((TOTAL_PIPELINES + 1))
if run_pipeline "01_basic_incremental"; then
    SUCCESSFUL_PIPELINES=$((SUCCESSFUL_PIPELINES + 1))
fi

# Pipeline 2: Mixed sync modes  
TOTAL_PIPELINES=$((TOTAL_PIPELINES + 1))
if run_pipeline "02_mixed_sync_modes"; then
    SUCCESSFUL_PIPELINES=$((SUCCESSFUL_PIPELINES + 1))
fi

# Pipeline 4: E-commerce analytics
TOTAL_PIPELINES=$((TOTAL_PIPELINES + 1))
if run_pipeline "04_ecommerce_analytics"; then
    SUCCESSFUL_PIPELINES=$((SUCCESSFUL_PIPELINES + 1))
fi

echo
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   Demo Results${NC}"
echo -e "${BLUE}============================================${NC}"

if [ $SUCCESSFUL_PIPELINES -eq $TOTAL_PIPELINES ]; then
    echo -e "${GREEN}✅ All $TOTAL_PIPELINES pipelines completed successfully!${NC}"
    echo
    echo "📁 Output files generated in: $OUTPUT_DIR"
    echo
    exit 0
else
    FAILED_PIPELINES=$((TOTAL_PIPELINES - SUCCESSFUL_PIPELINES))
    echo -e "${RED}❌ $FAILED_PIPELINES out of $TOTAL_PIPELINES pipelines failed${NC}"
    echo -e "${GREEN}✅ $SUCCESSFUL_PIPELINES pipelines succeeded${NC}"
    echo
    exit 1
fi 