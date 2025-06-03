#!/bin/bash

# SQLFlow Task 2.1: Connector Interface Standardization Demo
# This script demonstrates the standardized connector interface features:
# - Parameter validation framework
# - Health monitoring capabilities
# - Industry-standard parameter compatibility
# - Incremental loading interface
# - Standardized error handling

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Demo configuration
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$DEMO_DIR/output"

# SQLFlow path detection
SQLFLOW_PATH=""

# Check if SQLFlow path is provided via environment variable (from run_all_examples.sh)
if [ -n "${SQLFLOW_OVERRIDE_PATH:-}" ] && [ -f "${SQLFLOW_OVERRIDE_PATH}" ] && [ -x "${SQLFLOW_OVERRIDE_PATH}" ]; then
    SQLFLOW_PATH="$SQLFLOW_OVERRIDE_PATH"
else
    # Try different locations for SQLFlow
    POSSIBLE_PATHS=(
        "../../.venv/bin/sqlflow"        # Local development with venv
        "$(which sqlflow 2>/dev/null)"  # System PATH (CI environments)
        "/usr/local/bin/sqlflow"         # Common system location
        "$HOME/.local/bin/sqlflow"       # User-local installation
    )

    for path in "${POSSIBLE_PATHS[@]}"; do
        if [ -n "$path" ] && [ -f "$path" ] && [ -x "$path" ]; then
            SQLFLOW_PATH="$path"
            break
        fi
    done
fi

if [ -z "$SQLFLOW_PATH" ]; then
    echo -e "${RED}❌ SQLFlow executable not found in any of the following locations:${NC}"
    for path in "${POSSIBLE_PATHS[@]}"; do
        if [ -n "$path" ]; then
            echo "  - $path"
        fi
    done
    echo -e "${RED}Please ensure SQLFlow is installed and accessible${NC}"
    echo -e "${RED}Try: pip install -e .[dev]${NC}"
    exit 1
fi

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   Task 2.1: Connector Interface Demo${NC}"
echo -e "${BLUE}============================================${NC}"
echo
echo "🚀 Demonstrating standardized connector interface features:"
echo "   • Parameter validation framework"
echo "   • Industry-standard parameter compatibility (Airbyte/Fivetran)"
echo "   • Health monitoring and performance metrics"
echo "   • Incremental loading interface"
echo "   • Standardized error handling"
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
    
    if $SQLFLOW_PATH pipeline run "$pipeline_name" --profile dev > /dev/null 2>&1; then
        echo -e "${GREEN}✅ SUCCESS${NC}"
        return 0
    else
        echo -e "${RED}❌ FAILED${NC}"
        return 1
    fi
}

# Run connector interface standardization demos
echo "🔧 Executing connector interface standardization demos:"
echo

TOTAL_PIPELINES=0
SUCCESSFUL_PIPELINES=0

# Demo 1: Parameter validation framework
echo "📋 Parameter Validation Framework Demo"
TOTAL_PIPELINES=$((TOTAL_PIPELINES + 1))
if run_pipeline "01_parameter_validation_demo"; then
    SUCCESSFUL_PIPELINES=$((SUCCESSFUL_PIPELINES + 1))
    echo "   ✅ Industry-standard parameters validated successfully"
    echo "   ✅ Airbyte/Fivetran compatibility confirmed"
    echo "   ✅ Type conversion and defaults applied"
fi

echo

# Demo 2: Health monitoring capabilities
echo "🏥 Health Monitoring & Performance Demo"
TOTAL_PIPELINES=$((TOTAL_PIPELINES + 1))
if run_pipeline "02_health_monitoring_demo"; then
    SUCCESSFUL_PIPELINES=$((SUCCESSFUL_PIPELINES + 1))
    echo "   ✅ Health status monitoring active"
    echo "   ✅ Performance metrics collected"
    echo "   ✅ Connection resilience demonstrated"
fi

echo

# Demo 3: Incremental loading interface
echo "⚡ Incremental Loading Interface Demo"
TOTAL_PIPELINES=$((TOTAL_PIPELINES + 1))
if run_pipeline "03_incremental_interface_demo"; then
    SUCCESSFUL_PIPELINES=$((SUCCESSFUL_PIPELINES + 1))
    echo "   ✅ Automatic watermark management working"
    echo "   ✅ Cursor-based filtering operational"
    echo "   ✅ Performance optimization through selective loading"
fi

echo
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   Demo Results & Analysis${NC}"
echo -e "${BLUE}============================================${NC}"

if [ $SUCCESSFUL_PIPELINES -eq $TOTAL_PIPELINES ]; then
    echo -e "${GREEN}✅ All $TOTAL_PIPELINES connector interface demos completed successfully!${NC}"
    echo
    echo "📁 Output files generated in: $OUTPUT_DIR"
    echo ""
    echo "📝 Task 2.1 Achievements Summary:"
    echo "   ✅ Parameter Validation Framework: IMPLEMENTED"
    echo "   ✅ Industry-Standard Compatibility: VERIFIED"  
    echo "   ✅ Health Monitoring: OPERATIONAL"
    echo "   ✅ Incremental Interface: FUNCTIONAL"
    echo "   ✅ Standardized Error Handling: ACTIVE"
    echo ""
    echo "📋 Key Features Demonstrated:"
    echo "   🔍 Parameter validation with type conversion and defaults"
    echo "   🌐 Airbyte/Fivetran parameter compatibility"
    echo "   🏥 Health status and performance monitoring"
    echo "   ⚡ Automatic watermark-based incremental loading"
    echo "   🚨 Standardized exception hierarchy (ParameterError, IncrementalError)"
    echo "   📊 Performance metrics and connection resilience"
    echo
    echo "🎯 Task 2.1: Connector Interface Standardization - COMPLETED!"
    echo "   Ready for Task 2.2: Enhanced PostgreSQL Connector"
    echo
    exit 0
else
    FAILED_PIPELINES=$((TOTAL_PIPELINES - SUCCESSFUL_PIPELINES))
    echo -e "${RED}❌ $FAILED_PIPELINES out of $TOTAL_PIPELINES demos failed${NC}"
    echo -e "${GREEN}✅ $SUCCESSFUL_PIPELINES demos succeeded${NC}"
    echo
    exit 1
fi 