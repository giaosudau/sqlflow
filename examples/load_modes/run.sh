#!/bin/bash

# SQLFlow Load Modes Demo Runner
# This script validates, compiles, and runs all load modes demonstration pipelines
# Exits immediately if any step fails

set -e  # Exit immediately if any command fails
set -u  # Exit if undefined variable is used

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SQLFLOW_PATH=""
PROFILE="dev"

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

PIPELINES=("01_basic_load_modes" "02_multiple_merge_keys" "03_schema_compatibility")

# Function to print colored output
print_status() {
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

# Function to check if sqlflow is available
check_sqlflow() {
    print_status "Checking SQLFlow availability..."
    if [ -z "$SQLFLOW_PATH" ]; then
        print_error "SQLFlow executable not found in any of the following locations:"
        for path in "${POSSIBLE_PATHS[@]}"; do
            if [ -n "$path" ]; then
                echo "  - $path"
            fi
        done
        print_error "Please ensure SQLFlow is installed and accessible"
        print_error "Try: pip install -e .[dev]"
        exit 1
    fi
    print_success "SQLFlow found at $SQLFLOW_PATH"
}

# Function to create output directory
setup_environment() {
    print_status "Setting up environment..."
    mkdir -p output
    mkdir -p target/compiled
    print_success "Output directories created"
}

# Function to validate all pipelines
validate_pipelines() {
    print_status "Validating all pipelines..."
    echo "----------------------------------------"
    
    if $SQLFLOW_PATH pipeline validate; then
        print_success "All pipelines validated successfully"
    else
        print_error "Pipeline validation failed"
        exit 1
    fi
    echo "----------------------------------------"
}

# Function to compile all pipelines
compile_pipelines() {
    print_status "Compiling all pipelines..."
    echo "----------------------------------------"
    
    for pipeline in "${PIPELINES[@]}"; do
        print_status "Compiling $pipeline..."
        if $SQLFLOW_PATH pipeline compile "$pipeline" --profile "$PROFILE"; then
            print_success "Successfully compiled $pipeline"
        else
            print_error "Failed to compile $pipeline"
            exit 1
        fi
    done
    echo "----------------------------------------"
}

# Function to run all pipelines
run_pipelines() {
    print_status "Running all pipelines..."
    echo "----------------------------------------"
    
    for pipeline in "${PIPELINES[@]}"; do
        print_status "Running $pipeline..."
        if $SQLFLOW_PATH pipeline run "$pipeline" --profile "$PROFILE"; then
            print_success "Successfully executed $pipeline"
        else
            print_error "Failed to execute $pipeline"
            exit 1
        fi
        echo ""
    done
    echo "----------------------------------------"
}

# Function to show results
show_results() {
    print_status "Checking output files..."
    echo "----------------------------------------"
    
    if [ -d "output" ]; then
        echo "Generated output files:"
        ls -la output/ 2>/dev/null || echo "No output files found (running in memory mode)"
    else
        print_warning "Output directory not found"
    fi
    
    if [ -d "target/compiled" ]; then
        echo ""
        echo "Compiled pipeline plans:"
        ls -la target/compiled/ 2>/dev/null || echo "No compiled plans found"
    fi
    echo "----------------------------------------"
}

# Main execution function
main() {
    echo "========================================"
    echo "SQLFlow Load Modes Demo Runner"
    echo "========================================"
    echo ""
    
    # Check prerequisites
    check_sqlflow
    setup_environment
    
    # Run the demo pipeline
    validate_pipelines
    compile_pipelines
    run_pipelines
    show_results
    
    echo ""
    print_success "🎉 All load modes demonstrations completed successfully!"
    echo ""
    echo "What was demonstrated:"
    echo "  ✅ REPLACE mode - Create/replace tables"
    echo "  ✅ APPEND mode - Add records to existing tables" 
    echo "  ✅ MERGE mode - Update existing and insert new records"
    echo "  ✅ Multiple merge keys - Composite key merging"
    echo "  ✅ Schema compatibility - Cross-mode compatibility"
    echo ""
    echo "Next steps:"
    echo "  - Review the pipeline files in pipelines/"
    echo "  - Check compiled plans in target/compiled/"
    echo "  - Modify data files in data/ to test different scenarios"
    echo "  - Create your own pipelines using these patterns"
    echo ""
}

# Error handler
error_handler() {
    print_error "Demo failed at line $1"
    echo ""
    echo "Troubleshooting tips:"
    echo "  - Check that SQLFlow is properly installed"
    echo "  - Verify all data files exist in data/"
    echo "  - Run individual commands manually to debug"
    echo "  - Check pipeline files for syntax errors"
    exit 1
}

# Set error trap
trap 'error_handler $LINENO' ERR

# Run main function
main "$@" 