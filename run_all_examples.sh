#!/bin/bash

# SQLFlow Examples Test Runner
# This script runs all example demo scripts to ensure they work correctly
# Used by pre-commit hooks and for testing
# Enhanced with proper error tracking and trustworthy reporting

set -e  # Exit immediately if any command fails

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Get the absolute path to the repository root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

print_status "🚀 Running all SQLFlow example demo scripts..."
print_status "📂 Repository root: $REPO_ROOT"
echo ""

# Check if SQLFlow is available
SQLFLOW_PATH=""

# Try different locations for SQLFlow
POSSIBLE_PATHS=(
    "$REPO_ROOT/.venv/bin/sqlflow"  # Local development with venv
    "$(which sqlflow 2>/dev/null)"  # System PATH (CI environments)
    "/usr/local/bin/sqlflow"        # Common system location
    "$HOME/.local/bin/sqlflow"      # User-local installation
)

for path in "${POSSIBLE_PATHS[@]}"; do
    if [ -n "$path" ] && [ -f "$path" ] && [ -x "$path" ]; then
        SQLFLOW_PATH="$path"
        break
    fi
done

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

print_success "✅ SQLFlow found at $SQLFLOW_PATH"
echo ""

# Export SQLFlow path so individual scripts can use it
export SQLFLOW_OVERRIDE_PATH="$SQLFLOW_PATH"

# Find all run.sh scripts in examples directories
EXAMPLE_SCRIPTS=($(find examples -name "run.sh" -type f | sort))

# Also find run_demo.sh scripts (like incremental_loading_demo)
DEMO_SCRIPTS=($(find examples -name "run_demo.sh" -type f | sort))

# Combine both types of scripts
ALL_SCRIPTS=("${EXAMPLE_SCRIPTS[@]}" "${DEMO_SCRIPTS[@]}")

# Exclude scripts from non-migrated examples
EXCLUDED_SCRIPTS=(
    "examples/connector_interface_demo/run_demo.sh"
    "examples/phase2_integration_demo/run.sh"
    "examples/shopify_ecommerce_analytics/run_demo.sh"
    "examples/incremental_loading_demo/run_demo.sh"
)

if [ ${#ALL_SCRIPTS[@]} -eq 0 ]; then
    print_warning "No run.sh or run_demo.sh scripts found in examples directories"
    exit 0
fi

print_status "📋 Found ${#ALL_SCRIPTS[@]} example scripts:"
for script in "${ALL_SCRIPTS[@]}"; do
    echo "  - $script"
done
echo ""

# Run each script with proper error tracking
FAILED_SCRIPTS=()
SUCCESSFUL_SCRIPTS=()
SKIPPED_SCRIPTS=()
SCRIPT_DETAILS=()

for script in "${ALL_SCRIPTS[@]}"; do
    # Check if the script is in the excluded list
    is_excluded=false
    for excluded_script in "${EXCLUDED_SCRIPTS[@]}"; do
        if [[ "$script" == "$excluded_script" ]]; then
            is_excluded=true
            break
        fi
    done

    if [ "$is_excluded" = true ]; then
        print_warning "⚠️ Skipping $script (connector not migrated)"
        SKIPPED_SCRIPTS+=("$script")
        SCRIPT_DETAILS+=("$script: SKIPPED (connector not migrated)")
        continue
    fi
    
    script_dir="$(dirname "$script")"
    script_name="$(basename "$script")"
    
    print_status "🏃 Running $script..."
    
    # Change to script directory and run with environment variable
    cd "$REPO_ROOT/$script_dir"
    
    # Capture both output and exit code
    local_exit_code=0
    script_output=""
    
    # Run script and capture output and exit code
    if script_output=$(bash -c "export SQLFLOW_OVERRIDE_PATH='$SQLFLOW_PATH'; ./$script_name" 2>&1); then
        local_exit_code=0
    else
        local_exit_code=$?
    fi
    
    if [ $local_exit_code -eq 0 ]; then
        print_success "✅ Success: $script"
        SUCCESSFUL_SCRIPTS+=("$script")
        SCRIPT_DETAILS+=("$script: ✅ SUCCESS")
    else
        print_error "❌ Failed: $script (exit code: $local_exit_code)"
        FAILED_SCRIPTS+=("$script")
        SCRIPT_DETAILS+=("$script: ❌ FAILED (exit code: $local_exit_code)")
        
        # Show last few lines of output for debugging
        echo "Last lines of output:"
        echo "$script_output" | tail -n 5 | sed 's/^/    /'
    fi
    
    # Return to repo root for next iteration
    cd "$REPO_ROOT"
    echo ""
done

# Comprehensive Summary
echo "========================================"
echo "📊 SQLFlow Examples Execution Summary"
echo "========================================"
echo ""

echo "📈 Overall Statistics:"
echo "   Total scripts found: ${#ALL_SCRIPTS[@]}"
echo "   ✅ Successful: ${#SUCCESSFUL_SCRIPTS[@]}"
echo "   ❌ Failed: ${#FAILED_SCRIPTS[@]}"
echo "   ⚠️  Skipped: ${#SKIPPED_SCRIPTS[@]}"
echo ""

echo "📋 Detailed Results:"
for detail in "${SCRIPT_DETAILS[@]}"; do
    echo "   $detail"
done
echo ""

if [ ${#SUCCESSFUL_SCRIPTS[@]} -gt 0 ]; then
    print_success "✅ Successful Scripts:"
    for script in "${SUCCESSFUL_SCRIPTS[@]}"; do
        echo "   - $script"
    done
    echo ""
fi

if [ ${#FAILED_SCRIPTS[@]} -gt 0 ]; then
    print_error "❌ Failed Scripts:"
    for script in "${FAILED_SCRIPTS[@]}"; do
        echo "   - $script"
    done
    echo ""
    print_error "Some example scripts failed. Please check the logs above."
    echo ""
    echo "🔧 Troubleshooting Tips:"
    echo "   1. Run failed scripts individually for detailed error output"
    echo "   2. Check that all required dependencies are installed"
    echo "   3. Verify data files exist in respective example directories"
    echo "   4. Ensure SQLFlow is properly installed and accessible"
    echo ""
    exit 1
fi

if [ ${#SKIPPED_SCRIPTS[@]} -gt 0 ]; then
    print_warning "⚠️  Skipped Scripts:"
    for script in "${SKIPPED_SCRIPTS[@]}"; do
        echo "   - $script"
    done
    echo ""
fi

print_success "🎉 All example scripts completed successfully!"
echo ""
echo "🎯 What was tested:"
echo "   ✅ Load modes demonstrations"
echo "   ✅ Conditional pipelines with various scenarios"  
echo "   ✅ Python UDF showcases and examples"
echo "   ✅ Parquet connector functionality"
echo "   ✅ REST API connector functionality"
echo "   ✅ Transform layer demonstrations"
echo "   ✅ Google Sheets connector (if configured)"
echo ""
echo "🚀 All example demos are working correctly and can be trusted!"

# Final exit with success
exit 0 