#!/bin/bash

# 🐍 SQLFlow Python UDF Showcase
# ===============================
# A comprehensive demonstration of Python UDF capabilities in SQLFlow,
# covering scalar functions, table functions, and advanced processing patterns.

set -e # Exit on any error, though we trap errors for logging.

# --- Configuration ---
# Enhanced output colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script configuration
SQLFLOW_PATH=""
LOG_DIR="logs"
RUN_TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# --- Test Tracking ---
TOTAL_FEATURES=0
FAILED_FEATURES=0
FAILED_TESTS=()

# --- Utility Functions ---

# Find the SQLFlow executable
locate_sqlflow() {
    if [ -n "${SQLFLOW_OVERRIDE_PATH:-}" ] && [ -f "${SQLFLOW_OVERRIDE_PATH}" ] && [ -x "${SQLFLOW_OVERRIDE_PATH}" ]; then
        SQLFLOW_PATH="$SQLFLOW_OVERRIDE_PATH"
        return
    fi
    local possible_paths=(
        "../../.venv/bin/sqlflow"
        "$(which sqlflow 2>/dev/null)"
        "/usr/local/bin/sqlflow"
        "$HOME/.local/bin/sqlflow"
    )
    for path in "${possible_paths[@]}"; do
        if [ -n "$path" ] && [ -f "$path" ] && [ -x "$path" ]; then
            SQLFLOW_PATH="$path"
            return
        fi
    done
}

# Printing functions for structured output
print_header() { echo -e "\n${PURPLE}════════════════════════════════════════════════════════════════════\n📖 $1\n════════════════════════════════════════════════════════════════════${NC}\n"; }
print_section() { echo -e "\n${CYAN}🔹 $1\n$(printf '─%.0s' {1..60})${NC}"; }
print_step() { echo -e "${BLUE}➤ $1${NC}"; }
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }
print_result() { echo -e "${YELLOW}📊 $1${NC}"; }
print_info() { echo -e "${CYAN}💡 $1${NC}"; }

# Core testing function
test_feature() {
    local feature_name="$1"
    local command="$2"
    local log_file="$LOG_DIR/${feature_name// /_}.log"

    print_step "Running: $feature_name"
    TOTAL_FEATURES=$((TOTAL_FEATURES + 1))

    # Execute the command, capturing output
    if output=$(eval "$command" 2>&1); then
        print_success "PASSED: $feature_name"
    else
        FAILED_FEATURES=$((FAILED_FEATURES + 1))
        FAILED_TESTS+=("$feature_name")
        print_error "FAILED: $feature_name"
        echo -e "${RED}  - Error log: $log_file${NC}"
        echo "--- ERROR LOG for '$feature_name' ---" > "$log_file"
        echo "Timestamp: $(date)" >> "$log_file"
        echo "Command: $command" >> "$log_file"
        echo "-------------------------------------" >> "$log_file"
        echo "$output" >> "$log_file"
    fi
}

# Function to show a preview of a result file
show_results() {
    local file_path="$1"
    local description="$2"
    local max_rows="${3:-5}"
    
    if [ -f "$file_path" ]; then
        local row_count
        row_count=$(tail -n +2 "$file_path" | wc -l | tr -d ' ')
        print_result "$description ($row_count rows)"
        echo "Preview:"
        head -n $((max_rows + 1)) "$file_path" | column -t -s ',' | head -n $((max_rows + 1))
        if [ "$row_count" -gt "$max_rows" ]; then
            echo "... and $((row_count - max_rows)) more rows"
        fi
        echo ""
    else
        print_error "File not found: $file_path"
    fi
}


# --- Main Execution ---

main() {
    print_header "🐍 SQLFlow Python UDF Showcase"
    
    # --- Setup ---
    print_section "Setup & Initialization"
    locate_sqlflow
    
    print_step "Cleaning up previous run..."
    rm -rf output showcase_*.csv "$LOG_DIR"
    mkdir -p output "$LOG_DIR"
    print_success "Environment cleaned."

    print_step "Validating SQLFlow installation..."
    if [ -z "$SQLFLOW_PATH" ]; then
        print_error "SQLFlow executable not found."
        echo -e "${RED}Please ensure SQLFlow is installed and accessible.${NC}"
        exit 1
    fi
    print_success "SQLFlow CLI found at: $SQLFLOW_PATH"

    test_feature "UDF System Check" "$SQLFLOW_PATH udf list"
    
    # --- Part 1: Scalar UDFs ---
    print_header "📚 Part 1: Scalar UDFs - Text & Data Quality"
    print_info "Scalar UDFs process one value at a time. Ideal for text manipulation, validation, and simple calculations."
    
    print_section "Text Processing Pipeline"
    test_feature "Validate Text Processing Pipeline" "$SQLFLOW_PATH pipeline validate customer_text_processing"
    test_feature "Run Text Processing Pipeline" "$SQLFLOW_PATH pipeline run customer_text_processing --vars '{\"run_id\": \"showcase\", \"output_dir\": \"output\"}'"
    show_results "output/processed_customers_showcase.csv" "Processed Customer Data"
    
    print_section "Data Quality Pipeline"
    test_feature "Validate Data Quality Pipeline" "$SQLFLOW_PATH pipeline validate data_quality_check"
    test_feature "Run Data Quality Pipeline" "$SQLFLOW_PATH pipeline run data_quality_check --vars '{\"run_id\": \"showcase\", \"output_dir\": \"output\"}'"
    show_results "output/validated_customers_showcase.csv" "Customer Data Quality Report"

    # --- Part 2: Advanced Scalar UDFs (Table UDF Alternatives) ---
    print_header "🧮 Part 2: Advanced Scalar UDFs for Complex Analytics"
    print_info "This pattern uses chains of scalar UDFs with SQL window functions to perform complex table-like operations."
    
    print_section "Advanced Analytics Pipeline"
    test_feature "Validate Advanced Analytics Pipeline" "$SQLFLOW_PATH pipeline validate table_udf_alternatives"
    test_feature "Run Advanced Analytics Pipeline" "$SQLFLOW_PATH pipeline run table_udf_alternatives --vars '{\"run_id\": \"showcase\", \"output_dir\": \"output\"}'"
    show_results "output/comprehensive_analysis_showcase.csv" "Comprehensive Sales Analysis"

    # --- Part 3: SQL-Native Table UDFs ---
    print_header "🔄 Part 3: SQL-Native Table UDFs"
    print_info "SQLFlow's core feature: executing table UDFs directly in SQL via the FROM clause."

    print_section "SQL-Native Table UDF Pipeline"
    test_feature "Validate Table UDF Pipeline" "$SQLFLOW_PATH pipeline validate table_udf_demo"
    test_feature "Run Table UDF Pipeline" "$SQLFLOW_PATH pipeline run table_udf_demo --vars '{\"run_id\": \"showcase\", \"output_dir\": \"output\"}'"
    show_results "output/table_udf_demo_result_showcase.csv" "SQL-Native Table UDF Results" 5

    # --- Part 4: Programmatic & External Processing ---
    print_header "🚀 Part 4: Advanced Python Processing"
    print_info "For complex logic, UDFs can be used programmatically or in external processing scripts."
    
    test_feature "Run Advanced Python Processing Script" "python demonstrate_advanced_processing.py"
    show_results "output/programmatic_udf_results.csv" "Programmatic Table UDF Results" 5
    show_results "output/external_processing_results.csv" "External Python Processing Results" 5

    # --- Summary ---
    print_header "📊 Showcase Summary"
    
    local working_features=$((TOTAL_FEATURES - FAILED_FEATURES))
    
    if [ "$FAILED_FEATURES" -gt 0 ]; then
        print_error "Execution Summary: $working_features / $TOTAL_FEATURES features passed."
        print_section "Failed Features"
        for test_name in "${FAILED_TESTS[@]}"; do
            echo -e "${RED}- $test_name${NC}"
        done
        echo -e "\n${YELLOW}Please review the logs in the '$LOG_DIR' directory for details.${NC}"
        exit 1
    else
        print_success "🎉 All $TOTAL_FEATURES features executed successfully!"
        print_section "Key Demonstrations"
        echo "✅ Scalar UDFs for data cleaning and quality."
        echo "✅ Advanced analytics using chained scalar UDFs."
        echo "✅ Core SQL-native table UDFs in FROM clauses."
        echo "✅ Programmatic and external Python processing."
        print_info "\nAll generated outputs can be found in the 'output/' directory."
    fi
}

# Run the script
main 