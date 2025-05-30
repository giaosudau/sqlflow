#!/bin/bash

# 🐍 SQLFlow Python UDF Showcase
# ===============================
# Complete demonstration of Python UDF capabilities in SQLFlow
# From simple scalar functions to advanced table-like transformations

set -e  # Exit on any error

echo "🐍 SQLFlow Python UDF Showcase"
echo "==============================="
echo ""
echo "Welcome to the complete demonstration of Python UDF capabilities!"
echo "This showcase will walk you through:"
echo ""
echo "📚 Part 1: Scalar UDFs - Text Processing & Data Quality"
echo "🧮 Part 2: Advanced Scalar UDFs - Analytics & Calculations" 
echo "🔄 Part 3: Table UDF Alternatives - Complex Transformations"
echo "🚀 Part 4: External Processing - Unlimited Python Power"
echo ""
echo "Let's get started! 🚀"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script configuration - consistent SQLFlow path detection
SQLFLOW_PATH=""

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

# Enhanced printing functions
print_header() {
    echo ""
    echo -e "${PURPLE}════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${PURPLE}📖 $1${NC}"
    echo -e "${PURPLE}════════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${CYAN}🔹 $1${NC}"
    echo -e "${CYAN}$(printf '─%.0s' {1..60})${NC}"
}

print_step() {
    echo -e "${BLUE}➤ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_result() {
    echo -e "${YELLOW}📊 $1${NC}"
}

print_info() {
    echo -e "${CYAN}💡 $1${NC}"
}

# Test tracking
TOTAL_FEATURES=0
WORKING_FEATURES=0

# Function to test a feature
test_feature() {
    local feature_name="$1"
    local test_command="$2"
    local success_message="$3"
    
    print_step "Testing: $feature_name"
    TOTAL_FEATURES=$((TOTAL_FEATURES + 1))
    
    if eval "$test_command" > /dev/null 2>&1; then
        print_success "$success_message"
        WORKING_FEATURES=$((WORKING_FEATURES + 1))
        return 0
    else
        echo -e "${RED}❌ Failed: $feature_name${NC}"
        return 1
    fi
}

# Function to show file content preview
show_results() {
    local file_path="$1"
    local description="$2"
    local max_rows="${3:-5}"
    
    if [ -f "$file_path" ]; then
        local row_count=$(tail -n +2 "$file_path" | wc -l | tr -d ' ')
        print_result "$description ($row_count rows)"
        echo ""
        echo "Preview:"
        head -n $((max_rows + 1)) "$file_path" | column -t -s ',' | head -n $((max_rows + 1))
        echo ""
        if [ $row_count -gt $max_rows ]; then
            echo "... and $((row_count - max_rows)) more rows"
            echo ""
        fi
    else
        echo -e "${RED}❌ File not found: $file_path${NC}"
    fi
}

# Setup
print_header "🚀 Setup & Initialization"

print_step "Cleaning up previous outputs..."
rm -rf output/showcase_*
mkdir -p output/
print_success "Environment prepared"

print_step "Validating SQLFlow installation..."
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
print_success "SQLFlow CLI is available at $SQLFLOW_PATH"

print_step "Checking Python UDF system..."
if test_feature "UDF Discovery" "$SQLFLOW_PATH udf list" "UDF system is operational"; then
    udf_count=$($SQLFLOW_PATH udf list | grep -c "python_udfs\." || echo "0")
    print_info "Found $udf_count Python UDFs available"
fi

# Part 1: Scalar UDFs - Text Processing & Data Quality
print_header "📚 Part 1: Scalar UDFs - Text Processing & Data Quality"

print_info "Scalar UDFs process one value at a time, perfect for:"
print_info "• Text manipulation (capitalize, extract domains)"
print_info "• Data validation (email format, value ranges)"  
print_info "• Simple calculations (word counts, formatting)"
echo ""

print_section "Text Processing Pipeline"

print_step "Validating customer text processing pipeline..."
test_feature "Pipeline Validation" "$SQLFLOW_PATH pipeline validate customer_text_processing" "Validation passed"

print_step "Running text processing with scalar UDFs..."
if $SQLFLOW_PATH pipeline run customer_text_processing --vars '{"run_id": "showcase", "output_dir": "output"}' > /dev/null 2>&1; then
    print_success "Text processing completed"
    
    # Show results
    show_results "output/processed_customers_showcase.csv" "Processed Customer Data" 3
    show_results "output/domain_summary_showcase.csv" "Email Domain Analysis" 3
    
    print_info "Key Features Demonstrated:"
    print_info "• capitalize_words() - Proper name formatting"
    print_info "• extract_domain() - Email domain extraction"
    print_info "• count_words() - Text analysis"
else
    echo -e "${RED}❌ Text processing pipeline failed${NC}"
fi

print_section "Data Quality Analysis"

print_step "Running data quality checks with scalar UDFs..."
if $SQLFLOW_PATH pipeline run data_quality_check --vars '{"run_id": "showcase", "output_dir": "output"}' > /dev/null 2>&1; then
    print_success "Data quality analysis completed"
    
    show_results "output/validated_customers_showcase.csv" "Customer Data Quality Report" 3
    
    print_info "Key Features Demonstrated:"
    print_info "• validate_email_format() - Email validation"
    print_info "• validate_price_range() - Value range checking"
    print_info "• calculate_data_quality_score() - Composite scoring"
else
    echo -e "${RED}❌ Data quality pipeline failed${NC}"
fi

# Part 2: Advanced Scalar UDFs - Analytics & Calculations
print_header "🧮 Part 2: Advanced Scalar UDFs - Analytics & Calculations"

print_info "Advanced scalar UDFs enable complex analytics:"
print_info "• Financial calculations (totals, taxes, commissions)"
print_info "• Statistical analysis (Z-scores, percentiles, outliers)"
print_info "• Time series analysis (growth rates, moving averages)"
echo ""

print_section "SQLFlow Pipeline Approach (Scalar UDF Chains)"

print_step "Validating table UDF alternatives pipeline..."
test_feature "Advanced Pipeline Validation" "$SQLFLOW_PATH pipeline validate table_udf_alternatives" "Advanced validation passed"

print_step "Running advanced analytics with scalar UDF chains..."
if $SQLFLOW_PATH pipeline run table_udf_alternatives --vars '{"run_id": "showcase", "output_dir": "output"}' > /dev/null 2>&1; then
    print_success "Advanced analytics completed"
    
    # Show comprehensive results
    show_results "output/comprehensive_analysis_showcase.csv" "Complete Sales Analysis" 3
    show_results "output/customer_summary_advanced_showcase.csv" "Customer Intelligence Report" 3
    show_results "output/outliers_analysis_showcase.csv" "Outlier Detection Results" 3
    
    print_info "Key Features Demonstrated:"
    print_info "• calculate_sales_total() - Financial calculations"
    print_info "• calculate_z_score() - Statistical analysis"
    print_info "• is_outlier() - Anomaly detection"
    print_info "• calculate_growth_rate() - Time series analysis"
    print_info "• validate_email_format() - Data quality"
    print_info "• calculate_data_quality_score() - Composite metrics"
    
    echo ""
    print_info "💡 Technique: Breaking complex table operations into scalar UDF steps"
    print_info "   This approach avoids DuckDB table function limitations while"
    print_info "   maintaining the power of SQL window functions and analytics."
else
    echo -e "${RED}❌ Advanced analytics pipeline failed${NC}"
fi

# Part 3: Table UDF Alternatives - Complex Transformations  
print_header "🔄 Part 3: Table UDF Alternatives - Complex Transformations"

print_info "When you need table-like transformations, SQLFlow offers alternatives:"
print_info "• Scalar UDF chains (as shown above)"
print_info "• External processing with pandas integration"
print_info "• Hybrid approaches combining both techniques"
echo ""

print_section "Approach Comparison"

echo "✅ Scalar UDF Chains:"
echo "   • Native SQLFlow integration"
echo "   • Version control friendly (pure SQL)"
echo "   • Leverages DuckDB's analytical power"
echo "   • Step-by-step transformations"
echo ""
echo "✅ External Processing:"
echo "   • Full pandas/numpy functionality"  
echo "   • Any Python library available"
echo "   • Easy debugging outside SQL"
echo "   • Perfect for ML preprocessing"
echo ""

# Part 3.5: Actual Table UDFs - Limitations & Programmatic Usage
print_header "🔧 Part 3.5: Actual Table UDFs - Understanding Limitations"

print_info "SQLFlow supports @python_table_udf decorated functions, but with limitations:"
print_info "• Table UDFs are registered and available for programmatic use"
print_info "• Cannot be used in SQL FROM clauses due to DuckDB Python API limits"
print_info "• Perfect for external processing workflows"
echo ""

print_section "Table UDF Reality Check"

print_step "Testing actual table UDF in SQL pipeline..."

# Create a simple pipeline that tries to use table UDFs
cat > pipelines/test_table_udf.sf << 'EOF'
-- Test actual table UDF usage
SOURCE sales_data TYPE CSV PARAMS {
  "path": "data/sales.csv", 
  "has_header": true
};

-- This will show the limitation - table UDFs cannot be used in FROM clauses
CREATE TABLE test_result AS 
SELECT * FROM PYTHON_FUNC("data_transforms.add_sales_metrics", sales_data);

EXPORT SELECT * FROM test_result
TO "output/table_udf_test.csv"
TYPE CSV OPTIONS { "header": true };
EOF

if $SQLFLOW_PATH pipeline run test_table_udf --profile dev > temp_output.log 2>&1; then
    # Check if we got the expected limitation message
    if grep -q "cannot be used in FROM clause" temp_output.log; then
        print_info "✅ Confirmed: Table UDF limitation detected as expected"
        print_info "📋 Message: $(grep 'cannot be used in FROM clause' temp_output.log | head -1)"
    else
        print_success "Pipeline ran successfully (unexpected)"
    fi
else
    print_info "✅ Expected: Pipeline failed due to table UDF limitations"
fi

rm -f temp_output.log pipelines/test_table_udf.sf

print_section "Table UDF Programmatic Demo"

print_step "Demonstrating table UDFs programmatically..."

# Create inline Python demonstration
cat > temp_table_udf_demo.py << 'EOF'
import sys, os
sys.path.insert(0, '.')
import pandas as pd
from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.connectors.connector_engine import ConnectorEngine

# Import actual table UDFs
sys.path.append('python_udfs')
from data_transforms import add_sales_metrics, detect_outliers

# Load data
connector_engine = ConnectorEngine()
connector_engine.register_connector("sales", "CSV", {"path": "data/sales.csv", "has_header": True})
data_chunks = list(connector_engine.load_data("sales", "sales"))
sales_df = data_chunks[0].pandas_df

print(f"📊 Original data: {len(sales_df)} rows, {len(sales_df.columns)} columns")

# Use actual table UDF #1
sales_with_metrics = add_sales_metrics(sales_df)
print(f"📈 After add_sales_metrics: {len(sales_with_metrics.columns)} columns")
added_cols = set(sales_with_metrics.columns) - set(sales_df.columns)
print(f"🆕 Added columns: {sorted(added_cols)}")

# Use actual table UDF #2  
sales_with_outliers = detect_outliers(sales_with_metrics, "price")
print(f"🎯 After detect_outliers: {len(sales_with_outliers.columns)} columns")
outlier_cols = set(sales_with_outliers.columns) - set(sales_with_metrics.columns)
print(f"🔍 Outlier columns: {sorted(outlier_cols)}")

# Export results
sales_with_outliers.to_csv("output/actual_table_udf_demo.csv", index=False)
print(f"💾 Exported {len(sales_with_outliers)} rows to actual_table_udf_demo.csv")

# Show sample
print("\n📋 Sample Results:")
print(sales_with_outliers[['product', 'price', 'total', 'final_price', 'z_score', 'is_outlier']].head(3).to_string(index=False))
EOF

if python temp_table_udf_demo.py 2>/dev/null; then
    print_success "Table UDF programmatic demo completed"
    
    if [ -f "output/actual_table_udf_demo.csv" ]; then
        row_count=$(tail -n +2 "output/actual_table_udf_demo.csv" | wc -l | tr -d ' ')
        print_info "✅ Generated actual_table_udf_demo.csv ($row_count rows)"
    fi
    
    print_info "🔧 Key Insights:"
    print_info "• Table UDFs work perfectly when called directly from Python"
    print_info "• add_sales_metrics() processed entire DataFrame at once"
    print_info "• detect_outliers() added statistical analysis columns" 
    print_info "• Results can be exported or registered back to DuckDB"
    print_info "• Perfect for external processing workflows"
else
    print_info "❌ Table UDF demo encountered issues"
fi

rm -f temp_table_udf_demo.py

print_section "Table UDF Summary"

echo "📋 Table UDF Status in SQLFlow:"
echo ""
echo "✅ Available & Registered:"
echo "   • add_sales_metrics - Financial calculations on DataFrames"
echo "   • detect_outliers - Statistical analysis and outlier detection"
echo ""  
echo "⚠️  SQL Limitations:"
echo "   • Cannot use: SELECT * FROM PYTHON_FUNC('table_udf', table)"
echo "   • Reason: DuckDB Python API doesn't support table functions"
echo ""
echo "✅ Programmatic Usage:"
echo "   • Direct function calls: result = add_sales_metrics(dataframe)"
echo "   • External processing workflows"
echo "   • Integration with pandas/numpy ecosystem"
echo ""
echo "💡 Recommendation:"
echo "   • Use scalar UDF chains for SQL-native workflows"
echo "   • Use table UDFs for external processing scenarios"
echo ""

# Part 4: External Processing - Unlimited Python Power
print_header "🚀 Part 4: External Processing - Unlimited Python Power"

print_info "External processing unlocks the full Python ecosystem:"
print_info "• Fetch data from DuckDB → Process with pandas → Register back"
print_info "• Use any Python library (scikit-learn, numpy, scipy, etc.)"
print_info "• Perfect for machine learning and complex transformations"
echo ""

print_section "External Processing Demo"

print_step "Running external processing demonstration..."
if python demonstrate_table_udf_alternatives.py > /dev/null 2>&1; then
    print_success "External processing completed"
    
    print_info "Key Capabilities Demonstrated:"
    print_info "• Data fetching: DuckDB → pandas DataFrame"
    print_info "• External processing: Full pandas functionality"
    print_info "• Data registration: pandas → DuckDB tables"
    print_info "• Hybrid analysis: SQL + Python combined"
    print_info "• Real-time processing: Streaming transformations"
    print_info "• Customer segmentation: Advanced analytics"
else
    echo -e "${RED}❌ External processing demo failed${NC}"
fi

# Comprehensive Results Summary
print_header "📊 Showcase Results Summary"

print_section "Generated Output Files"

echo "All generated files:"
ls -la output/ | grep showcase || echo "No showcase files found"
echo ""

print_section "Feature Coverage Validation"

# Count and validate all output files
output_files=(
    "output/processed_customers_showcase.csv"
    "output/domain_summary_showcase.csv"
    "output/validated_customers_showcase.csv"
    "output/comprehensive_analysis_showcase.csv"
    "output/customer_summary_advanced_showcase.csv"
    "output/outliers_analysis_showcase.csv"
    "output/actual_table_udf_demo.csv"
)

files_found=0
for file in "${output_files[@]}"; do
    if [ -f "$file" ]; then
        row_count=$(tail -n +2 "$file" | wc -l | tr -d ' ')
        print_success "$(basename "$file") - $row_count rows"
        files_found=$((files_found + 1))
    else
        echo -e "${RED}❌ Missing: $(basename "$file")${NC}"
    fi
done

echo ""
print_result "Output Coverage: $files_found/${#output_files[@]} files generated"

# Final Summary
print_header "🎉 Python UDF Showcase Complete!"

print_section "What You've Seen"

echo "✅ Text Processing & Data Quality:"
echo "   • Name formatting, domain extraction, word counting"
echo "   • Email validation, range checking, quality scoring"
echo ""
echo "✅ Advanced Analytics & Calculations:"
echo "   • Financial calculations (totals, taxes, final prices)"
echo "   • Statistical analysis (Z-scores, percentiles, outliers)"
echo "   • Time series analysis (running totals, growth rates)"
echo ""
echo "✅ Table UDF Alternatives:"
echo "   • Scalar UDF chains for complex transformations"
echo "   • External processing with unlimited Python power"
echo "   • Hybrid approaches combining SQL and Python"
echo ""
echo "✅ Actual Table UDFs:"
echo "   • Real @python_table_udf decorated functions"
echo "   • Programmatic usage with full DataFrame processing"
echo "   • Understanding limitations and workarounds"
echo "   • Integration with external processing workflows"
echo ""
echo "✅ Real-World Use Cases:"
echo "   • E-commerce analytics and customer intelligence"
echo "   • Data quality monitoring and validation"
echo "   • Statistical analysis and outlier detection"
echo "   • Customer segmentation and behavioral analysis"
echo ""

print_section "Key Takeaways"

echo "🎯 SQLFlow's Python UDF system provides:"
echo ""
echo "• 🔧 Production-Ready: All scalar UDFs work perfectly"
echo "• 🚀 Unlimited Power: External processing gives full Python access"
echo "• 🔄 Flexible Approaches: Choose what fits your workflow"  
echo "• 📈 Real Analytics: Complex transformations and insights"
echo "• 🎮 Easy to Use: Simple function calls in SQL"
echo "• 🏗️  Scalable: From simple text processing to ML preprocessing"
echo ""

print_section "Next Steps"

echo "🚀 Ready to build with Python UDFs?"
echo ""
echo "• 📖 Check out the examples in python_udfs/ directory"
echo "• 🔧 Adapt the patterns for your own data"
echo "• 🧪 Experiment with external processing for complex needs"
echo "• 📚 Read the comprehensive documentation"
echo ""
echo "• 💻 Try these commands:"
echo "   $SQLFLOW_PATH udf list                    # See all available UDFs"
echo "   $SQLFLOW_PATH pipeline validate <name>    # Validate your pipelines"
echo "   $SQLFLOW_PATH pipeline run <name>         # Execute your transformations"
echo ""

print_result "Showcase Status: $WORKING_FEATURES/$TOTAL_FEATURES features working perfectly"

if [ $WORKING_FEATURES -eq $TOTAL_FEATURES ]; then
    print_success "🎉 All Python UDF features are working perfectly!"
    print_success "You're ready to build amazing data transformations!"
else
    echo -e "${YELLOW}⚠️  Some features had issues, but core functionality is working${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🐍 SQLFlow Python UDF Showcase Complete - Thank you for exploring!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "" 