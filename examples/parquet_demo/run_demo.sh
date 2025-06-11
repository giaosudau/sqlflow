#!/bin/bash

# Parquet Connector Demo Script
# Enhanced with proper error handling and exit code checking

set -e  # Exit immediately if any command fails

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo "🚀 Starting Parquet Connector Demo..."

# Create output directory and clean previous runs
mkdir -p output

# Clean previous database to avoid "table already exists" errors
rm -f output/parquet_demo.duckdb

# Generate sample data if it doesn't exist
if [ ! -f "data/sales.parquet" ]; then
    echo "📊 Creating sample Parquet data..."
    python3 create_sample_data.py
fi

# Track pipeline results
PIPELINE_RESULTS=()
FAILED_PIPELINES=()

echo "📊 Running Parquet data processing pipelines..."

# Run the sales analysis pipeline
echo "📈 Analyzing sales data..."
if sqlflow pipeline run analyze_sales --profile dev; then
    print_success "Sales analysis pipeline completed successfully"
    PIPELINE_RESULTS+=("analyze_sales:success")
else
    print_error "Sales analysis pipeline failed"
    PIPELINE_RESULTS+=("analyze_sales:failed")
    FAILED_PIPELINES+=("analyze_sales")
fi

# Run the monthly data processing pipeline
echo "📅 Processing monthly data with pattern matching..."
if sqlflow pipeline run process_monthly_data --profile dev; then
    print_success "Monthly data processing pipeline completed successfully"
    PIPELINE_RESULTS+=("process_monthly_data:success")
else
    print_error "Monthly data processing pipeline failed"
    PIPELINE_RESULTS+=("process_monthly_data:failed")
    FAILED_PIPELINES+=("process_monthly_data")
fi

# Check overall results
if [ ${#FAILED_PIPELINES[@]} -eq 0 ]; then
    print_success "Demo completed successfully!"
    echo "📁 Check the output/ directory for results"
    echo "🗄️  Database file: output/parquet_demo.duckdb"

    # Display some results if the pipelines ran successfully
    if [ -f "output/monthly_sales_summary.csv" ]; then
        echo ""
        echo "📋 Sample results from monthly sales summary:"
        head -n 5 output/monthly_sales_summary.csv
        
        echo ""
        echo "📊 Sample results from customer segment analysis:"
        if [ -f "output/customer_segment_analysis.csv" ]; then
            head -n 5 output/customer_segment_analysis.csv
        else
            print_warning "Customer segment analysis file not found"
        fi
    else
        echo ""
        echo "📋 Results available in DuckDB database:"
        echo "   sqlflow debug query 'SELECT * FROM sales_analysis LIMIT 5' --profile dev"
        echo "   sqlflow debug query 'SELECT * FROM monthly_trends LIMIT 5' --profile dev"
    fi

    echo ""
    print_success "🎉 Parquet connector demo complete!"
    echo "   Features demonstrated:"
    echo "   ✅ Single file reading with schema inference"
    echo "   ✅ Column selection for performance optimization"
    echo "   ✅ File pattern matching (data/sales_2024_*.parquet)"
    echo "   ✅ Multiple file combination"
    echo "   ✅ Complex analytics and joins"
    echo "   ✅ CSV export of results"
    
    exit 0
else
    print_error "Demo failed! ${#FAILED_PIPELINES[@]} pipeline(s) failed:"
    for pipeline in "${FAILED_PIPELINES[@]}"; do
        echo "  - $pipeline"
    done
    echo ""
    print_error "❌ Parquet connector demo failed!"
    echo "   Some features were not demonstrated due to pipeline failures."
    echo "   Check the error messages above for details."
    
    exit 1
fi 