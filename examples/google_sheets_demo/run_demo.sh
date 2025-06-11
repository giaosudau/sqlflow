#!/bin/bash

# Google Sheets Connector Demo Script
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

echo "🚀 Starting Google Sheets Connector Demo..."

# Check if required environment variables are set
CREDENTIALS_AVAILABLE=true
if [ -z "$GOOGLE_SHEETS_CREDENTIALS_FILE" ]; then
    print_warning "GOOGLE_SHEETS_CREDENTIALS_FILE environment variable not set"
    echo "   Please set it to the path of your Google service account credentials JSON file"
    echo "   Example: export GOOGLE_SHEETS_CREDENTIALS_FILE=/path/to/service-account-key.json"
    CREDENTIALS_AVAILABLE=false
fi

if [ -z "$GOOGLE_SHEETS_SPREADSHEET_ID" ]; then
    print_warning "GOOGLE_SHEETS_SPREADSHEET_ID environment variable not set"
    echo "   Please set it to your Google Sheets spreadsheet ID"
    echo "   Example: export GOOGLE_SHEETS_SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    CREDENTIALS_AVAILABLE=false
fi

# Create output directory and clean previous runs
mkdir -p output

# Clean previous database to avoid "table already exists" errors
rm -f output/google_sheets_demo.duckdb

# Track pipeline results
PIPELINE_RESULTS=()
FAILED_PIPELINES=()

echo "📊 Running Google Sheets data processing pipeline..."

# Only run if credentials are provided
if [ "$CREDENTIALS_AVAILABLE" = true ]; then
    # Run the contact cleaning pipeline
    echo "🧹 Processing contact data..."
    if sqlflow pipeline run load_contacts --profile dev; then
        print_success "Contact loading pipeline completed successfully"
        PIPELINE_RESULTS+=("load_contacts:success")
    else
        print_error "Contact loading pipeline failed"
        PIPELINE_RESULTS+=("load_contacts:failed")
        FAILED_PIPELINES+=("load_contacts")
    fi

    # Run the order analysis pipeline
    echo "📈 Analyzing order data..."
    if sqlflow pipeline run analyze_orders --profile dev; then
        print_success "Order analysis pipeline completed successfully"
        PIPELINE_RESULTS+=("analyze_orders:success")
    else
        print_error "Order analysis pipeline failed"
        PIPELINE_RESULTS+=("analyze_orders:failed")
        FAILED_PIPELINES+=("analyze_orders")
    fi
else
    print_warning "Skipping pipeline execution - Google Sheets credentials not provided"
    echo "   This demo requires Google Sheets API setup to run the actual pipelines"
    echo "   See README.md for setup instructions"
    
    # For CI/testing purposes, we'll exit successfully if credentials aren't provided
    # since this is expected in most test environments
    print_info "Demo completed successfully (credentials not configured - expected in CI)"
    echo "📁 Check the output/ directory for results (when credentials are configured)"
    echo "🗄️  Database file: output/google_sheets_demo.duckdb"
    echo ""
    print_success "🎉 Google Sheets Connector Demo complete!"
    echo "   Features that would be demonstrated with proper credentials:"
    echo "   ✅ Google Sheets API integration"
    echo "   ✅ Contact data cleaning and validation"
    echo "   ✅ Order data analysis and aggregation"
    echo "   ✅ Real-time spreadsheet data processing"
    exit 0
fi

# Check overall results (only reached if credentials were available)
if [ ${#FAILED_PIPELINES[@]} -eq 0 ]; then
    print_success "Demo completed successfully!"
    echo "📁 Check the output/ directory for results"
    echo "🗄️  Database file: output/google_sheets_demo.duckdb"

    # Display some results if the pipelines ran successfully
    echo ""
    echo "📋 Sample results from clean_contacts table:"
    if sqlflow debug query "SELECT contact_name, email_address, company_name, contact_status FROM clean_contacts LIMIT 5" --profile dev; then
        echo ""
    else
        print_warning "Could not query clean_contacts table"
    fi
    
    echo "📊 Sample results from order_summary table:"
    if sqlflow debug query "SELECT order_month, COUNT(*) as customers, SUM(total_amount) as monthly_revenue FROM order_summary GROUP BY order_month ORDER BY order_month DESC LIMIT 5" --profile dev; then
        echo ""
    else
        print_warning "Could not query order_summary table"
    fi

    echo ""
    print_success "🎉 Google Sheets Connector Demo complete!"
    echo "   Features demonstrated:"
    echo "   ✅ Google Sheets API integration"
    echo "   ✅ Contact data cleaning and validation"
    echo "   ✅ Order data analysis and aggregation"
    echo "   ✅ Real-time spreadsheet data processing"
    
    exit 0
else
    print_error "Demo failed! ${#FAILED_PIPELINES[@]} pipeline(s) failed:"
    for pipeline in "${FAILED_PIPELINES[@]}"; do
        echo "  - $pipeline"
    done
    echo ""
    print_error "❌ Google Sheets Connector demo failed!"
    echo "   Some features were not demonstrated due to pipeline failures."
    echo "   Check the error messages above for details."
    echo "   Verify your Google Sheets credentials and spreadsheet ID are correct."
    
    exit 1
fi 