#!/bin/bash

# REST API Connector Demo Script
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

echo "🚀 Starting REST API Connector Demo..."

# Create output directory and clean previous runs
mkdir -p output

# Clean previous database to avoid "table already exists" errors
rm -f output/rest_demo.duckdb

# Track pipeline results
PIPELINE_RESULTS=()
FAILED_PIPELINES=()

echo "📊 Running REST API data processing pipelines..."

# Run the user fetching pipeline
echo "👥 Fetching and analyzing user data..."
if sqlflow pipeline run fetch_users --profile dev; then
    print_success "User fetching pipeline completed successfully"
    PIPELINE_RESULTS+=("fetch_users:success")
else
    print_error "User fetching pipeline failed"
    PIPELINE_RESULTS+=("fetch_users:failed")
    FAILED_PIPELINES+=("fetch_users")
fi

# Run the posts analysis pipeline
echo "📝 Analyzing posts and content..."
if sqlflow pipeline run analyze_posts --profile dev; then
    print_success "Posts analysis pipeline completed successfully"
    PIPELINE_RESULTS+=("analyze_posts:success")
else
    print_error "Posts analysis pipeline failed"
    PIPELINE_RESULTS+=("analyze_posts:failed")
    FAILED_PIPELINES+=("analyze_posts")
fi

# Check overall results
if [ ${#FAILED_PIPELINES[@]} -eq 0 ]; then
    print_success "Demo completed successfully!"
    echo "📁 Check the output/ directory for results"
    echo "🗄️  Database file: output/rest_demo.duckdb"

    # Display some results if the pipelines ran successfully
    echo ""
    echo "📋 Results are available in the DuckDB database:"
    echo "   Database file: output/rest_demo.duckdb"
    echo "   You can query the data using any DuckDB client or tool"
    echo ""
    echo "📊 Available tables:"
    echo "   - user_summary: User statistics and demographics"
    echo "   - author_stats: Author posting statistics and metrics"
    echo "   - content_insights: Content analysis by post length"

    echo ""
    print_success "🎉 REST API Connector Demo completed!"
    echo "💡 This demo showed:"
    echo "   - Basic REST API data loading from JSONPlaceholder"
    echo "   - JSON data extraction and transformation"
    echo "   - Data enrichment by joining multiple API endpoints"
    echo "   - Statistical analysis and content categorization"
    
    exit 0
else
    print_error "Demo failed! ${#FAILED_PIPELINES[@]} pipeline(s) failed:"
    for pipeline in "${FAILED_PIPELINES[@]}"; do
        echo "  - $pipeline"
    done
    echo ""
    print_error "❌ REST API Connector demo failed!"
    echo "   Some features were not demonstrated due to pipeline failures."
    echo "   Check the error messages above for details."
    
    exit 1
fi 