#!/bin/bash

# REST API Connector Demo Script
echo "🚀 Starting REST API Connector Demo..."

# Create output directory
mkdir -p output

echo "📊 Running REST API data processing pipelines..."

# Run the user fetching pipeline
echo "👥 Fetching and analyzing user data..."
sqlflow pipeline run fetch_users --profile dev

# Run the posts analysis pipeline
echo "📝 Analyzing posts and content..."
sqlflow pipeline run analyze_posts --profile dev

echo "✅ Demo completed successfully!"
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
echo "🎉 REST API Connector Demo completed!"
echo "💡 This demo showed:"
echo "   - Basic REST API data loading from JSONPlaceholder"
echo "   - JSON data extraction and transformation"
echo "   - Data enrichment by joining multiple API endpoints"
echo "   - Statistical analysis and content categorization" 