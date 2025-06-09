#!/bin/bash

# Parquet Connector Demo Script
echo "🚀 Starting Parquet Connector Demo..."

# Create output directory
mkdir -p output

# Generate sample data if it doesn't exist
if [ ! -f "data/sales.parquet" ]; then
    echo "📊 Creating sample Parquet data..."
    python3 create_sample_data.py
fi

echo "📊 Running Parquet data processing pipelines..."

# Run the sales analysis pipeline
echo "📈 Analyzing sales data..."
sqlflow pipeline run analyze_sales --profile dev

# Run the monthly data processing pipeline
echo "📅 Processing monthly data with pattern matching..."
sqlflow pipeline run process_monthly_data --profile dev

echo "✅ Demo completed successfully!"
echo "📁 Check the output/ directory for results"
echo "🗄️  Database file: output/parquet_demo.duckdb"

# Display some results if the pipelines ran successfully
if [ -f "output/monthly_sales_summary.csv" ]; then
    echo ""
    echo "📋 Sample results from monthly sales summary:"
    head -n 5 output/monthly_sales_summary.csv
    
    echo ""
    echo "📊 Sample results from customer analysis:"
    head -n 5 output/customer_sales_analysis.csv
else
    echo ""
    echo "📋 Results available in DuckDB database:"
    echo "   sqlflow debug query 'SELECT * FROM sales_analysis LIMIT 5' --profile dev"
    echo "   sqlflow debug query 'SELECT * FROM monthly_trends LIMIT 5' --profile dev"
fi

echo ""
echo "🎉 Parquet connector demo complete!"
echo "   Features demonstrated:"
echo "   ✅ Single file reading with schema inference"
echo "   ✅ Column selection for performance optimization"
echo "   ✅ File pattern matching (data/sales_2024_*.parquet)"
echo "   ✅ Multiple file combination"
echo "   ✅ Complex analytics and joins"
echo "   ✅ CSV export of results" 