#!/bin/bash

# Google Sheets Connector Demo Script
echo "🚀 Starting Google Sheets Connector Demo..."

# Check if required environment variables are set
if [ -z "$GOOGLE_SHEETS_CREDENTIALS_FILE" ]; then
    echo "⚠️  Warning: GOOGLE_SHEETS_CREDENTIALS_FILE environment variable not set"
    echo "   Please set it to the path of your Google service account credentials JSON file"
    echo "   Example: export GOOGLE_SHEETS_CREDENTIALS_FILE=/path/to/service-account-key.json"
fi

if [ -z "$GOOGLE_SHEETS_SPREADSHEET_ID" ]; then
    echo "⚠️  Warning: GOOGLE_SHEETS_SPREADSHEET_ID environment variable not set"
    echo "   Please set it to your Google Sheets spreadsheet ID"
    echo "   Example: export GOOGLE_SHEETS_SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
fi

# Create output directory
mkdir -p output

echo "📊 Running Google Sheets data processing pipeline..."

# Only run if credentials are provided
if [ -n "$GOOGLE_SHEETS_CREDENTIALS_FILE" ] && [ -n "$GOOGLE_SHEETS_SPREADSHEET_ID" ]; then
    # Run the contact cleaning pipeline
    echo "🧹 Processing contact data..."
    sqlflow pipeline run load_contacts --profile dev

    # Run the order analysis pipeline
    echo "📈 Analyzing order data..."
    sqlflow pipeline run analyze_orders --profile dev
else
    echo "⚠️  Skipping pipeline execution - Google Sheets credentials not provided"
    echo "   This demo requires Google Sheets API setup to run the actual pipelines"
    echo "   See README.md for setup instructions"
fi

echo "✅ Demo completed successfully!"
echo "📁 Check the output/ directory for results"
echo "🗄️  Database file: output/google_sheets_demo.duckdb"

# Display some results (skip if no credentials provided)
if [ -n "$GOOGLE_SHEETS_CREDENTIALS_FILE" ] && [ -n "$GOOGLE_SHEETS_SPREADSHEET_ID" ]; then
    echo ""
    echo "📋 Sample results from clean_contacts table:"
    sqlflow debug query "SELECT contact_name, email_address, company_name, contact_status FROM clean_contacts LIMIT 5" --profile dev
    
    echo ""
    echo "📊 Sample results from order_summary table:"
    sqlflow debug query "SELECT order_month, COUNT(*) as customers, SUM(total_amount) as monthly_revenue FROM order_summary GROUP BY order_month ORDER BY order_month DESC LIMIT 5" --profile dev
else
    echo ""
    echo "💡 Set GOOGLE_SHEETS_CREDENTIALS_FILE and GOOGLE_SHEETS_SPREADSHEET_ID to see results"
fi 