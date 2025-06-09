# Google Sheets Demo

This demo showcases how to use SQLFlow to read and process data from Google Sheets spreadsheets.

## Overview

The demo demonstrates:
- Reading contact data from a Google Sheets spreadsheet
- Cleaning and standardizing the contact information
- Analyzing order data from another sheet
- Generating summary reports and insights

## Prerequisites

1. **Google Cloud Project** with Google Sheets API enabled
2. **Service Account** with appropriate permissions
3. **Credentials File** (JSON format) for the service account
4. **Google Sheets** with sample data shared with the service account

### Setting Up Google Sheets API Access

Follow the detailed setup instructions in the [Google Sheets Connector README](../../sqlflow/connectors/google_sheets/README.md).

## Sample Data Structure

### Contacts Sheet
Expected columns:
- `name`: Contact name
- `email`: Email address
- `phone`: Phone number
- `company`: Company name
- `status`: Contact status (Active/Inactive)

### Orders Sheet
Expected columns:
- `order_date`: Order date
- `customer_id`: Customer identifier
- `amount`: Order amount
- `product`: Product name

## Configuration

Set the following environment variables:

```bash
export GOOGLE_SHEETS_CREDENTIALS_FILE="/path/to/your/service-account-key.json"
export GOOGLE_SHEETS_SPREADSHEET_ID="your_spreadsheet_id_here"
export GOOGLE_SHEETS_SHEET_NAME="Contacts"  # Optional, defaults to "Contacts"
```

The spreadsheet ID can be found in the Google Sheets URL:
```
https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
```

## Running the Demo

1. **Set up your environment variables**:
   ```bash
   export GOOGLE_SHEETS_CREDENTIALS_FILE="/path/to/service-account-key.json"
   export GOOGLE_SHEETS_SPREADSHEET_ID="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
   ```

2. **Navigate to the demo directory**:
   ```bash
   cd examples/google_sheets_demo
   ```

3. **Run the demo**:
   ```bash
   ./run_demo.sh
   ```

## What the Demo Does

### 1. Contact Data Processing (`load_contacts.sql`)
- Reads contact data from the Google Sheets
- Cleans and standardizes names, emails, and phone numbers
- Normalizes contact status values
- Filters out invalid records (missing name or email)
- Adds processing timestamp

### 2. Order Analysis (`analyze_orders.sql`)
- Reads order data from the Google Sheets
- Groups orders by month and customer
- Calculates key metrics:
  - Total orders per customer per month
  - Total amount spent
  - Average order value
  - First and last order dates
- Filters out invalid orders (missing date or amount)

## Expected Output

The demo will create:
- `output/google_sheets_demo.duckdb`: SQLite database with processed data
- Two tables:
  - `clean_contacts`: Cleaned contact information
  - `order_summary`: Monthly order summaries by customer

Sample output:
```
📋 Sample results from clean_contacts table:
contact_name    | email_address        | company_name | contact_status
John Smith      | john@example.com     | Acme Corp    | Active
Jane Doe        | jane@company.com     | Tech Inc     | Active

📊 Sample results from order_summary table:
order_month | customers | monthly_revenue
2023-12-01  | 15        | 45000.00
2023-11-01  | 12        | 38500.00
```

## Troubleshooting

### Common Issues

1. **"Access denied" errors**:
   - Ensure your Google Sheet is shared with the service account email
   - Check that the service account has read permissions

2. **"Spreadsheet not found" errors**:
   - Verify the `GOOGLE_SHEETS_SPREADSHEET_ID` is correct
   - Ensure the spreadsheet is accessible to the service account

3. **"Sheet not found" errors**:
   - Check that the sheet names in the profile match your actual sheet names
   - Sheet names are case-sensitive

4. **Authentication errors**:
   - Verify the `GOOGLE_SHEETS_CREDENTIALS_FILE` path is correct
   - Ensure the JSON file is valid and not corrupted

### Testing Connection

You can test your Google Sheets connection before running the full demo:

```python
from sqlflow.connectors.google_sheets.source import GoogleSheetsSource

config = {
    "credentials_file": "/path/to/credentials.json",
    "spreadsheet_id": "your_spreadsheet_id",
    "sheet_name": "Contacts"
}

connector = GoogleSheetsSource(config)
result = connector.test_connection()
print(f"Connection test: {result.success}, Message: {result.message}")

# List available sheets
sheets = connector.discover()
print(f"Available sheets: {sheets}")
```

## Customization

You can customize this demo by:

1. **Modifying the SQL pipelines** to match your data structure
2. **Adding new transformations** in additional SQL files
3. **Changing the output format** by modifying the destination in `profiles/default.yml`
4. **Adding data validation** and quality checks

## Next Steps

- Explore incremental loading for large datasets
- Add data quality validation rules
- Set up automated scheduling with cron jobs
- Export results to other systems (databases, APIs, etc.)

For more advanced usage, see the [Google Sheets Connector documentation](../../sqlflow/connectors/google_sheets/README.md). 