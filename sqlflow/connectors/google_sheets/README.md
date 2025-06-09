# Google Sheets Connector

The Google Sheets connector allows SQLFlow to read data from Google Sheets spreadsheets using the Google Sheets API.

## Features

- **Read Support**: Read data from Google Sheets spreadsheets
- **Schema Discovery**: Automatically detect column types from sheet data
- **Flexible Range Selection**: Read specific ranges or entire sheets
- **Header Support**: Configurable header row handling
- **Incremental Loading**: Support for cursor-based incremental data loading
- **Multiple Sheets**: Discover and read from multiple sheets in a spreadsheet

## Prerequisites

1. **Google Cloud Project**: You need a Google Cloud project with the Google Sheets API enabled
2. **Service Account**: Create a service account with appropriate permissions
3. **Credentials File**: Download the service account key file (JSON format)
4. **Sheet Access**: Share your Google Sheet with the service account email address

### Setting up Google Sheets API Access

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Sheets API:
   - Go to "APIs & Services" > "Library"
   - Search for "Google Sheets API"
   - Click "Enable"
4. Create a service account:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Fill in the details and create
5. Generate a key file:
   - Click on the created service account
   - Go to "Keys" tab
   - Click "Add Key" > "Create new key"
   - Choose JSON format and download
6. Share your Google Sheet with the service account email address (found in the JSON file)

## Configuration

### Required Parameters

- `credentials_file`: Path to the Google service account credentials JSON file
- `spreadsheet_id`: The ID of the Google Sheets spreadsheet (found in the URL)
- `sheet_name`: Name of the sheet to read from

### Optional Parameters

- `range`: Specific range to read (e.g., "A1:D10"). If not specified, reads the entire sheet
- `has_header`: Whether the first row contains headers (default: true)

### Example Configuration

```yaml
# profiles/default.yml
sources:
  google_sheets_source:
    connector: google_sheets
    credentials_file: "/path/to/service-account-key.json"
    spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    sheet_name: "Sheet1"
    has_header: true
    range: "A1:E100"  # Optional: read specific range
```

## Usage Examples

### Basic Usage

```sql
-- Read all data from the configured sheet
SELECT * FROM google_sheets_source;
```

### Reading Specific Columns

```sql
-- Read only specific columns
SELECT name, email, age FROM google_sheets_source;
```

### Using with Transforms

```sql
-- Transform and load data
CREATE TABLE clean_contacts AS
SELECT 
    TRIM(name) as name,
    LOWER(email) as email,
    CAST(age AS INTEGER) as age
FROM google_sheets_source
WHERE email IS NOT NULL;
```

## Schema Detection

The connector automatically detects column types based on the data in the sheet:

- **String**: Default type for text data
- **Integer**: For numeric data without decimals
- **Float**: For numeric data with decimals
- **Boolean**: For true/false values
- **Date**: For date values (if properly formatted in the sheet)

## Incremental Loading

The Google Sheets connector supports incremental loading using a cursor field:

```yaml
# profiles/default.yml
sources:
  google_sheets_incremental:
    connector: google_sheets
    credentials_file: "/path/to/service-account-key.json"
    spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    sheet_name: "Orders"
    cursor_field: "order_date"
```

## Error Handling

The connector handles various error scenarios:

- **Authentication Errors**: Invalid credentials or expired tokens
- **Permission Errors**: Insufficient access to the spreadsheet
- **Not Found Errors**: Spreadsheet or sheet doesn't exist
- **API Quota Errors**: Google Sheets API rate limits

## Limitations

1. **Read-Only**: This connector only supports reading data, not writing
2. **API Limits**: Subject to Google Sheets API quotas and rate limits
3. **Data Types**: Limited type inference compared to structured databases
4. **Large Sheets**: Performance may degrade with very large sheets (>10,000 rows)

## Troubleshooting

### Common Issues

1. **"Access denied" errors**:
   - Ensure the service account email is shared with the spreadsheet
   - Check that the service account has the correct permissions

2. **"Spreadsheet not found" errors**:
   - Verify the spreadsheet_id is correct
   - Ensure the spreadsheet is accessible to the service account

3. **"Sheet not found" errors**:
   - Check that the sheet_name matches exactly (case-sensitive)
   - Use the `discover()` method to list available sheets

4. **Authentication errors**:
   - Verify the credentials file path is correct
   - Ensure the JSON file is valid and not corrupted
   - Check that the Google Sheets API is enabled in your project

### Testing Connection

You can test your connection using the connector's test method:

```python
from sqlflow.connectors.google_sheets.source import GoogleSheetsSource

config = {
    "credentials_file": "/path/to/credentials.json",
    "spreadsheet_id": "your_spreadsheet_id",
    "sheet_name": "Sheet1"
}

connector = GoogleSheetsSource(config)
result = connector.test_connection()
print(f"Connection test: {result.success}, Message: {result.message}")
```

## Performance Tips

1. **Use Specific Ranges**: Instead of reading entire sheets, specify ranges when possible
2. **Limit Columns**: Only select the columns you need
3. **Cache Results**: For frequently accessed data, consider caching in a local database
4. **Batch Processing**: For large datasets, process data in smaller chunks

## Security Considerations

1. **Credentials Storage**: Store service account keys securely and never commit them to version control
2. **Least Privilege**: Grant minimal necessary permissions to the service account
3. **Key Rotation**: Regularly rotate service account keys
4. **Access Monitoring**: Monitor access logs for unusual activity

## API Reference

For detailed API documentation, see the [Google Sheets API documentation](https://developers.google.com/sheets/api). 