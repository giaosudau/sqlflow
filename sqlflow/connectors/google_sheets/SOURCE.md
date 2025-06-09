# Google Sheets Source

## 📋 Configuration

To use this source, you must first follow the setup instructions in the main [Google Sheets Connector README](./README.md).

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `type` | `string` | Must be `"google_sheets"` | `"google_sheets"` |
| `credentials_file` | `string` | Path to the Google service account credentials JSON file. | `"/path/to/creds.json"` |
| `spreadsheet_id` | `string` | The ID of the Google Sheet (from the URL). | `"1BxiMVs0XRA5n..."` |
| `sheet_name` | `string` | The name of the specific sheet to read from. | `"Q1 Sales"` |

### Optional Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `range` | `string` | `None` | The A1 notation of the range to retrieve (e.g., `"A1:E100"`). If not set, reads the whole sheet. | `"C5:F50"` |
| `has_header` | `boolean` | `true` | Whether the first row of the data should be treated as a header. | `false` |

### Example Configuration

```yaml
# profiles/dev.yml
sources:
  quarterly_sales:
    type: "google_sheets"
    credentials_file: "/secure/google-sheets-key.json"
    spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    sheet_name: "Q1 Sales"
    range: "A1:G2000"  # Optional
```

## 🚀 Usage Examples

### Basic Read
This reads all data from the configured sheet and range.
```sql
-- pipelines/process_sheets.sql
FROM source('quarterly_sales')
SELECT *;
```

### Transforming Data
You can use SQLFlow's transformation capabilities on the data read from Google Sheets.
```sql
-- pipelines/process_sheets.sql
FROM source('quarterly_sales')
SELECT 
    "Product ID",
    "Sale Price" * 0.8 as discounted_price,
    "Date"
WHERE "Country" = 'USA';
```

## 📈 Incremental Loading

The connector can perform incremental loads by filtering the data based on a cursor column.

**Important**: This connector's incremental load works by **reading the entire sheet first, then filtering** the data within the pipeline. This can be inefficient for very large sheets. It does not perform an incremental fetch from the API.

### Configuration

To enable incremental loading, you need to specify the `sync_mode` and `cursor_field` in your source configuration.

- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: The column in your sheet that will be used to determine new rows (e.g., a timestamp or an auto-incrementing ID).

```yaml
# profiles/dev.yml
sources:
  incremental_sales:
    type: "google_sheets"
    credentials_file: "/secure/google-sheets-key.json"
    spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    sheet_name: "Sales Data"
    sync_mode: "incremental"
    cursor_field: "transaction_timestamp"
```

### Behavior

When a pipeline runs in incremental mode:
1.  SQLFlow retrieves the last saved maximum value (watermark) for the `cursor_field`.
2.  The connector reads the **entire sheet** into memory.
3.  It then filters the data, keeping only rows where the `cursor_field` value is greater than the watermark.
4.  After a successful pipeline run, SQLFlow updates the watermark with the new maximum value from the processed data.

## ❌ Limitations

- **Read-Only**: This connector only supports reading data from Google Sheets, not writing.
- **Performance**: The connector reads the entire specified sheet or range into memory before processing. Performance may be slow for very large sheets (e.g., over 100,000 rows).
- **API Quotas**: Usage is subject to Google Sheets API quotas and rate limits. The connector has built-in retries for transient errors.
- **Complex Data Types**: The connector performs basic schema inference. It may not correctly handle very complex nested data or unusual data formats.

## 🛠️ Troubleshooting

- **Access Denied / Permission Errors**: Ensure the service account's email is shared on the Google Sheet with at least "Viewer" permissions.
- **Spreadsheet/Sheet Not Found**: Double-check that the `spreadsheet_id` and `sheet_name` are correct. `sheet_name` is case-sensitive. You can use the `discover()` method in code to see available sheets.
- **Authentication Errors**: Verify the `credentials_file` path is correct and the JSON file is valid.

---

**Version**: 2.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported 