# Google Sheets Destination Connector

The Google Sheets Destination connector is not yet implemented in SQLFlow.

Writing data to Google Sheets would typically involve using the Google Sheets API to clear a range and then append new data.

If you have a use case for a Google Sheets destination, please [open an issue](https://github.com/sql-workflow/sqlflow/issues) on our GitHub repository to describe your requirements.

# Google Sheets Destination

The Google Sheets connector allows you to write data from your pipeline directly into a Google Sheet. This is useful for creating reports, sharing results with non-technical users, or using a sheet as a data source for other tools.

## Features
- **Authentication**: Securely connects using Google Cloud service account credentials.
- **Create New Sheets**: Can automatically create a new sheet (tab) in a spreadsheet if it doesn't exist.
- **Write Data**: Writes the results of a SQL query to the specified sheet.

## 📋 Configuration

To use the Google Sheets destination, configure it in your pipeline's `destination` block.

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"google_sheets"`. | ✅ | `"google_sheets"` |
| `credentials_json`| `string` | The JSON content of your Google Cloud service account key. Load this from an environment variable. | ✅ | `${GCP_CREDS_JSON}` |
| `spreadsheet_url` | `string` | The full URL of the Google Sheet. | ✅ | `"https://docs.google.com/spreadsheets/d/..."` |
| `sheet_name` | `string` | The name of the sheet (tab) to write to. If it doesn't exist, it will be created. | ✅ | `"Q1 Report"` |
| `write_mode` | `string` | Must be `"overwrite"`. The connector will clear the entire sheet and write the new data. | `overwrite` | |

### Example
Here is a complete example of a Google Sheets destination configuration:
```yaml
# In your pipeline.sql frontmatter
destination:
  type: "google_sheets"
  credentials_json: ${GCP_CREDS_JSON}
  spreadsheet_url: "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNoPqRsTuVwXyZ_1234567890"
  sheet_name: "Weekly Sales Summary"
  write_mode: "overwrite"
```

## ⚠️ Limitations
- **Overwrite Only**: The connector only supports overwriting the entire sheet. It cannot append or update specific rows.
- **Performance**: Writing very large datasets can be slow and is subject to Google Sheets API rate limits. This destination is best for summaries and reports, not large data dumps.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Write Modes**: Overwrite 