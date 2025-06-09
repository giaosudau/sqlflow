# Google Sheets Source Connector

The Google Sheets Source connector reads data from a specific sheet within a Google Spreadsheet. It uses a Google Cloud service account for authentication.

## ✅ Features

- **Direct Sheet Reading**: Reads data directly from a Google Spreadsheet.
- **Service Account Auth**: Uses standard Google Cloud service account credentials for secure, non-user-based access.
- **Sheet Discovery**: Can discover all the individual sheets (tabs) within a spreadsheet.
- **Schema Inference**: Infers a schema by using the first row of the sheet as the header.

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"google_sheets"`. | ✅ | `"google_sheets"` |
| `spreadsheet_id` | `string` | The ID of the Google Spreadsheet. You can find this in the URL: `.../spreadsheets/d/{spreadsheet_id}/edit`. | ✅ | `"1-AbcDeFgHiJkLmNoPqRsTuVwXyZ..."`|
| `credentials`| `string` | A JSON string containing your Google Cloud service account credentials, or a local path to the credentials JSON file. Recommended to use a variable. | ✅ | `"${GCP_CREDENTIALS_JSON}"` |

### Authentication
To use this connector, you must:
1.  Create a [Google Cloud service account](https://cloud.google.com/iam/docs/service-accounts-create).
2.  Enable the [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com) for your project.
3.  Create and download a JSON key for the service account.
4.  **Share the Google Sheet** with the service account's email address (e.g., `my-service-account@my-gcp-project.iam.gserviceaccount.com`) and give it at least "Viewer" permissions.

You can then pass the content of the JSON key file as the `credentials` parameter, ideally through an environment variable.

## 💡 Example

This example defines a source for a specific Google Spreadsheet and then loads the sheet named `Q1-Data` into a table.

```sql
-- 1. Define the connection to your Google Spreadsheet
SOURCE marketing_spreadsheet TYPE GOOGLE_SHEETS PARAMS {
    "spreadsheet_id": "1-AbcDeFgHiJkLmNoPqRsTuVwXyZ...",
    "credentials": "${GCP_CREDENTIALS_JSON}"
};

-- 2. Load the 'Q1-Data' sheet from that source
LOAD q1_marketing_data FROM marketing_spreadsheet OPTIONS {
    "object_name": "Q1-Data"
};
```
> The `object_name` in the `LOAD` step corresponds to the name of the tab you want to read from within the spreadsheet.

## 📈 Incremental Loading

This connector **does not** support incremental loading. It reads the entire specified sheet on every run.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported 