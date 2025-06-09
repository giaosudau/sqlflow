# Google Sheets Connector

The Google Sheets connector allows you to use a Google Sheet as a data source in your SQLFlow pipelines. It's a convenient way to pull in data from spreadsheets that are manually maintained or are the output of other tools like Google Forms.

## ✅ Features

- **Read Support**: Read data from any Google Sheet you have access to.
- **Schema Discovery**: Automatically detects column names and data types from your sheet.
- **Flexible Range**: Read an entire sheet or specify a particular range (e.g., `A1:D10`).
- **Incremental Loading**: Supports incrementally processing new rows from a sheet.
- **Connection Testing**: Verifies credentials and access to the specified sheet.

## ⚙️ Setup & Prerequisites

To use this connector, you need a Google Cloud service account with the Google Sheets API enabled.

1.  **Enable Google Sheets API**:
    -   In the [Google Cloud Console](https://console.cloud.google.com/), select your project.
    -   Go to "APIs & Services" > "Library", search for "Google Sheets API", and click "Enable".
2.  **Create a Service Account**:
    -   Go to "APIs & Services" > "Credentials".
    -   Click "Create Credentials" > "Service Account".
    -   Give it a name and create it. You do not need to grant it project-level roles.
3.  **Generate a JSON Key**:
    -   Click on the created service account.
    -   Go to the "Keys" tab, click "Add Key" > "Create new key".
    -   Select "JSON" and download the file. This is the `credentials_file` you will use in your configuration.
4.  **Share Your Google Sheet**:
    -   Open the Google Sheet you want to read.
    -   Click "Share" in the top right.
    -   Add the service account's email address (found as `client_email` in the JSON key file) as a "Viewer". Without this, you will get a permission denied error.

## 📖 Documentation

For detailed information on how to configure the Google Sheets source, see the full source documentation.

**[Google Sheets Source Documentation →](./SOURCE.md)** 