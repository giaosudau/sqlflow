# S3 Destination Connector

The S3 Destination connector writes data to files in Amazon S3. It determines the output file format based on the extension of the object key provided in the URI.

## ✅ Features

- **Multi-Format Support**: Writes data in `CSV`, `Parquet`, or `JSON` format.
- **Simple Configuration**: Uses a single `uri` to specify the output location.
- **Implicit Authentication**: Uses `boto3`'s default credential discovery (IAM roles, env vars, etc.).

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"s3"`. | ✅ | `"s3"` |
| `uri` | `string` | The full S3 URI specifying the bucket and the full path (key) of the output file. The file extension (`.csv`, `.parquet`, `.json`) determines the output format. | ✅ | `"s3://my-bucket/processed/report.csv"` |

### Authentication
The destination connector does not take explicit credentials in its configuration. It relies on the standard AWS credential discovery process used by `boto3`. For this to work, you must have your AWS credentials configured in the environment where SQLFlow is running, for example via:
- An IAM role (recommended)
- Environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)

### Example Configuration
```yaml
# profiles/dev.yml
destinations:
  s3_csv_report:
    type: "s3"
    uri: "s3://my-data-lake/reports/sales_summary.csv"

  s3_parquet_export:
    type: "s3"
    uri: "s3://my-data-lake/exports/processed_users.parquet"
```

## 🚀 Usage

The connector automatically detects the desired output format from the file extension in the `uri`.

- `s3://.../file.csv` → Writes a CSV file.
- `s3://.../file.parquet` → Writes a Parquet file.
- `s3://.../file.json` → Writes a JSON file.

### Example Pipeline
```sql
-- pipelines/export_to_s3.sql
FROM source('some_data')
SELECT *
TO destination('s3_csv_report');
```

## 📝 Write Modes

The S3 Destination connector supports **overwrite mode only**.

- **REPLACE (overwrite)**: If an object already exists at the specified `uri`, it will be completely replaced with the new data.

`APPEND` and `UPSERT` modes are **not supported**.

## ❌ Limitations

- **No Streaming Writes**: The connector buffers the entire pandas DataFrame in memory before uploading it to S3. It is not suitable for writing datasets that are larger than available RAM.
- **No Append/Upsert**: You cannot append to an existing S3 object. Each write operation overwrites the file.
- **No Format-Specific Options**: You cannot configure format-specific options like the delimiter for CSV files or the compression for Parquet files. The connector uses the default settings from pandas.
- **Simplified Authentication**: Unlike the S3 Source, the Destination does not allow for passing explicit credentials or a region in the configuration. It relies entirely on the execution environment's setup.

---
**Version**: 2.0 • **Status**: ✅ Production Ready • **Write Modes**: Replace Only 