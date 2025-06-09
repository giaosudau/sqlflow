# S3 Destination Connector

The S3 Destination connector writes data to files in Amazon S3 or S3-compatible services. It supports multiple file formats and partitioned writing, making it a flexible choice for data lake and archival use cases.

## ✅ Features

- **Multi-Format Support**: Writes data in `CSV`, `Parquet`, `JSON`, and `JSONL` formats.
- **Partitioned Writing**: Can partition output data into subdirectories based on column values.
- **Flexible Authentication**: Leverages `boto3`'s standard credential discovery (environment variables, IAM roles, etc.).
- **S3-Compatible Services**: Works with services like MinIO by specifying a custom `endpoint_url`.
- **Configurable Write Modes**: Supports replacing existing data.

## 📋 Configuration

When using the S3 destination in an `EXPORT` step, you configure it via the `TYPE` and `OPTIONS` clauses.

### EXPORT Options
| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `bucket` | `string` | The name of the destination S3 bucket. | ✅ | `"my-output-bucket"` |
| `key` | `string` | The full key (path and filename) for the output object. | ✅ | `"processed/data.parquet"` |
| `file_format`| `string` | The format of the output file. Supported: `csv`, `parquet`, `json`, `jsonl`. | ✅ | `"parquet"` |
| `mode` | `string` | The write mode. Only `"replace"` is currently supported. Defaults to `"replace"`. | | `"replace"` |
| `partition_cols` | `list[string]` | A list of column names to partition the output data by. | | `["year", "month"]` |
| `region` | `string` | The AWS region of the bucket (e.g., `us-east-1`). | | `"us-west-2"` |
| `access_key_id` | `string` | Your AWS access key ID. (Not recommended, prefer IAM roles). | | `"${AWS_KEY}"` |
| `secret_access_key` | `string` | Your AWS secret access key. | | `"${AWS_SECRET}"` |
| `endpoint_url` | `string` | The endpoint URL for an S3-compatible service. | | `"http://minio:9000"` |

### Format-Specific Options
These can also be included in the `OPTIONS` block.
| Parameter | Type | Default | Description |
|---|---|---|---|
| `csv_delimiter` | `string` | `,` | Delimiter to use for CSV files. |
| `csv_header` | `boolean`| `true` | Whether to write the header row for CSV files. |

---
## 💡 Examples

### Basic Export
This example exports the `processed_orders` table to a single Parquet file in S3.

```sql
EXPORT
  SELECT * FROM processed_orders
TO "s3://my-output-bucket/orders/all_orders.parquet"
TYPE S3
OPTIONS {
  "bucket": "my-output-bucket",
  "key": "orders/all_orders.parquet",
  "file_format": "parquet",
  "region": "us-east-1"
};
```

### Partitioned CSV Export
This example exports the `events` table into a partitioned structure, creating subdirectories for each year and month.

```sql
EXPORT
  SELECT event_id, event_type, payload, event_time, YEAR(event_time) as year, MONTH(event_time) as month
  FROM events
TO "s3://my-data-lake/events/"
TYPE S3
OPTIONS {
  "bucket": "my-data-lake",
  "key": "events/",
  "file_format": "csv",
  "partition_cols": ["year", "month"],
  "csv_delimiter": "\t"
};
```
The resulting S3 structure would look like:
```
my-data-lake/
└── events/
    ├── year=2024/
    │   ├── month=1/
    │   │   └── ...some-guid.csv
    │   └── month=2/
    │       └── ...some-guid.csv
    └── ...
```

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported 