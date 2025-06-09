# S3 Connector

The S3 connector provides a powerful and flexible way to work with data stored in Amazon S3 or other S3-compatible object storage services. It can read data from and write data to S3, supporting multiple file formats and authentication methods.

## 🚀 Quick Start

### Reading from S3
```yaml
# profiles/dev.yml
sources:
  s3_sales_data:
    type: "s3"
    bucket: "my-data-lake"
    path_prefix: "sales/2024/"
    file_format: "parquet"
    # Assumes credentials are set via environment variables or IAM role
```

### Writing to S3
```yaml
# profiles/dev.yml
destinations:
  s3_output:
    type: "s3"
    uri: "s3://my-data-lake/processed/report.csv"
```

### Use in Pipeline
```sql
-- pipelines/process_s3_data.sql
FROM source('s3_sales_data')
SELECT
  product_id,
  SUM(amount) as total_sales
GROUP BY product_id
TO destination('s3_output');
```

## 📋 Features

| Feature | Source | Destination |
|---|---|---|
| **Multi-Format Support** | ✅ | ✅ |
| **Authentication** | ✅ | ✅ |
| **Object Discovery** | ✅ | ➖ |
| **Write Modes** | See Docs | See Docs |

## 📖 Documentation

For detailed information on configuration, features, and limitations, please see the full documentation for the source and destination.

### 📥 Source Documentation
**[S3 Source Connector →](SOURCE.md)**

### 📤 Destination Documentation
**[S3 Destination Connector →](DESTINATION.md)** 