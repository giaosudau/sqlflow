# S3 Source Connector

The S3 Source connector reads data from files in Amazon S3, supporting multiple file formats, flexible authentication, partition filtering, and powerful discovery features with built-in cost management.

## ✅ Features

- **Multi-Format Support**: Natively reads `CSV`, `Parquet`, `JSON`, and `JSONL` files, with automatic format detection from file extensions.
- **Flexible Configuration**: Specify S3 paths using a full `uri` or separate `bucket` and `key`/`path_prefix` parameters.
- **Authentication**: Supports AWS access keys and automatically discovers credentials from environment variables or IAM roles.
- **Custom Endpoints**: Works with S3-compatible services like MinIO by specifying an `endpoint_url`.
- **Partition Awareness**: Discovers and filters data based on Hive-style partitioning in your S3 key structure (e.g., `year=2024/month=01/`).
- **Object Discovery**: Can discover all objects under a given `path_prefix`.
- **Cost Management**: Limit discovery by number of files or total data size to control costs during pipeline runs.
- **Incremental Loading**: Efficiently loads new or modified files based on their `last_modified` timestamp, avoiding re-scanning unchanged data.
- **Connection Testing**: Verifies credentials and bucket access.

## 📋 Configuration

### Recommended Configuration
This is the modern, recommended approach.

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"s3"`. | ✅ | `"s3"` |
| `bucket` | `string` | The name of the S3 bucket. | ✅ | `"my-data-lake"` |
| `path_prefix` | `string` | The prefix (folder path) to search for objects. | | `"raw/sales/2024/"`|
| `key` | `string` | The exact key (path) of a single S3 object. Use this or `path_prefix`. | | `"data/single_file.csv"`|
| `file_format`| `string` | The format of the files. Supported: `csv`, `parquet`, `json`, `jsonl`. | | `"parquet"` |

### Alternative Configuration (URI)
| Parameter | Type | Description | Required |
|---|---|---|:---:|
| `type` | `string` | Must be `"s3"`. | ✅ |
| `uri` | `string` | The full S3 URI to a specific object or prefix. | ✅ |
> **Note**: If `uri` is used, it overrides `bucket`, `key`, and `path_prefix`.

### Authentication & Endpoint
Authentication is handled by `boto3`'s standard credential discovery chain. The recommended approach is to use an IAM role.
| Parameter | Type | Description |
|---|---|---|
| `region` | `string` | The AWS region of the bucket (e.g., `us-east-1`). |
| `access_key_id` | `string` | Your AWS access key ID. |
| `secret_access_key` | `string` | Your AWS secret access key. |
| `endpoint_url` | `string` | The endpoint URL for an S3-compatible service (e.g., MinIO). |

### Partitioning & Filtering
| Parameter | Type | Description | Example |
|---|---|---|---|
| `partition_keys` | `list` or `string` | The names of the partition keys present in the S3 path (e.g., `year`, `month`). | `["year", "month"]` |
| `partition_filter` | `dict` | A dictionary to filter partitions. Only files matching these partition values will be read. | `{"year": "2024", "month": "01"}` |

### Format-Specific Options
| Parameter | Type | Default | Description |
|---|---|---|---|
| `csv_delimiter` | `string` | `,` | Delimiter for CSV files. |
| `csv_header` | `boolean`| `true` | Whether CSV files have a header row. |
| `json_flatten`| `boolean`| `true` | Whether to flatten nested JSON structures. |

### Cost Management & Sampling
| Parameter | Type | Default | Description |
|---|---|---|---|
| `max_files_per_run` | `integer` | `10000` | The maximum number of files to discover in a single run. |
| `max_data_size_gb` | `float` | `1000.0` | The maximum total size of data in GB to discover. |
| `dev_sampling` | `float` | `None` | A float between 0.0 and 1.0 to sample a fraction of discovered files. Useful for development. |
| `dev_max_files` | `integer` | `None` | The maximum number of files to process. Overrides other limits for development. |

---
## 📈 Incremental Loading
This connector supports an efficient incremental loading strategy based on file `last_modified` timestamps.

### Configuration
- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: For the S3 connector, this should be set to `"last_modified"`.

```yaml
# profiles/dev.yml
sources:
  s3_incremental:
    type: "s3"
    bucket: "my-streaming-data"
    path_prefix: "events/"
    file_format: "jsonl"
    sync_mode: "incremental"
    cursor_field: "last_modified"
```

### Behavior
When a pipeline runs in incremental mode, the connector lists objects in the specified `bucket` and `path_prefix` and filters them based on their `last_modified` time, comparing it to the last stored watermark. Only new or updated files are downloaded and processed. This avoids costly re-reading of unchanged data.

---
**Version**: 2.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported 