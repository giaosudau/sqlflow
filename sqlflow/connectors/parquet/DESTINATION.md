# Parquet Destination Connector

The Parquet Destination connector writes data to local Parquet files, a highly efficient, column-oriented data format.

## ✅ Features

- **Local File System**: Writes Parquet files to the local disk.
- **Partitioning**: Supports partitioning the output data into a directory structure based on column values.
- **Compression**: Allows specifying the compression algorithm for the output file.

## 📋 Configuration

When using the Parquet destination in an `EXPORT` step, you configure it via the `TYPE` and `OPTIONS` clauses.

### EXPORT Options
| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `path` | `string` | The local path for the output. If `partition_cols` is not used, this is the full file path. If `partition_cols` is used, this is the base directory. | ✅ | `"/data/processed/report.parquet"` |
| `partition_cols` | `list[string]` | A list of column names to partition the data by. | | `["country", "city"]` |
| `compression` | `string` | The compression codec to use. Common options include `snappy`, `gzip`, `brotli`, or `None`. | | `"snappy"` |

## 💡 Examples

### Basic Export to a Single File
This example exports the `analytics_summary` table to a single, compressed Parquet file.

```sql
EXPORT
  SELECT * FROM analytics_summary
TO "/data/final/summary.parquet"
TYPE PARQUET
OPTIONS {
  "path": "/data/final/summary.parquet",
  "compression": "gzip"
};
```

### Partitioned Export
This example exports the `user_events` table into a partitioned directory structure.

```sql
EXPORT
  SELECT user_id, event_type, event_ts, country, city FROM user_events
TO "/data/events/"
TYPE PARQUET
OPTIONS {
  "path": "/data/events/",
  "partition_cols": ["country", "city"]
};
```
This would create a local directory structure like:
```
/data/events/
├── country=US/
│   ├── city=New York/
│   │   └── ...some-guid.parquet
│   └── city=Chicago/
│       └── ...some-guid.parquet
└── country=CA/
    └── city=Toronto/
        └── ...some-guid.parquet
```

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported 