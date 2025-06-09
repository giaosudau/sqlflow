# Parquet Source Connector

The Parquet Source connector reads data from local Parquet files. It can read from a single `.parquet` file or discover and read all Parquet files within a specified directory.

## ✅ Features

- **Local File System**: Reads from the local filesystem where SQLFlow is executed.
- **Single File or Directory**: Can be configured to read a specific file or all `.parquet`/`.pq` files in a folder.
- **Streaming Reads**: Reads data in batches (DataChunks) for memory efficiency when processing large files.
- **Schema Discovery**: Automatically infers the data schema from the Parquet file metadata.

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"parquet"`. | ✅ | `"parquet"` |
| `path` | `string` | The local path to a single `.parquet` file or a directory containing Parquet files. | ✅ | `"/data/raw/users.parquet"` or `"/data/processed/sales/"`|

### Authentication
This connector reads from the local filesystem and does not require any authentication parameters.

## 💡 Example

This example defines a source named `local_events` that reads all Parquet files from the `/var/data/analytics/events` directory.

```sql
SOURCE local_events TYPE PARQUET PARAMS {
    "path": "/var/data/analytics/events"
};
```

You can then load this data into a table:
```sql
LOAD events_table FROM local_events;
```

## 📈 Incremental Loading

This connector **does not** support incremental loading. It is designed for full reads of files or directories.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported 