# Parquet Source

The Parquet Source connector provides efficient reading of Apache Parquet files with advanced features. For a general overview of the Parquet connector, see the [main README](./README.md).

## Configuration

The source is configured in your `profiles.yml` file.

### Profile Example

```yaml
# profiles/dev.yml
sources:
  s3_sales_data:
    type: "parquet"
    path: "data/analytics.parquet"

  # With column selection for performance
  metrics:
    type: "parquet"
    path: "data/large_dataset.parquet"
    columns": ["user_id", "event_type", "timestamp"]
    
  # Multiple files with patterns
  daily_metrics:
    type: parquet
    path: "data/metrics_*.parquet"
    combine_files: true
    columns: ["date", "metric_name", "value"]
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | string | ✅ | - | File path or glob pattern for files to read. |
| `columns` | list[string] | ❌ | null | A list of specific column names to read from the file(s). Improves performance by reducing I/O. |
| `combine_files` | boolean | ❌ | `true` | If `path` is a glob pattern that matches multiple files, this determines whether to combine them into a single dataset. |


### Path Patterns

The connector supports glob patterns in the `path` parameter for bulk file operations:

```
# All parquet files in a directory
"path": "data/*.parquet"

# Recursive search through subdirectories
"path": "data/**/*.parquet"

# Date-partitioned structure
"path": "data/year=*/month=*/*.parquet"

# Specific naming pattern
"path": "logs/app_log_*.parquet"
```

## Features

### Schema Inference
The connector automatically detects the schema from the Parquet file's metadata. You can use `DESCRIBE` on a source to see the inferred schema.

```sql
-- Schema is automatically inferred
SOURCE products PARAMS {
  "type": "parquet",
  "path": "catalog/products.parquet"
};

-- Use DESCRIBE to see the schema
DESCRIBE "products";
```

### Column Selection (Column Pruning)
Optimize performance by reading only the columns you need. This is a key advantage of columnar formats like Parquet.

```sql
-- Read specific columns only
SOURCE user_events PARAMS {
  "type": "parquet",
  "path": "events/user_activity.parquet",
  "columns": ["user_id", "event_timestamp", "page_url"]
};
```

### Multiple File Handling
You can read multiple Parquet files at once using a glob pattern.

```sql
-- Combine multiple files into a single DataFrame
SOURCE monthly_sales PARAMS {
  "type": "parquet",
  "path": "sales/monthly_*.parquet",
  "combine_files": true
};

-- Process files separately (useful for very large datasets)
-- This will process each matched file as an independent chunk.
SOURCE large_logs PARAMS {
  "type": "parquet",
  "path": "logs/app_*.parquet",
  "combine_files": false
};
```

### Incremental Loading
The connector supports incremental loading, but it is configured at the SQL level rather than via connector parameters.

```sql
-- Incremental loading with a timestamp cursor
SOURCE incremental_events PARAMS {
  "type": "parquet",
  "path": "events/daily_*.parquet"
}
INCREMENTAL BY event_timestamp;
```

## Troubleshooting

### Common Issues

**Pattern matches no files**
```
Error: No files found matching pattern: data/missing_*.parquet
```
- Verify the `path` pattern is correct and that the files exist.
- Check file permissions for the SQLFlow process.

**Column not found**
```
Warning: Column 'missing_col' not found in Parquet file.
```
- Check the column names in your query against the actual schema of the Parquet file. You can use `DESCRIBE` to verify the schema.

**Corrupt File**
- If you encounter errors related to file format, the Parquet file itself may be corrupted or not written correctly. Try reading it with another tool to verify its integrity. 