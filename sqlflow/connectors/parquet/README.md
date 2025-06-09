# Parquet Connector

The Parquet connector provides high-performance, efficient reading and writing of [Apache Parquet](https://parquet.apache.org/) files.

Apache Parquet is a columnar storage file format that is designed for efficient data storage and retrieval. It is highly optimized for use with big data processing frameworks and provides significant performance improvements over row-based file formats like CSV.

## Overview

This connector allows you to:
- **Read Parquet files** as a data source, with advanced features like schema inference, column selection, and file pattern matching.
- **Write Parquet files** as a data destination.

## Documentation

For detailed information on configuration, features, and limitations, please see the full documentation for the source and destination.

### 📥 Source Documentation
**[Parquet Source Connector →](./SOURCE.md)**

### 📤 Destination Documentation
**[Parquet Destination Connector →](./DESTINATION.md)**

## Configuration

### Basic Usage

```sql
-- Single Parquet file
SOURCE analytics TYPE PARQUET PARAMS {
  "path": "data/analytics.parquet"
};

-- Column selection for performance
SOURCE metrics TYPE PARQUET PARAMS {
  "path": "data/large_dataset.parquet",
  "columns": ["user_id", "event_type", "timestamp"]
};
```

### Advanced Configuration

```sql
-- Multiple files with patterns
SOURCE daily_metrics TYPE PARQUET PARAMS {
  "path": "data/metrics_*.parquet",
  "combine_files": true,
  "columns": ["date", "metric_name", "value"]
};

-- Date-partitioned data
SOURCE events TYPE PARQUET PARAMS {
  "path": "data/events/year=2024/month=*/*.parquet",
  "combine_files": true
};
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | string | ✅ | - | File path or glob pattern |
| `columns` | array | ❌ | null | Specific columns to read |
| `combine_files` | boolean | ❌ | true | Combine multiple files into single dataset |

### Path Patterns

The connector supports glob patterns for bulk file operations:

```sql
-- All parquet files in directory
"path": "data/*.parquet"

-- Date-partitioned structure
"path": "data/year=*/month=*/*.parquet"

-- Specific naming pattern
"path": "logs/app_log_*.parquet"
```

## Features

### Schema Inference

Automatic schema detection from Parquet metadata:

```sql
-- Schema is automatically inferred
SOURCE products TYPE PARQUET PARAMS {
  "path": "catalog/products.parquet"
};

-- Use DESCRIBE to see schema
DESCRIBE products;
```

### Column Selection

Optimize performance by reading only required columns:

```sql
-- Read specific columns only
SOURCE user_events TYPE PARQUET PARAMS {
  "path": "events/user_activity.parquet",
  "columns": ["user_id", "event_timestamp", "page_url"]
};
```

### Multiple File Handling

```sql
-- Combine multiple files
SOURCE monthly_sales TYPE PARQUET PARAMS {
  "path": "sales/monthly_*.parquet",
  "combine_files": true
};

-- Process files separately (for very large datasets)
SOURCE large_logs TYPE PARQUET PARAMS {
  "path": "logs/app_*.parquet",
  "combine_files": false
};
```

### Incremental Loading

```sql
-- Incremental loading with timestamp cursor
SOURCE incremental_events TYPE PARQUET PARAMS {
  "path": "events/daily_*.parquet",
  "combine_files": true
}
INCREMENTAL BY event_timestamp;
```

## Error Handling

The connector provides detailed error messages for common issues:

- **File not found**: Clear indication of missing files
- **Invalid format**: Helpful messages for corrupted Parquet files
- **Column mismatch**: Warnings when requested columns don't exist
- **Pattern matching**: Informative errors when no files match patterns

## Performance Tips

### Column Pruning
```sql
-- Only read necessary columns to reduce I/O
SOURCE optimized TYPE PARQUET PARAMS {
  "path": "large_dataset.parquet",
  "columns": ["id", "timestamp", "value"]  -- Instead of reading all columns
};
```

### File Patterns
```sql
-- Use specific patterns to avoid scanning unnecessary files
SOURCE filtered TYPE PARQUET PARAMS {
  "path": "data/year=2024/month=12/*.parquet"  -- Specific partition
};
```

### Memory Management
```sql
-- For very large files, consider not combining them
SOURCE streaming TYPE PARQUET PARAMS {
  "path": "huge_files_*.parquet",
  "combine_files": false  -- Process one file at a time
};
```

## Examples

### Basic Analytics Pipeline

```sql
-- Load sales data
SOURCE sales TYPE PARQUET PARAMS {
  "path": "data/sales.parquet"
};

-- Transform and aggregate
CREATE TABLE monthly_revenue AS
SELECT 
  DATE_TRUNC('month', sale_date) as month,
  SUM(amount) as total_revenue,
  COUNT(*) as transaction_count
FROM sales
GROUP BY month
ORDER BY month;

-- Export results
EXPORT monthly_revenue TO CSV "output/monthly_revenue.csv";
```

### Time-Series Processing

```sql
-- Load time-series data with column selection
SOURCE metrics TYPE PARQUET PARAMS {
  "path": "timeseries/metrics_*.parquet",
  "columns": ["timestamp", "metric_name", "value", "tags"],
  "combine_files": true
};

-- Calculate moving averages
CREATE TABLE smoothed_metrics AS
SELECT 
  timestamp,
  metric_name,
  value,
  AVG(value) OVER (
    PARTITION BY metric_name 
    ORDER BY timestamp 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ) as moving_avg
FROM metrics
ORDER BY metric_name, timestamp;
```

### Data Lake Processing

```sql
-- Process partitioned data lake structure
SOURCE events TYPE PARQUET PARAMS {
  "path": "datalake/events/year=*/month=*/*.parquet",
  "combine_files": true
};

-- Clean and standardize events
CREATE TABLE clean_events AS
SELECT 
  event_id,
  user_id,
  event_type,
  TIMESTAMP(event_time) as normalized_timestamp,
  JSON_EXTRACT_SCALAR(properties, '$.source') as source_system
FROM events
WHERE event_type IS NOT NULL;
```

## Troubleshooting

### Common Issues

**Pattern matches no files**
```
Error: No files found matching pattern: data/missing_*.parquet
```
- Verify the path pattern is correct
- Check file permissions
- Ensure files exist in the specified location

**Column not found**
```
Warning: Column 'missing_col' not found in Parquet file
```
- Check column names in the Parquet schema
- Use `DESCRIBE` to see available columns
- Remove non-existent columns from the `columns` parameter

**Memory issues with large files**
```
Error: Memory allocation failed
```
- Set `combine_files: false` for very large datasets
- Use column selection to reduce memory usage
- Process files in smaller batches

### Best Practices

1. **Use column selection** for large files to improve performance
2. **Verify file patterns** before running production pipelines
3. **Monitor memory usage** when combining many large files
4. **Use specific paths** rather than broad patterns when possible
5. **Test with small datasets** before scaling up

## Schema Examples

### Typical E-commerce Schema
```
user_id: int64
order_id: string
product_id: string
quantity: int32
price: double
order_timestamp: timestamp[ns]
```

### Analytics Events Schema
```
event_id: string
user_id: int64
event_type: string
timestamp: timestamp[ns]
properties: string (JSON)
session_id: string
```

### Financial Data Schema
```
transaction_id: string
account_id: int64
amount: decimal(18,2)
currency: string
transaction_date: date32
description: string
```

### Destination

The Parquet destination connector allows you to write data from your pipeline into a Parquet file.

```sql
EXPORT my_transformed_data TO PARQUET "output/final_data.parquet";
```

The destination takes a single path parameter for the output file.

## 📈 Incremental Loading

This connector supports incremental loading, allowing you to process only new rows since the last pipeline run.

### Configuration

To enable incremental loading, you need to specify the `sync_mode` and `cursor_field` in your source configuration.

- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: The column in your Parquet files that will be used to determine new rows (e.g., a timestamp or an auto-incrementing ID).

```yaml
# profiles/dev.yml
sources:
  incremental_events:
    type: parquet
    path: "data/events/daily_*.parquet"
    combine_files: true
    sync_mode: "incremental"
    cursor_field: "event_timestamp"
```

### Behavior

When a pipeline runs in incremental mode:
1.  SQLFlow retrieves the last saved maximum value (watermark) for the `cursor_field`.
2.  The connector reads the metadata of **all matching Parquet files** to determine the maximum value of the `cursor_field` in each file.
3.  It then reads only the files that could contain new data based on the watermark.
4.  Within those files, it filters for rows where the `cursor_field` value is greater than the watermark.
5.  After a successful pipeline run, SQLFlow updates the watermark with the new maximum value from the processed data.

**Important Note on Performance**: While more efficient than the CSV connector's incremental mode, this approach can still involve significant I/O if you have a large number of files matching your path pattern, as the connector needs to inspect each file's metadata. For optimal performance, use partitioned data structures where possible.

## Error Handling

The connector provides detailed error messages for common issues:

- **File not found**: Clear indication of missing files
- **Invalid format**: Helpful messages for corrupted Parquet files
- **Column mismatch**: Warnings when requested columns don't exist
- **Pattern matching**: Informative errors when no files match patterns

---

**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported 