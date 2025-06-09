# Parquet Destination

The Parquet Destination connector writes the results of a pipeline to a `.parquet` file. For a general overview of the Parquet connector, see the [main README](./README.md).

## Configuration

The destination is configured in your `profiles.yml` file.

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | `string` | Must be `"parquet"`. |
| `path` | `string` | The full file path for the output `.parquet` file. |

### Optional Parameters
The `ParquetDestination` connector passes any additional, non-standard options directly to the underlying `pandas.to_parquet` function. This allows for advanced control over the output file. For a full list of available options, please refer to the [official pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_parquet.html).

Common options include:
- `compression` (string, default 'snappy'): The compression method to use (e.g., `'snappy'`, `'gzip'`, `'brotli'`).
- `engine` (string, default 'auto'): The Parquet library to use (`'pyarrow'` or `'fastparquet'`).

### Example Configuration

```yaml
# profiles/dev.yml
destinations:
  # Simple configuration
  output_data:
    type: "parquet"
    path: "output/final_report.parquet"

  # Advanced configuration with options
  compressed_output:
    type: "parquet"
    path: "output/compressed_data.parquet"
    compression: "gzip" # Pass a pandas `to_parquet` option
```

## 🚀 Usage

You can write to a configured Parquet destination using the `WRITE` or `TO` keywords in your pipeline.

```sql
-- pipelines/export.sql
FROM source('my_data')
SELECT *
WHERE status = 'processed'
WRITE 'output_data';
```

## 📝 Write Modes

This connector only supports **overwrite mode**.

- **REPLACE (overwrite)**: If a file already exists at the specified `path`, it will be completely replaced with the new data.

`APPEND` and `UPSERT` modes are **not supported**. If you need to append data, you should read the existing file, combine it with your new data in your pipeline, and then use the destination to overwrite the original file with the combined result.

## ❌ Limitations

- **No Streaming Writes**: The connector buffers the entire result DataFrame in memory before writing it to a file. It is not suitable for writing datasets that are larger than available RAM.
- **No Automatic Directory Creation**: The directory for the output `path` must exist before the pipeline is run. The connector will not create it for you and will raise an error if it is missing. 