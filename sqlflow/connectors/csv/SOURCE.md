# CSV Source Connector

The CSV Source connector reads data from local Comma-Separated Values (CSV) files. It can be configured to handle different delimiters and files with or without headers.

## ✅ Features

- **Local File System**: Reads CSV files from the local disk where SQLFlow is running.
- **Custom Delimiters**: Supports custom delimiters, allowing it to parse files like TSV (Tab-Separated Values).
- **Header Detection**: Can be configured to treat the first row as a header or not.
- **Schema Inference**: Automatically infers column types from the CSV data.
- **Incremental Reads**: Supports a basic incremental loading pattern.

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"csv"`. | ✅ | `"csv"` |
| `path` | `string` | The local path to a single `.csv` file. | ✅ | `"/data/raw/users.csv"`|
| `delimiter` | `string` | The character used to separate fields. | `,` (default) | `"\t"` for TSV |
| `header` | `boolean` | Whether the first row of the CSV is a header row. | `true` (default) | `false` |

### Authentication
This connector reads from the local filesystem and does not require any authentication parameters.

## 💡 Example

This example defines a source named `raw_users` that reads a local CSV file.

```sql
SOURCE raw_users TYPE CSV PARAMS {
    "path": "/data/staging/new_users.csv",
    "header": true
};
```

You can then load this data into a table:
```sql
LOAD users_table FROM raw_users;
```

## 📈 Incremental Loading

This connector supports a basic incremental loading strategy suitable for files where new rows are always added to the end and a specific column (e.g., an ID or timestamp) is always increasing.

> **⚠️ Performance Warning**: The current implementation reads the **entire CSV file** into memory before filtering for new records based on the cursor value. This can be inefficient for very large CSV files.

### Behavior
When a pipeline runs in incremental mode, the connector:
1. Reads the whole CSV file specified in `path`.
2. Filters the resulting DataFrame to include only rows where the `cursor_field` is greater than the last saved `cursor_value`.
3. Processes the filtered (new) rows.

This assumes the data in the `cursor_field` is sorted in ascending order within the file.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported (with caveats)

## 📊 Schema Discovery

The connector automatically discovers the schema by:

1. **Reading Sample Data**: Analyzes first 100 rows for type inference
2. **Type Detection**: Uses pandas automatic type inference
3. **Column Names**: Extracted from header row or generated automatically
4. **Arrow Schema**: Converts to PyArrow schema for consistency

### Manual Schema Override

```yaml
# profiles/dev.yml
sources:
  typed_data:
    type: "csv"
    path: "data/data.csv"
    dtype:
      user_id: "int64"
      created_at: "datetime64[ns]"
      amount: "float64"
      category: "string"
```

## 🎯 Column Selection

Optimize performance by reading only required columns:

```python
# Via pipeline configuration
columns = ["user_id", "amount", "created_at"]
```

This reduces:
- **Memory usage** - Only selected columns loaded
- **Processing time** - Less data to transfer
- **Network I/O** - For remote files

## 🔍 Connection Testing

The connector validates files before processing:

### Validation Checks
- ✅ **File Existence**: File path is accessible
- ✅ **Read Permissions**: File can be opened for reading
- ✅ **Format Validation**: File can be parsed as CSV
- ✅ **Column Detection**: Headers and structure are valid

### Error Messages
- `"CSV file not found: {path}"` - File doesn't exist
- `"CSV file not readable: {path}"` - Permission denied
- `"CSV file is empty"` - No data in file
- `"CSV file format error: {details}"` - Parsing failed

## 🛠️ Troubleshooting

### Common Issues

**Issue**: `UnicodeDecodeError`
```yaml
# profiles/dev.yml
# ✅ Solution: Specify correct encoding
sources:
  my_source:
    type: csv
    path: path/to/my.csv
    encoding: "latin-1"  # or "cp1252", "iso-8859-1"
```

**Issue**: Wrong delimiter detection
```yaml
# profiles/dev.yml
# ✅ Solution: Explicitly set delimiter
sources:
  my_source:
    type: csv
    path: path/to/my.csv
    delimiter: ";"  # or "\t" for tab-separated
```

**Issue**: Header not detected correctly
```yaml
# profiles/dev.yml
# ✅ Solution: Configure header setting
sources:
  my_source:
    type: csv
    path: path/to/my.csv
    has_header: false  # if no header row
```

**Issue**: Memory usage with large files
```yaml
# profiles/dev.yml
# ✅ Solution: Use column selection and sampling
sources:
  my_source:
    type: csv
    path: path/to/my.csv
    # This reads only the first 50000 rows, it is not chunking
    nrows: 50000 
```

### Performance Optimization

**Large Files**:
- Use `nrows` parameter for sampling large files.
- Enable column selection to reduce memory.
- For true chunking and better performance, consider converting your data to Parquet.

**Character Issues**:
- Set appropriate `encoding` parameter.
- Use `na_values` to handle missing data markers.
- Specify `dtype` for consistent data types.

## ❌ Limitations

| Limitation | Description | Alternative |
|------------|-------------|-------------|
| **Single File** | Each connector instance handles one file | Use multiple source definitions |
| **No Compression** | Plain text CSV only | Use Parquet connector for compression |
| **In-Memory Incremental** | The connector loads the entire CSV file into memory before processing. This applies to both full and incremental reads, making it inefficient for large files. | For performant incremental loading on large datasets, use the Parquet connector or a database source. |
| **No Partitioning** | No built-in partition support | Use S3 connector for partitioned data |
| **Static Schema** | Schema determined at runtime | Use explicit `dtype` for consistency |

## 🔗 Related Documentation

- **[CSV Destination Connector](DESTINATION.md)** - Writing CSV files
- **[S3 Source Connector](../s3/SOURCE.md)** - CSV files in cloud storage
- **[Parquet Source Connector](../parquet/SOURCE.md)** - Compressed alternative
- **[Incremental Loading Guide](../../../docs/user-guides/incremental-loading.md)** - Advanced incremental patterns

## 📞 Support

- 📚 **Documentation**: [CSV Connector Overview](README.md)
- 🐛 **Issues**: [GitHub Issues](https://github.com/giaosudau/sqlflow/issues)
- 💬 **Community**: [Discord/Slack Community]
- 🏢 **Enterprise**: Contact for advanced features

---

**Version**: 2.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported 