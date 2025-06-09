# CSV Source Connector

The CSV Source connector allows you to read data from Comma-Separated Values (CSV) files into SQLFlow pipelines. It provides robust support for various CSV formats and encoding options.

## ✅ Features

- **File Format Support**: Standard CSV with customizable delimiters
- **Schema Discovery**: Automatic column detection and type inference
- **Encoding Support**: UTF-8, Latin-1, and other character encodings
- **Header Detection**: Configurable header row handling
- **Column Selection**: Read specific columns to optimize performance
- **Connection Testing**: Validate file access and format
- **Error Handling**: Comprehensive error reporting and validation

## 📋 Configuration

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `path` | `string` | Path to the CSV file | `"data/sales.csv"` |

### Optional Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `has_header` | `boolean` | `true` | Whether the first row contains column headers | `true` |
| `delimiter` | `string` | `","` | Field separator character | `","`, `";"`, `"\t"` |
| `encoding` | `string` | `"utf-8"` | Character encoding of the file | `"utf-8"`, `"latin-1"` |

### Additional Pandas Options

The connector accepts any additional parameters supported by `pandas.read_csv()`:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `nrows` | `integer` | Limit number of rows to read | `1000` |
| `skiprows` | `integer` | Number of rows to skip at start | `5` |
| `na_values` | `list` | Additional strings to recognize as NA/NaN | `["N/A", "NULL"]` |
| `dtype` | `dict` | Data type for columns | `{"amount": "float64"}` |

## 🚀 Usage Examples

### Basic Configuration

```yaml
# profiles/dev.yml
sources:
  sales_data:
    type: csv
    path: data/sales.csv
```

### Advanced Configuration

```yaml
# profiles/dev.yml
sources:
  advanced_csv:
    type: csv
    path: data/complex_data.csv
    has_header: true
    delimiter: ";"
    encoding: "latin-1"
```

### Custom Format Options

```yaml
# profiles/dev.yml
sources:
  custom_format:
    type: csv 
    path: data/data.tsv
    delimiter: "\t"
    na_values: ["N/A", "NULL", ""]
    dtype: {"id": "int64", "amount": "float64"}
```

### Use in Pipeline

```sql
-- pipelines/process_sales.sql
FROM source('sales_data')
SELECT 
    product,
    SUM(CAST(amount AS DECIMAL)) as total_sales,
    COUNT(*) as transaction_count
GROUP BY product
ORDER BY total_sales DESC;
```

## 📈 Incremental Loading

This connector supports incremental loading, allowing you to process only new rows since the last pipeline run.

### Configuration

To enable incremental loading, you need to specify the `sync_mode` and `cursor_field` in your source configuration.

- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: The column in your CSV that will be used to determine new rows (e.g., a timestamp or an auto-incrementing ID).

```yaml
# profiles/dev.yml
sources:
  incremental_sales:
    type: csv
    path: data/sales_updates.csv
    sync_mode: "incremental"
    cursor_field: "updated_at" 
```

### Behavior

When a pipeline runs in incremental mode:
1.  SQLFlow retrieves the last saved maximum value (watermark) for the `cursor_field`.
2.  The connector reads the **entire CSV file** into memory.
3.  It then filters the data, keeping only rows where the `cursor_field` value is greater than the watermark.
4.  After a successful pipeline run, SQLFlow updates the watermark with the new maximum value from the processed data.

**Important Note on Performance**: Because the connector must read the entire file on every run, this approach can be inefficient for very large CSV files. For better performance with large-scale incremental loads, consider using a more optimized file format like Parquet, or a database source.

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