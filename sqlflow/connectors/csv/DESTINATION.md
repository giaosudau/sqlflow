# CSV Destination Connector

The CSV Destination connector enables you to write processed data from SQLFlow pipelines to Comma-Separated Values (CSV) files. It provides flexible configuration options for output formatting and supports all standard CSV export features.

## ✅ Features

- **File Writing**: Write DataFrames to CSV files
- **Format Control**: Customize delimiters, headers, and encoding
- **Write Modes**: Append, replace, and overwrite operations
- **Index Control**: Configure row index inclusion
- **Encoding Support**: UTF-8, Latin-1, and other character encodings
- **Column Selection**: Write specific columns only
- **Quote Handling**: Control quoting behavior for special characters
- **Large Data Support**: Memory-efficient writing for large datasets

## 📋 Configuration

When using the CSV destination in an `EXPORT` step, you configure it via the `TYPE` and `OPTIONS` clauses.

### EXPORT Options
| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `path` | `string` | The local path for the output CSV file. | ✅ | `"/data/processed/report.csv"` |
| `delimiter` | `string` | The character to use as a field separator. | `,` (default) | `"\t"` |
| `header` | `boolean`| Whether to write the column names as the first row. | `true` (default) | `false` |

## 🚀 Usage Examples

### Basic Configuration

```yaml
# profiles/dev.yml
destinations:
  sales_output:
    type: "csv"
    path: "output/processed_sales.csv"
```

### Advanced Configuration with Options

```yaml
# profiles/dev.yml
destinations:
  formatted_output:
    type: "csv"
    path: "output/report.csv"
```

With pipeline-specific options:
```python
# Configure via pipeline or code
write_options = {
    "index": False,
    "sep": ";",
    "encoding": "latin-1",
    "quoting": 1  # Quote all fields
}
```

### Use in Pipeline

```sql
-- pipelines/export_results.sql
FROM source('input_data')
SELECT 
    customer_id,
    customer_name,
    SUM(order_amount) as total_spent,
    COUNT(*) as order_count
GROUP BY customer_id, customer_name
TO destination('sales_output');
```

## 📝 Write Modes and Behavior

### Default Behavior
- **Overwrites** existing files completely
- **Includes** column headers by default
- **Excludes** pandas index by default (recommended)
- **Uses** UTF-8 encoding

### Custom Write Options

```python
# Common configurations
write_options = {
    # Remove pandas index from output
    "index": False,
    
    # Use semicolon separator
    "sep": ";",
    
    # Don't include headers (for appending)
    "header": False,
    
    # Force quotes around all text fields
    "quoting": 1,
    
    # Use Windows line endings
    "line_terminator": "\r\n"
}
```

## 🔄 Append vs Replace Operations

### Replace Mode (Default)
```yaml
# profiles/dev.yml
destinations:
  daily_report:
    type: "csv"
    path: "reports/daily_report.csv"
    # Overwrites file completely each run
```

### Append Mode
```python
# For append operations, disable headers after first write
write_options = {
    "mode": "a",      # Append mode
    "header": False,  # Skip headers when appending
    "index": False
}
```

## 📊 Data Type Handling

The connector automatically handles pandas data types:

| Pandas Type | CSV Output | Notes |
|-------------|------------|-------|
| `int64` | `123` | Integer values |
| `float64` | `123.45` | Decimal numbers |
| `datetime64` | `2024-01-15 10:30:00` | ISO format timestamps |
| `bool` | `True`/`False` | Boolean values |
| `object` (string) | `"text"` | Text data (quoted if needed) |
| `category` | `"category_name"` | Categorical data as strings |

### Custom Data Formatting

```python
# Pre-format data before writing
df['amount'] = df['amount'].round(2)  # 2 decimal places
df['date'] = df['date'].dt.strftime('%Y-%m-%d')  # Custom date format
df['percentage'] = (df['percentage'] * 100).astype(str) + '%'  # Add % symbol
```

## 🎯 Column Selection and Ordering

### Select Specific Columns
```python
# Write only specific columns
write_options = {
    "columns": ["customer_id", "total_amount", "created_date"],
    "index": False
}
```

### Reorder Columns
```python
# Reorder columns in output
column_order = ["id", "name", "amount", "date"]
write_options = {
    "columns": column_order,
    "index": False
}
```

## 🔍 File Validation

The connector performs basic validation:

### Pre-Write Checks
- ✅ **Directory exists** or can be created
- ✅ **Write permissions** to target location
- ✅ **Valid file path** format

### Error Handling
- `"Directory not found: {path}"` - Parent directory missing
- `"Permission denied: {path}"` - No write access
- `"Invalid file path: {path}"` - Malformed path

## 🛠️ Troubleshooting

### Common Issues

**Issue**: File permission errors
```
✅ Solution: Check directory permissions
# Ensure output directory exists and is writable
mkdir -p output/
chmod 755 output/
```

**Issue**: Character encoding problems
```
✅ Solution: Specify encoding explicitly
write_options = {"encoding": "utf-8-sig"}  # Adds BOM for Excel
```

**Issue**: Excel compatibility
```
✅ Solution: Use Excel-friendly settings
write_options = {
    "sep": ",",
    "encoding": "utf-8-sig",  # BOM for Excel
    "index": False,
    "quoting": 1  # Quote all text fields
}
```

**Issue**: Large file performance
```
✅ Solution: Use chunked processing
# Process data in smaller batches
# Consider Parquet format for large datasets
```

### Performance Optimization

**Large Files**:
- Consider using Parquet connector for better performance
- Process data in chunks if memory is limited
- Use appropriate data types to reduce file size

**Network Storage**:
- Write to local storage first, then copy
- Use compression-enabled formats when possible
- Monitor write speeds and adjust batch sizes

## ❌ Limitations

| Limitation | Description | Alternative |
|------------|-------------|-------------|
| **No Compression** | Plain text CSV only | Use Parquet connector |
| **Single File** | Each destination writes one file | Use multiple destination definitions |
| **No Partitioning** | No built-in partition support | Use S3 connector for partitioned output |
| **Memory Usage** | Large DataFrames held in memory | Process in smaller chunks |
| **No Transactional Writes** | Partial writes on errors | Implement error handling in pipeline |

## 📈 Best Practices

### File Organization
```
output/
├── daily/
│   ├── sales_2024-01-15.csv
│   └── sales_2024-01-16.csv
├── monthly/
│   └── sales_2024-01.csv
└── yearly/
    └── sales_2024.csv
```

### Naming Conventions
```yaml
# profiles/dev.yml
# Use timestamps in filenames
destinations:
  timestamped_output:
    type: "csv"
    path: "output/data_{run_date}.csv"
```

### Data Quality
- Validate data before writing
- Handle null values appropriately
- Use consistent date/time formats
- Round numeric values to appropriate precision

## 🔗 Related Documentation

- **[CSV Source Connector](SOURCE.md)** - Reading CSV files
- **[S3 Destination Connector](../s3/DESTINATION.md)** - CSV files in cloud storage
- **[Parquet Destination Connector](../parquet/DESTINATION.md)** - Compressed alternative
- **[Pipeline Configuration Guide](../../../docs/user-guides/pipeline-configuration.md)** - Advanced pipeline patterns

## 📞 Support

- 📚 **Documentation**: [CSV Connector Overview](README.md)
- 🐛 **Issues**: [GitHub Issues](https://github.com/giaosudau/sqlflow/issues)
- 💬 **Community**: [Discord/Slack Community]
- 🏢 **Enterprise**: Contact for advanced features

---

**Version**: 2.0 • **Status**: ✅ Production Ready • **Write Modes**: ✅ Replace, Append 