# CSV Connector

The CSV connector provides robust support for reading from and writing to Comma-Separated Values (CSV) files. It's one of the most commonly used connectors in SQLFlow, offering both source and destination capabilities with comprehensive configuration options.

## 🚀 Quick Start

### Reading CSV Files
```yaml
# profiles/dev.yml
sources:
  sales_data:
    type: "csv"
    path: "data/sales.csv"
    has_header: true
    delimiter: ","
    encoding: "utf-8"
```

### Writing CSV Files
```yaml
# profiles/dev.yml
destinations:
  processed_sales:
    type: "csv"
    path: "output/processed_sales.csv"
```

### Use in Pipeline
```sql
-- pipelines/process_sales.sql
FROM source('sales_data')
SELECT product, SUM(amount) as total_sales
GROUP BY product
TO destination('processed_sales');
```

## 📋 Features

| Feature | Source | Destination | Description |
|---------|--------|-------------|-------------|
| **File Reading** | ✅ | ➖ | Read data from CSV files |
| **File Writing** | ➖ | ✅ | Write data to CSV files |
| **Schema Discovery** | ✅ | ➖ | Automatic column detection |
| **Custom Delimiters** | ✅ | ✅ | Support for custom separators |
| **Header Support** | ✅ | ✅ | Optional header row handling |
| **Encoding Support** | ✅ | ✅ | UTF-8, Latin-1, and other encodings |
| **Connection Testing** | ✅ | ➖ | Validate file access |
| **Incremental Loading** | ➖ | ➖ | Cursor-based incremental reads |
| **Write Modes** | ➖ | ✅ | Append, replace modes |

## 📖 Documentation

### 📥 Source Documentation
**[CSV Source Connector →](SOURCE.md)**
- Complete configuration reference
- Reading examples and use cases
- Incremental loading setup
- Troubleshooting guide

### 📤 Destination Documentation  
**[CSV Destination Connector →](DESTINATION.md)**
- Write configuration options
- Output formatting
- Performance optimization
- Error handling

## 💡 Common Use Cases

### Data Import/Export
- **ETL Pipelines**: Extract data from CSV files for transformation
- **Data Export**: Output processed results to CSV for external tools
- **Backup and Archive**: Create CSV backups of processed data

### Data Integration
- **Legacy System Integration**: Connect with systems that export CSV
- **Spreadsheet Analysis**: Process data from Excel/Google Sheets exports
- **Reporting**: Generate CSV reports for business users

### Development and Testing
- **Sample Data**: Use CSV files for development and testing
- **Data Validation**: Compare processed data with expected CSV outputs
- **CI/CD**: Automated testing with CSV datasets

## ⚡ Performance Considerations

### Optimization Tips
- Use **chunked reading** for large files (configured via `batch_size`)
- Enable **column selection** to reduce memory usage  
- Choose appropriate **encoding** to prevent character issues
- Use **streaming mode** for files larger than available memory

### Limitations
- **Single File Processing**: Each connector instance handles one file
- **Memory Usage**: Large files may require chunked processing
- **No Compression**: Plain CSV only (use Parquet for compressed data)

## 🔗 Related Connectors

- **[Parquet Connector](../parquet/README.md)** - For compressed columnar data
- **[S3 Connector](../s3/README.md)** - For CSV files in cloud storage
- **[Google Sheets Connector](../google_sheets/README.md)** - For spreadsheet data

## 🤝 Examples

Browse real-world usage examples:
- **[Conditional Pipelines](../../../examples/conditional_pipelines/)** - CSV-based conditional logic
- **[Load Modes Demo](../../../examples/load_modes/)** - Different loading strategies
- **[Transform Layer Demo](../../../examples/transform_layer_demo/)** - Multi-stage CSV processing

## 📞 Support

Need help with the CSV connector?
- 📚 **Documentation**: Review the [Source](SOURCE.md) and [Destination](DESTINATION.md) guides
- 🐛 **Bug Reports**: [GitHub Issues](https://github.com/giaosudau/sqlflow/issues)
- 💬 **Community**: Join our [Discord/Slack] for questions
- 🏢 **Enterprise**: Contact support for enterprise features

---

**Version**: 2.0 • **Status**: ✅ Production Ready 