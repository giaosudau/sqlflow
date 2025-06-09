# S3 Connector

The S3 connector provides comprehensive support for reading from and writing to Amazon S3 and S3-compatible storage services. It supports multiple file formats, advanced filtering, cost optimization features, and both incremental and full data loading patterns.

## Key Features

- ✅ **Multi-Format Support**: CSV, JSON, JSONL, Parquet, TSV, and more
- ✅ **Cost Optimization**: Intelligent path scanning and request minimization
- ✅ **Partition Awareness**: Optimized reading of partitioned datasets
- ✅ **Incremental Loading**: Time-based and cursor-based incremental patterns
- ✅ **Advanced Filtering**: File pattern matching and content filtering
- ✅ **Compression Support**: Automatic handling of gzip, bz2, and other formats
- ✅ **Schema Inference**: Automatic schema detection and validation
- ✅ **Flexible Authentication**: AWS credentials, IAM roles, and temporary tokens

## Supported File Formats

| Format | Read | Write | Compression | Notes |
|--------|------|-------|-------------|-------|
| **CSV** | ✅ | ✅ | ✅ | Custom delimiters, headers |
| **JSON** | ✅ | ✅ | ✅ | Single objects and arrays |
| **JSONL** | ✅ | ✅ | ✅ | Newline-delimited JSON |
| **Parquet** | ✅ | ✅ | ✅ | Column-based optimization |
| **TSV** | ✅ | ✅ | ✅ | Tab-separated values |
| **TXT** | ✅ | ❌ | ✅ | Plain text files |

## Use Cases

- **Data Lake Operations**: Large-scale data ingestion and processing
- **ETL Pipelines**: Extract-transform-load workflows
- **Data Archival**: Long-term data storage and retrieval
- **Analytics Workloads**: Data preparation for analysis
- **Backup and Recovery**: Data backup and restoration processes
- **Cross-System Integration**: Moving data between different systems

## Quick Start

### As a Source (Legacy URI format)
```yaml
sources:
  s3_data:
    connector: s3
    uri: "s3://my-bucket/data/file.csv"
    aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

### As a Source (New parameter format)
```yaml
sources:
  s3_data:
    connector: s3
    bucket: "my-bucket"
    key: "data/file.csv"
    format: "csv"
    aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

### As a Destination
```yaml
destinations:
  s3_output:
    connector: s3
    bucket: "my-output-bucket"
    key: "output/results.parquet"
    format: "parquet"
    mode: "replace"
    aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

## Documentation

- **[Source Configuration](SOURCE.md)** - Complete source configuration and features
- **[Destination Configuration](DESTINATION.md)** - Complete destination configuration and features

## Authentication Methods

### AWS Credentials
```yaml
aws_access_key_id: "AKIA..."
aws_secret_access_key: "secret..."
aws_session_token: "token..."  # Optional for temporary credentials
```

### IAM Role (recommended for EC2/ECS)
```yaml
# No credentials needed - uses instance profile
region: "us-west-2"
```

### Environment Variables
```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="secret..."
export AWS_DEFAULT_REGION="us-west-2"
```

## Performance Optimization

### Cost-Efficient Reading
- **Path Scanning**: Intelligent directory traversal to minimize API calls
- **Batch Operations**: Group multiple file operations together
- **Conditional Requests**: Use ETags and last-modified dates
- **Selective Loading**: Read only required columns for columnar formats

### Large Dataset Handling
- **Parallel Processing**: Concurrent file processing
- **Streaming**: Memory-efficient processing of large files
- **Partitioning**: Leverage S3 partitioning schemes
- **Compression**: Automatic compression/decompression

## Error Handling

The S3 connector includes comprehensive error handling for:
- **Network Issues**: Automatic retries with exponential backoff
- **Authentication Failures**: Clear error messages and troubleshooting
- **File Not Found**: Graceful handling of missing files
- **Format Errors**: Detailed parsing error reporting
- **Quota Limits**: Rate limiting and throttling management

## Limitations

- **Large Files**: Individual files larger than memory may require streaming
- **Cross-Region**: Higher latency for cross-region operations
- **API Limits**: Subject to S3 request rate limits
- **Consistency**: S3 eventual consistency model may affect immediate reads
- **Costs**: Data transfer and API request costs apply

## Best Practices

1. **Use Partitioning**: Organize data in S3 using logical partitions
2. **Leverage Compression**: Use compression to reduce storage and transfer costs
3. **Optimize File Sizes**: Use 100MB-1GB file sizes for optimal performance
4. **Monitor Costs**: Track S3 usage and optimize access patterns
5. **Use IAM Roles**: Prefer IAM roles over static credentials
6. **Regional Proximity**: Keep compute and storage in the same region 