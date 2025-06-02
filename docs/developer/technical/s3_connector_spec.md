# Enhanced S3 Connector Technical Specification

**Document Version:** 1.0  
**Date:** January 2025  
**Task:** 2.3 - Enhanced S3 Connector  
**Status:** Phase 2.3.1 - Documentation

---

## Overview

The Enhanced S3 Connector implements industry-standard parameter compatibility with advanced cost management, partition awareness, and multi-format support for production-ready S3 data operations.

## Key Features

### 1. Cost Management
- **Spending Limits**: Configurable cost limits with automatic enforcement
- **Real-time Monitoring**: Track data scanned, requests made, and estimated costs
- **Development Sampling**: Reduce costs with configurable sampling rates
- **Performance Metrics**: Monitor operation costs and optimization opportunities

### 2. Partition Awareness
- **Automatic Detection**: Detect Hive-style and date-based partition patterns
- **Optimized Scanning**: Reduce scan costs by >70% using partition pruning
- **Intelligent Filtering**: Generate optimized prefixes for incremental loading
- **Pattern Recognition**: Support for custom partition schemes

### 3. Multiple File Format Support
- **Format Support**: CSV, Parquet, JSON, JSONL, TSV, Avro
- **Compression**: GZIP, Snappy, LZ4, BROTLI compression support
- **Schema Evolution**: Handle schema changes across file formats
- **Performance Optimization**: Format-specific optimizations

### 4. Industry-Standard Parameters
- **Airbyte Compatibility**: Direct parameter mapping from Airbyte configurations
- **Fivetran Compatibility**: Support for Fivetran-style parameter naming
- **Backward Compatibility**: Existing SQLFlow configurations continue to work
- **Parameter Precedence**: New parameter names take precedence over legacy names

## Technical Architecture

### Cost Management Implementation

```python
class S3CostManager:
    """Advanced cost tracking and limit enforcement."""
    
    COST_PER_GB_REQUEST = 0.0004  # USD per GB for GET requests
    COST_PER_1000_REQUESTS = 0.0004  # USD per 1000 GET requests
    
    def estimate_cost(self, files: List[S3Object]) -> Dict[str, float]:
        """Estimate total operation cost before execution."""
        
    def check_cost_limit(self, estimated_cost: float) -> bool:
        """Prevent operations exceeding cost limits."""
        
    def track_operation(self, data_size_bytes: int, num_requests: int) -> None:
        """Real-time cost tracking during operations."""
```

### Partition Management Implementation

```python
class S3PartitionManager:
    """Intelligent partition detection and optimization."""
    
    def detect_partition_pattern(self, s3_keys: List[str]) -> Optional[PartitionPattern]:
        """Automatically detect partition schemes."""
        
    def build_optimized_prefix(self, base_prefix: str, cursor_value: Any, cursor_field: str) -> str:
        """Generate partition-aware prefixes for efficient scanning."""
        
    def estimate_scan_reduction(self, pattern: PartitionPattern, filter_criteria: Dict) -> float:
        """Calculate expected cost reduction from partition pruning."""
```

### Format-Specific Optimizations

```python
class S3FormatHandler:
    """Format-specific reading and writing optimizations."""
    
    def read_parquet_optimized(self, buffer: io.BytesIO, filters: Dict) -> Iterator[DataChunk]:
        """Parquet reading with pushdown filters and column pruning."""
        
    def read_csv_streaming(self, buffer: io.BytesIO, batch_size: int) -> Iterator[DataChunk]:
        """Memory-efficient CSV reading with streaming."""
        
    def write_with_compression(self, data: DataChunk, format: str, compression: str) -> bytes:
        """Optimized writing with format-specific compression."""
```

## Parameter Specification

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `bucket` | string | S3 bucket name | `"analytics-data"` |

### Optional Parameters

#### Connection & Authentication
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `region` | string | `"us-east-1"` | AWS region |
| `access_key_id` | string | `None` | AWS access key ID |
| `secret_access_key` | string | `None` | AWS secret access key |
| `session_token` | string | `None` | AWS session token |
| `endpoint_url` | string | `None` | Custom S3 endpoint (for MinIO, etc.) |
| `use_ssl` | boolean | `true` | Use SSL/TLS for connections |

#### File Operations
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path_prefix` | string | `""` | S3 key prefix for filtering |
| `file_format` | string | `"csv"` | File format: csv, parquet, json, jsonl, tsv, avro |
| `compression` | string | `None` | Compression: gzip, snappy, lz4, brotli |

#### Cost Management
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cost_limit_usd` | float | `100.0` | Maximum cost per operation (USD) |
| `max_files_per_run` | integer | `10000` | Maximum files to process per run |
| `max_data_size_gb` | float | `1000.0` | Maximum data to scan per run (GB) |
| `dev_sampling` | float | `None` | Sampling rate for development (0.0-1.0) |
| `dev_max_files` | integer | `None` | Max files for development |

#### Partition Awareness
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partition_keys` | array | `None` | Partition column names |
| `partition_filter` | object | `None` | Partition filter criteria |

#### Performance
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | integer | `10000` | Rows per batch |
| `parallel_workers` | integer | `4` | Parallel processing workers |

#### Format-Specific Parameters

**CSV Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `csv_delimiter` | string | `","` | CSV field delimiter |
| `csv_header` | boolean | `true` | First row contains headers |
| `csv_encoding` | string | `"utf-8"` | Character encoding |

**Parquet Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parquet_columns` | array | `None` | Columns to read (column pruning) |
| `parquet_filters` | array | `None` | Pushdown filters |

**JSON Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `json_flatten` | boolean | `true` | Flatten nested JSON |
| `json_max_depth` | integer | `10` | Maximum nesting depth |

## Backward Compatibility

### Legacy Parameter Mapping

| Legacy Parameter | New Parameter | Notes |
|-----------------|---------------|-------|
| `prefix` | `path_prefix` | Automatically mapped |
| `format` | `file_format` | Automatically mapped |
| `access_key` | `access_key_id` | Automatically mapped |
| `secret_key` | `secret_access_key` | Automatically mapped |

### Migration Examples

**Legacy Configuration:**
```json
{
    "bucket": "my-bucket",
    "prefix": "data/",
    "format": "csv",
    "access_key": "AKIA...",
    "secret_key": "xyz..."
}
```

**New Configuration (backward compatible):**
```json
{
    "bucket": "my-bucket",
    "path_prefix": "data/",
    "file_format": "csv",
    "access_key_id": "AKIA...",
    "secret_access_key": "xyz...",
    "cost_limit_usd": 50.0,
    "partition_keys": ["year", "month", "day"]
}
```

## Cost Management Examples

### Development Environment
```json
{
    "bucket": "analytics-data",
    "path_prefix": "events/",
    "file_format": "parquet",
    "cost_limit_usd": 5.0,
    "dev_sampling": 0.1,
    "dev_max_files": 100
}
```

### Production Environment
```json
{
    "bucket": "analytics-data",
    "path_prefix": "events/",
    "file_format": "parquet",
    "cost_limit_usd": 500.0,
    "max_data_size_gb": 1000.0,
    "partition_keys": ["year", "month", "day"],
    "partition_filter": {
        "year": "2024",
        "month": ["01", "02", "03"]
    }
}
```

## Partition Awareness Examples

### Hive-Style Partitions
```
s3://bucket/data/year=2024/month=01/day=15/file1.parquet
s3://bucket/data/year=2024/month=01/day=16/file2.parquet
```

**Configuration:**
```json
{
    "bucket": "bucket",
    "path_prefix": "data/",
    "partition_keys": ["year", "month", "day"],
    "partition_filter": {"year": "2024", "month": "01"}
}
```

### Date-Based Partitions
```
s3://bucket/events/2024/01/15/events.json
s3://bucket/events/2024/01/16/events.json
```

**Configuration:**
```json
{
    "bucket": "bucket",
    "path_prefix": "events/",
    "file_format": "json",
    "sync_mode": "incremental",
    "cursor_field": "event_timestamp"
}
```

## Performance Expectations

### Cost Reduction Targets
- **Partition Pruning**: >70% reduction in scan costs
- **Column Pruning**: >50% reduction for wide tables
- **Development Sampling**: >90% cost reduction in dev environments
- **Format Optimization**: 5-10x performance improvement with Parquet

### Scalability Targets
- **File Count**: Support for >100,000 files per bucket
- **Data Volume**: Handle >1TB datasets efficiently
- **Concurrent Operations**: Support for parallel processing
- **Memory Usage**: Constant memory usage regardless of dataset size

## Error Handling

### Cost Limit Enforcement
```python
class CostLimitError(ConnectorError):
    """Raised when operations would exceed cost limits."""
    
    def __init__(self, estimated_cost: float, limit: float):
        message = f"Operation cost ${estimated_cost:.2f} exceeds limit ${limit:.2f}"
        super().__init__("S3", message)
```

### Partition Error Handling
```python
class PartitionError(ConnectorError):
    """Raised for partition-related issues."""
    
    def __init__(self, partition_issue: str):
        message = f"Partition error: {partition_issue}"
        super().__init__("S3", message)
```

### Format Error Handling
```python
class FormatError(ConnectorError):
    """Raised for file format issues."""
    
    def __init__(self, format_issue: str):
        message = f"Format error: {format_issue}"
        super().__init__("S3", message)
```

## Testing Strategy

### Unit Tests
- Parameter validation for all supported configurations
- Cost estimation accuracy
- Partition pattern detection
- Format-specific reading/writing
- Error handling for edge cases

### Integration Tests
- End-to-end operations with real S3/MinIO
- Cost limit enforcement under load
- Partition pruning effectiveness
- Multi-format compatibility
- Performance benchmarking

### Demo Scenarios
- Development environment with cost controls
- Production environment with partitioning
- Multi-format data pipeline
- Cost optimization demonstrations
- Migration from legacy configurations

## Success Criteria

### Technical Criteria
- ✅ All pytest tests passing (>90% coverage)
- ✅ Cost management prevents unexpected charges
- ✅ Partition awareness reduces scan costs by >70%
- ✅ Multi-format support works reliably
- ✅ Backward compatibility maintained

### Performance Criteria
- ✅ Memory usage remains constant for streaming operations
- ✅ Parallel processing improves throughput
- ✅ Format-specific optimizations provide measurable benefits
- ✅ Cost estimation accuracy within 10% of actual costs

### Demo Criteria
- ✅ Cost controls prevent runaway operations
- ✅ Partition optimization visibly reduces costs
- ✅ Multi-format pipeline works end-to-end
- ✅ Migration scenarios work seamlessly
- ✅ Error handling provides clear guidance

## Implementation Timeline

- **Day 1**: Documentation complete
- **Day 2-4**: Implementation with testing
- **Day 4**: Testing validation
- **Day 5**: Demo verification and commit

## References

- [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/)
- [Parquet Format Specification](https://parquet.apache.org/docs/)
- [Hive Partitioning Documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables)
- [SQLFlow Testing Standards](docs/04_testing_standards.md)
- [Task 2.3 Implementation Plan](sqlflow_connector_implementation_tasks.md#task-23-enhanced-s3-connector) 