# In-Memory Connector

The In-Memory connector provides a lightweight, memory-based data storage solution primarily used for testing, development, and small-scale data processing. It allows you to store and retrieve data directly in memory without requiring external databases or file systems.

## Key Features

- ✅ **Lightning Fast**: No disk I/O, all operations in memory
- ✅ **Zero Setup**: No external dependencies or configuration required
- ✅ **Full CRUD Support**: Complete source and destination functionality
- ✅ **Schema Inference**: Automatic schema detection from data
- ✅ **Testing Friendly**: Perfect for unit tests and development
- ✅ **Data Validation**: Type checking and constraint validation

## Use Cases

- **Unit Testing**: Mock data sources for testing pipelines
- **Data Prototyping**: Quick data experimentation and development
- **Temporary Storage**: Intermediate data processing steps
- **Small Datasets**: Simple data transformations without external storage

## Quick Start

### As a Source
```yaml
sources:
  memory_data:
    connector: in_memory
    table_name: "test_data"
```

### As a Destination
```yaml
destinations:
  memory_output:
    connector: in_memory
    table_name: "results"
    mode: "append"  # append, replace, merge
```

## Documentation

- **[Source Configuration](SOURCE.md)** - Complete source configuration and features
- **[Destination Configuration](DESTINATION.md)** - Complete destination configuration and features

## Limitations

- **Memory Constrained**: Limited by available system memory
- **Non-Persistent**: Data lost when process terminates
- **Single Process**: Not suitable for distributed processing
- **No Concurrency**: Not thread-safe for concurrent operations

## Best Practices

1. **Use for Testing**: Ideal for unit tests and development environments
2. **Small Data Only**: Keep datasets under 100MB for optimal performance
3. **Clear After Use**: Explicitly clear memory to prevent accumulation
4. **Validate Early**: Use schema validation to catch issues early 