# In-Memory Destination

## Overview

The In-Memory Destination connector allows you to write data to an in-memory data store (global Python dictionary). This is primarily used for testing, development, and temporary data processing scenarios where you need fast data storage without external dependencies.

## Configuration

### Required Parameters

- **`table_name`**: The name of the in-memory table to write to
  ```yaml
  table_name: "my_results"
  ```

### Optional Parameters

- **`mode`**: Write mode for the destination (default: "append")
  ```yaml
  mode: "append"  # append, replace, merge
  ```

- **`merge_keys`**: Column names to use for merge operations (required for merge mode)
  ```yaml
  merge_keys: ["id", "email"]
  ```

- **`batch_size`**: Number of rows to write per batch (default: 10000)
  ```yaml
  batch_size: 5000
  ```

- **`create_table_if_not_exists`**: Create table if it doesn't exist (default: true)
  ```yaml
  create_table_if_not_exists: false
  ```

## Features

- ✅ **Multiple Write Modes**: Append, replace, and merge operations
- ✅ **Batch Writing**: Configurable batch sizes for large datasets
- ✅ **Merge Support**: Upsert functionality with custom merge keys
- ✅ **Auto Table Creation**: Automatically create tables if they don't exist
- ✅ **Type Preservation**: Maintains data types during writes
- ✅ **Fast Writes**: Direct memory access with no I/O overhead
- ✅ **Schema Validation**: Ensures data compatibility
- ❌ **Persistence**: Data lost when process terminates
- ❌ **Concurrent Writes**: Not thread-safe for concurrent operations

## Write Modes

### Append Mode
Adds new rows to the existing table without removing existing data.

```yaml
destinations:
  results:
    connector: in_memory
    table_name: "user_activity"
    mode: "append"
```

### Replace Mode
Replaces all existing data in the table with new data.

```yaml
destinations:
  results:
    connector: in_memory
    table_name: "daily_summary"
    mode: "replace"
```

### Merge Mode
Updates existing rows and inserts new ones based on merge keys.

```yaml
destinations:
  results:
    connector: in_memory
    table_name: "user_profiles"
    mode: "merge"
    merge_keys: ["user_id"]
```

## Usage Examples

### Basic Configuration

```yaml
destinations:
  test_output:
    connector: in_memory
    table_name: "results"
    mode: "append"
```

### Batch Writing

```yaml
destinations:
  large_output:
    connector: in_memory
    table_name: "transactions"
    mode: "append"
    batch_size: 1000
```

### Merge with Multiple Keys

```yaml
destinations:
  user_data:
    connector: in_memory
    table_name: "users"
    mode: "merge"
    merge_keys: ["user_id", "email"]
```

### Controlled Table Creation

```yaml
destinations:
  strict_output:
    connector: in_memory
    table_name: "validated_data"
    mode: "append"
    create_table_if_not_exists: false
```

## Python Usage

### Basic Writing

```python
from sqlflow.connectors.in_memory import InMemoryDestination
import pandas as pd

# Create destination
destination = InMemoryDestination({
    "table_name": "results",
    "mode": "append"
})

# Write data
data = pd.DataFrame({
    'id': [1, 2, 3],
    'value': [100, 200, 300]
})

destination.write(data)
```

### Merge Operations

```python
# Set up existing data
from sqlflow.connectors.in_memory.storage import memory_store
existing_data = pd.DataFrame({
    'id': [1, 2],
    'name': ['Alice', 'Bob'],
    'score': [85, 90]
})
memory_store.set_table("users", existing_data)

# Configure merge destination
destination = InMemoryDestination({
    "table_name": "users",
    "mode": "merge",
    "merge_keys": ["id"]
})

# Write updated data
new_data = pd.DataFrame({
    'id': [2, 3],  # Update id=2, insert id=3
    'name': ['Bob Updated', 'Charlie'],
    'score': [95, 88]
})

destination.write(new_data)
```

### Reading Results

```python
# After writing, read the results
from sqlflow.connectors.in_memory.storage import memory_store
result_df = memory_store.get_table("results")
print(result_df)
```

## Troubleshooting

### Common Issues

**Table Creation Failed**
```
Error: Cannot create table 'my_table' - table creation disabled
```
**Solution**: Enable table creation or create table manually
```yaml
create_table_if_not_exists: true
```

**Merge Key Not Found**
```
Error: Merge key 'user_id' not found in source data
```
**Solution**: Ensure merge keys exist in your data
```python
print(f"Available columns: {data.columns.tolist()}")
```

**Memory Full**
```
Error: Insufficient memory to write data
```
**Solution**: Reduce batch size or clear unused tables
```yaml
batch_size: 1000
```

**Data Type Mismatch**
```
Error: Cannot merge - data type mismatch for column 'age'
```
**Solution**: Ensure consistent data types
```python
data['age'] = data['age'].astype(int)
```

### Performance Tips

1. **Optimal Batch Size**: Use 1000-10000 rows per batch for best performance
2. **Memory Management**: Clear unused tables regularly
3. **Merge Performance**: Use minimal merge keys for better performance
4. **Data Types**: Use consistent data types to avoid conversion overhead

## Limitations

- **Memory Only**: Data must fit in available system memory
- **No Persistence**: Data lost when application stops
- **Simple Merging**: Basic merge functionality, not full SQL joins
- **Single Process**: Cannot share data between processes
- **No Transactions**: No rollback capability for failed writes
- **Type Constraints**: Limited to pandas-supported data types

## Integration with Testing

### Unit Test Example

```python
import pytest
import pandas as pd
from sqlflow.connectors.in_memory import InMemoryDestination
from sqlflow.connectors.in_memory.storage import memory_store

class TestMyPipeline:
    def setup_method(self):
        # Clear any existing data
        memory_store.clear_all()
    
    def teardown_method(self):
        # Clean up after test
        memory_store.clear_all()
    
    def test_append_writing(self):
        destination = InMemoryDestination({
            "table_name": "test_output",
            "mode": "append"
        })
        
        # Write first batch
        data1 = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
        destination.write(data1)
        
        # Write second batch
        data2 = pd.DataFrame({'id': [3, 4], 'value': [30, 40]})
        destination.write(data2)
        
        # Verify results
        result = memory_store.get_table("test_output")
        assert len(result) == 4
        assert result['id'].tolist() == [1, 2, 3, 4]
    
    def test_merge_writing(self):
        # Set up existing data
        existing = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
        memory_store.set_table("users", existing)
        
        destination = InMemoryDestination({
            "table_name": "users",
            "mode": "merge",
            "merge_keys": ["id"]
        })
        
        # Write merge data
        merge_data = pd.DataFrame({'id': [2, 3], 'name': ['B2', 'C']})
        destination.write(merge_data)
        
        # Verify merge results
        result = memory_store.get_table("users")
        assert len(result) == 3
        assert result[result['id'] == 2]['name'].iloc[0] == 'B2'
```

## Advanced Configuration

### Custom Memory Store

```python
from sqlflow.connectors.in_memory.storage import MemoryStore

# Create custom store
custom_store = MemoryStore()

# Use with destination (requires custom implementation)
```

### Memory Management

```python
# Monitor memory usage
memory_store.get_memory_usage()

# Get table statistics
memory_store.get_table_info("my_table")

# Clear specific table
memory_store.clear_table("my_table")

# Clear all tables
memory_store.clear_all()
```

### Data Validation

```python
# Validate before writing
destination = InMemoryDestination({
    "table_name": "users",
    "mode": "merge",
    "merge_keys": ["id"],
    "validate_schema": True
})

# This will validate data types and constraints
destination.write(your_data)
``` 