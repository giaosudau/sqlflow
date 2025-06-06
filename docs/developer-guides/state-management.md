# SQLFlow State Management & Incremental Loading

## Overview

SQLFlow's state management system provides reliable, atomic incremental loading capabilities for data pipelines. The implementation focuses on simplicity and correctness, using DuckDB as the state backend to ensure consistency with the main analytics engine.

**Core Philosophy: Atomic Operations Without Complexity**
- Watermarks updated only after successful completion
- No artificial checkpoints within operations
- Clear failure and recovery semantics
- SQL-native incremental patterns

## Current Implementation Status

### ✅ Implemented Components

**State Management Infrastructure**:
- `WatermarkManager`: Atomic watermark management with transaction support
- `DuckDBStateBackend`: Persistent state storage using DuckDB tables
- Industry-standard parameter parsing (`sync_mode`, `cursor_field`, `primary_key`)
- Automatic incremental loading integration with LocalExecutor

**Connector Integration**:
- Enhanced PostgreSQL connector with incremental support
- CSV connector with incremental filtering capabilities
- S3 connector with partition-aware incremental loading
- Automatic watermark persistence across pipeline runs

### 🚧 Development Status

**Phase 2 Completed**: Enhanced state management and industry-standard connectors
**Phase 3 In Progress**: SaaS connectors (Shopify, Stripe, HubSpot)

## Technical Architecture

### Watermark Manager

The `WatermarkManager` class handles atomic watermark operations:

```python
# Source: sqlflow/core/state/watermark_manager.py
class WatermarkManager:
    """Manages watermarks for incremental loading operations."""
    
    def __init__(self, state_backend: StateBackend):
        self.backend = state_backend
    
    def get_source_watermark(self, pipeline: str, source: str, cursor_field: str) -> Any:
        """Get last processed value for incremental loading."""
        key = self.get_state_key(pipeline, source, source, cursor_field)
        return self.backend.get(key)
    
    def update_source_watermark(self, pipeline: str, source: str, 
                               cursor_field: str, value: Any) -> None:
        """Update watermark atomically after successful operation."""
        key = self.get_state_key(pipeline, source, source, cursor_field)
        with self.backend.transaction():
            self.backend.set(key, value, datetime.utcnow())
```

### DuckDB State Backend

State persistence uses DuckDB tables for simplicity and consistency:

```sql
-- Watermark state table schema
CREATE TABLE IF NOT EXISTS sqlflow_watermarks (
    id INTEGER PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    target_table VARCHAR NOT NULL,
    cursor_field VARCHAR NOT NULL,
    cursor_value VARCHAR NOT NULL,  -- JSON for complex types
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sync_mode VARCHAR NOT NULL,
    UNIQUE(pipeline_name, source_name, target_table, cursor_field)
);

-- Execution history for debugging
CREATE TABLE IF NOT EXISTS sqlflow_execution_history (
    id INTEGER PRIMARY KEY,
    watermark_id INTEGER REFERENCES sqlflow_watermarks(id),
    execution_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_end TIMESTAMP,
    rows_processed INTEGER,
    status VARCHAR DEFAULT 'running',
    error_message TEXT
);
```

### LocalExecutor Integration

The LocalExecutor automatically handles incremental loading when `sync_mode: "incremental"` is specified:

```python
# Source: sqlflow/core/executors/local_executor.py
def _execute_source_definition(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute SOURCE with automatic incremental support."""
    sync_mode = step_config.get("sync_mode", "full_refresh")
    
    if sync_mode == "incremental":
        return self._execute_incremental_source_definition(step_config)
    else:
        return self._execute_full_refresh_source_definition(step_config)

def _execute_incremental_source_definition(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute incremental SOURCE with automatic watermark management."""
    source_name = step_config["name"]
    cursor_field = step_config["cursor_field"]
    
    # Get current watermark
    last_cursor_value = self.watermark_manager.get_source_watermark(
        pipeline=self.pipeline_name,
        source=source_name,
        cursor_field=cursor_field
    )
    
    # Read incremental data
    connector = self.connector_manager.get_connector(source_name)
    max_cursor_value = last_cursor_value
    
    for chunk in connector.read_incremental(
        object_name=source_name,
        cursor_field=cursor_field,
        cursor_value=last_cursor_value
    ):
        # Store chunk and track max cursor value
        self.table_data[source_name] = chunk
        chunk_max = self._get_max_cursor_value(chunk, cursor_field)
        if chunk_max and (not max_cursor_value or chunk_max > max_cursor_value):
            max_cursor_value = chunk_max
    
    # Update watermark after successful read
    if max_cursor_value != last_cursor_value:
        self.watermark_manager.update_source_watermark(
            pipeline=self.pipeline_name,
            source=source_name,
            cursor_field=cursor_field,
            value=max_cursor_value
        )
```

## Industry-Standard Parameters

SQLFlow uses parameter conventions compatible with Airbyte and Fivetran for easy migration:

### Incremental Loading Parameters

```sql
-- Industry-standard incremental configuration
SOURCE orders TYPE POSTGRES PARAMS {
    "host": "${DB_HOST}",
    "database": "ecommerce",
    "table": "orders",
    "sync_mode": "incremental",        -- full_refresh, incremental, cdc
    "cursor_field": "updated_at",      -- Field for incremental tracking
    "primary_key": ["order_id"],       -- Array format (Airbyte/Fivetran standard)
    "replication_start_date": "2024-01-01T00:00:00Z"
};
```

### Supported Sync Modes

**full_refresh** (Default):
- Loads complete dataset on every run
- Replaces existing data completely
- No state management required

**incremental**:
- Loads only new/updated records since last run
- Requires `cursor_field` parameter
- Automatic watermark management
- Efficient for large datasets

**cdc** (Future):
- Change data capture for real-time updates
- Planned for Phase 4 development

## Connector Implementation Patterns

### Supporting Incremental Loading

Connectors implement incremental support through standardized methods:

```python
# Example from PostgreSQL connector
def supports_incremental(self) -> bool:
    """Check if connector supports incremental loading."""
    return True

def read_incremental(self, object_name: str, cursor_field: str, 
                    cursor_value: Optional[Any] = None, **kwargs) -> Iterator[DataChunk]:
    """Read data incrementally with cursor-based filtering."""
    
    # Build query with WHERE clause for incremental filtering
    base_query = self._build_incremental_query(object_name, cursor_field)
    
    # Add cursor filter if watermark exists
    params = []
    if cursor_value is not None:
        base_query += f' WHERE "{cursor_field}" > %s'
        params.append(cursor_value)
    
    # Execute and yield data chunks
    return self._execute_query_chunks(base_query, params)
```

### CSV Connector Example

The CSV connector implements simple in-memory filtering:

```python
# Source: sqlflow/connectors/csv_connector.py
def read_incremental(self, object_name: str, cursor_field: str, 
                    cursor_value: Optional[Any] = None, **kwargs) -> Iterator[DataChunk]:
    """Read CSV with automatic filtering based on cursor field."""
    
    # Read all data first (simple approach for CSV)
    chunks = list(self.read(object_name, columns, None, batch_size))
    
    for chunk in chunks:
        df = chunk.pandas_df
        
        if cursor_value is not None and cursor_field in df.columns:
            # Filter data based on cursor value
            filtered_df = df[df[cursor_field] > cursor_value]
            yield DataChunk(filtered_df)
        else:
            yield chunk
```

## Usage Patterns

### Basic Incremental Loading

```sql
-- Define incremental source
SOURCE customer_updates TYPE POSTGRES PARAMS {
    "host": "localhost",
    "database": "crm",
    "table": "customers",
    "sync_mode": "incremental",
    "cursor_field": "updated_at"
};

-- Load incrementally
LOAD customers FROM customer_updates MODE APPEND;
```

### Multi-Source Incremental Pipeline

```sql
-- Multiple incremental sources
SOURCE orders TYPE POSTGRES PARAMS {
    "table": "orders",
    "sync_mode": "incremental",
    "cursor_field": "updated_at"
};

SOURCE products TYPE POSTGRES PARAMS {
    "table": "products", 
    "sync_mode": "incremental",
    "cursor_field": "modified_date"
};

-- Each maintains separate watermarks
LOAD order_data FROM orders MODE APPEND;
LOAD product_data FROM products MODE UPSERT UPSERT_KEYS product_id;
```

### Partition-Aware Incremental (S3)

```sql
-- S3 with partition optimization
SOURCE events TYPE S3 PARAMS {
    "bucket": "data-lake",
    "prefix": "events/",
    "file_format": "parquet",
    "sync_mode": "incremental",
    "cursor_field": "event_timestamp",
    "partition_keys": ["year", "month", "day"]
};

-- Automatically optimizes file scanning based on watermarks
LOAD event_data FROM events MODE APPEND;
```

## State Management CLI Commands

SQLFlow provides CLI commands for state inspection and management:

```bash
# List all watermarks
sqlflow state list

# Show watermarks for specific pipeline
sqlflow state list --pipeline my_pipeline

# Reset watermark for source
sqlflow state reset --pipeline my_pipeline --source orders --cursor-field updated_at

# Show state backend information
sqlflow state info
```

## Error Handling and Recovery

### Watermark Isolation

Failures during incremental loading don't corrupt watermark state:

```python
# From integration tests
def test_error_preserves_watermark_state(self):
    """Test that errors during incremental loading don't corrupt state."""
    initial_watermark = "2024-01-15 10:00:00"
    
    # Set initial watermark
    watermark_manager.update_source_watermark(
        pipeline="test_pipeline",
        source="orders", 
        cursor_field="updated_at",
        value=initial_watermark
    )
    
    # Simulate error during read
    with mock_connector_error():
        result = executor._execute_source_definition(incremental_step)
        assert result["status"] == "error"
    
    # Verify watermark unchanged
    preserved_watermark = watermark_manager.get_source_watermark(
        pipeline="test_pipeline",
        source="orders",
        cursor_field="updated_at"
    )
    assert preserved_watermark == initial_watermark
```

### Recovery Patterns

**Automatic Recovery**: Incremental loading automatically resumes from last successful watermark.

**Manual Recovery**: CLI commands allow watermark reset for data reprocessing.

**Partial Failure Handling**: Each source maintains independent watermarks, so failures in one source don't affect others.

## Performance Considerations

### Watermark Storage Optimization

- Indexed lookups using unique constraints
- Minimal overhead for watermark operations
- Transaction-based atomic updates

### Memory Management

- Streaming data processing with DataChunk iteration
- Constant memory usage regardless of dataset size
- Efficient cursor value extraction and comparison

### Query Optimization

- WHERE clause generation for database sources
- Partition pruning for cloud storage sources
- Batch processing with configurable sizes

## Debugging and Monitoring

### Debug Logging

The state management system provides comprehensive logging:

```python
# Structured logging for watermark operations
logger.info(f"Incremental SOURCE {source_name}: last_cursor_value={last_cursor_value}")
logger.info(f"Updated watermark for {source_name}: {new_watermark}")
logger.debug(f"Incremental filtering: {filtered_rows} rows after cursor {cursor_value}")
```

### Execution History

The `sqlflow_execution_history` table tracks incremental runs for monitoring:

```sql
-- Query execution history
SELECT 
    w.pipeline_name,
    w.source_name,
    w.cursor_field,
    h.execution_start,
    h.rows_processed,
    h.status
FROM sqlflow_watermarks w
JOIN sqlflow_execution_history h ON w.id = h.watermark_id
ORDER BY h.execution_start DESC;
```

## Testing Framework

### Integration Tests

State management includes comprehensive integration tests:

```python
# Source: tests/integration/core/test_complete_incremental_loading_flow.py
class TestCompleteIncrementalLoadingFlow:
    def test_initial_incremental_load_processes_all_data(self):
        """Test initial load establishes watermark correctly."""
        
    def test_subsequent_incremental_load_filters_by_watermark(self):
        """Test subsequent loads use watermark filtering."""
        
    def test_incremental_load_updates_watermark_after_success(self):
        """Test watermark updates after successful operation."""
        
    def test_multiple_sources_maintain_separate_watermarks(self):
        """Test independent watermark management per source."""
```

### Unit Tests

```python
# Source: tests/unit/core/state/test_watermark_manager.py
class TestWatermarkManager:
    def test_watermark_creation_and_retrieval(self):
        """Test basic watermark operations."""
        
    def test_atomic_watermark_updates(self):
        """Test transaction-based atomic updates."""
        
    def test_concurrent_watermark_access(self):
        """Test thread-safe watermark operations."""
```

## Future Development

### Planned Enhancements

**Schema Evolution Handling** (Phase 4):
- Automatic detection of source schema changes
- Policy-based evolution strategies
- Migration guidance for breaking changes

**Advanced CDC Support** (Phase 4):
- Real-time change data capture
- Event-based incremental processing
- Stream processing integration

**Cross-Environment State Management** (Future):
- State synchronization across environments
- Backup and restore capabilities
- State migration tools

### Implementation Notes

**Important**: Future development priorities may change based on:
- User feedback and real-world usage patterns
- Technical discoveries during implementation
- Community contributions and requirements
- Resource availability and project priorities

The current implementation provides a solid foundation for incremental loading while maintaining simplicity and reliability.

## Related Documentation

- [Extending SQLFlow](extending-sqlflow.md) - Building connectors with incremental support
- [UDF System](udf-system.md) - User-defined functions and data processing
- [Architecture Overview](architecture-overview.md) - System design and technical decisions 