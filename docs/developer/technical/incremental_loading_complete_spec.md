# SQLFlow Complete Incremental Loading Specification

**Document Version:** 1.0  
**Date:** January 2025  
**Author:** Phase 2 Implementation Team  
**Status:** Phase 2.0.1 - Documentation Phase  

---

## Executive Summary

This document specifies the complete incremental loading integration for SQLFlow Phase 2, bridging the critical gap between implemented watermark management, industry-standard parameter parsing, and actual automatic incremental execution.

### Current State Analysis

**✅ What Works (Phase 1 Completed):**
- Industry-standard parameters (`sync_mode`, `cursor_field`, `primary_key`) are parsed and validated
- WatermarkManager infrastructure exists with atomic updates
- DuckDBStateBackend persists watermarks correctly
- Enhanced debugging and logging infrastructure operational

**❌ Critical Gap Identified:**
- SOURCE parameters with `sync_mode: "incremental"` don't trigger automatic incremental behavior
- Connectors read full datasets regardless of watermark values
- LOAD operations use manual MERGE instead of watermark-based filtering
- No automatic cursor value extraction and watermark updates

### Solution Overview

Implement **automatic watermark-based filtering** where:
1. SOURCE with `sync_mode: "incremental"` automatically retrieves last watermark
2. Connector applies filtering during read (WHERE clauses, file filtering, etc.)
3. LocalExecutor updates watermarks after successful LOAD operations
4. Performance improvements demonstrated through reduced data processing

---

## Technical Architecture

### 1. Enhanced SOURCE Execution Flow

**Current Flow:**
```
SOURCE definition → Parse parameters → Store config → LOAD → Read full data → Apply MODE
```

**New Flow:**
```
SOURCE definition → Parse parameters → Check sync_mode → 
IF incremental:
  → Get watermark → Read filtered data → Update watermark
ELSE:
  → Read full data (existing behavior)
→ Apply MODE
```

### 2. Connector Interface Extensions

**New Required Methods:**
```python
class Connector(ABC):
    @abstractmethod
    def read_incremental(self, cursor_field: str, last_cursor_value: Any, **kwargs) -> Iterator[pd.DataFrame]:
        """Read data incrementally based on cursor field and watermark value."""
        pass
    
    def supports_incremental(self) -> bool:
        """Return True if connector supports incremental reading."""
        return True  # Default implementation
```

### 3. LocalExecutor Integration Points

**Modified Execution Methods:**
```python
class LocalExecutor:
    def _execute_source_step(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced source execution with automatic incremental support."""
        sync_mode = step_config.get("sync_mode", "full_refresh")
        
        if sync_mode == "incremental":
            return self._execute_incremental_source_automatic(step_config)
        else:
            return self._execute_full_refresh_source(step_config)
    
    def _execute_incremental_source_automatic(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SOURCE with automatic incremental filtering."""
        # Implementation details below
```

---

## Implementation Specification

### Phase 2.0.1: Documentation (This Document)

**Deliverables:**
- ✅ Complete technical specification (this document)
- ✅ API interface definitions  
- ✅ Connector modification requirements
- ✅ Success criteria and test plans

### Phase 2.0.2: Core Implementation

#### A. LocalExecutor Enhancements

**File:** `sqlflow/core/executors/local_executor.py`

**New Method: `_execute_incremental_source_automatic`**
```python
def _execute_incremental_source_automatic(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute SOURCE step with automatic incremental filtering and watermark management."""
    source_name = step_config["name"]
    cursor_field = step_config["cursor_field"]
    
    # Validate incremental requirements
    if not cursor_field:
        raise ExecutionError(f"SOURCE {source_name} with sync_mode='incremental' requires cursor_field")
    
    # Get pipeline context for watermark key generation
    pipeline_name = getattr(self, 'pipeline_name', 'default_pipeline')
    
    # Get current watermark for this source
    last_cursor_value = self.watermark_manager.get_source_watermark(
        pipeline=pipeline_name,
        source=source_name,
        target=source_name,  # Use source name as target for now
        column=cursor_field
    )
    
    logger.info(f"Incremental SOURCE {source_name}: last_cursor_value={last_cursor_value}")
    
    # Get connector and validate incremental support
    connector = self.connector_manager.get_connector(source_name)
    if not connector.supports_incremental():
        logger.warning(f"Connector {source_name} doesn't support incremental, falling back to full refresh")
        return self._execute_full_refresh_source(step_config)
    
    try:
        # Read incremental data with automatic filtering
        max_cursor_value = last_cursor_value
        total_rows = 0
        
        for chunk in connector.read_incremental(
            cursor_field=cursor_field,
            last_cursor_value=last_cursor_value,
            **step_config.get("params", {})
        ):
            # Store chunk for subsequent LOAD operations
            self.engine.register_temp_data(source_name, chunk)
            total_rows += len(chunk)
            
            # Track maximum cursor value in this chunk
            if cursor_field in chunk.columns:
                chunk_max = chunk[cursor_field].max()
                if pd.notna(chunk_max) and (not max_cursor_value or chunk_max > max_cursor_value):
                    max_cursor_value = chunk_max
        
        # Update source watermark after successful read (atomic)
        if max_cursor_value != last_cursor_value:
            self.watermark_manager.update_source_watermark(
                pipeline=pipeline_name,
                source=source_name,
                target=source_name,
                column=cursor_field,
                value=max_cursor_value
            )
            logger.info(f"Updated watermark for {source_name}.{cursor_field}: {last_cursor_value} → {max_cursor_value}")
        
        return {
            "status": "success",
            "source_name": source_name,
            "sync_mode": "incremental",
            "previous_watermark": last_cursor_value,
            "new_watermark": max_cursor_value,
            "rows_processed": total_rows,
            "incremental": True
        }
        
    except Exception as e:
        logger.error(f"Incremental source read failed for {source_name}: {e}")
        # Don't update watermark on failure
        raise ExecutionError(f"Incremental source read failed: {str(e)}") from e
```

#### B. CSV Connector Incremental Support

**File:** `sqlflow/connectors/csv_connector.py`

**New Method Implementation:**
```python
def read_incremental(self, cursor_field: str, last_cursor_value: Any, **kwargs) -> Iterator[pd.DataFrame]:
    """Read CSV with automatic filtering based on cursor field."""
    # Read full CSV first (for simple implementation)
    df = self.read(**kwargs)
    
    # Apply incremental filtering if watermark exists
    if last_cursor_value is not None and cursor_field in df.columns:
        # Convert cursor field to appropriate type for comparison
        cursor_series = pd.to_datetime(df[cursor_field], errors='coerce')
        if cursor_series.notna().any():
            # DateTime comparison
            last_cursor_dt = pd.to_datetime(last_cursor_value, errors='coerce')
            if pd.notna(last_cursor_dt):
                filtered_df = df[cursor_series > last_cursor_dt]
                logger.info(f"CSV incremental: filtered {len(df)} → {len(filtered_df)} rows using {cursor_field} > {last_cursor_value}")
                yield filtered_df
                return
        else:
            # Non-datetime comparison (strings, numbers)
            filtered_df = df[df[cursor_field] > last_cursor_value]
            logger.info(f"CSV incremental: filtered {len(df)} → {len(filtered_df)} rows using {cursor_field} > {last_cursor_value}")
            yield filtered_df
            return
    
    # If no filtering possible, return full dataset
    logger.info(f"CSV incremental: no filtering applied, returning {len(df)} rows")
    yield df

def supports_incremental(self) -> bool:
    """CSV connector supports incremental reading."""
    return True
```

#### C. PostgreSQL Connector Incremental Support

**File:** `sqlflow/connectors/postgres_connector.py`

**New Method Implementation:**
```python
def read_incremental(self, cursor_field: str, last_cursor_value: Any, **kwargs) -> Iterator[pd.DataFrame]:
    """Read PostgreSQL table with WHERE clause filtering for incremental loading."""
    table_name = kwargs.get("table", kwargs.get("object_name"))
    if not table_name:
        raise ConnectorError(self.name, "table parameter required for PostgreSQL read")
    
    # Build base query
    base_query = f"SELECT * FROM {table_name}"
    
    # Add WHERE clause for incremental filtering
    if last_cursor_value is not None:
        # Handle different cursor value types
        if isinstance(last_cursor_value, (str, pd.Timestamp)):
            where_clause = f"WHERE {cursor_field} > '{last_cursor_value}'"
        else:
            where_clause = f"WHERE {cursor_field} > {last_cursor_value}"
        
        incremental_query = f"{base_query} {where_clause} ORDER BY {cursor_field}"
        logger.info(f"PostgreSQL incremental query: {incremental_query}")
    else:
        incremental_query = f"{base_query} ORDER BY {cursor_field}"
        logger.info(f"PostgreSQL initial load query: {incremental_query}")
    
    # Execute query and yield results
    try:
        conn = self.connection_pool.getconn()
        try:
            df = pd.read_sql(incremental_query, conn)
            logger.info(f"PostgreSQL incremental: retrieved {len(df)} rows")
            yield df
        finally:
            self.connection_pool.putconn(conn)
    except Exception as e:
        raise ConnectorError(self.name, f"Incremental read failed: {str(e)}") from e

def supports_incremental(self) -> bool:
    """PostgreSQL connector supports incremental reading."""
    return True
```

### Phase 2.0.3: Comprehensive Testing

#### A. Unit Tests

**File:** `tests/unit/core/executors/test_incremental_integration.py`

**Test Cases:**
1. `test_incremental_source_with_csv_connector`
2. `test_incremental_source_with_postgres_connector`
3. `test_watermark_persistence_across_runs`
4. `test_incremental_fallback_to_full_refresh`
5. `test_incremental_error_handling`
6. `test_cursor_field_validation`

#### B. Integration Tests

**File:** `tests/integration/core/test_complete_incremental_flow.py`

**Test Scenarios:**
1. End-to-end incremental loading with CSV
2. End-to-end incremental loading with PostgreSQL
3. Watermark persistence verification
4. Performance comparison (incremental vs full refresh)
5. Error recovery and state consistency

### Phase 2.0.4: Demo Implementation

#### A. Real Incremental Demo Pipeline

**File:** `examples/incremental_loading_demo/pipelines/real_incremental_demo.sf`

```sql
-- Real incremental loading demo
SOURCE orders TYPE CSV PARAMS {
    "path": "${data_dir}/orders_incremental.csv",
    "sync_mode": "incremental", 
    "cursor_field": "updated_at",
    "primary_key": ["order_id"]
};

LOAD orders_table FROM orders; -- Should automatically use watermarks
```

#### B. Demo Verification Scripts

**File:** `examples/incremental_loading_demo/run_real_incremental_demo.sh`

```bash
#!/bin/bash

set -e

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQLFLOW_ROOT="$(cd "$DEMO_DIR/../.." && pwd)"

echo "🚀 SQLFlow Phase 2 Real Incremental Loading Demo"
echo "================================================="

# Setup
echo "📝 Setting up demo data..."
cp "$DEMO_DIR/data/orders_base.csv" "$DEMO_DIR/data/orders_incremental.csv"

# Initial run
echo "🔄 Running initial load..."
cd "$SQLFLOW_ROOT"
sqlflow pipeline run "$DEMO_DIR/pipelines/real_incremental_demo.sf" --profile dev

echo "📊 Initial load completed. Checking results..."
sqlflow query "SELECT COUNT(*) as initial_count FROM orders_table"

# Add new data
echo "📝 Adding incremental data..."
echo "1001,Widget D,2024-01-16 10:00:00,pending" >> "$DEMO_DIR/data/orders_incremental.csv"
echo "1002,Widget E,2024-01-16 11:00:00,shipped" >> "$DEMO_DIR/data/orders_incremental.csv"

# Incremental run  
echo "🔄 Running incremental load..."
sqlflow pipeline run "$DEMO_DIR/pipelines/real_incremental_demo.sf" --profile dev

echo "📊 Incremental load completed. Checking results..."
sqlflow query "SELECT COUNT(*) as final_count FROM orders_table"

echo "✅ Demo completed! Check watermark values:"
sqlflow state list
```

---

## Success Criteria

### Technical Criteria

1. **Automatic Incremental Behavior:**
   - SOURCE with `sync_mode: "incremental"` triggers watermark-based filtering
   - No manual intervention required for incremental loading
   - Connector reads only new/updated records automatically

2. **Watermark Management:**
   - Watermarks stored and retrieved correctly
   - Atomic updates after successful operations
   - Proper error handling without watermark corruption

3. **Performance Improvements:**
   - >50% reduction in rows processed for incremental runs
   - Measurable performance improvement demonstrated
   - Memory usage remains constant regardless of total dataset size

4. **Error Handling:**
   - Clear error messages for configuration issues
   - Graceful fallback to full refresh when incremental fails
   - State consistency maintained during failures

### Testing Criteria

1. **Unit Test Coverage:** >95% for all new incremental loading code
2. **Integration Tests:** All scenarios pass without mocks
3. **Performance Tests:** Benchmarks show expected improvements
4. **Error Tests:** All failure scenarios handled gracefully

### Demo Criteria

1. **End-to-End Functionality:** Demo runs without manual intervention
2. **Performance Visible:** Clear before/after comparison shown
3. **Watermark Persistence:** Watermark values correctly updated
4. **User Experience:** Intuitive parameter usage demonstrated

---

## Implementation Timeline

### Day 1: Documentation & Planning ✅
- ✅ Complete technical specification (this document)
- ✅ Define API interfaces and modifications
- ✅ Plan testing strategy and success criteria

### Day 2: Core Implementation
- Implement LocalExecutor incremental integration
- Add CSV connector incremental support
- Add PostgreSQL connector incremental support  
- Create basic integration tests

### Day 3: Testing, Demo & Validation
- Complete comprehensive test suite
- Implement real incremental demo
- Performance testing and benchmarking
- Error scenario validation
- Final integration and commit (only if all tests pass)

---

## Risk Mitigation

### Technical Risks

1. **Connector Compatibility Issues**
   - **Mitigation:** Start with CSV (simple), extend to PostgreSQL (complex)
   - **Fallback:** Graceful degradation to full refresh

2. **Watermark Type Handling**
   - **Mitigation:** Comprehensive type conversion logic
   - **Testing:** Multiple cursor field types (datetime, string, numeric)

3. **Performance Regression**
   - **Mitigation:** Benchmark before/after implementation
   - **Monitoring:** Memory usage and execution time tracking

### Integration Risks

1. **Existing Pipeline Compatibility**
   - **Mitigation:** Only affect sources with `sync_mode: "incremental"`
   - **Testing:** Extensive backward compatibility validation

2. **State Corruption**
   - **Mitigation:** Atomic watermark operations
   - **Recovery:** Clear state reset procedures

---

## Future Enhancements

### Phase 2+ Improvements

1. **Advanced Cursor Types**
   - Composite cursors (multiple fields)
   - Binary cursors for APIs
   - Custom cursor logic

2. **Connector-Specific Optimizations**
   - PostgreSQL: Streaming queries for large datasets
   - S3: File modification time tracking
   - APIs: Proper pagination handling

3. **Advanced Error Recovery**
   - Automatic retry with exponential backoff
   - Circuit breaker patterns
   - Dead letter queue for failed records

This specification provides a comprehensive foundation for implementing complete incremental loading integration in SQLFlow Phase 2, bridging the identified gap and enabling true automatic incremental behavior. 