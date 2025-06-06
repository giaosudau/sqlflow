# Building SQLFlow Transforms

## Overview

SQLFlow's transform layer extends SQL with custom transformation capabilities while maintaining SQL-native patterns and automatic optimization. This guide covers the architecture, implementation patterns, and best practices for building production-ready transforms.

**Transform Philosophy**
- **SQL-native approach**: Enhance SQL rather than replace it
- **Automatic optimization**: Built-in performance tuning and strategy selection
- **State management**: Integrated incremental processing with watermark tracking
- **Schema evolution**: Automatic handling of data structure changes

## 🏗️ Transform Architecture

### Transform Modes

SQLFlow supports multiple transformation strategies, each optimized for specific data patterns:

```sql
-- REPLACE Mode - Full table replacement
CREATE TABLE daily_sales MODE REPLACE AS
SELECT order_date, SUM(amount) as total_sales
FROM orders GROUP BY order_date;

-- APPEND Mode - Insert new records only
CREATE TABLE audit_log MODE APPEND AS
SELECT * FROM new_events WHERE event_date = CURRENT_DATE;

-- UPSERT Mode - Insert or update based on keys
CREATE TABLE customer_profiles MODE UPSERT KEY (customer_id) AS
SELECT customer_id, latest_address, latest_phone
FROM customer_updates;

-- INCREMENTAL Mode - Time-based processing with automatic watermarks
CREATE TABLE hourly_metrics MODE INCREMENTAL BY timestamp LOOKBACK '2 hours' AS
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    AVG(value) as avg_value
FROM sensor_data 
WHERE timestamp > @last_watermark AND timestamp <= @end_time
GROUP BY hour;
```

### State Management Integration

SQLFlow automatically handles state management for incremental transforms. See **[State Management](state-management.md)** for detailed information on:

- **Watermark Management**: Automatic tracking of processed data timestamps
- **Incremental Patterns**: Efficient processing of only new/changed data
- **Recovery Semantics**: Reliable failure handling and restart capabilities
- **Performance Optimization**: Sub-10ms watermark lookups with caching

Your transforms benefit from this automatically without additional implementation.

## 📋 Transform Implementation Patterns

### Simple Aggregation Transform

```sql
-- Automatic state management and optimization
CREATE TABLE customer_metrics MODE INCREMENTAL BY updated_at AS
SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    MAX(updated_at) as last_order_date
FROM orders
WHERE updated_at > @last_watermark
GROUP BY customer_id;
```

### Complex Window Function Transform

```sql
-- Advanced analytics with automatic optimization
CREATE TABLE customer_trends MODE INCREMENTAL BY order_date AS
SELECT 
    customer_id,
    order_date,
    amount,
    -- Running totals
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS UNBOUNDED PRECEDING
    ) as running_total,
    -- Moving averages
    AVG(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS 30 PRECEDING
    ) as avg_30_day
FROM orders
WHERE order_date > @last_watermark;
```

### Multi-Source Transform

```sql
-- Combine multiple sources with automatic dependency resolution
CREATE TABLE enriched_orders MODE UPSERT KEY (order_id) AS
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_tier,
    p.product_category,
    -- Enrichment logic
    CASE 
        WHEN c.customer_tier = 'VIP' THEN o.amount * 0.1
        WHEN c.customer_tier = 'Premium' THEN o.amount * 0.05
        ELSE 0
    END as loyalty_discount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.updated_at > @last_watermark;
```

## 🔧 Advanced Transform Features

### Custom Strategy Implementation

For complex use cases, you can implement custom transformation strategies:

```python
from sqlflow.core.engines.duckdb.transform.incremental_strategies import IncrementalStrategy

class CustomTransformStrategy(IncrementalStrategy):
    """Custom strategy for specialized data processing."""
    
    def execute(self, source: DataSource, target: str, **params) -> LoadResult:
        """Implement custom transformation logic."""
        
        # Get current watermark automatically
        last_watermark = self.watermark_manager.get_transform_watermark(
            target, source.time_column
        )
        
        # Implement custom processing logic
        result = self._process_data(source, last_watermark, **params)
        
        # Update watermark automatically
        if result.rows_processed > 0:
            new_watermark = self._get_max_timestamp(result.data)
            self.watermark_manager.update_watermark(
                target, source.time_column, new_watermark
            )
        
        return result
```

### Schema Evolution Handling

SQLFlow automatically handles schema changes:

```python
# Automatic schema evolution policies
class SchemaEvolutionPolicy:
    """Handle schema changes automatically."""
    
    def handle_new_column(self, table: str, column: str, type: str):
        """Add new columns automatically with appropriate defaults."""
        pass
        
    def handle_type_widening(self, table: str, column: str, old_type: str, new_type: str):
        """Widen column types when compatible (e.g., INT -> BIGINT).""" 
        pass
        
    def handle_incompatible_change(self, table: str, column: str, change: str):
        """Handle breaking changes with data preservation."""
        pass
```

### Data Quality Integration

Built-in data quality validation:

```sql
-- Automatic quality validation during transforms
CREATE TABLE validated_customers MODE UPSERT KEY (customer_id) 
QUALITY CHECK (
    completeness > 0.95,
    uniqueness = 1.0,
    validity > 0.99
) AS
SELECT 
    customer_id,
    email,
    phone,
    created_at
FROM raw_customers
WHERE email IS NOT NULL 
  AND email LIKE '%@%'
  AND updated_at > @last_watermark;
```

## 📊 Performance Optimization

### Intelligent Strategy Selection

SQLFlow automatically selects the optimal transformation strategy:

```python
# Automatic strategy selection based on data characteristics
class IntelligentStrategyManager:
    """Select optimal strategy based on data patterns."""
    
    def select_strategy(self, data_profile: DataProfile) -> TransformStrategy:
        """Choose strategy based on data characteristics."""
        
        # Append-only data + high volume → AppendStrategy (~0.1ms/row)
        if data_profile.insert_rate > 0.8 and data_profile.update_rate < 0.1:
            return AppendStrategy()
            
        # Primary key + mixed changes → UpsertStrategy (~0.5ms/row)
        if data_profile.has_primary_key and data_profile.change_rate > 0.3:
            return UpsertStrategy()
            
        # Time-based + incremental → IncrementalStrategy (~0.3ms/row)
        if data_profile.has_time_column and data_profile.temporal_pattern:
            return IncrementalStrategy()
        
        # Default fallback
        return ReplaceStrategy()
```

### Partition Management

Automatic partition optimization for large datasets:

```python
# Automatic partition detection and optimization
class PartitionManager:
    """Optimize queries with automatic partition management."""
    
    def optimize_query(self, query: str, time_range: TimeRange) -> str:
        """Add partition pruning to queries automatically."""
        
        # Detect time-based partitioning
        partitions = self.detect_partitions(query)
        
        # Add partition filters
        if partitions:
            query = self.add_partition_filters(query, time_range)
            
        return query
```

### Caching and Memory Management

Built-in performance optimizations:

- **Watermark Caching**: <10ms lookups for repeated operations
- **Query Plan Caching**: Reuse optimized execution plans
- **Memory Management**: Automatic spilling for large datasets
- **Parallel Execution**: Multi-core processing where beneficial

## 🧪 Testing Transform Logic

### Unit Testing

```python
import pytest
from sqlflow.core.engines.duckdb.transform import TransformEngine

class TestCustomTransforms:
    def test_incremental_aggregation(self):
        """Test incremental aggregation logic."""
        engine = TransformEngine()
        
        # Set up test data
        engine.execute("""
            CREATE TABLE orders AS 
            SELECT * FROM VALUES 
                (1, '2024-01-01'::DATE, 100.0),
                (2, '2024-01-02'::DATE, 200.0)
            AS t(order_id, order_date, amount)
        """)
        
        # Test transform
        result = engine.execute("""
            CREATE TABLE daily_sales MODE INCREMENTAL BY order_date AS
            SELECT order_date, SUM(amount) as total_sales
            FROM orders
            WHERE order_date > @last_watermark
            GROUP BY order_date
        """)
        
        assert result.rows_processed == 2
        assert result.watermark_updated is not None
```

### Integration Testing

```python
def test_end_to_end_transform():
    """Test complete transform pipeline."""
    
    # Load source data
    executor.execute([
        {"type": "SOURCE", "name": "orders", "connector": "CSV", ...},
        {"type": "LOAD", "source": "orders", "table": "raw_orders"}
    ])
    
    # Execute transform
    executor.execute([{
        "type": "SQL", 
        "query": """
            CREATE TABLE processed_orders MODE INCREMENTAL BY updated_at AS
            SELECT customer_id, COUNT(*) as order_count
            FROM raw_orders
            WHERE updated_at > @last_watermark
            GROUP BY customer_id
        """
    }])
    
    # Verify results
    result = executor.duckdb_engine.execute_query(
        "SELECT COUNT(*) FROM processed_orders"
    ).fetchone()
    
    assert result[0] > 0
```

## 🔍 Monitoring and Debugging

### Transform Metrics

Monitor transform performance:

```python
def get_transform_metrics(transform_name: str) -> Dict[str, Any]:
    """Get comprehensive transform metrics."""
    return {
        "execution_time_ms": get_execution_time(transform_name),
        "rows_processed": get_rows_processed(transform_name),
        "watermark_lag_minutes": get_watermark_lag(transform_name),
        "error_rate": get_error_rate(transform_name),
        "strategy_used": get_strategy_type(transform_name)
    }
```

### Debugging Tools

Built-in debugging capabilities:

```sql
-- Explain transform execution plan
EXPLAIN CREATE TABLE customer_metrics MODE INCREMENTAL BY updated_at AS
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id;

-- Show watermark status
SHOW WATERMARKS FOR customer_metrics;

-- Display transform history
SELECT * FROM sqlflow_transform_history WHERE table_name = 'customer_metrics';
```

### Error Handling

Robust error handling with recovery:

```python
class TransformErrorHandler:
    """Handle transform errors with automatic recovery."""
    
    def handle_schema_error(self, error: SchemaError):
        """Handle schema evolution errors."""
        if error.is_compatible_change():
            self.apply_schema_evolution(error.table, error.changes)
        else:
            self.trigger_manual_intervention(error)
            
    def handle_data_quality_error(self, error: QualityError):
        """Handle data quality violations."""
        if error.severity == "WARNING":
            self.log_quality_warning(error)
        else:
            self.quarantine_bad_data(error.data)
```

## 🚀 Best Practices

### Design Principles

1. **Idempotent Operations**: Transforms should produce the same results when run multiple times
2. **Incremental Processing**: Use incremental modes for efficiency with large datasets
3. **Schema Flexibility**: Design for schema evolution and changes over time
4. **Error Resilience**: Handle data quality issues and schema changes gracefully
5. **Performance Awareness**: Consider data volume and processing patterns

### Configuration Management

```sql
-- Use configuration for flexibility
SET transform_batch_size = 50000;
SET incremental_lookback = '1 hour';
SET quality_threshold = 0.95;

CREATE TABLE optimized_transform MODE INCREMENTAL BY timestamp AS
SELECT * FROM source_data 
WHERE quality_score > ${quality_threshold};
```

### Documentation

Document transform logic and dependencies:

```sql
-- Document transform purpose and dependencies
/* 
Transform: Customer Lifetime Value Calculation
Dependencies: customers, orders, returns
Update Frequency: Hourly
Business Logic: Calculate CLV based on historical order data
Quality Checks: Completeness > 95%, No negative values
*/
CREATE TABLE customer_ltv MODE INCREMENTAL BY updated_at AS
SELECT 
    customer_id,
    SUM(order_value) - SUM(return_value) as lifetime_value,
    COUNT(DISTINCT order_id) as order_count
FROM customer_order_summary
WHERE updated_at > @last_watermark
GROUP BY customer_id;
```

## 📚 Learning Resources

### Transform Examples

Study existing transform patterns:

- **Transform Layer Specification**: `docs/developer/technical/implementation/transform_layer_specification.md`
- **Incremental Loading Examples**: `examples/phase2_integration_demo/`
- **State Management Patterns**: `sqlflow/core/engines/duckdb/transform/`

### Documentation References

- **[State Management](state-management.md)**: Watermark tracking and incremental patterns
- **[Architecture Overview](architecture-overview.md)**: System design and optimization principles  
- **[Transform Specification](../developer/technical/implementation/transform_layer_specification.md)**: Complete technical implementation details

---

**Ready to build transforms?** Start with simple incremental patterns and gradually add complexity. The transform layer handles state management, optimization, and monitoring automatically. 