# UPSERT Mode Reference

## Overview

UPSERT (INSERT or UPDATE) mode allows you to merge data from a source into a target table. It updates existing records based on specified key columns and inserts new records that don't exist.

SQLFlow's UPSERT implementation is optimized for DuckDB and provides:
- **Transaction safety** with automatic rollback on errors
- **Detailed metrics** showing inserted vs updated row counts
- **Enhanced error messages** with specific column and type information
- **High performance** using DuckDB's native capabilities

## Basic Syntax

### LOAD Statement UPSERT
```sql
LOAD target_table FROM source_table MODE UPSERT KEY column1, column2;
```

### Transform Statement UPSERT
```sql
CREATE TABLE target_table MODE UPSERT KEY column1, column2 AS
SELECT * FROM source_table;
```

## Key Features

### 1. Upsert Key Specification

Upsert keys determine which records to update vs insert:

```sql
-- Single key
LOAD users FROM users_updates MODE UPSERT KEY user_id;

-- Multiple keys (composite key)
LOAD inventory FROM inventory_updates MODE UPSERT KEY product_id, warehouse_id;

-- Parentheses syntax (optional)
LOAD customers FROM customer_updates MODE UPSERT KEY (customer_id, email);
```

### 2. Operation Logic

For each record in the source:
1. **Check if exists**: Look for matching records in target using upsert keys
2. **Update**: If exists, update all non-key columns with source values
3. **Insert**: If doesn't exist, insert the complete record

### 3. Transaction Safety

All UPSERT operations are wrapped in transactions:
```sql
BEGIN TRANSACTION;
  -- Update existing records
  -- Insert new records  
COMMIT;
```

On any error, the transaction is automatically rolled back.

## Validation and Error Handling

### Schema Validation

SQLFlow validates that:
- All upsert key columns exist in both source and target
- Upsert key data types are compatible
- Overall schema compatibility between source and target

### Enhanced Error Messages

Instead of generic errors, you get specific guidance:

```
UPSERT key validation failed:
  - Upsert key 'user_id' does not exist in source 'users_updates'. 
    Available columns: name, email, is_active
  - Upsert key 'customer_id' has incompatible types: 
    source='VARCHAR', target='INTEGER'. 
    Upsert keys must have compatible types.
```

### Common Error Scenarios

| Error | Cause | Solution |
|-------|--------|----------|
| `Upsert key 'X' does not exist in source` | Column missing from source | Add column to source or change upsert key |
| `Upsert key 'X' does not exist in target` | Column missing from target | Create target table with correct schema |
| `incompatible types` | Type mismatch between source/target | Cast to compatible types in source query |
| `requires at least one upsert key` | Empty upsert keys list | Specify at least one KEY column |

## Performance Optimization

### DuckDB-Optimized Implementation

SQLFlow uses an optimized UPDATE/INSERT pattern:
1. **CREATE TEMPORARY VIEW** for source data
2. **UPDATE** existing records using JOIN
3. **INSERT** new records using NOT EXISTS
4. **DROP** temporary view

This approach:
- Handles large datasets efficiently
- Supports concurrent access
- Provides better performance than MERGE in most cases

### Best Practices

1. **Index upsert keys** in target table for better performance
2. **Use minimal upsert keys** - fewer keys = faster lookups
3. **Consider partitioning** for very large tables
4. **Monitor metrics** to understand insert vs update ratios

## Detailed Metrics

UPSERT operations return comprehensive metrics:

```json
{
  "status": "success",
  "operation": "UPSERT", 
  "target_table": "users",
  "source_table": "users_updates",
  "mode": "UPSERT",
  "upsert_keys": ["user_id"],
  "rows_loaded": 1000,
  "rows_inserted": 200,
  "rows_updated": 800,
  "final_row_count": 5200
}
```

### User-Friendly Output

Console output shows clear operation results:
```
🔄 Upserted users (200 new, 800 updated)
```

## Examples

### Basic User Updates
```sql
-- Source definition
SOURCE users_api TYPE CSV PARAMS {
  "path": "users_daily.csv",
  "sync_mode": "full_refresh"
};

-- Load with UPSERT
LOAD users FROM users_api MODE UPSERT KEY user_id;
```

### E-commerce Inventory Management
```sql
-- Multi-key UPSERT for inventory
SOURCE inventory_feed TYPE CSV PARAMS {
  "path": "inventory_updates.csv"
};

LOAD product_inventory FROM inventory_feed 
MODE UPSERT KEY product_id, warehouse_id;
```

### Customer Data Enrichment
```sql
-- Upsert with data transformation
CREATE TABLE enriched_customers MODE UPSERT KEY customer_id AS
SELECT 
  c.customer_id,
  c.name,
  c.email,
  COALESCE(u.updated_phone, c.phone) as phone,
  COALESCE(u.updated_address, c.address) as address,
  CURRENT_TIMESTAMP as last_updated
FROM customers c
LEFT JOIN customer_updates u ON c.customer_id = u.customer_id;
```

### Incremental Data Processing
```sql
-- Combine with incremental loading
SOURCE orders_stream TYPE POSTGRES PARAMS {
  "connection": "${DB_CONN}",
  "table": "orders",
  "sync_mode": "incremental", 
  "cursor_field": "updated_at"
};

LOAD orders_fact FROM orders_stream MODE UPSERT KEY order_id;
```

## Comparison with Other Modes

| Aspect | REPLACE | APPEND | UPSERT |
|--------|---------|--------|--------|
| **Existing data** | Dropped | Preserved | Updated |
| **New records** | All inserted | All inserted | Only new inserted |
| **Duplicates** | Prevented | Allowed | Prevented |
| **Performance** | Fast | Fastest | Moderate |
| **Use case** | Full refresh | Log data | Master data |

## Troubleshooting

### Performance Issues

If UPSERT is slow:
1. Check that upsert key columns are indexed
2. Consider reducing the number of upsert keys
3. Use smaller batch sizes for very large datasets
4. Monitor memory usage during operation

### Data Consistency

To ensure data consistency:
1. Always validate upsert key uniqueness in source
2. Use appropriate data types for keys
3. Handle NULL values in upsert keys appropriately
4. Test with representative data volumes

### Memory Management

For large UPSERT operations:
1. Configure DuckDB memory limits appropriately
2. Consider processing in smaller batches
3. Monitor disk usage for temporary operations
4. Use streaming where possible

## Integration with Other Features

### Incremental Loading
```sql
SOURCE user_events TYPE POSTGRES PARAMS {
  "sync_mode": "incremental",
  "cursor_field": "event_time"
};

LOAD user_activity FROM user_events MODE UPSERT KEY user_id, event_date;
```

### Variable Substitution
```sql
SET upsert_key = "customer_id";
LOAD customers FROM customer_feed MODE UPSERT KEY ${upsert_key};
```

### Conditional Processing
```sql
IF ${environment} = "prod" THEN
  LOAD users FROM users_prod MODE UPSERT KEY user_id;
ELSE  
  LOAD users FROM users_test MODE REPLACE;
END IF;
``` 