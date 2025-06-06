# Migration Guide: From Airbyte/Fivetran to SQLFlow

This guide helps you migrate your existing ELT pipelines from Airbyte, Fivetran, or similar tools to SQLFlow. SQLFlow supports industry-standard parameters, making migration straightforward while providing additional flexibility and performance benefits.

## Overview

SQLFlow's Phase 1 features include full compatibility with industry-standard ELT parameters:

- **sync_mode**: `"full_refresh"` or `"incremental"`
- **primary_key**: Field used for MERGE operations  
- **cursor_field**: Field used for incremental loading watermarks

These parameters work exactly like Airbyte and Fivetran, ensuring a smooth migration path.

## Parameter Mapping

### Airbyte to SQLFlow

| Airbyte Parameter | SQLFlow Parameter | Notes |
|-------------------|-------------------|-------|
| `sync_mode` | `sync_mode` | Identical - `"full_refresh"` or `"incremental"` |
| `cursor_field` | `cursor_field` | Identical - timestamp or ID field |
| `primary_key` | `primary_key` | Identical - field for unique identification |
| `destination_sync_mode` | Load `MODE` | `append` → `APPEND`, `overwrite` → `REPLACE`, `append_dedup` → `MERGE` |

### Fivetran to SQLFlow

| Fivetran Concept | SQLFlow Equivalent | Notes |
|------------------|-------------------|-------|
| Primary Key | `primary_key` parameter | Used for MERGE operations |
| Incremental Updates | `sync_mode: "incremental"` | Automatic watermark management |
| Full Resync | `sync_mode: "full_refresh"` | Complete table replacement |
| Change Data Capture | `cursor_field` | Timestamp or sequence field |

## Migration Examples

### Example 1: Basic Incremental Source

**Airbyte Configuration:**
```yaml
source:
  type: file
  file_format: csv
  sync_mode: incremental
  cursor_field: updated_at
  primary_key: user_id
```

**SQLFlow Equivalent:**
```sql
SOURCE users TYPE CSV PARAMS {
    "path": "data/users.csv",
    "has_header": true,
    "sync_mode": "incremental",
    "cursor_field": "updated_at", 
    "primary_key": "user_id"
};

LOAD users_table FROM users MODE UPSERT KEY (user_id);
```

### Example 2: Full Refresh Source

**Airbyte Configuration:**
```yaml
source:
  type: file
  file_format: csv
  sync_mode: full_refresh
```

**SQLFlow Equivalent:**
```sql
SOURCE products TYPE CSV PARAMS {
    "path": "data/products.csv",
    "has_header": true,
    "sync_mode": "full_refresh"
};

LOAD products_table FROM products MODE REPLACE;
```

### Example 3: Multiple Sources with Different Sync Modes

**Airbyte Configuration:**
```yaml
sources:
  - name: users
    sync_mode: incremental
    cursor_field: updated_at
    primary_key: user_id
  - name: products  
    sync_mode: full_refresh
  - name: orders
    sync_mode: incremental
    cursor_field: order_date
    primary_key: order_id
```

**SQLFlow Equivalent:**
```sql
-- Incremental users
SOURCE users TYPE CSV PARAMS {
    "path": "data/users.csv",
    "has_header": true,
    "sync_mode": "incremental",
    "cursor_field": "updated_at",
    "primary_key": "user_id"
};

-- Full refresh products
SOURCE products TYPE CSV PARAMS {
    "path": "data/products.csv", 
    "has_header": true,
    "sync_mode": "full_refresh"
};

-- Incremental orders
SOURCE orders TYPE CSV PARAMS {
    "path": "data/orders.csv",
    "has_header": true,
    "sync_mode": "incremental",
    "cursor_field": "order_date",
    "primary_key": "order_id"
};

-- Load all sources
LOAD users_table FROM users MODE UPSERT KEY (user_id);
LOAD products_table FROM products MODE REPLACE;
LOAD orders_table FROM orders MODE UPSERT KEY (order_id);
```

## Advanced Migration Scenarios

### Composite Primary Keys

**Airbyte:**
```yaml
primary_key: ["user_id", "account_id"]
```

**SQLFlow:**
```sql
SOURCE user_accounts TYPE CSV PARAMS {
    "path": "data/user_accounts.csv",
    "has_header": true,
    "sync_mode": "incremental",
    "cursor_field": "updated_at",
    "primary_key": "user_id,account_id"  -- Comma-separated
};

LOAD user_accounts_table FROM user_accounts 
MODE UPSERT KEY (user_id, account_id);
```

### Custom Transformations

Unlike Airbyte/Fivetran, SQLFlow allows you to add transformations directly in the pipeline:

```sql
SOURCE raw_events TYPE CSV PARAMS {
    "path": "data/events.csv",
    "has_header": true,
    "sync_mode": "incremental",
    "cursor_field": "event_timestamp",
    "primary_key": "event_id"
};

LOAD events_table FROM raw_events MODE UPSERT KEY (event_id);

-- Add transformations not possible in Airbyte/Fivetran
CREATE TABLE processed_events AS
SELECT 
    event_id,
    user_id,
    event_type,
    DATE(event_timestamp) as event_date,
    EXTRACT(HOUR FROM event_timestamp) as event_hour,
    -- Custom business logic
    CASE 
        WHEN event_type = 'purchase' THEN 'revenue'
        WHEN event_type = 'signup' THEN 'acquisition'
        ELSE 'engagement'
    END as event_category
FROM events_table;

-- Create aggregations in the same pipeline
CREATE OR REPLACE TABLE daily_metrics AS
SELECT 
    event_date,
    event_category,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM processed_events
GROUP BY event_date, event_category;
```

## Benefits Over Airbyte/Fivetran

### 1. Cost Efficiency
- **No per-row pricing**: SQLFlow processes unlimited data without usage-based fees
- **Local execution**: Runs on your infrastructure, eliminating data transfer costs
- **Open source**: No vendor lock-in or licensing fees

### 2. Performance Advantages  
- **Automatic watermark management**: Zero-configuration incremental loading
- **Optimized execution**: Native DuckDB performance for analytical workloads
- **Batch processing**: Efficient handling of large datasets

### 3. Enhanced Capabilities
- **Custom transformations**: SQL logic directly in pipelines
- **Advanced debugging**: Built-in query tracing and performance metrics  
- **Flexible scheduling**: Run on any orchestrator (Airflow, Prefect, cron)
- **Version control**: Pipeline definitions in git with your code

### 4. Developer Experience
- **Local development**: Test pipelines on your machine
- **IDE integration**: Full syntax highlighting and validation
- **Command-line tools**: Validate, compile, and run pipelines locally

## Migration Checklist

### Pre-Migration
- [ ] Document current Airbyte/Fivetran sources and sync modes
- [ ] Identify primary keys and cursor fields for each source
- [ ] Map destination table schemas
- [ ] Note any custom transformations or dbt models

### Migration Steps
- [ ] Install SQLFlow: `pip install sqlflow`
- [ ] Create project structure with `sqlflow init`
- [ ] Convert source configurations using parameter mapping
- [ ] Set up profiles for different environments
- [ ] Create pipeline files with SOURCE and LOAD statements
- [ ] Add transformations as CREATE TABLE statements
- [ ] Test with sample data using `sqlflow pipeline run`

### Post-Migration
- [ ] Validate data accuracy between old and new systems
- [ ] Update downstream dependencies (BI tools, reports)
- [ ] Schedule pipeline execution in your orchestrator
- [ ] Set up monitoring and alerting
- [ ] Decommission old Airbyte/Fivetran connections

## Common Migration Patterns

### Pattern 1: Simple Incremental Replacement

Replace Airbyte source with SQLFlow SOURCE:

```sql
-- Before: Airbyte connector
-- After: SQLFlow source
SOURCE customers TYPE CSV PARAMS {
    "path": "data/customers.csv",
    "sync_mode": "incremental",
    "cursor_field": "updated_at",
    "primary_key": "customer_id"
};

LOAD customers_table FROM customers MODE UPSERT KEY (customer_id);
```

### Pattern 2: Full Pipeline with Transformations

Replace Airbyte + dbt with unified SQLFlow pipeline:

```sql
-- Extract (replaces Airbyte)
SOURCE orders TYPE CSV PARAMS {
    "path": "data/orders.csv",
    "sync_mode": "incremental", 
    "cursor_field": "order_date",
    "primary_key": "order_id"
};

SOURCE customers TYPE CSV PARAMS {
    "path": "data/customers.csv",
    "sync_mode": "incremental",
    "cursor_field": "updated_at", 
    "primary_key": "customer_id"
};

-- Load
LOAD orders_table FROM orders MODE UPSERT KEY (order_id);
LOAD customers_table FROM customers MODE UPSERT KEY (customer_id);

-- Transform (replaces dbt models)
CREATE OR REPLACE TABLE customer_metrics AS
SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_spent,
    MIN(o.order_date) as first_order,
    MAX(o.order_date) as last_order
FROM customers_table c
LEFT JOIN orders_table o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email;
```

### Pattern 3: Multi-Environment Deployment

Use profiles for different environments:

**profiles/dev.yml:**
```yaml
engines:
  duckdb:
    mode: memory
variables:
  data_dir: "sample_data"
  sync_mode: "full_refresh"  # Use full refresh for dev
```

**profiles/production.yml:**
```yaml
engines:
  duckdb:
    mode: persistent
    path: "/data/warehouse.duckdb"
variables:
  data_dir: "s3://production-data"
  sync_mode: "incremental"  # Use incremental for prod
```

**Pipeline:**
```sql
SOURCE orders TYPE CSV PARAMS {
    "path": "${data_dir}/orders.csv",
    "sync_mode": "${sync_mode}",
    "cursor_field": "order_date",
    "primary_key": "order_id"
};
```

## Troubleshooting

### Common Issues

**Issue:** Validation errors about sync_mode values
```
Error: Field 'sync_mode' must be one of ['full_refresh', 'incremental']
```
**Solution:** Use exact Airbyte/Fivetran values: `"full_refresh"` or `"incremental"`

**Issue:** Primary key not working with UPSERT
```
Error: UPSERT mode requires KEY to be specified
```
**Solution:** Ensure LOAD statement includes `MODE UPSERT KEY (field_name)`

**Issue:** Cursor field not recognized
```
Error: Column 'updated_at' not found
```
**Solution:** Verify cursor field exists in source data and matches case exactly

### Performance Optimization

1. **Use appropriate sync modes:**
   - `full_refresh` for small reference data
   - `incremental` for large transactional data

2. **Optimize cursor fields:**
   - Use indexed timestamp columns when possible
   - Consider sequence numbers for high-frequency updates

3. **Batch operations:**
   - Group related sources in single pipeline
   - Use CREATE OR REPLACE for derived tables

## Getting Help

- **Documentation**: Check [SQLFlow docs](../reference/syntax.md) for syntax details
- **Examples**: See `examples/incremental_loading_demo/` for working examples  
- **Validation**: Use `sqlflow pipeline validate` to check syntax
- **Community**: Join discussions about migration strategies

The migration from Airbyte/Fivetran to SQLFlow typically reduces infrastructure costs by 70%+ while providing better performance and development experience. The industry-standard parameters ensure compatibility while opening up new possibilities for pipeline optimization and customization. 