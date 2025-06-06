# Core Concepts

The four building blocks of SQLFlow: **SOURCE**, **LOAD**, **CREATE TABLE**, and **EXPORT**.

## Overview

Every SQLFlow pipeline uses these four concepts:
- **SOURCE**: Tell SQLFlow where your data is
- **LOAD**: Bring data into your pipeline with different modes
- **CREATE TABLE**: Transform data with SQL using different modes
- **EXPORT**: Save results to files

## SOURCE: Define Your Data

SOURCE tells SQLFlow where to find your data - CSV files, databases, APIs, etc.

### CSV Files
```sql
SOURCE customers TYPE CSV PARAMS {
    "path": "data/customers.csv",
    "has_header": true
};
```

### Database Tables
```sql
SOURCE orders TYPE POSTGRES PARAMS {
    "host": "localhost", 
    "port": 5432,
    "dbname": "shop",
    "table": "orders"
};
```

### API Sources
```sql
SOURCE users TYPE REST PARAMS {
    "url": "https://api.company.com/users",
    "auth_token": "your-token"
};
```

## LOAD: Bring Data In

LOAD moves data from sources into your pipeline. There are three modes that control how data is loaded:

### REPLACE Mode (Default)
Replaces the entire table - good for small datasets and full refreshes.

```sql
SOURCE sales_data TYPE CSV PARAMS {
    "path": "data/sales.csv",
    "has_header": true
};

LOAD sales_table FROM sales_data;
-- Same as: LOAD sales_table FROM sales_data MODE REPLACE;
```

**When to use**: Small datasets, daily full refreshes, development/testing.

### APPEND Mode  
Adds new data without removing existing data - good for logs and event data.

```sql
SOURCE new_orders TYPE CSV PARAMS {
    "path": "data/today_orders.csv", 
    "has_header": true
};

LOAD orders_table FROM new_orders MODE APPEND;
```

**When to use**: Log files, event streams, incremental data loads, time-series data.

### UPSERT Mode
Updates existing records and adds new ones based on key columns - good for master data.

```sql
SOURCE customer_updates TYPE CSV PARAMS {
    "path": "data/customer_changes.csv",
    "has_header": true  
};

-- Update existing customers, add new ones
LOAD customers_table FROM customer_updates MODE UPSERT KEY customer_id;

-- Multiple keys for composite keys
LOAD inventory FROM inventory_updates MODE UPSERT KEY product_id, warehouse_id;
```

**When to use**: Customer data, product catalogs, any data that needs updates without duplicates.

## CREATE TABLE: Transform Data

CREATE TABLE lets you write SQL to transform your data. SQLFlow automatically runs them in the right order. You can also use different modes for transforms.

### Basic Transformation (REPLACE Mode - Default)
```sql
CREATE TABLE customer_summary AS
SELECT 
    country,
    COUNT(*) as customer_count,
    AVG(age) as avg_age
FROM customers
GROUP BY country
ORDER BY customer_count DESC;
```

### Transform with APPEND Mode
Adds new data to existing table - useful for incremental processing.

```sql
-- Process new data and add to existing summary
CREATE TABLE daily_sales MODE APPEND AS
SELECT 
    DATE(order_date) as date,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
FROM orders
WHERE order_date = CURRENT_DATE;
```

### Transform with UPSERT Mode
Updates existing records or inserts new ones - perfect for slowly changing dimensions.

```sql
-- Update customer metrics, insert new customers
CREATE TABLE customer_metrics MODE UPSERT KEY customer_id AS
SELECT 
    customer_id,
    COUNT(order_id) as total_orders,
    SUM(amount) as lifetime_value,
    MAX(order_date) as last_order_date
FROM orders
GROUP BY customer_id;
```

### Transform Mode Use Cases

| Mode | Use Case | Example |
|------|----------|---------|
| **REPLACE** | Daily reports, full rebuilds | `CREATE TABLE daily_summary AS SELECT...` |
| **APPEND** | Time-series data, logs | `CREATE TABLE sales_history MODE APPEND AS SELECT...` |
| **UPSERT** | Customer profiles, inventory | `CREATE TABLE customers MODE UPSERT KEY id AS SELECT...` |

### Joining Tables
```sql
CREATE TABLE customer_orders AS
SELECT 
    c.customer_id,
    c.name,
    c.country,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.country;
```

### Filtering and Calculations
```sql
CREATE TABLE high_value_customers AS
SELECT 
    *,
    CASE 
        WHEN total_spent >= 1000 THEN 'VIP'
        WHEN total_spent >= 500 THEN 'Premium'
        ELSE 'Standard'
    END as customer_tier
FROM customer_orders
WHERE total_spent > 100;
```

## EXPORT: Save Results

EXPORT saves your transformed data to files or databases.

### CSV Files
```sql
EXPORT SELECT * FROM customer_summary
TO "output/customer_summary.csv"
TYPE CSV
OPTIONS { "header": true };
```

### Excel Files
```sql
EXPORT SELECT * FROM high_value_customers
TO "output/vip_customers.xlsx"
TYPE EXCEL
OPTIONS { "sheet_name": "VIP Customers" };
```

### Database Tables
```sql
EXPORT SELECT * FROM daily_summary
TO "summary_table"
TYPE POSTGRES;
```

## Complete Example

Here's a complete pipeline showing all four concepts with different modes:

```sql
-- 1. SOURCE: Define where data comes from
SOURCE customers TYPE CSV PARAMS {
    "path": "data/customers.csv",
    "has_header": true
};

SOURCE orders TYPE CSV PARAMS {
    "path": "data/orders.csv", 
    "has_header": true
};

-- 2. LOAD: Bring data into pipeline with different modes
LOAD customer_data FROM customers MODE REPLACE;
LOAD order_data FROM orders MODE APPEND;  -- Add today's orders

-- 3. CREATE TABLE: Transform data with UPSERT mode
CREATE TABLE customer_metrics MODE UPSERT KEY customer_id AS
SELECT 
    c.customer_id,
    c.name,
    c.country,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_order_value
FROM customer_data c
LEFT JOIN order_data o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.country;

-- Daily summary with APPEND mode for historical tracking
CREATE TABLE daily_summary MODE APPEND AS
SELECT 
    CURRENT_DATE as summary_date,
    COUNT(DISTINCT customer_id) as active_customers,
    SUM(total_revenue) as total_revenue
FROM customer_metrics
WHERE total_orders > 0;

-- Top customers - REPLACE mode for current snapshot
CREATE TABLE top_customers AS
SELECT *
FROM customer_metrics
WHERE total_revenue > 500
ORDER BY total_revenue DESC
LIMIT 20;

-- 4. EXPORT: Save results
EXPORT SELECT * FROM customer_metrics
TO "output/all_customers.csv"
TYPE CSV OPTIONS { "header": true };

EXPORT SELECT * FROM top_customers
TO "output/top_customers.csv"
TYPE CSV OPTIONS { "header": true };
```

## How SQLFlow Runs Your Pipeline

1. **Reads your `.sf` file** and finds all the SOURCE, LOAD, CREATE TABLE, and EXPORT statements
2. **Figures out the order** - creates customers table before using it in joins
3. **Applies the right mode** - REPLACE, APPEND, or UPSERT for each operation
4. **Runs each step** - loads data, transforms it, exports results
5. **Handles errors** - clear messages if something goes wrong

## Best Practices

### Choose the Right Mode
```sql
-- Good: Use REPLACE for snapshots
CREATE TABLE daily_snapshot AS SELECT * FROM current_data;

-- Good: Use APPEND for historical data
CREATE TABLE sales_history MODE APPEND AS SELECT * FROM daily_sales;

-- Good: Use UPSERT for master data
CREATE TABLE customers MODE UPSERT KEY customer_id AS SELECT * FROM customer_updates;
```

### Use Clear Names
```sql
-- Good: Descriptive names
CREATE TABLE monthly_sales_summary AS...
CREATE TABLE customer_profiles MODE UPSERT KEY customer_id AS...

-- Avoid: Confusing names
CREATE TABLE temp1 AS...
CREATE TABLE data AS...
```

### One Step at a Time
```sql
-- Good: Break complex logic into steps
CREATE TABLE clean_orders AS
SELECT * FROM orders WHERE amount > 0;

CREATE TABLE order_summary AS
SELECT customer_id, SUM(amount) FROM clean_orders GROUP BY customer_id;

-- Avoid: Everything in one complex query
CREATE TABLE complex_summary AS
SELECT ... FROM (SELECT ... FROM (SELECT ... FROM orders) ...) ...;
```

## Quick Reference

| Concept | Purpose | Example |
|---------|---------|---------|
| **SOURCE** | Define data location | `SOURCE name TYPE CSV PARAMS {...}` |
| **LOAD** | Bring data in | `LOAD table FROM source MODE REPLACE/APPEND/UPSERT` |
| **CREATE TABLE** | Transform data | `CREATE TABLE result MODE REPLACE/APPEND/UPSERT AS SELECT...` |
| **EXPORT** | Save results | `EXPORT SELECT * TO "file.csv" TYPE CSV` |

## Load Modes Summary

| Mode | LOAD Usage | CREATE TABLE Usage | Best For |
|------|------------|-------------------|----------|
| **REPLACE** | `LOAD table FROM source` | `CREATE TABLE name AS SELECT...` | Full refreshes, small datasets |
| **APPEND** | `LOAD table FROM source MODE APPEND` | `CREATE TABLE name MODE APPEND AS SELECT...` | Logs, time-series, incremental data |
| **UPSERT** | `LOAD table FROM source MODE UPSERT KEY id` | `CREATE TABLE name MODE UPSERT KEY id AS SELECT...` | Master data, profiles, deduplication |

## Common Patterns

### Simple File Processing
```sql
CREATE TABLE data AS SELECT * FROM read_csv_auto('input.csv');
CREATE TABLE summary AS SELECT category, COUNT(*) FROM data GROUP BY category;
EXPORT SELECT * FROM summary TO "output.csv" TYPE CSV;
```

### Incremental Data Processing
```sql
-- Load new data only
LOAD daily_orders FROM new_orders MODE APPEND;

-- Update metrics incrementally
CREATE TABLE customer_metrics MODE UPSERT KEY customer_id AS
SELECT customer_id, COUNT(*), SUM(amount) FROM daily_orders GROUP BY customer_id;
```

### Master Data Updates
```sql
-- Update customer profiles with latest information
LOAD customers FROM customer_updates MODE UPSERT KEY customer_id;

-- Rebuild derived tables
CREATE TABLE customer_segments MODE REPLACE AS
SELECT customer_id, 
       CASE WHEN total_spent > 1000 THEN 'VIP' ELSE 'Standard' END as segment
FROM customers;
```