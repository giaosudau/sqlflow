# Pipeline Development Guide

Learn how to build effective SQLFlow pipelines - from simple data processing to complex multi-step workflows.

## What Are Pipelines?

A pipeline is a sequence of data operations that SQLFlow runs in the right order. Think of it as a recipe:
1. Gather ingredients (load data)
2. Process them (transform with SQL)  
3. Serve the result (export files)

SQLFlow automatically figures out the order based on what each step needs.

## Quick Example

```sql
-- Complete customer analytics pipeline
CREATE TABLE customers AS
SELECT * FROM read_csv_auto('data/customers.csv');

CREATE TABLE orders AS  
SELECT * FROM read_csv_auto('data/orders.csv');

CREATE TABLE customer_summary AS
SELECT 
    c.country,
    COUNT(*) as customer_count,
    AVG(c.age) as avg_age,
    COALESCE(SUM(o.price * o.quantity), 0) as total_revenue
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;

EXPORT SELECT * FROM customer_summary
TO "output/customer_summary.csv"
TYPE CSV OPTIONS { "header": true };
```

This processes customer and order data to create country-level analytics.

## Pipeline Structure

### Simple Pattern: Load → Transform → Export
Most pipelines follow this basic pattern:

```sql
-- 1. Load data
CREATE TABLE sales AS
SELECT * FROM read_csv_auto('data/sales.csv');

-- 2. Transform data  
CREATE TABLE monthly_summary AS
SELECT 
    DATE_TRUNC('month', order_date) as month,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
FROM sales
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;

-- 3. Export results
EXPORT SELECT * FROM monthly_summary
TO "output/monthly_sales.csv"
TYPE CSV OPTIONS { "header": true };
```

### Multi-Source Pattern: Combine → Transform → Export
When you need data from multiple sources:

```sql
-- Load from different sources
SOURCE customers TYPE POSTGRES PARAMS {
    "host": "localhost",
    "dbname": "shop",
    "table": "customers"
};
LOAD customer_data FROM customers;

CREATE TABLE orders AS
SELECT * FROM read_csv_auto('data/orders.csv');

-- Combine and transform
CREATE TABLE customer_analytics AS
SELECT 
    c.customer_id,
    c.name,
    c.segment,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as lifetime_value
FROM customer_data c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.segment;

-- Export results
EXPORT SELECT * FROM customer_analytics
TO "output/customer_analytics.csv"
TYPE CSV OPTIONS { "header": true };
```

## How to Build Pipelines

### Step 1: Start with `sqlflow init`
Always start with a working foundation:

```bash
sqlflow init my-analytics-project
cd my-analytics-project
ls pipelines/    # See example pipelines
sqlflow pipeline run customer_analytics  # Test it works
```

### Step 2: Understand Your Data
Look at your input data first:

```sql
-- Start simple - just load and examine
CREATE TABLE raw_data AS
SELECT * FROM read_csv_auto('data/mydata.csv');

-- Look at the first few rows
EXPORT SELECT * FROM raw_data LIMIT 10
TO "output/sample.csv"
TYPE CSV OPTIONS { "header": true };
```

### Step 3: Build Step by Step
Add one transformation at a time:

```sql
-- Step 1: Clean data
CREATE TABLE clean_data AS
SELECT 
    customer_id,
    order_date,
    amount
FROM raw_data
WHERE amount > 0 
  AND order_date IS NOT NULL;

-- Step 2: Add calculations  
CREATE TABLE enriched_data AS
SELECT 
    *,
    CASE 
        WHEN amount >= 1000 THEN 'high-value'
        WHEN amount >= 100 THEN 'medium-value'
        ELSE 'low-value'
    END as order_tier
FROM clean_data;

-- Step 3: Summarize
CREATE TABLE summary AS
SELECT 
    order_tier,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM enriched_data
GROUP BY order_tier;

-- Export each step to verify
EXPORT SELECT * FROM summary
TO "output/summary.csv"
TYPE CSV OPTIONS { "header": true };
```

### Step 4: Test and Refine
Run your pipeline often to catch issues early:

```bash
# Run the pipeline
sqlflow pipeline run my_pipeline

# Check the outputs
ls output/
cat output/summary.csv
```

## Data Loading Strategies

Choose the right approach for your data:

### CSV Files (Simple)
```sql
-- For small to medium CSV files
CREATE TABLE data AS SELECT * FROM read_csv_auto('data/file.csv');
```

### Database Tables
```sql
-- For live database data
SOURCE users TYPE POSTGRES PARAMS {
    "host": "localhost",
    "dbname": "shop",
    "table": "users"
};
LOAD user_data FROM users;
```

### Multiple Files
```sql
-- Process multiple related files
CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');
CREATE TABLE orders AS SELECT * FROM read_csv_auto('data/orders.csv');
CREATE TABLE products AS SELECT * FROM read_csv_auto('data/products.csv');
```

### Large Datasets
```sql
-- For bigger datasets, use SOURCE/LOAD with modes
SOURCE large_table TYPE POSTGRES PARAMS {
    "host": "datawarehouse",
    "table": "sales_history"
};
LOAD sales FROM large_table MODE REPLACE;
```

## Transformation Patterns

### Layer Your Transformations
Break complex logic into steps:

```sql
-- Layer 1: Raw data cleaning
CREATE TABLE clean_orders AS
SELECT 
    order_id,
    customer_id,
    DATE(order_date) as order_date,
    amount
FROM raw_orders
WHERE amount > 0 
  AND order_date IS NOT NULL;

-- Layer 2: Business logic
CREATE TABLE enriched_orders AS
SELECT 
    o.*,
    c.customer_segment,
    CASE 
        WHEN amount >= 1000 THEN 'high-value'
        ELSE 'standard'
    END as order_type
FROM clean_orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;

-- Layer 3: Final aggregation
CREATE TABLE final_summary AS
SELECT 
    customer_segment,
    order_type,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue
FROM enriched_orders
GROUP BY customer_segment, order_type;
```

### Common SQL Patterns

#### Date Analysis
```sql
CREATE TABLE daily_sales AS
SELECT 
    DATE(order_date) as date,
    COUNT(*) as orders,
    SUM(amount) as revenue,
    AVG(amount) as avg_order_value
FROM orders
GROUP BY DATE(order_date)
ORDER BY date;
```

#### Customer Segmentation  
```sql
CREATE TABLE customer_segments AS
SELECT 
    customer_id,
    SUM(amount) as total_spent,
    COUNT(*) as order_count,
    CASE 
        WHEN SUM(amount) >= 5000 THEN 'VIP'
        WHEN SUM(amount) >= 1000 THEN 'Premium'
        ELSE 'Standard'
    END as segment
FROM orders
GROUP BY customer_id;
```

#### Top N Analysis
```sql
CREATE TABLE top_products AS
SELECT 
    product_name,
    SUM(quantity) as total_sold,
    SUM(price * quantity) as revenue
FROM order_items
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 20;
```

## Variables for Flexibility

Make pipelines reusable with variables:

```sql
SET report_date = "${date|2023-06-01}";
SET min_amount = "${min_amount|100}";

CREATE TABLE daily_orders AS
SELECT * FROM orders 
WHERE DATE(order_date) = '${report_date}'
  AND amount >= ${min_amount};

EXPORT SELECT * FROM daily_orders
TO "output/orders_${report_date}.csv"
TYPE CSV OPTIONS { "header": true };
```

Run with different values:
```bash
sqlflow pipeline run daily_report --var date=2023-07-01 --var min_amount=500
```

## Common Patterns

### Data Quality Checks
```sql
-- Check for missing data
CREATE TABLE data_quality AS
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as missing_emails,
    COUNT(CASE WHEN age IS NULL THEN 1 END) as missing_ages
FROM customers;

EXPORT SELECT * FROM data_quality
TO "output/quality_report.csv"
TYPE CSV OPTIONS { "header": true };
```

### Incremental Processing
```sql
-- Process only new data
SET last_processed = "${last_processed|2023-01-01}";

CREATE TABLE new_orders AS
SELECT * FROM orders
WHERE order_date > '${last_processed}';

CREATE TABLE updated_summary AS
SELECT 
    DATE(order_date) as date,
    COUNT(*) as new_orders,
    SUM(amount) as new_revenue
FROM new_orders
GROUP BY DATE(order_date);
```

### Multiple Outputs
```sql
-- Create several related reports
CREATE TABLE customer_summary AS
SELECT country, COUNT(*) as customers FROM customers GROUP BY country;

CREATE TABLE order_summary AS  
SELECT country, COUNT(*) as orders, SUM(amount) as revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY country;

-- Export each summary
EXPORT SELECT * FROM customer_summary
TO "output/customers_by_country.csv" TYPE CSV;

EXPORT SELECT * FROM order_summary  
TO "output/orders_by_country.csv" TYPE CSV;
```

## Performance Tips

### Start Small
Test with small datasets first:
```sql
-- Test with a sample
CREATE TABLE sample_data AS
SELECT * FROM read_csv_auto('data/large_file.csv') LIMIT 1000;
```

### Use Efficient SQL
- Filter early: `WHERE` clauses before `JOIN`
- Use appropriate `GROUP BY` and `ORDER BY`
- Avoid `SELECT *` in final outputs

```sql
-- Good: Filter then join
CREATE TABLE recent_orders AS
SELECT * FROM orders WHERE order_date >= '2023-01-01';

CREATE TABLE customer_recent_orders AS
SELECT c.name, COUNT(r.order_id) as recent_orders
FROM customers c
LEFT JOIN recent_orders r ON c.customer_id = r.customer_id
GROUP BY c.name;
```

### Export What You Need
```sql
-- Don't export everything - be selective
EXPORT SELECT 
    country,
    customer_count,
    total_revenue
FROM customer_summary
WHERE customer_count > 10
TO "output/significant_countries.csv"
TYPE CSV OPTIONS { "header": true };
```

## Troubleshooting

### Pipeline Fails
1. **Check the error message** - SQLFlow gives specific errors
2. **Run steps individually** - Comment out parts to isolate issues
3. **Verify data files exist** - Check file paths
4. **Test with small data** - Use `LIMIT 10` to debug

### "Table not found" Errors
Make sure you create tables before using them:
```sql
-- Wrong order
CREATE TABLE summary AS SELECT * FROM customers;  -- Error!
CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');

-- Right order
CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');
CREATE TABLE summary AS SELECT * FROM customers;  -- Works!
```

### Memory Issues with Large Data
Use SOURCE/LOAD instead of reading files directly:
```sql
-- For large files, use SOURCE/LOAD
SOURCE big_data TYPE CSV PARAMS {
    "path": "data/large_file.csv",
    "has_header": true
};
LOAD data_table FROM big_data;
```

### Slow Performance  
1. **Filter early** - Use `WHERE` clauses to reduce data
2. **Avoid complex nested queries** - Break into steps
3. **Check data types** - Ensure joins use same types

## Best Practices

### Use Clear Names
```sql
-- Good: Descriptive names
CREATE TABLE monthly_sales_summary AS...
CREATE TABLE high_value_customers AS...

-- Avoid: Unclear names  
CREATE TABLE temp1 AS...
CREATE TABLE final AS...
```

### Document Your Pipeline
```sql
-- Customer Analytics Pipeline
-- Processes daily order data and creates customer segments
-- Outputs: customer_summary.csv, daily_sales.csv

SET process_date = "${date|2023-06-01}";

-- Load raw data
CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');
-- etc...
```

### Test Thoroughly
1. **Test with sample data** first
2. **Run frequently** during development  
3. **Verify outputs** look correct
4. **Test with different variables**

```bash
# Test the pipeline
sqlflow pipeline run my_pipeline

# Verify outputs
ls -la output/
head output/summary.csv
```

### Start Simple, Add Complexity
Begin with basic functionality and add features:

```sql
-- Version 1: Basic summary
CREATE TABLE summary AS
SELECT country, COUNT(*) FROM customers GROUP BY country;

-- Version 2: Add revenue
CREATE TABLE summary AS  
SELECT 
    country, 
    COUNT(*) as customers,
    SUM(total_orders) as revenue
FROM customers GROUP BY country;

-- Version 3: Add segmentation
-- etc...
```

## Complete Example

Here's a full pipeline that demonstrates good practices:

```sql
-- E-commerce Analytics Pipeline
-- Author: Data Team
-- Purpose: Daily customer and sales analysis
-- Inputs: customers.csv, orders.csv
-- Outputs: customer_analytics.csv, daily_summary.csv

-- Configuration
SET process_date = "${date|2023-06-01}";
SET min_order_value = "${min_order|50}";

-- Load data
CREATE TABLE customers AS
SELECT 
    customer_id,
    name,
    country,
    age,
    registration_date
FROM read_csv_auto('data/customers.csv')
WHERE registration_date IS NOT NULL;

CREATE TABLE orders AS
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    status
FROM read_csv_auto('data/orders.csv')
WHERE status = 'completed'
  AND amount >= ${min_order_value};

-- Transform: Customer metrics
CREATE TABLE customer_metrics AS
SELECT 
    c.customer_id,
    c.name,
    c.country,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as lifetime_value,
    AVG(o.amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.country;

-- Transform: Daily summary
CREATE TABLE daily_summary AS
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) as active_customers,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE DATE(o.order_date) = '${process_date}'
GROUP BY c.country
ORDER BY total_revenue DESC;

-- Export results
EXPORT SELECT * FROM customer_metrics
WHERE total_orders > 0
TO "output/customer_analytics_${process_date}.csv"
TYPE CSV OPTIONS { "header": true };

EXPORT SELECT * FROM daily_summary
TO "output/daily_summary_${process_date}.csv"  
TYPE CSV OPTIONS { "header": true };
```

Run it:
```bash
# Use defaults
sqlflow pipeline run ecommerce_analytics

# Specific date and minimum order value
sqlflow pipeline run ecommerce_analytics --var date=2023-07-15 --var min_order=100
```

This pipeline demonstrates:
- Clear documentation and purpose
- Configurable variables with defaults
- Data cleaning and validation
- Logical transformation steps  
- Multiple targeted outputs
- Meaningful file names with dates

---

**Next Steps**: [Advanced Patterns](advanced-patterns.md) | [Performance Tuning](performance-tuning.md) | [Pipeline Testing](testing.md) 