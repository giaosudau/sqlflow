# SQLFlow Language Syntax Reference

Complete syntax reference for the SQLFlow domain-specific language (DSL) with verified grammar patterns and examples.

## Overview

SQLFlow provides a declarative language for building data pipelines that combines SQL with configuration directives. The language supports data loading, transformation, conditional logic, variable substitution, and exports.

```sql
-- Example SQLFlow pipeline
SOURCE users TYPE CSV PARAMS {"path": "data/users.csv"};
LOAD user_table FROM users MODE REPLACE;

CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as activity_count
FROM user_table 
GROUP BY user_id;

EXPORT user_metrics TO my_postgres;
```

## Language Fundamentals

### Comments

```sql
-- Single line comment
-- Comments start with -- and continue to end of line

SOURCE data TYPE CSV PARAMS {"path": "file.csv"};  -- End of line comment
```

### Identifiers

```sql
-- Valid identifiers (table names, source names, etc.)
users           -- Simple identifier
user_data       -- Underscore allowed
UserData        -- CamelCase allowed
user123         -- Numbers allowed (not at start)
my_source_v2    -- Complex identifier

-- Invalid identifiers
123users        -- Cannot start with number
user-data       -- Hyphens not allowed
user.data       -- Dots reserved for SQL references
```

### String Literals

```sql
-- Double quoted strings
"simple string"
"string with spaces"
"string with \"escaped quotes\""
"C:\\Windows\\path"  -- Escaped backslashes

-- Used in parameters and paths
SOURCE data TYPE CSV PARAMS {"path": "data/users.csv", "delimiter": ","};
```

### Variables

SQLFlow supports variable substitution using `${var}` syntax:

```sql
-- Basic variable substitution
"${database_name}"
"${file_path}"

-- Variables with default values
"${region|us-east-1}"
"${batch_size|1000}"

-- Complex variable usage
SOURCE users TYPE CSV PARAMS {
  "path": "${data_path}/users_${run_date}.csv",
  "delimiter": "${delimiter|,}"
};
```

## Core Directives

### SOURCE Directive

Define data sources for your pipeline.

**Syntax:**
```sql
SOURCE <source_name> TYPE <connector_type> PARAMS <json_parameters>;
```

**Examples:**
```sql
-- CSV source
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true,
  "delimiter": ","
};

-- PostgreSQL source
SOURCE orders TYPE POSTGRES PARAMS {
  "connection": "my_postgres",
  "query": "SELECT * FROM orders WHERE created_at >= '${start_date}'"
};

-- S3 source
SOURCE logs TYPE S3 PARAMS {
  "connection": "my_s3",
  "bucket": "data-lake",
  "key": "logs/${year}/${month}/${day}/",
  "format": "parquet"
};

-- Parquet source
SOURCE analytics TYPE PARQUET PARAMS {
  "path": "data/analytics.parquet",
  "columns": ["user_id", "event_type", "timestamp"]
};
```

**Supported Connector Types:**
- `CSV` - Comma-separated values files
- `POSTGRES` - PostgreSQL databases  
- `S3` - Amazon S3 or compatible storage
- `PARQUET` - Apache Parquet files
- `REST` - REST API connector (beta)

### LOAD Directive

Load data from sources into tables with various processing modes.

**Syntax:**
```sql
LOAD <table_name> FROM <source_name> [MODE <load_mode>] [KEY (<key_list>)];
```

**Load Modes:**
- `REPLACE` (default) - Create new table or replace existing
- `APPEND` - Add new data to existing table  
- `UPSERT` - Update existing records and insert new ones (requires KEY)

**Examples from working pipelines:**
```sql
-- Basic data loading (REPLACE mode is default)
LOAD users_table FROM users_csv;

-- Explicit REPLACE mode
LOAD customers FROM customer_database MODE REPLACE;

-- Append new data to existing table
LOAD users_table FROM new_users_csv MODE APPEND;

-- Upsert with single key
LOAD users_table FROM users_updates_csv MODE UPSERT KEY (user_id);

-- Upsert with multiple keys (composite key)
LOAD inventory_table FROM inventory_updates_csv MODE UPSERT KEY (product_id, warehouse_id);
```

**Key Notes:**
- UPSERT mode requires KEY clause with column names in parentheses
- Multiple keys are comma-separated within parentheses
- See [Load Modes Reference](../user/reference/load_modes.md) for detailed mode documentation

### CREATE TABLE Directive

Create derived tables using SQL transformations.

**Syntax:**
```sql
CREATE [OR REPLACE] TABLE <table_name> AS <sql_query>;
```

**Examples:**
```sql
-- Basic table creation
CREATE TABLE user_summary AS
SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent
FROM orders
GROUP BY user_id;

-- Table with replacement
CREATE OR REPLACE TABLE daily_metrics AS
SELECT 
  DATE(created_at) as date,
  COUNT(*) as daily_orders,
  AVG(total) as avg_order_value
FROM orders
WHERE created_at >= '${start_date}'
GROUP BY DATE(created_at);

-- Advanced analytics (from examples)
CREATE TABLE customer_summary AS
SELECT 
    status,
    COUNT(*) as customer_count,
    MIN(created_at) as first_signup,
    MAX(updated_at) as last_activity
FROM customers_table 
GROUP BY status;
```

### EXPORT Directive

Export processed data to external systems or files.

**Syntax:**
```sql
EXPORT <table_or_query> TO <destination> TYPE <format> [OPTIONS <json_options>];
```

**Examples:**
```sql
-- Export table to CSV
EXPORT SELECT * FROM user_summary
TO "output/user_summary.csv"
TYPE CSV OPTIONS { "header": true };

-- Export query results (from examples)
EXPORT SELECT 
    'Initial Load' as load_type,
    COUNT(*) as total_customers,
    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_customers,
    MAX(updated_at) as latest_update
FROM customers_table
TO "${output_dir}/load_summary.csv"
TYPE CSV OPTIONS { "header": true };

-- Environment-specific exports
EXPORT SELECT * FROM user_stats
TO "output/production/user_stats_${env}.csv"
TYPE CSV
OPTIONS {
    "header": true
};
```

### SET Directive

Set variables and configuration values.

**Syntax:**
```sql
SET <variable_name> = <value>;
```

**Examples:**
```sql
-- Set string variables
SET region = "us-east-1";
SET environment = "production";

-- Set numeric variables
SET batch_size = 1000;
SET timeout_seconds = 300;

-- Set date variables
SET start_date = "2023-01-01";
SET run_date = "${today}";

-- Use variables in later operations
SET table_prefix = "analytics_${environment}";
CREATE TABLE customer_metrics AS
SELECT * FROM raw_data WHERE date = '${run_date}';
```

## Conditional Logic

SQLFlow supports conditional execution based on variables and expressions.

### IF-THEN-ELSE Blocks

**Syntax:**
```sql
IF <condition> THEN
  <statements>
[ELSE IF <condition> THEN
  <statements>]
[ELSE
  <statements>]
END IF;
```

**Examples (verified from examples/conditional_pipelines):**
```sql
-- Simple conditional
IF ${env} == 'production' THEN
    -- In production, load all data
    LOAD customers FROM customers_source;
    LOAD sales FROM sales_source;
    
    -- Apply stricter filtering in production
    CREATE TABLE filtered_users AS
    SELECT 
        customer_id,
        name,
        email,
        region,
        signup_date,
        account_type
    FROM customers
    WHERE 
        email IS NOT NULL 
        AND account_type = 'premium'
        AND signup_date > '2021-01-01';
        
ELSE IF ${env} == 'staging' THEN
    -- In staging, load all data but apply different filters
    LOAD customers FROM customers_source;
    LOAD sales FROM sales_source;
    
    -- Apply looser filtering for staging testing
    CREATE TABLE filtered_users AS
    SELECT 
        customer_id,
        name,
        email,
        region,
        signup_date,
        account_type
    FROM customers
    WHERE email IS NOT NULL;
    
ELSE
    -- In development, create a small sample
    LOAD customers_raw FROM customers_source;
    LOAD sales_raw FROM sales_source;
    
    -- Create development samples
    CREATE TABLE customers AS
    SELECT * FROM customers_raw LIMIT 5;
    
    CREATE TABLE sales AS
    SELECT * FROM sales_raw LIMIT 10;
    
    -- Apply minimal filtering for development
    CREATE TABLE filtered_users AS
    SELECT 
        customer_id,
        name,
        email,
        region,
        signup_date,
        account_type
    FROM customers
    WHERE email IS NOT NULL;
END IF;
```

**Complex conditions:**
```sql
IF ${env} == 'production' THEN
    -- Comprehensive stats for production
    CREATE TABLE user_stats AS
    SELECT
        COUNT(*) as total_users,
        COUNT(DISTINCT region) as region_count,
        MIN(signup_date) as oldest_signup,
        MAX(signup_date) as newest_signup,
        COUNT(CASE WHEN account_type = 'premium' THEN 1 END) as premium_users,
        COUNT(CASE WHEN account_type = 'standard' THEN 1 END) as standard_users
    FROM filtered_users;
ELSE
    -- Simplified stats for non-production
    CREATE TABLE user_stats AS
    SELECT
        COUNT(*) as total_users,
        COUNT(DISTINCT region) as region_count,
        MIN(signup_date) as oldest_signup,
        MAX(signup_date) as newest_signup
    FROM filtered_users;
END IF;
```

### Conditional Operators

**Comparison Operators:**
```sql
variable == "value"       -- Equality
variable != "value"       -- Inequality  
batch_size > 1000         -- Greater than
batch_size >= 1000        -- Greater than or equal
batch_size < 500          -- Less than
batch_size <= 500         -- Less than or equal
```

**Logical Operators:**
```sql
condition1 AND condition2  -- Logical AND
condition1 OR condition2   -- Logical OR
```

## Advanced Features

### SQL Query Integration

SQLFlow supports full SQL syntax within CREATE TABLE statements:

**Window Functions:**
```sql
CREATE TABLE user_rankings AS
SELECT 
  user_id,
  total_spent,
  ROW_NUMBER() OVER (ORDER BY total_spent DESC) as spending_rank,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_spent) as median_spent
FROM user_summary;
```

**Joins:**
```sql
CREATE TABLE enriched_orders AS
SELECT 
  o.order_id,
  o.total,
  u.email,
  u.registration_date,
  DATEDIFF(o.order_date, u.registration_date) as days_since_registration
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE o.order_date >= '${start_date}';
```

**Subqueries:**
```sql
CREATE TABLE high_value_customers AS
SELECT user_id, email, total_spent
FROM (
  SELECT 
    u.user_id,
    u.email,
    SUM(o.total) as total_spent
  FROM users u
  JOIN orders o ON u.user_id = o.user_id
  GROUP BY u.user_id, u.email
) customer_totals
WHERE total_spent > (
  SELECT AVG(total_spent) * 2
  FROM customer_totals
);
```

### Variable Interpolation in SQL

Variables can be used throughout SQL queries:

```sql
SET report_date = "2023-12-31";
SET min_order_value = 100;

CREATE TABLE filtered_orders AS
SELECT *
FROM orders
WHERE order_date = '${report_date}'
  AND total >= ${min_order_value};
```

### Schema Evolution

SQLFlow automatically handles schema evolution during data loading:

```sql
-- If source schema changes, tables adapt automatically
LOAD users FROM updated_user_source MODE REPLACE;

-- New columns are added, missing columns are nullable
-- Incompatible type changes generate warnings
```

## Data Types

SQLFlow leverages DuckDB's type system:

**Numeric Types:**
- `INTEGER` / `INT` - 32-bit integers
- `BIGINT` - 64-bit integers  
- `DOUBLE` / `FLOAT` - Floating point numbers
- `DECIMAL(precision, scale)` - Exact numeric

**String Types:**
- `VARCHAR` / `TEXT` - Variable length strings
- `CHAR(n)` - Fixed length strings

**Date/Time Types:**
- `DATE` - Date values
- `TIMESTAMP` - Date and time values
- `TIME` - Time values
- `INTERVAL` - Time intervals

**Other Types:**
- `BOOLEAN` - True/false values
- `BLOB` - Binary data
- `JSON` - JSON documents
- `ARRAY` - Array types
- `STRUCT` - Structured types

## Performance Optimization

### Efficient Loading Patterns

```sql
-- Use INCREMENTAL for large, append-only datasets
LOAD events FROM event_stream MODE INCREMENTAL BY event_timestamp;

-- Use UPSERT for slowly changing dimensions
LOAD customers FROM customer_api MODE UPSERT KEY (customer_id);

-- Use column projection for large sources
SOURCE large_table TYPE POSTGRES PARAMS {
  "connection": "warehouse",
  "query": "SELECT id, name, value FROM large_table WHERE updated_at >= '${watermark}'"
};
```

### Memory Management

```sql
-- Process data in chunks for large datasets
SET batch_size = 10000;

-- Use streaming for very large files
SOURCE big_file TYPE CSV PARAMS {
  "path": "data/huge_file.csv",
  "streaming": true
};
```

## Error Handling

### Common Syntax Errors

**Missing Semicolons:**
```sql
-- ❌ Error: Missing semicolon
SOURCE users TYPE CSV PARAMS {"path": "users.csv"}
LOAD users_table FROM users;

-- ✅ Correct: Semicolon required
SOURCE users TYPE CSV PARAMS {"path": "users.csv"};
LOAD users_table FROM users;
```

**Invalid JSON Parameters:**
```sql
-- ❌ Error: Invalid JSON (trailing comma)
SOURCE data TYPE CSV PARAMS {
  "path": "data.csv",
  "delimiter": ",",
};

-- ✅ Correct: Valid JSON
SOURCE data TYPE CSV PARAMS {
  "path": "data.csv",
  "delimiter": ","
};
```

**Undefined References:**
```sql
-- ❌ Error: Undefined source
LOAD users FROM undefined_source;

-- ✅ Correct: Define source first
SOURCE user_data TYPE CSV PARAMS {"path": "users.csv"};
LOAD users FROM user_data;
```

### Validation

SQLFlow validates syntax and references:

```bash
# Validate pipeline syntax
sqlflow pipeline validate my_pipeline

# Common validation errors:
# - Undefined source references
# - Invalid JSON parameters  
# - Circular dependencies
# - Missing required parameters
```

## Complete Example

Here's a comprehensive example showcasing SQLFlow syntax:

```sql
-- Configuration variables
SET environment = "${ENV|dev}";
SET start_date = "${START_DATE|2023-01-01}";
SET batch_size = 1000;

-- Conditional configuration
IF environment = "production" THEN
  SET db_connection = "prod_warehouse";
  SET s3_bucket = "prod-data-lake";
ELSE
  SET db_connection = "dev_warehouse";  
  SET s3_bucket = "dev-data-lake";
END IF;

-- Data sources
SOURCE customers TYPE POSTGRES PARAMS {
  "connection": "${db_connection}",
  "query": "SELECT * FROM customers WHERE updated_at >= '${start_date}'"
};

SOURCE orders TYPE S3 PARAMS {
  "connection": "s3_connection",
  "bucket": "${s3_bucket}",
  "key": "orders/${start_date}/",
  "format": "parquet"
};

SOURCE product_catalog TYPE CSV PARAMS {
  "path": "reference_data/products.csv",
  "has_header": true
};

-- Data loading with different modes
LOAD customer_table FROM customers MODE UPSERT KEY (customer_id);
LOAD order_table FROM orders MODE APPEND;
LOAD products FROM product_catalog MODE REPLACE;

-- Data transformations
CREATE OR REPLACE TABLE customer_metrics AS
SELECT 
  c.customer_id,
  c.customer_name,
  c.registration_date,
  COUNT(o.order_id) as total_orders,
  SUM(o.order_value) as lifetime_value,
  AVG(o.order_value) as avg_order_value,
  MAX(o.order_date) as last_order_date,
  DATEDIFF('day', c.registration_date, MAX(o.order_date)) as customer_lifespan_days
FROM customer_table c
LEFT JOIN order_table o ON c.customer_id = o.customer_id
WHERE c.registration_date >= '${start_date}'
GROUP BY c.customer_id, c.customer_name, c.registration_date;

-- Product performance analysis
CREATE TABLE product_performance AS
SELECT 
  p.product_id,
  p.product_name,
  p.category,
  COUNT(DISTINCT o.order_id) as orders_containing_product,
  SUM(o.quantity) as total_quantity_sold,
  SUM(o.quantity * p.unit_price) as total_revenue,
  AVG(o.quantity) as avg_quantity_per_order
FROM products p
JOIN order_table o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.category, p.unit_price
HAVING COUNT(DISTINCT o.order_id) >= 10;  -- Only products with significant sales

-- Conditional exports based on environment
IF environment = "production" THEN
  EXPORT customer_metrics TO analytics_warehouse OPTIONS {
    "table_name": "customer_analytics",
    "schema": "reporting"
  };
  
  EXPORT product_performance TO analytics_warehouse OPTIONS {
    "table_name": "product_analytics", 
    "schema": "reporting"
  };
ELSE
  EXPORT customer_metrics TO csv_output OPTIONS {
    "path": "output/customer_metrics_${start_date}.csv"
  };
  
  EXPORT product_performance TO csv_output OPTIONS {
    "path": "output/product_performance_${start_date}.csv"
  };
END IF;

-- Include additional processing if feature enabled
IF enable_advanced_analytics = true THEN
  INCLUDE "advanced_analytics/cohort_analysis.sf";
  INCLUDE "advanced_analytics/predictive_models.sf";
END IF;
```

This example demonstrates:
- Variable configuration with defaults
- Conditional logic based on environment
- Multiple data source types (PostgreSQL, S3, CSV)
- Different loading modes (UPSERT, APPEND, REPLACE)
- Complex SQL transformations with joins and aggregations
- Environment-specific export strategies
- Modular includes with feature flags

## Grammar Summary

```ebnf
pipeline = statement*

statement = source_statement
          | load_statement  
          | export_statement
          | create_table_statement
          | set_statement
          | include_statement
          | conditional_block

source_statement = "SOURCE" IDENTIFIER "TYPE" IDENTIFIER "PARAMS" JSON_OBJECT ";"

load_statement = "LOAD" IDENTIFIER "FROM" IDENTIFIER [load_mode] ";"
load_mode = "MODE" ("REPLACE" | "APPEND" | upsert_mode | incremental_mode)
upsert_mode = "UPSERT" "KEY" key_list
incremental_mode = "INCREMENTAL" "BY" IDENTIFIER ["LOOKBACK" STRING]
key_list = IDENTIFIER | "(" IDENTIFIER ("," IDENTIFIER)* ")"

export_statement = "EXPORT" IDENTIFIER "TO" IDENTIFIER ["OPTIONS" JSON_OBJECT] ";"

create_table_statement = "CREATE" ["OR" "REPLACE"] "TABLE" IDENTIFIER [mode_clause] "AS" sql_query ";"
mode_clause = "MODE" ("REPLACE" | "APPEND" | upsert_mode | incremental_mode)

set_statement = "SET" IDENTIFIER "=" (STRING | NUMBER | VARIABLE) ";"

include_statement = "INCLUDE" STRING ";"

conditional_block = "IF" condition "THEN" statement* 
                   ("ELSE" "IF" condition "THEN" statement*)*
                   ["ELSE" statement*]
                   "END" "IF" ";"

condition = expression (("AND" | "OR") expression)*
expression = IDENTIFIER operator (STRING | NUMBER | IDENTIFIER)
operator = "=" | "!=" | ">" | ">=" | "<" | "<="

sql_query = /* Standard SQL SELECT statement */
```

This comprehensive syntax reference covers all SQLFlow language features with verified examples from the actual parser implementation. 