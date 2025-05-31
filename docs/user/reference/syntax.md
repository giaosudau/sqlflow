# SQLFlow Syntax Reference

This document describes the syntax supported by SQLFlow, a domain-specific language (DSL) for data transformation and processing pipelines.

## Table of Contents

- [Overview](#overview)
- [Comments](#comments)
- [Variables and Variable Substitution](#variables-and-variable-substitution)
- [Directives](#directives)
  - [SET](#set)
  - [SOURCE](#source)
  - [LOAD](#load)
  - [CREATE](#create)
  - [EXPORT](#export)
  - [INCLUDE](#include)
- [Conditional Execution](#conditional-execution)
- [SQL Support](#sql-support)
- [Python UDFs](#python-udfs)
- [Examples](#examples)

## Overview

SQLFlow is a specialized language designed to define and execute data transformation pipelines. It combines SQL's familiarity with pipeline-specific directives to create powerful, readable data workflows.

A SQLFlow pipeline consists of a series of statements that define data sources, load data, transform it using SQL, and export the results. Each statement typically ends with a semicolon (`;`).

## Comments

SQLFlow supports two comment styles:

```sql
-- This is a single-line comment using double dashes

// This is also a single-line comment using double slashes
```

## Variables and Variable Substitution

### Defining Variables

Variables can be defined using the `SET` directive:

```sql
SET variable_name = "value";
```

### Variable Substitution

Variables can be referenced within strings using the `${variable_name}` syntax:

```sql
SET date = "2023-10-25";
EXPORT 
  SELECT * FROM table_name
TO "output/data_${date}.csv"
TYPE CSV
OPTIONS { "header": true };
```

### Default Values

You can provide default values for variables that might not be set using the pipe (`|`) operator:

```sql
SET output_path = "${output_dir|target}/result.csv";
```

In this example, if `output_dir` is not defined, the value "target" will be used.

When providing default values:
1. Default values with spaces must be quoted, e.g. `${region|"us-east-1"}`
2. Empty variable values (`""`) will cause validation errors during compilation
3. Self-referential variables (e.g. `SET use_csv = "${use_csv|true}";`) are supported and will use the default value if not explicitly set

## Directives

### SET

The `SET` directive assigns a value to a variable.

**Syntax:**
```sql
SET variable_name = "value";
```

### SOURCE

The `SOURCE` directive defines a data source that can be loaded into the pipeline.

**Basic Syntax:**
```sql
SOURCE source_name TYPE source_type PARAMS {
  "param1": "value1",
  "param2": "value2"
};
```

**Example:**
```sql
SOURCE customers TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};
```

**Alternative Syntax:**
```sql
SOURCE source_name (
  connector_type = "CONNECTOR_TYPE",
  param1 = "value1",
  param2 = "value2"
);
```

#### Industry-Standard Parameters

SQLFlow supports industry-standard parameters compatible with Airbyte, Fivetran, and other ELT tools:

**Incremental Loading Parameters:**
```sql
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true,
  "sync_mode": "incremental",
  "primary_key": "user_id",
  "cursor_field": "updated_at"
};
```

**Supported Parameters:**

| Parameter | Type | Description | Values |
|-----------|------|-------------|---------|
| `sync_mode` | string | How to sync data | `"full_refresh"`, `"incremental"` |
| `primary_key` | string | Primary key field for MERGE operations | Any column name |
| `cursor_field` | string | Field used for incremental loading watermarks | Timestamp or ID column |

**Parameter Usage Examples:**

```sql
-- Full refresh (default behavior)
SOURCE products TYPE CSV PARAMS {
  "path": "data/products.csv",
  "has_header": true,
  "sync_mode": "full_refresh"
};

-- Incremental loading with timestamp cursor
SOURCE orders TYPE CSV PARAMS {
  "path": "data/orders.csv", 
  "has_header": true,
  "sync_mode": "incremental",
  "primary_key": "order_id",
  "cursor_field": "created_at"
};

-- Incremental loading with ID cursor
SOURCE events TYPE CSV PARAMS {
  "path": "data/events.csv",
  "has_header": true, 
  "sync_mode": "incremental",
  "primary_key": "event_id",
  "cursor_field": "sequence_id"
};
```

**Migration from Other Tools:**
These parameters follow industry standards, making migration from Airbyte or Fivetran straightforward:

```sql
-- Airbyte-style configuration
SOURCE airbyte_users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "sync_mode": "incremental",     -- Same as Airbyte
  "cursor_field": "updated_at",   -- Same as Airbyte  
  "primary_key": "id"             -- Same as Airbyte
};
```

### LOAD

The `LOAD` directive loads data from a defined source into a table.

**Syntax:**
```sql
LOAD table_name FROM source_name;
```

**Extended Syntax with Parameters:**
```sql
LOAD table_name FROM SOURCE source_name
WITH (
  param1 = "value1",
  param2 = "value2"
);
```

### CREATE

The `CREATE` directive creates a new table using SQL.

**Syntax:**
```sql
CREATE TABLE table_name AS
SELECT
  column1,
  column2,
  expression AS column3
FROM source_table
WHERE condition;
```

**CREATE OR REPLACE Syntax:**
```sql
CREATE OR REPLACE TABLE table_name AS
SELECT
  column1,
  column2,
  expression AS column3
FROM source_table
WHERE condition;
```

**Example:**
```sql
-- Standard table creation
CREATE TABLE customer_summary AS
SELECT 
    customer_id,
    COUNT(order_id) as order_count,
    SUM(amount) as total_spent
FROM orders_table
GROUP BY customer_id;

-- Replace existing table
CREATE OR REPLACE TABLE daily_metrics AS
SELECT 
    DATE(order_date) as date,
    COUNT(*) as orders,
    SUM(amount) as revenue
FROM orders_table
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(order_date);
```

The `CREATE OR REPLACE` variant will:
- Drop the existing table if it exists
- Create a new table with the same name
- Useful for incremental pipelines that need to refresh derived tables

### EXPORT

The `EXPORT` directive exports data to an external destination.

**Syntax:**
```sql
EXPORT
  SELECT * FROM table_name
TO "destination_uri"
TYPE export_type
OPTIONS {
  "option1": "value1",
  "option2": "value2"
};
```

**Example:**
```sql
EXPORT 
  SELECT * FROM sales_summary
TO "target/sales_summary_${date}.csv"
TYPE CSV
OPTIONS { 
  "header": true
};
```

**Alternative Syntax:**
```sql
EXPORT TO destination_name
FROM table_name
WITH (
  param1 = "value1",
  param2 = "value2"
);
```

### INCLUDE

The `INCLUDE` directive includes external SQLFlow files or Python modules.

**Syntax:**
```sql
INCLUDE "path/to/file" AS alias;
```

**Example:**
```sql
INCLUDE "common/utils.sf" AS utils;
INCLUDE "examples/python_udfs/example_udf.py";
```

## Conditional Execution

SQLFlow supports conditional execution with `IF`, `ELSE IF` (or `ELSEIF`), `ELSE`, and `END IF` keywords.

**Syntax:**
```sql
IF condition THEN
  -- statements
ELSE IF another_condition THEN
  -- statements
ELSE
  -- statements
END IF;
```

**Example:**
```sql
IF ${env} == 'production' THEN
  LOAD users FROM SOURCE postgres_source
  WITH (
    query = "SELECT * FROM users WHERE status = 'active'"
  );
ELSE IF ${env} == 'staging' THEN
  LOAD users FROM SOURCE postgres_source
  WITH (
    query = "SELECT * FROM users WHERE status = 'active' LIMIT 1000"
  );
ELSE
  LOAD users FROM SOURCE s3_source
  WITH (
    path = "sample_data/users.csv",
    format = "csv",
    header = "true"
  );
END IF;
```

## SQL Support

SQLFlow supports standard SQL syntax within `CREATE TABLE` statements and `EXPORT` directives. This includes:

- SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY clauses
- JOINs (INNER, LEFT, RIGHT, FULL)
- Common functions (aggregations, string functions, date functions)
- Subqueries and Common Table Expressions (CTEs)

## Python UDFs

SQLFlow supports calling Python user-defined functions (UDFs) from SQL statements.

⚠️ **Important Limitation**: Table UDFs cannot be called directly in SQL FROM clauses due to DuckDB Python API limitations.

### **Scalar UDFs (Fully Supported)**
Scalar UDFs process data row-by-row and work seamlessly in SQL:

**Syntax:**
```sql
SELECT PYTHON_FUNC("module.function_name", arg1, arg2) FROM table;
```

**Example:**
```sql
-- Include Python module
INCLUDE "examples/python_udfs/text_utils.py";

-- Use scalar UDF in SQL
CREATE TABLE processed_customers AS
SELECT
  customer_id,
  PYTHON_FUNC("text_utils.capitalize_words", name) AS formatted_name,
  PYTHON_FUNC("text_utils.extract_domain", email) AS email_domain,
  PYTHON_FUNC("text_utils.count_words", notes) AS note_word_count
FROM customers_table;
```

### **Table UDFs (Programmatic Only)**
Table UDFs work when called directly from Python but not in SQL FROM clauses:

```python
# ✅ Works: Direct Python call
from python_udfs.data_transforms import add_sales_metrics
processed_data = add_sales_metrics(sales_df)

# ❌ Doesn't work: SQL FROM clause
# SELECT * FROM PYTHON_FUNC("module.table_function", sub_query);
```

### **Alternative Approaches for Complex Transformations**

For table-like transformations, use these recommended approaches:

**1. External Processing:**
```python
# Fetch data → Process with pandas → Register back
sales_df = engine.execute_query("SELECT * FROM sales").fetchdf()
processed_df = add_sales_metrics_external(sales_df)
engine.connection.register("processed_sales", processed_df)
```

**2. Scalar UDF Chains:**
```sql
-- Break complex operations into scalar UDF steps
CREATE TABLE sales_with_totals AS
SELECT *, 
  PYTHON_FUNC("table_udf_alternatives.calculate_sales_total", price, quantity) AS total
FROM raw_sales;

CREATE TABLE sales_with_tax AS
SELECT *, 
  PYTHON_FUNC("table_udf_alternatives.calculate_sales_tax", total) AS tax
FROM sales_with_totals;
```

## Examples

### Basic Pipeline

```sql
-- Define data sources
SOURCE sales TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

-- Load data into tables
LOAD sales_table FROM sales;

-- Transform data
CREATE TABLE daily_sales AS
SELECT
  order_date,
  COUNT(DISTINCT order_id) AS orders,
  SUM(total_amount) AS revenue
FROM sales_table
GROUP BY order_date
ORDER BY order_date;

-- Export results
EXPORT 
  SELECT * FROM daily_sales
TO "output/daily_sales_${date}.csv"
TYPE CSV
OPTIONS { 
  "header": true
};
```

### Conditional Pipeline

```sql
-- Use different data sources based on environment
IF ${env} == 'production' THEN
  -- Production settings
  SET max_records = 1000000;
ELSE
  -- Development settings
  SET max_records = 1000;
END IF;

-- Create filtered data
CREATE TABLE filtered_data AS
SELECT *
FROM source_data
LIMIT ${max_records};
```

### Pipeline with Python UDFs

```sql
-- Include Python module
INCLUDE "utils/data_processors.py";

-- Process data with UDFs
CREATE TABLE enriched_data AS
SELECT
  id,
  name,
  PYTHON_FUNC("data_processors.normalize_text", description) AS normalized_description,
  PYTHON_FUNC("data_processors.sentiment_score", description) AS sentiment
FROM input_data;
```
