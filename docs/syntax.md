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

## Directives

### SET

The `SET` directive assigns a value to a variable.

**Syntax:**
```sql
SET variable_name = "value";
```

### SOURCE

The `SOURCE` directive defines a data source that can be loaded into the pipeline.

**Syntax:**
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

The SOURCE directive also supports an alternative syntax:
```sql
SOURCE source_name (
  connector_type = "CONNECTOR_TYPE",
  param1 = "value1",
  param2 = "value2"
);
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

**Syntax:**
```sql
-- Scalar UDF (returns a single value)
SELECT PYTHON_FUNC("module.function_name", arg1, arg2) FROM table;

-- Table UDF (returns a result set)
SELECT * FROM PYTHON_FUNC("module.function_name", arg1, arg2);
CREATE TABLE result_table AS 
SELECT * FROM PYTHON_FUNC("module.function_name", sub_query);
```

**Example:**
```sql
-- Include Python module
INCLUDE "examples/python_udfs/example_udf.py";

-- Use scalar UDF
CREATE TABLE discounted_products AS
SELECT
  product_id,
  name,
  price,
  PYTHON_FUNC("example_udf.calculate_discount", price) AS discount_amount
FROM products_table;

-- Use table UDF with a subquery
CREATE TABLE sales_summary AS
SELECT * FROM PYTHON_FUNC("example_udf.sales_summary", 
  SELECT product_id, category, price, quantity FROM products_table
);
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
