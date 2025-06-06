# SQLFlow Fundamentals

Learn the basics of SQLFlow - a tool that turns SQL into data pipelines.

## What is SQLFlow?

SQLFlow lets you write SQL to process data files and databases. It automatically figures out the order to run your SQL statements and handles the connections for you.

### Why Use SQLFlow?
- **Familiar SQL**: If you know SQL, you already know most of SQLFlow
- **Automatic ordering**: Write SQL statements in any order, SQLFlow runs them correctly
- **Simple setup**: One command creates a working project
- **Fast results**: Process thousands of rows in seconds

## Quick Start

Create a new project:
```bash
sqlflow init my-project
cd my-project
```

Run the example pipeline:
```bash
sqlflow pipeline run customer_analytics
```

This processes sample customer data and creates summary reports in seconds.

## Basic Example

Here's a complete pipeline that analyzes customer data:

```sql
-- Load customer data from CSV
CREATE TABLE customers AS
SELECT * FROM read_csv_auto('data/customers.csv');

-- Summarize by country and tier
CREATE TABLE customer_summary AS
SELECT 
    country,
    tier,
    COUNT(*) as customer_count,
    AVG(age) as avg_age
FROM customers
GROUP BY country, tier
ORDER BY customer_count DESC;

-- Save results to CSV
EXPORT SELECT * FROM customer_summary
TO "output/customer_summary.csv"
TYPE CSV OPTIONS { "header": true };
```

That's it! SQLFlow handles the rest.

## Core Concepts

### Pipelines
A pipeline is a file with `.sf` extension containing SQL statements. SQLFlow runs the statements in dependency order.

### Tables
Use `CREATE TABLE` to transform data:
```sql
CREATE TABLE summary AS
SELECT category, COUNT(*) FROM products GROUP BY category;
```

### Data Sources
Load data from files or databases:
```sql
-- From CSV files
CREATE TABLE sales AS SELECT * FROM read_csv_auto('data/sales.csv');

-- From databases (requires SOURCE definition)
SOURCE customers TYPE POSTGRES PARAMS {
    "host": "localhost",
    "dbname": "shop", 
    "table": "customers"
};
LOAD customer_data FROM customers;
```

### Exports
Save results to files:
```sql
EXPORT SELECT * FROM summary
TO "output/results.csv"
TYPE CSV OPTIONS { "header": true };
```

## Project Structure

When you run `sqlflow init`, you get:
- `pipelines/` - Your `.sf` pipeline files
- `data/` - Input CSV files  
- `output/` - Results go here
- `profiles/` - Database connection settings

## Variables

Make pipelines flexible with variables:

```sql
SET output_dir = "reports";
SET date = "2023-06-01";

CREATE TABLE daily_sales AS
SELECT * FROM sales WHERE order_date = '${date}';

EXPORT SELECT * FROM daily_sales
TO "${output_dir}/sales_${date}.csv"
TYPE CSV OPTIONS { "header": true };
```

Run with different values:
```bash
sqlflow pipeline run analytics --var date=2023-07-01
```

## Common Issues

### "Table not found" error
**Problem**: Pipeline can't find a table
**Solution**: Make sure you create tables before using them

```sql
-- Wrong order
CREATE TABLE summary AS SELECT * FROM customers;  -- Error: customers doesn't exist yet
CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');

-- Correct order  
CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');
CREATE TABLE summary AS SELECT * FROM customers;
```

### "File not found" error
**Problem**: Can't find data file
**Solution**: Check the file path

```sql
-- Make sure the file exists
CREATE TABLE data AS SELECT * FROM read_csv_auto('data/myfile.csv');
```

### Variables not working
**Problem**: `${variable}` appears literally instead of the value
**Solution**: Define the variable first

```sql
-- Define before using
SET table_name = "customers";
CREATE TABLE ${table_name}_summary AS SELECT * FROM ${table_name};
```

## Next Steps

- [Core Concepts](core-concepts.md) - Detailed guide to SOURCE, LOAD, CREATE TABLE, and EXPORT
- [Variables](variables.md) - Advanced variable usage and conditional logic
- [Pipeline Development](../pipelines/README.md) - Building complex pipelines

## Getting Help

1. **Try the examples**: `sqlflow init` creates working examples you can modify
2. **Check the error message**: SQLFlow error messages usually tell you exactly what's wrong
3. **Start simple**: Begin with one table and one export, then add complexity 