# SQLFlow Tutorial: From Business Requirements to Working Pipeline

This tutorial demonstrates how to translate business requirements into a working data pipeline using SQLFlow. We'll walk through a complete example from initial business needs to a deployed, running pipeline.

## Business Scenario

Imagine you're a data analyst at an e-commerce company. The marketing team has requested a daily report that:

1. Combines yesterday's sales data (from CSV exports)
2. Enriches it with customer data (from PostgreSQL database)
3. Calculates key metrics like revenue by product category and region
4. Generates a summary report and uploads it to S3 for the BI dashboard
5. Sends notification data to a REST API endpoint

## Step 1: Setting Up Your SQLFlow Project

First, let's create a new SQLFlow project:

```bash
# Initialize a new project
sqlflow init ecommerce_analytics

# Navigate to the project directory
cd ecommerce_analytics
```

This creates a standard project structure with directories for pipelines, models, and connectors.

## Step 2: Understanding Available Data Sources

### Sales Data (CSV)
Daily sales data is exported as CSV files with the following structure:
```order_id,customer_id,product_id,quantity,price,order_date
1001,5678,PRD-123,2,49.99,2023-10-25
1002,8765,PRD-456,1,129.99,2023-10-25
...
```

### Customer Data (PostgreSQL)
Customer information is stored in a PostgreSQL database:
```sql
-- customers table
CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100),
  region VARCHAR(50),
  signup_date DATE
);
```

### Product Data (PostgreSQL)
Product information is also in PostgreSQL:
```sql
-- products table
CREATE TABLE products (
  product_id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(100),
  category VARCHAR(50),
  price DECIMAL(10,2)
);
```

## Step 3: Creating the Pipeline

Create a new file `pipelines/daily_sales_report.sf` with the following content:

```sql
-- Set up variables
SET run_date = "${run_date|2023-10-25}";

-- Define data sources
SOURCE sample TYPE CSV PARAMS {
  "path": "data/sales_${run_date}.csv",
  "has_header": true
};

-- Load data into tables
LOAD raw_data FROM sample;

-- Transform data
CREATE TABLE sales_enriched AS
SELECT
  s.order_id,
  s.customer_id,
  c.name AS customer_name,
  c.region,
  s.product_id,
  p.name AS product_name,
  p.category,
  s.quantity,
  s.price,
  (s.quantity * s.price) AS total_amount,
  s.order_date
FROM raw_data s
JOIN customers c ON s.customer_id = c.customer_id
JOIN products p ON s.product_id = p.product_id;

-- Create summary tables
CREATE TABLE category_summary AS
SELECT
  category,
  COUNT(DISTINCT order_id) AS num_orders,
  SUM(quantity) AS total_items,
  SUM(total_amount) AS total_revenue
FROM sales_enriched
GROUP BY category
ORDER BY total_revenue DESC;

CREATE TABLE region_summary AS
SELECT
  region,
  COUNT(DISTINCT order_id) AS num_orders,
  COUNT(DISTINCT customer_id) AS num_customers,
  SUM(total_amount) AS total_revenue
FROM sales_enriched
GROUP BY region
ORDER BY total_revenue DESC;

-- Export results to S3 for BI dashboard
EXPORT
  SELECT * FROM category_summary
TO "s3://analytics/reports/category_summary_${run_date}.parquet"
TYPE S3
OPTIONS { 
  "format": "parquet",
  "compression": "snappy"
};

EXPORT
  SELECT * FROM region_summary
TO "s3://analytics/reports/region_summary_${run_date}.parquet"
TYPE S3
OPTIONS { 
  "format": "parquet",
  "compression": "snappy"
};

-- Send notification to REST API
EXPORT
  SELECT 
    '${run_date}' AS report_date,
    (SELECT COUNT(*) FROM raw_data) AS total_orders,
    (SELECT SUM(total_amount) FROM sales_enriched) AS daily_revenue
TO "https://api.example.com/notifications"
TYPE REST
OPTIONS {
  "method": "POST",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "Bearer ${API_TOKEN}"
  }
};
```

Note how the pipeline uses parameterized variables:
- `${run_date}` - Date of the data to process, with default "2023-10-25"
- `${API_TOKEN}` - Authentication token for the REST API

These variables can be passed when executing the pipeline, allowing for easy scheduling and automation.

## Step 4: Validating the Pipeline

Before running the pipeline, let's compile it to validate the syntax and see the execution plan:

```bash
# Compile the pipeline to validate syntax and see execution plan
sqlflow pipeline compile daily_sales_report

# The execution plan is automatically saved to the project's target directory
# View the execution plan
cat target/compiled/daily_sales_report.json
```

The output should look similar to:

```
Compiled pipeline 'daily_sales_report'
Found 2 operations in the execution plan
  - source_sample
  - load_raw_data

Operation types:
  - source_definition: 1
  - load: 1

Execution plan written to /path/to/project/target/compiled/daily_sales_report.json
```

The execution plan shows the DAG (Directed Acyclic Graph) of operations that will be performed, including:
1. Loading data from CSV and PostgreSQL
2. Joining tables to create enriched sales data
3. Creating summary tables
4. Exporting results to S3 and REST API

## Step 5: Running the Pipeline

Now that we've validated the pipeline, let's run it with specific parameters:

```bash
# Run the pipeline with a specific date
sqlflow pipeline run daily_sales_report --vars '{"run_date": "2023-10-25", "DB_CONN": "postgresql://user:pass@localhost:5432/ecommerce", "API_TOKEN": "your-api-token"}'
```

This command:
1. Sets the `run_date` variable to "2023-10-25"
2. Provides database connection string and API token
3. Executes the pipeline end-to-end

## Step 6: Monitoring and Troubleshooting

If any issues occur during pipeline execution, SQLFlow provides detailed error messages and the ability to resume from failure points. You can check the logs and status of each operation.

## Step 7: Scheduling the Pipeline

For daily execution, you can set up a cron job or use an orchestration tool like Airflow:

```bash
# Example cron job (runs daily at 2 AM)
0 2 * * * cd /path/to/ecommerce_analytics && sqlflow pipeline run daily_sales_report --vars '{"run_date": "$(date -d "yesterday" +\%Y-\%m-\%d)"}'
```

## Conclusion

In this tutorial, we've demonstrated how to:
1. Translate business requirements into a SQLFlow pipeline
2. Use multiple data sources (CSV and PostgreSQL)
3. Perform transformations using SQL
4. Export results to multiple destinations (S3 and REST API)
5. Parameterize the pipeline for flexible execution
6. Validate and run the pipeline

SQLFlow's SQL-centric approach makes it easy to go from business requirements to a working data pipeline without writing complex code or managing multiple tools.

## Next Steps

- Explore more advanced features like Python UDFs for complex transformations
- Add tests to ensure data quality
- Create visualizations of your pipeline using `sqlflow viz`
- Extend the pipeline with custom connectors for additional data sources or destinations
