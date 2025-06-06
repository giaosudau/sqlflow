# SQLFlow Glossary

> Quick reference for technical terms used in SQLFlow documentation. Terms are organized by concept area with examples for clarity.

## Core Concepts

### Pipeline Terms

**Pipeline**
: A sequence of data operations defined in a `.sf` file that loads, transforms, and exports data.
: *Example*: `analytics.sf` containing SOURCE, LOAD, and EXPORT statements.

**SOURCE**
: A declaration of where data comes from (files, databases, APIs).
: *Example*: `SOURCE customers TYPE CSV PARAMS { "path": "data/customers.csv" };`

**LOAD Mode**
: How data is added to tables (REPLACE, APPEND, or UPSERT).
: *Example*: `LOAD users FROM source MODE APPEND;`

**Workspace**
: The root directory of your SQLFlow project containing pipelines, data, and configuration.
: *Example*: A directory with `pipelines/`, `data/`, and `profiles/` folders.

## Data Operations

### Load Modes

**REPLACE Mode**
: Completely overwrites an existing table with new data.
: *Use Case*: Daily snapshots, small tables that need full refresh.

**APPEND Mode**
: Adds new records to an existing table without modifying existing data.
: *Use Case*: Log data, time-series data, historical records.

**UPSERT Mode**
: Updates existing records and inserts new ones based on key columns.
: *Use Case*: Customer profiles, product catalogs, slowly changing dimensions.

### Data Transformation

**CREATE TABLE AS**
: SQL operation that creates a new table from a query result.
: *Example*: `CREATE TABLE summary AS SELECT * FROM data;`

**Incremental Processing**
: Processing only new or changed data since the last run.
: *Example*: Loading only today's orders instead of all historical orders.

## Configuration

### Variables

**Environment Variables**
: Values stored in your system or `.env` file for sensitive data.
: *Example*: Database passwords, API keys.

**Profile Variables**
: Settings defined in `profiles/*.yml` files for different environments.
: *Example*: Development vs. production database connections.

**Pipeline Variables**
: Values set with `SET` statements or passed via command line.
: *Example*: `SET processing_date = "2025-06-05";`

### File Types

**.sf File**
: SQLFlow pipeline definition file containing data operations.
: *Example*: `analytics.sf`, `daily_processing.sf`

**.yml File**
: YAML configuration file for profiles and settings.
: *Example*: `profiles/production.yml`

**.env File**
: Environment file for sensitive configuration values.
: *Example*: `.env` containing database passwords.

## Execution

**Execution Plan**
: The optimized sequence of operations SQLFlow will perform.
: *Example*: Loading customer data before joining with orders.

**Pipeline Run**
: A single execution of a pipeline from start to finish.
: *Example*: `sqlflow pipeline run analytics`

## Common Patterns

**Data Lake**
: Large storage area for raw data files.
: *Example*: S3 bucket containing CSV and Parquet files.

**Data Warehouse**
: Database optimized for analytics and reporting.
: *Example*: PostgreSQL database with customer analytics tables.

**ETL/ELT**
: Extract, Transform, Load / Extract, Load, Transform.
: *Example*: Loading CSV files, transforming data, saving to database.

## Performance Terms

**Batch Processing**
: Processing data in groups rather than one at a time.
: *Example*: Loading 1000 records at once instead of one by one.

**Incremental Updates**
: Processing only new or changed data since last run.
: *Example*: Only loading today's orders into analytics.

**Optimization**
: Improving pipeline performance and resource usage.
: *Example*: Using indexes for faster UPSERT operations.
