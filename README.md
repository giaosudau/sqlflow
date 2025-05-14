# SQLFlow

SQLFlow is a SQL-based data pipeline tool that enables users to define, execute, visualize, and manage data transformations using a SQL-based domain-specific language (DSL).

## Features

- Define data sources and transformations using a SQL-based syntax
- Create modular, reusable pipeline components
- Execute pipelines locally or in a distributed environment
- Visualize pipelines as interactive DAGs (Directed Acyclic Graphs)
- Manage project structure with a standardized approach
- Connect to various data sources through an extensible connector framework

## Installation

```bash
pip install sqlflow
```

## Quick Start

```bash
# Initialize a new project
sqlflow init my_project

# Create a pipeline
cd my_project
# Edit pipelines/my_pipeline.sf

# Run the pipeline
sqlflow pipeline run my_pipeline

# Compile the pipeline
sqlflow pipeline compile my_pipeline

# List available pipelines
sqlflow pipeline list
```

## SQLFlow SQL Syntax Reference

SQLFlow uses a SQL-based DSL for defining data pipelines. Here's a quick reference:

### Defining Data Sources

```sql
-- Define a CSV data source
SOURCE sales TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

-- Define a PostgreSQL data source
SOURCE customers TYPE POSTGRES PARAMS {
  "connection_string": "${DB_CONN}",
  "query": "SELECT * FROM customers"
};
```

### Loading Data

```sql
-- Load data from a source into a table
LOAD target_table FROM source_name;
```

### Transformations

```sql
-- Create a new table from a transformation
CREATE TABLE enriched_data AS
SELECT 
  s.order_id,
  s.customer_id,
  c.name AS customer_name,
  s.product_id,
  p.name AS product_name,
  s.quantity,
  s.price,
  (s.quantity * s.price) AS total_amount
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
JOIN products p ON s.product_id = p.product_id;
```

### Exporting Data

```sql
-- Export data to a file
EXPORT
  SELECT * FROM summary_table
TO "s3://bucket/path/file_${date}.parquet"
TYPE S3
OPTIONS { 
  "format": "parquet",
  "compression": "snappy"
};

-- Export data to a REST API
EXPORT
  SELECT * FROM notifications
TO "https://api.example.com/endpoint"
TYPE REST
OPTIONS {
  "method": "POST",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "Bearer ${API_TOKEN}"
  }
};
```

### Using Variables

Variables can be injected at runtime:

```bash
sqlflow pipeline run my_pipeline --vars '{"date": "2023-10-25", "API_TOKEN": "secret-token"}'
```

Or defined in your project's `sqlflow.yml` configuration file.

## Documentation

For more information, see the [documentation](https://sqlflow.readthedocs.io).

## License

MIT

## Profile Configuration Example

To configure the DuckDB engine, set the path in your profile YAML (e.g., `profiles/default.yml`) under the `engines` key:

```yaml
engines:
  duckdb:
    type: duckdb
    path: target/default.db
```

- `type`: The engine type (currently only 'duckdb' is supported).
- `path`: Path to the DuckDB database file. Use `:memory:` for an in-memory database.

The executor will use this path for all transformations and table storage. If not set, it defaults to `:memory:`.
