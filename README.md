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
# Initialize a new project (creates profiles/dev.yml by default)
sqlflow init my_project

# Create a pipeline
cd my_project
# Edit pipelines/my_pipeline.sf

# Run the pipeline (uses dev profile by default)
sqlflow pipeline run my_pipeline

# Run with a different profile (e.g., production)
sqlflow pipeline run my_pipeline --profile production

# Compile the pipeline
sqlflow pipeline compile my_pipeline

# List available pipelines
sqlflow pipeline list
```

## Profile-Driven Configuration

SQLFlow uses **profiles** to manage all environment and engine settings. Profiles are YAML files in the `profiles/` directory. The default profile is `dev`, used for local development and testing.

### Example: `profiles/dev.yml`
```yaml
engines:
  duckdb:
    mode: memory  # Fast, in-memory mode for local/dev (no data persisted)
    memory_limit: 2GB
# Add variables, paths, or connectors as needed
```

### Example: `profiles/production.yml`
```yaml
engines:
  duckdb:
    mode: persistent  # Data is saved to disk
    path: target/prod.db
    memory_limit: 8GB
# Add production variables, paths, or connectors as needed
```

- To use a profile, run: `sqlflow pipeline run my_pipeline --profile production`
- If no profile is specified, `dev` is used by default.

## DuckDB Modes: Memory vs Persistent

- **Memory mode**: Fast, ephemeral, no files written. Great for local dev/testing. Data is lost after the process exits.
- **Persistent mode**: Data is saved to disk at the path you specify. Use for production or when you need to keep results.
- Switch modes by editing your profile YAML (`mode: memory` or `mode: persistent` and set `path`).

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

Or defined in your profile YAML under `variables:`.

## Documentation

For more information, see the [documentation](https://sqlflow.readthedocs.io).

## License

MIT

## FAQ

**Q: Where do I configure DuckDB and other engine settings?**
A: In your profile YAML files (e.g., `profiles/dev.yml`, `profiles/production.yml`).

**Q: How do I switch between memory and persistent DuckDB modes?**
A: Set `mode: memory` for in-memory (dev) or `mode: persistent` and a `path` for persistent (prod) in your profile YAML.

**Q: How do I add new environments?**
A: Just add a new profile YAML (e.g., `profiles/staging.yml`) and run with `--profile staging`.
