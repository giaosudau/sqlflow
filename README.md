# SQLFlow

SQLFlow is a SQL-based data pipeline tool that enables users to define, execute, visualize, and manage data transformations using a SQL-based domain-specific language (DSL).

## Features

- Define data sources and transformations using a SQL-based syntax
- Create modular, reusable pipeline components
- Execute pipelines locally or in a distributed environment
- Visualize pipelines as interactive DAGs (Directed Acyclic Graphs)
- Manage project structure with a standardized approach
- Connect to various data sources through an extensible connector framework
- **Profile-driven configuration** for managing environments (dev, prod, etc.)
- **Configurable DuckDB engine** for in-memory (fast, ephemeral) or persistent (saves to disk) operation.

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

# Run the pipeline (uses dev profile by default, typically in-memory DuckDB)
sqlflow pipeline run my_pipeline

# Run with a different profile (e.g., production with persistent DuckDB)
sqlflow pipeline run my_pipeline --profile production

# Compile the pipeline
sqlflow pipeline compile my_pipeline

# List available pipelines
sqlflow pipeline list
```

## Profile-Driven Configuration

SQLFlow uses **profiles** to manage all environment and engine settings. Profiles are YAML files located in the `profiles/` directory of your project (e.g., `my_project/profiles/`). When you initialize a project, a `profiles/dev.yml` is created by default.

You can define different profiles for various environments such as development (`dev`), testing (`staging`), and production (`production`). Each profile can specify distinct engine configurations, variables, connector parameters, and other settings.

### Example: `profiles/dev.yml`
This profile is typically used for local development and testing.
```yaml
engines:
  duckdb:
    mode: memory  # Fast, in-memory mode for local/dev (no data persisted)
    memory_limit: 2GB
# Add variables, paths, or connector settings specific to development
```

### Example: `profiles/production.yml`
This profile would be used for production runs, often with persistent storage.
```yaml
engines:
  duckdb:
    mode: persistent  # Data is saved to disk
    path: target/prod.db # Path where the DuckDB database file will be stored. SQLFlow uses this exact path.
    memory_limit: 8GB
# Add production variables, paths, or connector settings
```

- To use a specific profile during a pipeline run, use the `--profile` option:
  `sqlflow pipeline run my_pipeline --profile production`
- If no profile is specified, SQLFlow defaults to using the `dev` profile if `profiles/dev.yml` exists.

## DuckDB Modes: Memory vs Persistent

SQLFlow's integrated DuckDB engine can operate in two distinct modes, configured within your active profile:

-   **Memory mode (`mode: memory`)**:
    *   This is the default for the `dev` profile.
    *   DuckDB runs entirely in-memory. Operations are very fast.
    *   No data is written to disk; all tables (including transforms) are ephemeral and lost when the SQLFlow process exits.
    *   Ideal for local development, testing, and scenarios where persistence is not required.
    *   Example in profile:
        ```yaml
        engines:
          duckdb:
            mode: memory
            memory_limit: 2GB
        ```

-   **Persistent mode (`mode: persistent`)**:
    *   Recommended for production environments or when data needs to be preserved.
    *   DuckDB writes data to a specified database file on disk. SQLFlow will use the exact `path` you provide.
    *   All tables created during the pipeline, including intermediate transform tables, are persisted in this file.
    *   You must specify a `path` for the database file.
    *   Example in profile:
        ```yaml
        engines:
          duckdb:
            mode: persistent
            path: target/my_production_data.db # All tables will be saved here using this exact path.
            memory_limit: 8GB
        ```

Switching between modes is as simple as changing the `mode` (and `path` for persistent) in your profile YAML and running your pipeline with that profile.

Also, note that for each pipeline run, the specific run artifact directory `target/run/<pipeline_name>/` is cleared and recreated to ensure a clean state for execution logs and artifacts for that particular run.

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

Or defined in your profile YAML under a `variables:` key. These profile variables can be overridden by `--vars`.

## Documentation

For more information, see the [documentation](https://sqlflow.readthedocs.io).

## License

MIT

## FAQ

**Q: Where do I configure DuckDB and other engine settings?**
A: All engine settings, including DuckDB's mode (memory or persistent) and path (for persistent mode), are configured in your profile YAML files (e.g., `profiles/dev.yml`, `profiles/production.yml`) under the `engines` key. SQLFlow uses the exact `path` specified for persistent DuckDB files.

**Q: How do I switch between memory and persistent DuckDB modes?**
A: Edit the active profile's YAML file. For DuckDB:
   - For in-memory mode (common for dev): Set `engines.duckdb.mode: memory`.
   - For persistent mode (common for prod): Set `engines.duckdb.mode: persistent` and provide a `engines.duckdb.path: path/to/your/db_file.db`. SQLFlow will use this exact path.
   Then, run your pipeline using that profile (e.g., `sqlflow pipeline run my_pipeline --profile your_profile_name`).

**Q: How do I add new environments (e.g., staging)?**
A: Create a new YAML file in your `profiles/` directory (e.g., `profiles/staging.yml`). Configure the desired engine settings, variables, and connectors within this file. Then, run your pipeline with `sqlflow pipeline run my_pipeline --profile staging`.

**Q: Are transform tables saved when using DuckDB in persistent mode?**
A: Yes. When DuckDB is configured in `persistent` mode in your active profile, all tables created during the pipeline execution, including intermediate transformation tables (e.g., from `CREATE TABLE ... AS SELECT ...` statements), are saved within the specified DuckDB database file.
