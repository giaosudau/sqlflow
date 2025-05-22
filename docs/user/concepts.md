# SQLFlow Core Concepts

> **MVP Status**: This guide is part of SQLFlow's MVP documentation. The feature is fully implemented, but this guide may be expanded with more examples in future updates.

This document introduces the core concepts and terminology used in SQLFlow, providing a foundation for understanding how SQLFlow works.

## The SQLFlow Approach

SQLFlow is built on a simple idea: data engineers and analysts already know SQL, so why not use SQL for the entire data pipeline? SQLFlow extends SQL with a few key directives that enable complete data workflows, from ingestion to transformation to export.

## Key Components

### Pipeline File (.sf)

A SQLFlow pipeline is defined in a `.sf` file, which contains a series of SQL statements and SQLFlow directives. This file defines:
- Data sources to read from
- Tables to create
- Transformations to apply
- Where to export results

Example:
```sql
-- This is a simple SQLFlow pipeline file (.sf)
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_data FROM users;

CREATE TABLE user_stats AS
SELECT
  country,
  COUNT(*) AS user_count
FROM users_data
GROUP BY country;

EXPORT
  SELECT * FROM user_stats
TO "output/user_stats.csv"
TYPE CSV
OPTIONS { "header": true };
```

### Directives

Directives are special commands in SQLFlow that extend standard SQL:

| Directive | Purpose |
|-----------|---------|
| `SOURCE`  | Define a data source that can be loaded |
| `LOAD`    | Load data from a source into a table |
| `CREATE TABLE` | Transform data using SQL |
| `EXPORT`  | Send results to an external destination |
| `SET`     | Define variables for use in the pipeline |
| `INCLUDE` | Include external files or Python modules |
| `IF/ELSE` | Conditional execution of pipeline sections |

### Execution Model

SQLFlow follows a directed acyclic graph (DAG) execution model:

1. **Parsing**: SQLFlow parses the `.sf` file
2. **Planning**: Dependencies between operations are identified
3. **Execution**: Operations are executed in the correct order
4. **Monitoring**: Progress and errors are tracked

### Engines

SQLFlow uses DuckDB as its primary execution engine:

- **In-memory Mode**: Fast for development but data is lost after execution
- **Persistent Mode**: Data is saved to disk, ideal for production

### Profiles

Profiles are configuration files that define how SQLFlow should run in different environments:

- **Development**: Fast, in-memory processing
- **Production**: Persistent storage, more resources
- **Custom**: For specific use cases or environments

### Connectors

Connectors allow SQLFlow to read from and write to various data sources:

- **File Connectors**: CSV, Parquet, JSON
- **Database Connectors**: PostgreSQL, MySQL
- **Cloud Connectors**: S3, Google Sheets
- **API Connectors**: REST APIs

### Python UDFs

Python User-Defined Functions extend SQLFlow's capabilities:

- **Scalar UDFs**: Process one row at a time
- **Table UDFs**: Process entire tables at once
- **Custom Logic**: When SQL isn't enough

## Data Flow

A typical data flow in SQLFlow follows these steps:

1. **Define Sources**: Specify where data comes from
2. **Load Data**: Bring data into the SQLFlow workspace
3. **Transform**: Apply SQL transformations
4. **Export**: Send results to their destination

## Dependency Management

SQLFlow automatically manages dependencies between operations:

- Tables must be created before they can be queried
- Sources must be defined before they can be loaded
- Exports depend on the tables they reference

This dependency tracking allows SQLFlow to:
- Execute operations in the correct order
- Optimize the execution plan
- Visualize the pipeline as a DAG

## Variables and Parameterization

Variables make pipelines dynamic and reusable:

- Define with `SET` directive
- Reference with `${variable_name}` syntax
- Pass via CLI with `--vars` option
- Set defaults with `${variable_name|default_value}`

## Related Concepts

### Project Structure

A standard SQLFlow project includes:
- `pipelines/`: Pipeline definition files
- `profiles/`: Environment configurations
- `python_udfs/`: Python User-Defined Functions
- `data/`: Input data files
- `output/`: Results and exports

### Compilation vs. Execution

SQLFlow separates compilation from execution:
- **Compilation**: `sqlflow pipeline compile` checks for errors
- **Execution**: `sqlflow pipeline run` executes the pipeline

## Related Resources

- [Getting Started Guide](getting_started.md)
- [Syntax Reference](reference/syntax.md)
- [Working with Profiles](guides/profiles.md)
- [Working with Variables](guides/variables.md) 