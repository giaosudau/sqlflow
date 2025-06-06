# Working with Profiles in SQLFlow

> **MVP Status**: This guide is part of SQLFlow's MVP documentation. The feature is fully implemented, but this guide may be expanded with more examples in future updates.

Profiles in SQLFlow allow you to define different configurations for different environments, such as development, testing, and production. This guide explains how to create, configure, and use profiles in your SQLFlow projects.

## What are Profiles?

Profiles are YAML configuration files that define:
- DuckDB engine settings (memory/persistent)
- Connector configurations
- Environment variables
- Logging settings

By using profiles, you can:
- Switch between environments with a single flag
- Avoid hardcoding configuration in pipelines
- Ensure consistent settings across pipeline runs
- Customize resources for different environments

## Profile Format Specification

SQLFlow profiles must follow a specific YAML structure. The basic format is:

```yaml
# Required: Engine configuration
engines:
  duckdb:
    mode: memory|persistent  # Required: DuckDB mode
    path: "path/to/db.duckdb"  # Required for persistent mode
    memory_limit: 2GB  # Optional: Memory limit (default: 2GB)

# Optional: Logging configuration
log_level: info  # Optional: debug, info, warning, error
log_file: "logs/sqlflow.log"  # Optional: Log to file

# Optional: Module-specific log levels
module_log_levels:
  sqlflow.core.engines: info
  sqlflow.connectors: debug

# Optional: Variables available to pipelines
variables:
  environment: development
  data_dir: "data"
  output_dir: "output"

# Optional: Connector configurations
connectors:
  postgres:
    type: POSTGRES
    params:
      host: "localhost"
      port: 5432
      dbname: "mydb"
      user: "username"
      password: "password"
```

### Required Fields

| Field | Description | Values |
|-------|-------------|---------|
| `engines.duckdb.mode` | DuckDB operation mode | `memory` or `persistent` |
| `engines.duckdb.path` | Database file path | Required when `mode: persistent` |

### Optional Fields

| Field | Description | Default |
|-------|-------------|---------|
| `engines.duckdb.memory_limit` | Memory limit for DuckDB | `2GB` |
| `log_level` | Global log level | `info` |
| `log_file` | Log output file | Console only |
| `variables` | Pipeline variables | `{}` |
| `connectors` | Connector configurations | `{}` |

## Profile Validation

SQLFlow automatically validates profile structure when loading. Common validation errors and fixes:

### ❌ Nested Profile Format (Deprecated)
```yaml
# INCORRECT - Don't use profile name as wrapper
dev:
  engines:
    duckdb:
      mode: memory
```

### ✅ Correct Profile Format
```yaml
# CORRECT - Direct structure
engines:
  duckdb:
    mode: memory
```

### ❌ Missing Path for Persistent Mode
```yaml
# INCORRECT - Missing path
engines:
  duckdb:
    mode: persistent
    # Missing path field!
```

### ✅ Persistent Mode with Path
```yaml
# CORRECT - Path specified
engines:
  duckdb:
    mode: persistent
    path: "target/production.db"
```

### ❌ Deprecated Field Names
```yaml
# INCORRECT - Old field name
engines:
  duckdb:
    mode: persistent
    database_path: "target/db.duckdb"  # Should be 'path'
```

### ✅ Current Field Names
```yaml
# CORRECT - Current field name
engines:
  duckdb:
    mode: persistent
    path: "target/db.duckdb"
```

## Default Profiles

A new SQLFlow project comes with a `profiles/dev.yml` file that configures the development environment:

```yaml
# profiles/dev.yml
engines:
  duckdb:
    mode: memory        # DuckDB runs in-memory (no persistence)
    memory_limit: 2GB   # Memory limit for DuckDB
log_level: info
```

## Creating a Production Profile

Create a production profile for persistent storage:

```yaml
# profiles/production.yml
engines:
  duckdb:
    mode: persistent
    path: target/production.db  # Where to store the DuckDB database
    memory_limit: 4GB
log_level: warning
```

## Profile Structure

A profile can include these sections:

```yaml
# General configuration
log_level: info
log_file: "logs/sqlflow.log"  # Optional file logging

# Engine configuration
engines:
  duckdb:
    mode: memory|persistent
    path: "path/to/db_file.db"  # Required if mode is persistent
    memory_limit: 4GB
    
# Default variables
variables:
  environment: "production"
  s3_bucket: "my-company-data"
  
# Connector configurations
connectors:
  postgres:
    type: POSTGRES
    params:
      host: "localhost"
      port: 5432
      dbname: "warehouse"
      user: "sqlflow"
  
  s3:
    type: S3
    params:
      region: "us-east-1"
      access_key: "${AWS_ACCESS_KEY}"
      secret_key: "${AWS_SECRET_KEY}"
      bucket: "data-lake"
```

## DuckDB Persistence Modes

### Memory Mode (Default)
- **Use case**: Development and testing
- **Performance**: Fast execution
- **Persistence**: Data lost when SQLFlow exits
- **Configuration**:
  ```yaml
  engines:
    duckdb:
      mode: memory
      memory_limit: 2GB
  ```

### Persistent Mode
- **Use case**: Production and data persistence
- **Performance**: Slower but reliable
- **Persistence**: Data saved to disk file
- **Configuration**:
  ```yaml
  engines:
    duckdb:
      mode: persistent
      path: target/production.db  # Required!
      memory_limit: 4GB
  ```

## Using Profiles

### Running with a Profile
```bash
# Use default 'dev' profile
sqlflow pipeline run my_pipeline

# Use specific profile
sqlflow pipeline run my_pipeline --profile production

# List pipelines with specific profile
sqlflow pipeline list --profile test
```

### Profile Discovery
SQLFlow looks for profiles in this order:
1. `profiles/<profile_name>.yml` in current directory
2. `profiles/<profile_name>.yml` in parent directories (up to project root)

## Environment Variables in Profiles

Profiles support environment variable substitution:

```yaml
engines:
  duckdb:
    mode: persistent
    path: "${DB_PATH:-target/default.db}"  # With default fallback

variables:
  api_key: "${API_KEY}"  # Required environment variable
  database_url: "${DATABASE_URL}"

connectors:
  postgres:
    type: POSTGRES
    params:
      host: "${POSTGRES_HOST}"
      password: "${POSTGRES_PASSWORD}"
```

## Best Practices

### Profile Organization
- Use descriptive profile names: `dev`, `test`, `staging`, `production`
- Keep sensitive data in environment variables
- Document required environment variables

### Development Workflow
```yaml
# profiles/dev.yml - Fast development
engines:
  duckdb:
    mode: memory
    memory_limit: 2GB
log_level: debug
```

### Production Deployment
```yaml
# profiles/production.yml - Reliable production
engines:
  duckdb:
    mode: persistent
    path: "/data/production.db"
    memory_limit: 8GB
log_level: warning
log_file: "/logs/sqlflow.log"
```

### Testing Configuration
```yaml
# profiles/test.yml - Isolated testing
engines:
  duckdb:
    mode: persistent  # Keep test data for debugging
    path: "target/test.db"
    memory_limit: 1GB
log_level: debug
```

## Troubleshooting Profiles

### Common Errors

**Error**: `Invalid profile structure`
```
Solution: Check profile format against specification above
```

**Error**: `DuckDB persistent mode requires 'path' field`
```yaml
# Fix: Add path field
engines:
  duckdb:
    mode: persistent
    path: "target/mydb.duckdb"  # Add this line
```

**Error**: `Profile not found`
```
Solution: Ensure profiles/<name>.yml exists in project directory
```

### Validation Messages

SQLFlow provides helpful validation messages:

```
Profile 'dev' uses deprecated nested format. 
Please update profiles/dev.yml to remove the 'dev:' wrapper.
```

```
Profile 'prod' uses deprecated 'database_path' field. 
Please rename it to 'path' in profiles/prod.yml
```

### Migration from Old Format

If you have profiles in the old format, update them:

```yaml
# OLD FORMAT (deprecated)
my_profile:
  engines:
    duckdb:
      database_path: "target/db.duckdb"
```

```yaml
# NEW FORMAT (current)
engines:
  duckdb:
    mode: persistent
    path: "target/db.duckdb"
```

## Advanced Configuration

### Multiple Connectors
```yaml
connectors:
  warehouse:
    type: POSTGRES
    params:
      host: "warehouse.company.com"
      dbname: "analytics"
  
  data_lake:
    type: S3
    params:
      bucket: "company-data-lake"
      region: "us-west-2"
```

### Environment-Specific Variables
```yaml
variables:
  # Development
  debug_mode: true
  sample_data: true
  
  # Data sources
  customers_table: "dev_customers"
  orders_table: "dev_orders"
```

### Performance Tuning
```yaml
engines:
  duckdb:
    mode: persistent
    path: "/fast-ssd/production.db"
    memory_limit: 16GB  # Increase for large datasets
```

For more advanced configuration options, see the [SQLFlow Configuration Reference](../reference/configuration.md). 