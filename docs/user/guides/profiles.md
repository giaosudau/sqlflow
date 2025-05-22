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
    default:
      host: "db.example.com"
      port: 5432
      user: "sqlflow"
      password: "${DB_PASSWORD}"  # Can reference env variables
  s3:
    default:
      region: "us-east-1"
      access_key: "${AWS_ACCESS_KEY}"
      secret_key: "${AWS_SECRET_KEY}"
```

## Using Profiles with CLI

Specify which profile to use when running a pipeline:

```bash
# Development (in-memory, fast)
sqlflow pipeline run my_pipeline

# Production (persistent storage)
sqlflow pipeline run my_pipeline --profile production
```

If no profile is specified, SQLFlow uses the `dev` profile by default.

## Environment Variables in Profiles

You can reference environment variables in your profiles using the `${ENV_VAR}` syntax:

```yaml
# profiles/production.yml
connectors:
  postgres:
    default:
      host: "${DB_HOST|localhost}"
      user: "${DB_USER|postgres}"
      password: "${DB_PASSWORD}"
```

The environment variables will be resolved at runtime.

## Multiple Named Connections

Profiles can define multiple named connections for the same connector type:

```yaml
# profiles/production.yml
connectors:
  postgres:
    analytics_db:
      host: "analytics.example.com"
      user: "analyst"
      password: "${ANALYTICS_DB_PASSWORD}"
    customer_db:
      host: "customers.example.com"
      user: "service"
      password: "${CUSTOMER_DB_PASSWORD}"
```

Reference these in your pipelines:

```sql
SOURCE customers TYPE POSTGRES(customer_db) PARAMS {
  "query": "SELECT * FROM customers"
};
```

## Best Practices for Profiles

1. **Keep sensitive data out of profiles**: Use environment variables for credentials
2. **Create profiles for each environment**: At minimum, `dev`, `test`, and `production`
3. **Organize profiles by purpose**: Consider creating task-specific profiles like `etl`, `reporting`
4. **Set appropriate resources**: Adjust memory limits based on environment capabilities
5. **Version control profiles**: Include profiles in version control, but exclude any with secrets

## Example: Complete Production Profile

```yaml
# profiles/production.yml

# Engine configuration
engines:
  duckdb:
    mode: persistent
    path: "/data/sqlflow/production.db"
    memory_limit: 8GB
    
# Logging
log_level: info
log_file: "/var/log/sqlflow/production.log"
    
# Default variables
variables:
  environment: "production"
  data_date: "${DATA_DATE}"
  s3_bucket: "company-production-data"
  
# Connector configurations
connectors:
  postgres:
    default:
      host: "${DB_HOST}"
      port: 5432
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      database: "production"
      sslmode: "require"
  s3:
    default:
      region: "us-east-1"
      access_key: "${AWS_ACCESS_KEY}"
      secret_key: "${AWS_SECRET_KEY}"
  rest_api:
    notification_service:
      base_url: "https://api.example.com/v1"
      auth_token: "${API_TOKEN}"
```

## Related Resources

- [Getting Started Guide](../getting_started.md)
- [Working with Variables](variables.md)
- [Logging Configuration](logging.md)
- [Connector Reference](../reference/connectors.md) 