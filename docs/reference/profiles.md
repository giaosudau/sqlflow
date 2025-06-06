# SQLFlow Profile Configuration Reference

Complete reference for SQLFlow profile configuration with verified schemas and validation patterns.

## Overview

Profiles define environment-specific configurations for SQLFlow pipelines, including database connections, execution engines, variables, and logging settings. Profiles enable seamless deployment across development, staging, and production environments.

**Quick Reference:**
```yaml
# profiles/dev.yml
engines:
  duckdb:
    mode: memory
    memory_limit: 2GB

log_level: debug

variables:
  environment: development
  batch_size: 1000

connections:
  my_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: analytics
    username: sqlflow_user
    password: ${DB_PASSWORD}
```

## Profile Schema

### Core Structure

```yaml
# Complete profile schema
engines:                    # Required: Execution engine configuration
  duckdb:
    mode: memory|persistent # Required: Engine mode
    path: "path/to/db"     # Required for persistent mode
    memory_limit: 2GB      # Optional: Memory limit

log_level: debug|info|warning|error  # Optional: Logging level
log_file: "path/to/log.txt"         # Optional: Log file path

variables:                 # Optional: Pipeline variables
  key1: value1
  key2: ${ENV_VAR}

connections:              # Optional: Named connections
  connection_name:
    type: connector_type
    param1: value1
    param2: ${ENV_VAR}
```

## Engine Configuration

### DuckDB Engine

SQLFlow uses DuckDB as its primary execution engine with two operational modes:

**Memory Mode (Development):**
```yaml
engines:
  duckdb:
    mode: memory            # Data stored in RAM only
    memory_limit: 2GB       # Optional memory limit
```

**Persistent Mode (Production):**
```yaml
engines:
  duckdb:
    mode: persistent        # Data persisted to disk
    path: "target/production.db"  # Required: Database file path
    memory_limit: 8GB       # Optional memory limit
```

**Advanced Configuration:**
```yaml
engines:
  duckdb:
    mode: persistent
    path: "/data/sqlflow.db"
    memory_limit: 16GB
    threads: 8              # Number of threads
    max_memory: "75%"       # Percentage of system memory
    temp_directory: "/tmp/duckdb"  # Temporary file location
```

### Memory Limit Formats

```yaml
# Supported memory limit formats
memory_limit: 2GB          # Gigabytes
memory_limit: 2048MB       # Megabytes
memory_limit: "75%"        # Percentage of system memory
memory_limit: 2147483648   # Bytes (integer)
```

## Connection Configuration

### PostgreSQL Connections

**Basic Connection:**
```yaml
connections:
  prod_db:
    type: postgres
    host: prod-db.company.com
    port: 5432
    database: analytics
    username: sqlflow_user
    password: ${POSTGRES_PASSWORD}
```

**Advanced PostgreSQL:**
```yaml
connections:
  secure_postgres:
    type: postgres
    host: db.company.com
    port: 5432
    database: data_warehouse
    username: sqlflow_user
    password: ${DB_PASSWORD}
    schema: analytics                    # Default schema
    ssl_mode: require                    # SSL configuration
    ssl_cert: /path/to/client.crt       # Client certificate
    ssl_key: /path/to/client.key        # Client key
    ssl_ca: /path/to/ca.crt             # CA certificate
    connect_timeout: 30                  # Connection timeout (seconds)
    application_name: sqlflow            # Application identifier
```

**Connection Pool Settings:**
```yaml
connections:
  pooled_postgres:
    type: postgres
    host: db.company.com
    database: analytics
    username: sqlflow_user
    password: ${DB_PASSWORD}
    pool_settings:
      min_connections: 2      # Minimum pool size
      max_connections: 10     # Maximum pool size
      connection_timeout: 30   # Connection timeout
      idle_timeout: 300       # Idle connection timeout
```

### S3 Connections

**Basic S3 Configuration:**
```yaml
connections:
  s3_data:
    type: s3
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    region: us-east-1
    bucket: company-data-lake
```

**Advanced S3 Configuration:**
```yaml
connections:
  advanced_s3:
    type: s3
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    region: us-west-2
    bucket: analytics-bucket
    endpoint_url: https://s3.company.com  # Custom endpoint (MinIO, etc.)
    use_ssl: true                         # SSL/TLS encryption
    addressing_style: path                # path or virtual-hosted
    signature_version: s3v4               # Signature version
    max_pool_connections: 10              # Connection pool size
```

**S3 with IAM Role:**
```yaml
connections:
  s3_iam:
    type: s3
    region: us-east-1
    bucket: secure-data-bucket
    use_iam_role: true        # Use IAM instance profile/role
```

### Shopify Connections

```yaml
connections:
  shopify_store:
    type: shopify
    shop_name: my-store       # Shopify store name
    access_token: ${SHOPIFY_ACCESS_TOKEN}
    api_version: "2024-01"    # API version
    rate_limit_delay: 0.5     # Delay between requests (seconds)
    timeout: 30               # Request timeout
```

### REST API Connections

```yaml
connections:
  api_service:
    type: rest
    base_url: https://api.company.com
    headers:
      Authorization: Bearer ${API_TOKEN}
      Content-Type: application/json
      User-Agent: SQLFlow/1.0
    timeout: 30               # Request timeout
    retries: 3                # Number of retries
    retry_delay: 1            # Delay between retries
```

## Variable Configuration

### Basic Variables

```yaml
variables:
  environment: production
  region: us-east-1
  batch_size: 10000
  debug_mode: false
  start_date: "2024-01-01"
  data_path: "/data/warehouse"
```

### Environment Variable References

```yaml
variables:
  # Direct environment variable reference
  api_key: ${API_KEY}
  
  # With default values
  region: ${AWS_REGION|us-east-1}
  batch_size: ${BATCH_SIZE|5000}
  debug: ${DEBUG_MODE|false}
  
  # Complex references
  database_url: ${DB_HOST}:${DB_PORT|5432}/${DB_NAME}
  s3_path: s3://${S3_BUCKET}/${S3_PREFIX|data}/
```

### Computed Variables

```yaml
variables:
  # Date-based variables
  current_date: "{{ 'now' | strftime('%Y-%m-%d') }}"
  year_month: "{{ 'now' | strftime('%Y-%m') }}"
  
  # Environment-specific settings
  batch_size: "{{ 10000 if environment == 'production' else 1000 }}"
  log_level: "{{ 'warning' if environment == 'production' else 'debug' }}"
```

## Logging Configuration

### Basic Logging

```yaml
# Simple log level setting
log_level: debug    # debug, info, warning, error

# Log to file
log_level: info
log_file: logs/sqlflow.log
```

### Advanced Logging

```yaml
logging:
  level: info                    # Global log level
  file: /var/log/sqlflow.log    # Log file path
  max_size: 100MB               # Maximum file size
  backup_count: 5               # Number of backup files
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  
  # Component-specific logging
  loggers:
    sqlflow.connectors:
      level: debug
    sqlflow.udfs:
      level: warning
    duckdb:
      level: error
```

### Structured Logging

```yaml
logging:
  level: info
  format: json                  # JSON structured logging
  file: logs/sqlflow.jsonl      # JSON Lines format
  fields:
    service: sqlflow
    environment: ${ENVIRONMENT}
    version: "1.0.0"
```

## Profile Examples

### Development Profile

```yaml
# profiles/dev.yml
engines:
  duckdb:
    mode: memory              # Fast, non-persistent
    memory_limit: 2GB

log_level: debug              # Verbose logging
log_file: logs/dev.log

variables:
  environment: development
  batch_size: 1000            # Small batches for testing
  debug_mode: true
  data_path: data/sample

connections:
  dev_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: dev_analytics
    username: dev_user
    password: ${DEV_DB_PASSWORD}
```

### Staging Profile

```yaml
# profiles/staging.yml
engines:
  duckdb:
    mode: persistent          # Persistent for testing
    path: target/staging.db
    memory_limit: 4GB

log_level: info
log_file: logs/staging.log

variables:
  environment: staging
  batch_size: 5000
  debug_mode: false
  data_path: /staging/data

connections:
  staging_postgres:
    type: postgres
    host: staging-db.company.com
    port: 5432
    database: staging_analytics
    username: sqlflow_staging
    password: ${STAGING_DB_PASSWORD}
    ssl_mode: require
    
  staging_s3:
    type: s3
    aws_access_key_id: ${STAGING_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${STAGING_AWS_SECRET_ACCESS_KEY}
    region: us-east-1
    bucket: staging-data-lake
```

### Production Profile

```yaml
# profiles/production.yml
engines:
  duckdb:
    mode: persistent          # Persistent storage
    path: /data/production/sqlflow.db
    memory_limit: 16GB        # Large memory allocation
    threads: 8

log_level: warning            # Minimal logging
log_file: /var/log/sqlflow.log

variables:
  environment: production
  batch_size: 50000           # Large batches for efficiency
  debug_mode: false
  data_path: /data/warehouse

connections:
  prod_postgres:
    type: postgres
    host: prod-db.company.com
    port: 5432
    database: analytics
    username: sqlflow_prod
    password: ${PROD_DB_PASSWORD}
    ssl_mode: require
    ssl_cert: /etc/ssl/certs/client.crt
    ssl_key: /etc/ssl/private/client.key
    pool_settings:
      min_connections: 5
      max_connections: 20
      
  prod_s3:
    type: s3
    region: us-east-1
    bucket: prod-data-lake
    use_iam_role: true        # Use IAM role instead of keys
    
  shopify_prod:
    type: shopify
    shop_name: company-store
    access_token: ${SHOPIFY_PROD_TOKEN}
    api_version: "2024-01"
```

## Profile Management

### Profile Selection

```bash
# Use specific profile
sqlflow pipeline run analytics --profile production

# Set default profile via environment
export SQLFLOW_PROFILE=staging
sqlflow pipeline run analytics

# List available profiles
sqlflow profile list
```

### Profile Validation

```bash
# Validate specific profile
sqlflow profile validate production

# Validate all profiles
sqlflow profile validate

# Test connections in profile
sqlflow profile validate production --test-connections
```

### Profile Migration

**Legacy Format (Auto-fixed):**
```yaml
# Old format (deprecated)
profile_name:
  engines:
    duckdb:
      database_path: "target/db.duckdb"
```

**New Format:**
```yaml
# Current format
engines:
  duckdb:
    mode: persistent
    path: "target/db.duckdb"
```

## Environment Variables

### Required Environment Variables

```bash
# Database credentials
export DB_PASSWORD="secure_password"
export POSTGRES_PASSWORD="postgres_secret"

# AWS credentials
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."

# API tokens
export SHOPIFY_ACCESS_TOKEN="shpat_..."
export API_TOKEN="bearer_token_here"

# Environment identification
export ENVIRONMENT="production"
export SQLFLOW_PROFILE="production"
```

### Environment Variable Security

```bash
# Use secure environment variable management
# AWS Secrets Manager
aws secretsmanager get-secret-value \
  --secret-id prod/sqlflow/db-password \
  --query SecretString --output text

# Docker secrets
docker run -d \
  --secret source=db-password,target=/run/secrets/db-password \
  my-sqlflow-image

# Kubernetes secrets
kubectl create secret generic sqlflow-secrets \
  --from-literal=db-password=secure_password
```

## Validation & Error Handling

### Common Validation Errors

**Missing Required Fields:**
```yaml
# ❌ Missing required 'engines' section
variables:
  environment: dev
```
*Solution: Add `engines:` section*

**Invalid DuckDB Configuration:**
```yaml
# ❌ Persistent mode without path
engines:
  duckdb:
    mode: persistent
    # Missing required 'path' field
```
*Solution: Add `path: "target/db.duckdb"`*

**Invalid Memory Limit:**
```yaml
# ❌ Invalid memory format
engines:
  duckdb:
    mode: memory
    memory_limit: "invalid"
```
*Solution: Use valid format like `2GB`, `1024MB`, or `75%`*

### Connection Testing

```bash
# Test PostgreSQL connection
sqlflow connect test prod_postgres

# Test with verbose output
sqlflow connect test prod_postgres --verbose

# Test all connections
sqlflow connect test --all
```

**Connection Test Output:**
```
✅ Connection 'prod_postgres' test successful!
  🔗 Host: prod-db.company.com:5432
  📊 Database: analytics
  👤 User: sqlflow_prod
  ⏱️  Connection time: 45ms
  🔒 SSL: enabled
```

## Security Best Practices

### Credential Management

```yaml
# ✅ Use environment variables for secrets
connections:
  secure_db:
    type: postgres
    host: db.company.com
    password: ${DB_PASSWORD}    # Never hardcode passwords

# ✅ Use IAM roles when possible
connections:
  secure_s3:
    type: s3
    region: us-east-1
    use_iam_role: true         # Preferred over access keys
```

### SSL/TLS Configuration

```yaml
# ✅ Always use SSL in production
connections:
  secure_postgres:
    type: postgres
    host: db.company.com
    ssl_mode: require          # Enforce SSL
    ssl_cert: /path/to/client.crt
    ssl_key: /path/to/client.key
    ssl_ca: /path/to/ca.crt
```

### Access Control

```yaml
# ✅ Use dedicated service accounts
connections:
  restricted_db:
    type: postgres
    username: sqlflow_readonly  # Read-only user
    # Grant minimal required permissions
```

## Performance Tuning

### Memory Optimization

```yaml
engines:
  duckdb:
    mode: persistent
    memory_limit: "75%"         # Use 75% of system memory
    threads: 8                  # Match CPU cores
    temp_directory: /fast/ssd   # Use fast storage for temp files
```

### Connection Pooling

```yaml
connections:
  optimized_postgres:
    type: postgres
    host: db.company.com
    pool_settings:
      min_connections: 2        # Keep minimum connections alive
      max_connections: 10       # Limit concurrent connections
      connection_timeout: 30    # Quick connection timeout
      idle_timeout: 300         # Close idle connections
```

### Batch Processing

```yaml
variables:
  # Environment-specific batch sizes
  batch_size: "{{ 50000 if environment == 'production' else 1000 }}"
  parallel_streams: "{{ 8 if environment == 'production' else 2 }}"
```

## Profile Templates

### Minimal Profile Template

```yaml
# profiles/minimal.yml
engines:
  duckdb:
    mode: memory

variables:
  environment: development
```

### Complete Profile Template

```yaml
# profiles/complete.yml
engines:
  duckdb:
    mode: persistent
    path: "target/{{ environment }}.db"
    memory_limit: 4GB
    threads: 4

log_level: info
log_file: "logs/{{ environment }}.log"

variables:
  environment: development
  batch_size: 5000
  parallel_streams: 2
  debug_mode: false

connections:
  main_db:
    type: postgres
    host: ${DB_HOST}
    port: ${DB_PORT|5432}
    database: ${DB_NAME}
    username: ${DB_USER}
    password: ${DB_PASSWORD}
    ssl_mode: prefer
    
  data_storage:
    type: s3
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    region: ${AWS_REGION|us-east-1}
    bucket: ${S3_BUCKET}
```

---

**Related Documentation:**
- [Connecting Data Sources Guide](../user-guides/connecting-data-sources.md) - Using connections in pipelines
- [Connector Reference](connectors.md) - Complete connector parameter specifications
- [CLI Commands Reference](cli-commands.md) - Profile management commands 