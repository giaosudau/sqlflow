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

# Advanced sections (from actual examples)
connectors:               # Alternative connection format
  postgres:
    type: POSTGRES
    params:
      host: localhost
      port: 5432

performance:              # Performance tuning
  batch_size: 1000
  max_memory_mb: 2048

logging:                  # Advanced logging configuration
  level: INFO
  format: structured
  
testing:                  # Testing configuration
  enabled: true
  data_validation: true
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

**Alternative Format (connectors section):**
```yaml
connectors:
  postgres:
    type: POSTGRES
    params:
      host: "postgres"
      port: 5432
      database: "demo"      # Use "database" (not "dbname")
      username: "sqlflow"   # Use "username" (not "user")
      password: "sqlflow123"
      schema: "public"
      sslmode: "prefer"
      connect_timeout: 30
      pool_size: 5
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

**Alternative Format (connectors section):**
```yaml
connectors:
  s3:
    type: S3
    params:
      endpoint_url: "http://minio:9000"
      access_key_id: "minioadmin"
      secret_access_key: "minioadmin"
      region: "us-east-1"
      bucket: "sqlflow-demo"
      use_ssl: false
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

### REST API Connections

```yaml
connectors:
  rest:
    type: REST
    params:
      base_url: "http://mockserver:1080"
      auth_method: "bearer"
      auth_params:
        token: "${API_TOKEN}"
```

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
  
  # From actual examples
  date: ${DATE}
  API_TOKEN: ${API_TOKEN}
  DB_CONN: "postgresql://sqlflow:sqlflow123@postgres:5432/ecommerce"
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

### Automatic .env File Integration

SQLFlow automatically loads environment variables from `.env` files in your project root:

**Create .env file:**
```bash
# Create template
sqlflow env template

# Check status
sqlflow env check

# List variables
sqlflow env list
```

**Example .env file:**
```bash
# Database connections
POSTGRES_PASSWORD=secure_password
DB_HOST=localhost
DB_PORT=5432

# API credentials
API_TOKEN=your_token_here
SHOPIFY_TOKEN=shpat_token

# Environment settings
ENVIRONMENT=production
DATE=2024-01-01
BATCH_SIZE=5000
```

**Automatic Usage:**
Variables from `.env` files are automatically available in profiles without any additional configuration:

```yaml
# profiles/production.yml - uses .env variables automatically
variables:
  environment: ${ENVIRONMENT}      # From .env file
  api_token: ${API_TOKEN}         # From .env file
  batch_size: ${BATCH_SIZE|1000}  # From .env file with default

connections:
  postgres:
    type: postgres
    host: ${DB_HOST}              # From .env file
    password: ${POSTGRES_PASSWORD} # From .env file
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

### Advanced Logging (from examples)

```yaml
logging:
  level: "INFO"                    # Log level
  format: "structured"             # Output format
  output: "console"                # Output destination
  include_performance_metrics: true
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

### Development Profile (from examples)

```yaml
# profiles/dev.yml
engines:
  duckdb:
    mode: memory              # Fast, non-persistent
    memory_limit: 2GB

variables:
  date: ${DATE}
  API_TOKEN: ${API_TOKEN}
  DB_CONN: "postgresql://sqlflow:sqlflow123@postgres:5432/ecommerce"

connectors:
  postgres:
    type: POSTGRES
    params:
      host: "postgres"
      port: 5432
      dbname: "ecommerce"
      user: "sqlflow"
      password: "sqlflow123"
      
  s3:
    type: S3
    params:
      endpoint_url: "http://minio:9000"
      region: "us-east-1"
      access_key_id: "minioadmin"
      secret_access_key: "minioadmin"
      bucket: "analytics"
```

### Docker Environment Profile (from examples)

```yaml
# profiles/docker.yml
engines:
  duckdb:
    mode: persistent
    path: "/app/target/demo.duckdb"
    memory_limit: 2GB

log_level: info

variables:
  data_dir: "/app/data"
  output_dir: "/app/output"
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  demo_mode: true
  benchmark_mode: true
  environment: "docker"

connectors:
  postgres:
    type: POSTGRES
    params:
      host: "postgres"
      port: 5432
      dbname: "demo"
      user: "sqlflow"
      password: "sqlflow123"
      schema: "public"
      sslmode: "prefer"
      connect_timeout: 30
      pool_size: 5
    
  s3:
    type: S3
    params:
      endpoint_url: "http://minio:9000"
      access_key_id: "minioadmin"
      secret_access_key: "minioadmin"
      region: "us-east-1"
      bucket: "sqlflow-demo"
      use_ssl: false

performance:
  batch_size: 1000
  max_memory_mb: 2048
  parallel_workers: 2
  
logging:
  level: "INFO"
  format: "structured"
  output: "console"
  include_performance_metrics: true
  
testing:
  enabled: true
  data_validation: true
  performance_benchmarks: true
  error_simulation: false
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

```