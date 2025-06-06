# SQLFlow Connectors Reference

Comprehensive reference for all SQLFlow connectors with parameter specifications and usage patterns.

## Overview

SQLFlow connectors provide standardized interfaces to external data sources and destinations. All connectors follow industry-standard parameter conventions compatible with Airbyte and Fivetran for easy migration.

**Quick Reference:**
```sql
-- CSV Files
SOURCE data TYPE CSV PARAMS {"path": "data/file.csv", "has_header": true};

-- PostgreSQL Database
SOURCE orders TYPE POSTGRES PARAMS {"connection": "my_db", "table": "orders"};

-- S3 Storage
SOURCE logs TYPE S3 PARAMS {"connection": "s3", "bucket": "data", "key": "logs/"};

-- Parquet Files
SOURCE analytics TYPE PARQUET PARAMS {"path": "data/analytics.parquet"};
```

## Production Ready Connectors ✅

### CSV Connector

Load data from CSV files with comprehensive format support.

**Type:** `CSV`

**Common Parameters:**
```sql
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",           -- File path (required)
  "has_header": true,                 -- First row contains headers
  "delimiter": ",",                   -- Field separator
  "encoding": "utf-8"                 -- File encoding
};
```

**Advanced Parameters:**
```sql
SOURCE messy_data TYPE CSV PARAMS {
  "path": "data/complex.csv",
  "has_header": true,
  "delimiter": "|",                   -- Custom delimiter
  "quote_char": "'",                  -- Quote character
  "escape_char": "\\",                -- Escape character
  "null_values": ["NULL", "N/A", ""], -- Null value patterns
  "skip_rows": 2,                     -- Skip header rows
  "max_rows": 10000,                  -- Limit rows loaded
  "ignore_errors": true,              -- Skip malformed rows
  "date_format": "%Y-%m-%d %H:%M",    -- Custom date parsing
  "columns": ["id", "name", "email"]  -- Specific columns only
};
```

**Pattern Matching:**
```sql
-- Load multiple files
SOURCE sales TYPE CSV PARAMS {
  "path": "data/sales_*.csv",         -- Wildcard pattern
  "has_header": true,
  "combine_files": true               -- Combine into single table
};
```

### PostgreSQL Connector

Connect to PostgreSQL databases with full SQL support and incremental loading.

**Type:** `POSTGRES`

**Basic Connection:**
```sql
-- Using named connection (recommended)
SOURCE customers TYPE POSTGRES PARAMS {
  "connection": "my_postgres",        -- From profiles/connections
  "table": "customers"                -- Table name
};

-- Direct connection parameters
SOURCE orders TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "database": "ecommerce",
  "username": "user",
  "password": "${DB_PASSWORD}",       -- Environment variable
  "table": "orders"
};
```

**Custom Queries:**
```sql
SOURCE customer_metrics TYPE POSTGRES PARAMS {
  "connection": "prod_db",
  "query": """
    SELECT 
      customer_id,
      COUNT(*) as order_count,
      SUM(total_amount) as total_spent
    FROM orders 
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY customer_id
  """
};
```

**Incremental Loading:**
```sql
SOURCE recent_orders TYPE POSTGRES PARAMS {
  "connection": "prod_db",
  "table": "orders",
  "sync_mode": "incremental",         -- Enable incremental loading
  "cursor_field": "updated_at",       -- Timestamp column
  "primary_key": ["order_id"]         -- Array format (industry standard)
};
```

**Schema and Filtering:**
```sql
SOURCE sales_data TYPE POSTGRES PARAMS {
  "connection": "warehouse",
  "table": "sales",
  "schema": "analytics",              -- Database schema
  "where_clause": "status = 'completed'", -- Row filtering
  "columns": ["id", "amount", "date"] -- Column selection
};
```

### S3 Connector

Access data from AWS S3 and compatible storage services.

**Type:** `S3`

**Basic Usage:**
```sql
-- Using named connection
SOURCE data TYPE S3 PARAMS {
  "connection": "my_s3",              -- From profiles/connections
  "bucket": "data-lake",
  "key": "customers/customer_data.csv",
  "format": "csv"                     -- File format
};

-- Direct credentials
SOURCE logs TYPE S3 PARAMS {
  "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
  "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
  "region": "us-east-1",
  "bucket": "company-logs",
  "key": "application/2024/01/app.log",
  "format": "json"
};
```

**Pattern Matching:**
```sql
-- Load multiple files
SOURCE events TYPE S3 PARAMS {
  "connection": "s3_data",
  "bucket": "events",
  "key_pattern": "events/2024/*/events_*.parquet", -- Wildcard pattern
  "format": "parquet"
};
```

**Partitioned Data:**
```sql
SOURCE partitioned_events TYPE S3 PARAMS {
  "connection": "s3_data",
  "bucket": "data-lake",
  "key_pattern": "events/year={year}/month={month}/*.json",
  "format": "json",
  "partition_columns": ["year", "month"] -- Partition awareness
};
```

**Supported Formats:**
- `csv` - Comma-separated values
- `json` - JSON Lines format
- `parquet` - Apache Parquet
- `jsonl` - JSON Lines (alias for json)

### Parquet Connector

High-performance columnar data loading with schema inference.

**Type:** `PARQUET`

**Basic Usage:**
```sql
SOURCE analytics TYPE PARQUET PARAMS {
  "path": "data/analytics.parquet"    -- File path (required)
};
```

**Column Selection:**
```sql
SOURCE metrics TYPE PARQUET PARAMS {
  "path": "data/large_dataset.parquet",
  "columns": ["user_id", "event_type", "timestamp"] -- Load specific columns
};
```

**Multiple Files:**
```sql
SOURCE daily_metrics TYPE PARQUET PARAMS {
  "path": "data/metrics_*.parquet",   -- Pattern matching
  "combine_files": true               -- Combine into single table
};
```

## Beta Connectors 🚧

### Shopify Connector

Connect to Shopify stores for e-commerce analytics.

**Type:** `SHOPIFY`

**Configuration:**
```sql
SOURCE shopify_orders TYPE SHOPIFY PARAMS {
  "shop_name": "my-store",
  "access_token": "${SHOPIFY_TOKEN}",
  "resource": "orders",               -- orders, customers, products
  "created_at_min": "2024-01-01"      -- Date filtering
};
```

**Supported Resources:**
- `orders` - Order data
- `customers` - Customer information
- `products` - Product catalog
- `transactions` - Payment transactions

### REST API Connector

Generic REST API connectivity with authentication and pagination.

**Type:** `REST`

**Basic API Call:**
```sql
SOURCE api_data TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "method": "GET",
  "headers": {
    "Authorization": "Bearer ${API_TOKEN}",
    "Content-Type": "application/json"
  },
  "format": "json"
};
```

**Paginated API:**
```sql
SOURCE paginated_data TYPE REST PARAMS {
  "url": "https://api.example.com/orders",
  "method": "GET",
  "headers": {"Authorization": "Bearer ${API_TOKEN}"},
  "pagination": {
    "type": "offset",
    "limit_param": "limit",
    "offset_param": "offset",
    "limit": 100
  },
  "format": "json"
};
```

## Experimental Connectors 🔬

### Google Sheets Connector

Connect to Google Sheets for collaborative data workflows.

**Type:** `GOOGLE_SHEETS`

**Basic Usage:**
```sql
SOURCE sheet_data TYPE GOOGLE_SHEETS PARAMS {
  "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
  "range": "Sheet1!A1:E",
  "credentials_path": "${GOOGLE_CREDENTIALS_PATH}"
};
```

## Export Connectors

### CSV Export

Export data to CSV files with formatting options.

```sql
EXPORT customer_summary TO "output/customers.csv" TYPE CSV PARAMS {
  "delimiter": ",",
  "quote_char": "\"",
  "include_header": true,
  "date_format": "%Y-%m-%d"
};
```

### PostgreSQL Export

Export data directly to PostgreSQL tables.

```sql
EXPORT processed_data TO my_postgres.analytics.customer_metrics TYPE POSTGRES PARAMS {
  "mode": "replace",                  -- replace, append, upsert
  "create_table": true,               -- Auto-create table
  "batch_size": 10000                 -- Insert batch size
};
```

### S3 Export

Export data to S3 storage in various formats.

```sql
EXPORT results TO s3://my-bucket/results/ TYPE S3 PARAMS {
  "connection": "my_s3",
  "format": "parquet",                -- csv, json, parquet
  "partition_by": ["year", "month"],  -- Partition columns
  "compression": "gzip"               -- Compression type
};
```

## Industry Standard Parameters

SQLFlow uses parameter conventions compatible with Airbyte and Fivetran:

### Incremental Loading Parameters
```sql
{
  "sync_mode": "incremental",         -- full_refresh, incremental, cdc
  "cursor_field": "updated_at",       -- Timestamp field for incremental
  "primary_key": ["id"],              -- Array format (industry standard)
  "replication_start_date": "2024-01-01T00:00:00Z"
}
```

### Connection Parameters
```sql
{
  "host": "database.company.com",
  "port": 5432,
  "database": "analytics",
  "username": "sqlflow_user",
  "password": "${DB_PASSWORD}",       -- Environment variable
  "schema": "public",                 -- Default schema
  "ssl_mode": "require"               -- SSL configuration
}
```

### Performance Parameters
```sql
{
  "batch_size": 10000,                -- Rows per batch
  "timeout_seconds": 300,             -- Connection timeout
  "max_connections": 5,               -- Connection pool size
  "fetch_size": 5000,                 -- Query fetch size
  "parallel_streams": 4               -- Parallel processing
}
```

## Connection Configuration

Connections are defined in profile files for reusability:

```yaml
# profiles/production.yml
connections:
  prod_db:
    type: postgres
    host: prod-db.company.com
    port: 5432
    database: analytics
    username: sqlflow_user
    password: ${POSTGRES_PASSWORD}
    
  s3_data:
    type: s3
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    region: us-east-1
    bucket: company-data-lake
```

**Usage:**
```sql
-- Reference by name
SOURCE orders TYPE POSTGRES PARAMS {
  "connection": "prod_db",
  "table": "orders"
};

SOURCE events TYPE S3 PARAMS {
  "connection": "s3_data",
  "key": "events/2024/01/events.parquet",
  "format": "parquet"
};
```

### Profile-Based Configuration Loading

SQLFlow automatically loads connector configurations from the active profile, eliminating the need to duplicate connection details across pipelines:

**Profile Structure:**
```yaml
# profiles/production.yml
engines:
  duckdb:
    mode: persistent
    path: "/data/production.db"

# Connector configurations loaded automatically
connections:
  warehouse:
    type: postgres
    host: warehouse.company.com
    port: 5432
    database: analytics
    username: sqlflow_prod
    password: ${WAREHOUSE_PASSWORD}
    
  data_lake:
    type: s3
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    region: us-east-1
    bucket: prod-data-lake

# Variables available to all pipelines
variables:
  environment: production
  batch_size: 50000
```

**Pipeline Usage:**
```sql
-- Simply reference connection by name
SOURCE customers TYPE POSTGRES PARAMS {
  "connection": "warehouse",
  "table": "customers"
};

SOURCE events TYPE S3 PARAMS {
  "connection": "data_lake",
  "key": "events/${YEAR}/${MONTH}/",
  "format": "parquet"
};
```

### Automatic .env File Integration

SQLFlow automatically loads environment variables from `.env` files in your project root, providing seamless integration with connector configurations:

**Create .env file:**
```bash
# Create template
sqlflow env template

# Check current status
sqlflow env check

# List loaded variables
sqlflow env list
```

**Example .env file:**
```bash
# Database credentials
WAREHOUSE_PASSWORD=secure_db_password
POSTGRES_HOST=localhost
POSTGRES_USER=sqlflow_user

# Cloud credentials  
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=my-data-bucket

# API credentials
SHOPIFY_TOKEN=shpat_...
API_KEY=your_api_key
```

**Automatic Loading:**
Environment variables are automatically available in connector configurations without any additional setup:

```yaml
# profiles/dev.yml - automatically uses .env values
connections:
  local_db:
    type: postgres
    host: ${POSTGRES_HOST}      # From .env file
    username: ${POSTGRES_USER}  # From .env file
    password: ${WAREHOUSE_PASSWORD}  # From .env file
    
  s3_storage:
    type: s3
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}     # From .env file
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}  # From .env file
    bucket: ${S3_BUCKET}        # From .env file
```

**Pipeline Integration:**
```sql
-- No additional configuration needed
SOURCE customers TYPE POSTGRES PARAMS {
  "connection": "local_db",
  "table": "customers"
};

-- Variables from .env also available directly
SOURCE api_data TYPE REST PARAMS {
  "url": "https://api.service.com/data",
  "headers": {
    "Authorization": "Bearer ${API_KEY}"  -- From .env file
  }
};
```

**Variable Priority:**
1. CLI `--vars` parameters (highest)
2. Profile `variables` section
3. Pipeline `SET` statements
4. `.env` file variables
5. System environment variables
6. Default values (`${VAR|default}`) (lowest)

### Profile Management Commands

```bash
# List available profiles
sqlflow profile list

# Validate profile configuration
sqlflow profile validate production

# Test connections in profile
sqlflow connect test warehouse --profile production

# List connections in profile
sqlflow connect list --profile production
```

## Error Handling

### Connection Testing
```bash
# Test connection configuration
sqlflow connect test prod_db

# Test with verbose output
sqlflow connect test prod_db --verbose
```

### Common Error Patterns

**Connection Refused:**
```
❌ Connection 'prod_db' test failed!
  🚫 Error: Connection refused (host: localhost:5432)
  💡 Check if PostgreSQL is running
  💡 Verify host and port configuration
```

**Authentication Failed:**
```
❌ Authentication failed for user 'sqlflow_user'
  💡 Check username and password
  💡 Verify user has required permissions
```

**File Not Found:**
```
❌ CSV file not found: data/missing.csv
  💡 Check file path and permissions
  💡 Use absolute path if needed
```

### Resilience Features

SQLFlow automatically provides:
- **Retry Logic**: Automatic retry with exponential backoff
- **Circuit Breaker**: Prevent cascading failures
- **Connection Pooling**: Efficient resource usage
- **Rate Limiting**: Respect API limits
- **Error Recovery**: Graceful degradation

## Performance Optimization

### Batch Processing
```sql
-- Optimize large datasets
SOURCE large_table TYPE POSTGRES PARAMS {
  "connection": "prod_db",
  "table": "large_table",
  "batch_size": 50000,                -- Larger batches for performance
  "fetch_size": 10000,                -- Optimize memory usage
  "parallel_streams": 4               -- Parallel processing
};
```

### Column Selection
```sql
-- Load only needed columns
SOURCE optimized TYPE POSTGRES PARAMS {
  "connection": "prod_db",
  "table": "wide_table",
  "columns": ["id", "name", "created_at"] -- Reduce data transfer
};
```

### Query Pushdown
```sql
-- Filter at source
SOURCE filtered TYPE POSTGRES PARAMS {
  "connection": "prod_db",
  "query": """
    SELECT id, name, amount
    FROM transactions
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    AND status = 'completed'
  """
};
```

## Security Best Practices

### Environment Variables
```bash
# Set credentials securely
export DB_PASSWORD="secure_password"
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
```

### Least Privilege
```sql
-- Create dedicated user with minimal permissions
CREATE USER sqlflow_user WITH PASSWORD 'secure_password';
GRANT SELECT ON analytics.orders TO sqlflow_user;
GRANT SELECT ON analytics.customers TO sqlflow_user;
```

### Connection Security
```yaml
# Enable SSL for database connections
connections:
  secure_db:
    type: postgres
    host: db.company.com
    ssl_mode: require
    ssl_cert: /path/to/client.crt
    ssl_key: /path/to/client.key
```

---

**Related Documentation:**
- [Connecting Data Sources Guide](../user-guides/connecting-data-sources.md) - User-focused patterns
- [Building Connectors](../developer-guides/building-connectors.md) - Custom connector development
- [Profile Configuration](profiles.md) - Connection management 