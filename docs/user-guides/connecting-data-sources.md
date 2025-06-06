# Connecting Data Sources with SQLFlow

**Problem**: "I have data in different places (CSV files, databases, cloud storage) and need to bring it all together for analysis."

**Solution**: SQLFlow's unified SOURCE and LOAD system works with all your data sources using the same simple patterns.

## 🎯 What You'll Learn

This guide shows you how to connect to all your data sources:
- ✅ **CSV Files**: Local files, network shares, and data exports
- ✅ **Databases**: PostgreSQL, MySQL, SQLite (and more coming)
- ✅ **Cloud Storage**: AWS S3, Google Cloud Storage, Azure Blob
- ✅ **APIs**: REST APIs, GraphQL, and custom endpoints
- ✅ **Load Modes**: REPLACE, APPEND, UPSERT for different scenarios
- ✅ **Data Quality**: Validation and error handling patterns

**Complete examples**: All code in this guide comes from working examples in [`/examples/`](../../examples/).

## 🚀 Quick Start: Connect Your First Data Source

### Problem: "I have CSV files and need to start analyzing immediately"

**2-minute solution:**

```bash
# Create project with CSV data
sqlflow init data_loading_demo
cd data_loading_demo

# Run basic data loading example
sqlflow pipeline run basic_load_modes

# See all your data loaded
ls output/
head output/users_loaded.csv
```

**What you get:**
- CSV files automatically loaded into tables
- Data validation and type detection
- Ready-to-query tables for analysis
- Export-ready results

## 📊 Data Source Connection Patterns

### 1. CSV Files - The Foundation

**Problem**: "I have CSV files from exports, downloads, or data dumps"

**Complete example**: [`/examples/load_modes/`](../../examples/load_modes/)

```sql
-- Basic CSV Loading
-- File: pipelines/csv_loading.sf

-- Single CSV file
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true,
  "delimiter": ",",
  "encoding": "utf-8"
};

LOAD users FROM users_csv;

-- Multiple CSV files (pattern matching)
SOURCE sales_files TYPE CSV PARAMS {
  "path": "data/sales_*.csv",
  "has_header": true,
  "combine_files": true
};

LOAD all_sales FROM sales_files;

-- CSV with custom configuration
SOURCE products_csv TYPE CSV PARAMS {
  "path": "data/products.csv",
  "has_header": true,
  "delimiter": "|",
  "quote_char": "'",
  "escape_char": "\\",
  "null_values": ["NULL", "N/A", ""]
};

LOAD products FROM products_csv;
```

**Advanced CSV options:**

```sql
-- Handle problematic CSV files
SOURCE messy_data_csv TYPE CSV PARAMS {
  "path": "data/messy_data.csv",
  "has_header": true,
  "skip_rows": 2,                    -- Skip first 2 rows
  "max_rows": 10000,                 -- Only load first 10k rows
  "ignore_errors": true,             -- Skip bad rows
  "date_format": "%Y-%m-%d %H:%M",   -- Custom date format
  "auto_detect_types": true          -- Let SQLFlow detect types
};

LOAD clean_data FROM messy_data_csv;
```

**Business value:**
- Instant data loading from any CSV source
- Automatic type detection and validation
- Error handling for messy real-world data
- Support for complex CSV formats

### 2. PostgreSQL - Production Database Integration

**Problem**: "I need to connect to our production PostgreSQL database"

**Complete example**: [`/examples/phase2_integration_demo/`](../../examples/phase2_integration_demo/)

**Installation:**
```bash
# Install PostgreSQL support
pip install "sqlflow-core[postgres]"
```

**Profile configuration:**
```yaml
# profiles/production.yml
name: "production"
engine:
  type: "duckdb"
  mode: "persistent"

connections:
  prod_db:
    type: "postgresql"
    host: "prod-db.company.com"
    port: 5432
    database: "analytics"
    username: "sqlflow_user"
    password: "${PG_PASSWORD}"  # Environment variable
    schema: "public"
```

**Usage in pipelines:**
```sql
-- Load from PostgreSQL
SOURCE customers_pg TYPE POSTGRESQL PARAMS {
  "connection": "prod_db",
  "query": "SELECT * FROM customers WHERE active = true",
  "schema": "public"
};

LOAD active_customers FROM customers_pg;

-- Load specific table
SOURCE orders_pg TYPE POSTGRESQL PARAMS {
  "connection": "prod_db", 
  "table": "orders",
  "schema": "sales",
  "where_clause": "order_date >= CURRENT_DATE - INTERVAL '30 days'"
};

LOAD recent_orders FROM orders_pg;

-- Custom query with parameters
SOURCE customer_metrics_pg TYPE POSTGRESQL PARAMS {
  "connection": "prod_db",
  "query": """
    SELECT 
      customer_id,
      COUNT(*) as order_count,
      SUM(total_amount) as total_spent
    FROM orders 
    WHERE order_date >= $1 
    GROUP BY customer_id
  """,
  "parameters": ["{{ vars.start_date }}"]
};

LOAD customer_metrics FROM customer_metrics_pg;
```

**Environment variables for security:**
```bash
# Set database password securely
export PG_PASSWORD="your_secure_password"

# Run pipeline with production profile
sqlflow pipeline run data_integration --profile production
```

**Business value:**
- Direct access to production data
- Secure credential management
- Query optimization and filtering
- Real-time data integration

### 3. AWS S3 - Cloud Storage Integration

**Problem**: "Our data is stored in S3 buckets and needs to be processed"

**Complete example**: [`/examples/phase2_integration_demo/`](../../examples/phase2_integration_demo/) (with MinIO S3-compatible)

**Installation:**
```bash
# Install S3 support
pip install "sqlflow-core[aws]"
```

**Profile configuration:**
```yaml
# profiles/cloud.yml
name: "cloud"
engine:
  type: "duckdb"
  mode: "persistent"

connections:
  s3_data:
    type: "s3"
    aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    region: "us-east-1"
    bucket: "company-data-lake"
```

**Usage in pipelines:**
```sql
-- Load CSV from S3
SOURCE s3_customers TYPE S3 PARAMS {
  "connection": "s3_data",
  "key": "customers/customer_data.csv",
  "format": "csv",
  "has_header": true
};

LOAD customers_from_s3 FROM s3_customers;

-- Load multiple files with pattern
SOURCE s3_sales TYPE S3 PARAMS {
  "connection": "s3_data", 
  "key_pattern": "sales/2024/*/sales_*.parquet",
  "format": "parquet"
};

LOAD sales_2024 FROM s3_sales;

-- Load with partitioning
SOURCE s3_events TYPE S3 PARAMS {
  "connection": "s3_data",
  "key_pattern": "events/year=${year}/month=${month}/*.json",
  "format": "json",
  "partition_columns": ["year", "month"]
};

LOAD partitioned_events FROM s3_events;
```

**Business value:**
- Access to cloud data lakes
- Support for multiple file formats
- Efficient partitioned data loading
- Cost-effective cloud storage integration

### 4. REST APIs - External Data Integration

**Problem**: "I need to pull data from APIs and external services"

**Complete example**: [`/examples/shopify_ecommerce_analytics/`](../../examples/shopify_ecommerce_analytics/) (Shopify API)

```sql
-- Basic API connection
SOURCE api_customers TYPE REST_API PARAMS {
  "url": "https://api.company.com/customers",
  "method": "GET",
  "headers": {
    "Authorization": "Bearer ${API_TOKEN}",
    "Content-Type": "application/json"
  },
  "format": "json",
  "json_path": "$.data[*]"  -- Extract data array
};

LOAD api_customers_data FROM api_customers;

-- Paginated API calls
SOURCE api_orders TYPE REST_API PARAMS {
  "url": "https://api.company.com/orders",
  "method": "GET", 
  "headers": {
    "Authorization": "Bearer ${API_TOKEN}"
  },
  "pagination": {
    "type": "offset",
    "limit_param": "limit", 
    "offset_param": "offset",
    "limit": 100
  },
  "format": "json"
};

LOAD all_orders FROM api_orders;

-- POST request with parameters
SOURCE api_analytics TYPE REST_API PARAMS {
  "url": "https://api.analytics.com/reports",
  "method": "POST",
  "headers": {
    "Authorization": "Bearer ${ANALYTICS_TOKEN}",
    "Content-Type": "application/json"
  },
  "body": {
    "start_date": "{{ vars.start_date }}",
    "end_date": "{{ vars.end_date }}",
    "metrics": ["revenue", "orders", "customers"]
  },
  "format": "json"
};

LOAD analytics_report FROM api_analytics;
```

**Business value:**
- Integration with external services
- Automated data collection from APIs
- Support for authentication and pagination
- Real-time data from external systems

## 🔄 Load Modes: How to Handle Different Data Scenarios

### REPLACE Mode (Default)

**When to use**: Initial loads, full refreshes, small datasets

```sql
-- Replace existing data completely
LOAD customers FROM customers_csv;  -- Default is REPLACE mode

-- Explicit REPLACE mode
LOAD customers FROM customers_csv MODE REPLACE;
```

**Use cases:**
- ✅ Daily full data refreshes
- ✅ Small reference tables
- ✅ Development and testing
- ❌ Large tables (inefficient)
- ❌ Preserving historical data

### APPEND Mode

**When to use**: Adding new data without changing existing records

```sql
-- Add new records to existing table
LOAD orders FROM new_orders_csv MODE APPEND;

-- Append with date filtering
SOURCE daily_sales TYPE CSV PARAMS {
  "path": "data/sales_{{ vars.date }}.csv",
  "has_header": true
};

LOAD sales FROM daily_sales MODE APPEND;
```

**Use cases:**
- ✅ Daily incremental loads
- ✅ Log data and events
- ✅ Time-series data
- ❌ Data with updates
- ❌ Duplicate handling needed

### UPSERT Mode

**When to use**: Updating existing records and inserting new ones

```sql
-- Single key upsert
LOAD customers FROM customer_updates_csv MODE UPSERT KEY customer_id;

-- Composite key upsert 
LOAD inventory FROM inventory_updates_csv MODE UPSERT KEY product_id, warehouse_id;

-- Upsert with conflict handling
LOAD products FROM product_updates_csv MODE UPSERT KEY product_id, sku;
```

**Use cases:**
- ✅ Customer data updates
- ✅ Product catalog maintenance
- ✅ Inventory management
- ✅ Slowly changing dimensions

**Complete example**: [`/examples/load_modes/`](../../examples/load_modes/)

## 🛡️ Data Quality and Validation

### Schema Validation

**Problem**: "I need to ensure data quality and catch schema issues early"

```sql
-- Schema validation example
SOURCE validated_customers TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true,
  "schema_validation": {
    "required_columns": ["customer_id", "email", "name"],
    "column_types": {
      "customer_id": "INTEGER",
      "email": "VARCHAR",
      "age": "INTEGER",
      "signup_date": "DATE"
    },
    "allow_extra_columns": true,
    "strict_types": false
  }
};

LOAD validated_customers FROM validated_customers;
```

### Data Quality Checks

```sql
-- Built-in data quality monitoring
CREATE TABLE data_quality_report AS
SELECT 
    'customers' as source_table,
    COUNT(*) as total_rows,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(customer_id) as non_null_ids,
    COUNT(email) as non_null_emails,
    COUNT(CASE WHEN email LIKE '%@%' THEN 1 END) as valid_emails,
    MIN(signup_date) as earliest_signup,
    MAX(signup_date) as latest_signup
FROM customers;

-- Export quality report
EXPORT data_quality_report TO 'output/data_quality_report.csv' TYPE CSV;
```

### Error Handling

```sql
-- Handle errors gracefully
SOURCE problematic_data TYPE CSV PARAMS {
  "path": "data/messy_data.csv",
  "has_header": true,
  "error_handling": {
    "on_error": "skip_row",     -- Options: skip_row, stop, log_only
    "max_errors": 100,          -- Stop after 100 errors
    "error_log_path": "errors/data_errors.log"
  }
};

LOAD clean_data FROM problematic_data;
```

## 🔧 Advanced Connection Patterns

### 1. Connection Pooling and Performance

```yaml
# profiles/high_performance.yml
connections:
  fast_db:
    type: "postgresql"
    host: "db.company.com"
    database: "analytics"
    username: "sqlflow"
    password: "${DB_PASSWORD}"
    pool_settings:
      min_connections: 2
      max_connections: 10
      connection_timeout: 30
      query_timeout: 300
```

### 2. Data Transformation During Loading

```sql
-- Transform data during load
SOURCE raw_sales TYPE CSV PARAMS {
  "path": "data/raw_sales.csv",
  "has_header": true
};

-- Load with transformations
LOAD processed_sales FROM (
    SELECT 
        order_id,
        customer_id,
        UPPER(product_name) as product_name,
        CAST(order_date AS DATE) as order_date,
        price * quantity as total_amount,
        CASE 
            WHEN price * quantity > 1000 THEN 'High Value'
            WHEN price * quantity > 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END as order_category
    FROM raw_sales
    WHERE price > 0 AND quantity > 0
);
```

### 3. Incremental Loading Patterns

```sql
-- Incremental loading with watermarks
SOURCE incremental_orders TYPE POSTGRESQL PARAMS {
  "connection": "prod_db",
  "query": """
    SELECT * FROM orders 
    WHERE updated_at > $1
    ORDER BY updated_at
  """,
  "parameters": ["{{ watermark.last_updated_at }}"]
};

LOAD orders FROM incremental_orders MODE UPSERT KEY order_id;

-- Update watermark for next run
UPDATE watermarks 
SET last_updated_at = (SELECT MAX(updated_at) FROM orders)
WHERE table_name = 'orders';
```

## 🌐 Multi-Source Data Integration

### Problem: "I need to combine data from multiple sources"

**Complete example**: [`/examples/phase2_integration_demo/`](../../examples/phase2_integration_demo/)

```sql
-- Multi-source integration pipeline
-- File: pipelines/multi_source_integration.sf

-- Load customers from PostgreSQL
SOURCE pg_customers TYPE POSTGRESQL PARAMS {
  "connection": "prod_db",
  "table": "customers",
  "schema": "crm"
};

LOAD customers FROM pg_customers;

-- Load orders from S3
SOURCE s3_orders TYPE S3 PARAMS {
  "connection": "s3_data",
  "key": "orders/current_month/*.parquet",
  "format": "parquet"
};

LOAD orders FROM s3_orders MODE APPEND;

-- Load product data from API
SOURCE api_products TYPE REST_API PARAMS {
  "url": "https://api.inventory.com/products",
  "headers": {"Authorization": "Bearer ${INVENTORY_TOKEN}"},
  "format": "json"
};

LOAD products FROM api_products MODE UPSERT KEY product_id;

-- Combine all sources for analysis
CREATE TABLE integrated_sales_analysis AS
SELECT 
    c.customer_id,
    c.name as customer_name,
    c.tier as customer_tier,
    p.name as product_name,
    p.category as product_category,
    o.order_date,
    o.quantity,
    o.price,
    o.quantity * o.price as total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days';

-- Export integrated analysis
EXPORT integrated_sales_analysis TO 'output/integrated_sales_analysis.csv' TYPE CSV;
```

## 🚨 Troubleshooting Data Connection Issues

### 1. Connection Problems

**Problem**: "Can't connect to database/API"

**Solutions**:
```bash
# Test connection separately
sqlflow connection test prod_db

# Check profile configuration
sqlflow profile validate production

# Test with minimal query
sqlflow query "SELECT 1" --profile production
```

**Common issues:**
- ❌ **Wrong credentials**: Check username/password
- ❌ **Network access**: Verify firewall/VPN settings
- ❌ **SSL/TLS issues**: Configure SSL settings
- ❌ **Timeout issues**: Increase timeout values

### 2. Data Type Issues

**Problem**: "Data types don't match or conversion fails"

**Solutions**:
```sql
-- Explicit type casting
SOURCE typed_data TYPE CSV PARAMS {
  "path": "data/messy_types.csv",
  "has_header": true,
  "column_types": {
    "date_field": "DATE",
    "number_field": "DECIMAL(10,2)",
    "text_field": "VARCHAR(255)"
  },
  "type_conversion": {
    "date_format": "%Y-%m-%d",
    "null_values": ["", "NULL", "N/A"],
    "strict_mode": false
  }
};

-- Handle conversion in SQL
LOAD clean_data FROM (
    SELECT 
        TRY_CAST(date_field AS DATE) as date_field,
        TRY_CAST(number_field AS DECIMAL) as number_field,
        COALESCE(text_field, 'Unknown') as text_field
    FROM typed_data
    WHERE TRY_CAST(date_field AS DATE) IS NOT NULL
);
```

### 3. Performance Issues

**Problem**: "Data loading is too slow"

**Solutions**:
```sql
-- Use batch loading
SOURCE large_dataset TYPE CSV PARAMS {
  "path": "data/very_large_file.csv", 
  "has_header": true,
  "batch_size": 10000,        -- Process in batches
  "parallel_load": true       -- Use multiple threads
};

-- Optimize with filtering
SOURCE filtered_data TYPE POSTGRESQL PARAMS {
  "connection": "prod_db",
  "query": """
    SELECT * FROM large_table 
    WHERE date_column >= CURRENT_DATE - INTERVAL '7 days'
    AND important_flag = true
  """,
  "fetch_size": 5000          -- Optimize fetch size
};
```

### 4. Memory Issues

**Problem**: "Out of memory errors with large datasets"

**Solutions**:
```bash
# Use persistent mode instead of memory
sqlflow pipeline run data_loading --profile production

# Process in chunks
sqlflow pipeline run data_loading --vars '{"chunk_size": 10000}'
```

```sql
-- Streaming processing
SOURCE streaming_data TYPE CSV PARAMS {
  "path": "data/huge_file.csv",
  "has_header": true,
  "streaming": true,          -- Process row by row
  "chunk_size": 1000         -- Process in small chunks
};
```

## 📚 Next Steps

### **Ready to connect your data?**

1. **Start Simple**: Begin with CSV files to understand patterns
2. **Add Databases**: Connect to your production systems
3. **Cloud Integration**: Move to cloud storage for scale
4. **API Integration**: Pull in external data sources

### **Example Projects**

- **Basic CSV Loading**: [`/examples/load_modes/`](../../examples/load_modes/)
- **PostgreSQL Integration**: [`/examples/phase2_integration_demo/`](../../examples/phase2_integration_demo/)
- **Multi-Source Integration**: [`/examples/phase2_integration_demo/`](../../examples/phase2_integration_demo/)

### **Learn More**

- [Building Analytics Pipelines](building-analytics-pipelines.md) - What to do with your connected data
- [Troubleshooting Guide](troubleshooting.md) - Solve common issues
- [CLI Reference](../reference/cli-commands.md) - Complete command guide

---

**🎯 Key Takeaway**: SQLFlow's unified connection system works the same way regardless of your data source. Learn the pattern once, use it everywhere. 