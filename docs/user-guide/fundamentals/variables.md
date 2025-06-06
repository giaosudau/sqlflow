# Variables and Dynamic Pipelines

Make your pipelines flexible with variables - change dates, file paths, and settings without editing code.

## What Are Variables?

Variables let you write one pipeline that works in different situations:
- Process different date ranges  
- Switch between test and production data
- Change file paths and settings
- Create reusable pipelines

## Variable Sources

SQLFlow gets variables from multiple sources, using this priority order:

1. **Command line** (highest): `--var name=value`
2. **Profile files**: Variables in `profiles/environment.yml`
3. **SET statements**: Variables defined in your pipeline
4. **Environment variables**: From your system or `.env` file
5. **Default values** (lowest): `${var|default}` fallback

## Environment Variables (.env file)

SQLFlow automatically loads environment variables from a `.env` file in your project root. This is perfect for secrets, database connections, and environment-specific settings.

### Create .env File
```bash
# Create a template .env file
sqlflow env template

# Check if .env file exists
sqlflow env check

# List available environment variables
sqlflow env list
```

### .env File Example
```bash
# Database connections
DATABASE_URL=postgresql://user:password@localhost/mydb
POSTGRES_HOST=localhost
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword

# API keys and tokens
API_KEY=your_api_key_here
AUTH_TOKEN=your_auth_token_here

# Environment configuration
ENVIRONMENT=development
DEBUG=true

# Data paths
DATA_DIR=/path/to/data
OUTPUT_DIR=/path/to/output

# Custom variables
BATCH_SIZE=1000
REGION=us-west-2
```

### Use Environment Variables in Pipelines
```sql
-- Use environment variables with ${VARIABLE_NAME}
SOURCE customers TYPE POSTGRES PARAMS {
    "host": "${POSTGRES_HOST}",
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}",
    "dbname": "shop"
};

-- Environment variables with defaults
SET output_dir = "${OUTPUT_DIR|output}";
SET batch_size = "${BATCH_SIZE|1000}";

CREATE TABLE processed_data AS
SELECT * FROM customers LIMIT ${batch_size};

EXPORT SELECT * FROM processed_data
TO "${output_dir}/customers.csv"
TYPE CSV OPTIONS { "header": true };
```

### Manage Environment Variables
```bash
# List all environment variables
sqlflow env list

# List variables with specific prefix
sqlflow env list --prefix DATABASE

# Get a specific variable
sqlflow env get DATABASE_URL

# Check .env file status
sqlflow env check
```

## Basic Variables with SET

Use SET to define variables in your pipeline:

```sql
SET date = "2023-06-01";
SET output_dir = "reports";

CREATE TABLE daily_sales AS
SELECT * FROM read_csv_auto('data/sales_${date}.csv');

EXPORT SELECT * FROM daily_sales
TO "${output_dir}/sales_${date}.csv"
TYPE CSV OPTIONS { "header": true };
```

## Using Variables

Put variables in your SQL using `${variable_name}`:

### In File Paths
```sql
SET data_folder = "data";
SET report_date = "2023-06-01";

CREATE TABLE orders AS
SELECT * FROM read_csv_auto('${data_folder}/orders_${report_date}.csv');
```

### In SQL Queries
```sql
SET min_amount = "100";
SET status = "completed";

CREATE TABLE filtered_orders AS
SELECT * FROM orders 
WHERE amount >= ${min_amount} 
  AND status = '${status}';
```

### In Export Names
```sql
SET environment = "prod";
SET timestamp = "20230601";

EXPORT SELECT * FROM summary
TO "output/${environment}_report_${timestamp}.csv"
TYPE CSV OPTIONS { "header": true };
```

## Default Values

Give variables default values in case they're not provided:

```sql
SET date = "${date|2023-01-01}";
SET environment = "${environment|dev}";
SET max_rows = "${max_rows|1000}";

CREATE TABLE recent_data AS
SELECT * FROM sales_table 
WHERE order_date >= '${date}'
LIMIT ${max_rows};
```

If `date` isn't provided, it uses `2023-01-01`.

## Setting Variables from Command Line

Override variables when you run the pipeline:

```bash
# Use default values
sqlflow pipeline run analytics

# Override specific variables
sqlflow pipeline run analytics --var date=2023-07-01 --var environment=prod

# Override multiple variables
sqlflow pipeline run analytics --var date=2023-07-01 --var max_rows=5000
```

## Variable Priority Examples

Understanding how SQLFlow chooses variable values:

### Example 1: Database Connection
```bash
# .env file
DATABASE_HOST=localhost

# profiles/prod.yml
variables:
  DATABASE_HOST: prod-db.company.com

# Command line
sqlflow pipeline run etl --var DATABASE_HOST=test-db.company.com
```

Result: Uses `test-db.company.com` (command line wins)

### Example 2: With Defaults
```sql
-- Pipeline file
SET region = "${REGION|us-east-1}";
SET debug = "${DEBUG|false}";
```

```bash
# .env file
REGION=us-west-2

# Command line (DEBUG not provided)
sqlflow pipeline run analytics --var region=eu-west-1
```

Result: 
- `region = "eu-west-1"` (command line)
- `debug = "true"` (from .env file)

## Conditional Logic with IF

Use IF statements to make pipelines adapt to different conditions:

### Environment-Based Processing
```sql
SET environment = "${ENVIRONMENT|dev}";

IF ${environment} = "prod" THEN
    -- Production: Use database
    SOURCE customers TYPE POSTGRES PARAMS {
        "host": "${DATABASE_HOST}",
        "user": "${DATABASE_USER}",
        "password": "${DATABASE_PASSWORD}",
        "dbname": "production"
    };
ELSE
    -- Development: Use CSV file
    SOURCE customers TYPE CSV PARAMS {
        "path": "data/test_customers.csv",
        "has_header": true
    };
END IF;

LOAD customer_data FROM customers;
```

### Different Data Sources
```sql
SET data_source = "${DATA_SOURCE|csv}";

IF ${data_source} = "database" THEN
    SOURCE orders TYPE POSTGRES PARAMS {
        "host": "${DATABASE_HOST}",
        "table": "orders"
    };
ELSEIF ${data_source} = "api" THEN
    SOURCE orders TYPE REST PARAMS {
        "url": "${API_URL}",
        "auth_token": "${API_KEY}"
    };
ELSE
    SOURCE orders TYPE CSV PARAMS {
        "path": "data/orders.csv",
        "has_header": true
    };
END IF;
```

## Common Patterns

### Environment Configuration
```sql
-- Variables from .env file and command line
SET environment = "${ENVIRONMENT|dev}";
SET debug_mode = "${DEBUG|false}";
SET data_dir = "${DATA_DIR|data}";
SET output_dir = "${OUTPUT_DIR|output}";

-- Use environment-specific settings
IF ${environment} = "prod" THEN
    SET batch_size = "${BATCH_SIZE|10000}";
    SET table_prefix = "prod_";
ELSE
    SET batch_size = "${BATCH_SIZE|1000}";
    SET table_prefix = "dev_";
END IF;

CREATE TABLE ${table_prefix}customer_summary AS
SELECT 
    country,
    COUNT(*) as customer_count
FROM customers
GROUP BY country;

EXPORT SELECT * FROM ${table_prefix}customer_summary
TO "${output_dir}/${environment}_summary.csv"
TYPE CSV OPTIONS { "header": true };
```

### Database Connections
```sql
-- All connection details from environment variables
SOURCE customers TYPE POSTGRES PARAMS {
    "host": "${DB_HOST|localhost}",
    "port": "${DB_PORT|5432}",
    "user": "${DB_USER|postgres}",
    "password": "${DB_PASSWORD}",
    "dbname": "${DB_NAME|shop}"
};

-- SSL settings for production
IF ${ENVIRONMENT} = "prod" THEN
    SOURCE customers TYPE POSTGRES PARAMS {
        "host": "${DB_HOST}",
        "user": "${DB_USER}",
        "password": "${DB_PASSWORD}",
        "dbname": "${DB_NAME}",
        "sslmode": "require"
    };
END IF;
```

### API Integration
```sql
-- API configuration from environment
SET api_url = "${API_URL|https://api.example.com}";
SET api_key = "${API_KEY}";
SET rate_limit = "${API_RATE_LIMIT|100}";

SOURCE users TYPE REST PARAMS {
    "url": "${api_url}/users",
    "auth_token": "${api_key}",
    "rate_limit": ${rate_limit}
};
```

## Profile Variables

Store common variables in profile files for different environments:

```yaml
# profiles/prod.yml
name: "prod"
variables:
  ENVIRONMENT: "production"
  DB_HOST: "prod-db.company.com"
  BATCH_SIZE: "10000"
  DEBUG: "false"

# profiles/dev.yml  
name: "dev"
variables:
  ENVIRONMENT: "development"
  DB_HOST: "localhost"
  BATCH_SIZE: "1000"
  DEBUG: "true"
```

Use in pipelines:
```sql
SOURCE customers TYPE POSTGRES PARAMS {
    "host": "${DB_HOST}",
    "dbname": "shop"
};

SET batch_size = "${BATCH_SIZE|1000}";
```

Run with specific profile:
```bash
sqlflow pipeline run analytics --profile prod
```

## Best Practices

### Use .env for Secrets
```bash
# .env file (never commit to git!)
DATABASE_PASSWORD=super_secret_password
API_KEY=secret_api_key
AWS_SECRET_ACCESS_KEY=secret_key

# .env.example file (commit this as template)
DATABASE_PASSWORD=your_password_here
API_KEY=your_api_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

### Provide Sensible Defaults
```sql
-- Users can run without any setup
SET start_date = "${START_DATE|2023-01-01}";
SET end_date = "${END_DATE|2023-12-31}";
SET output_format = "${OUTPUT_FORMAT|csv}";
SET debug_mode = "${DEBUG|false}";
```

### Document Your Variables
```sql
-- Customer Analytics Pipeline
-- 
-- Environment Variables (set in .env file):
-- - DATABASE_HOST: Database server (default: localhost)
-- - DATABASE_USER: Database username
-- - DATABASE_PASSWORD: Database password
-- - OUTPUT_DIR: Output directory (default: output)
-- 
-- CLI Variables:
-- - date: Processing date (default: 2023-01-01)
-- - environment: Environment name (default: dev)

SET process_date = "${date|2023-01-01}";
SET environment = "${ENVIRONMENT|dev}";
SET output_dir = "${OUTPUT_DIR|output}";
```

### Environment-Specific Profiles
```yaml
# profiles/local.yml
name: "local"
variables:
  ENVIRONMENT: "local"
  DATA_DIR: "./data"
  OUTPUT_DIR: "./output"

# profiles/staging.yml
name: "staging"
variables:
  ENVIRONMENT: "staging"
  DATA_DIR: "/shared/staging/data"
  OUTPUT_DIR: "/shared/staging/output"

# profiles/prod.yml  
name: "prod"
variables:
  ENVIRONMENT: "production"
  DATA_DIR: "/data/production"
  OUTPUT_DIR: "/data/production/output"
```

## Complete Example

```sql
-- E-commerce Analytics Pipeline with Environment Variables
-- 
-- Required .env variables:
-- - DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD
-- - API_KEY (for customer enrichment)
-- 
-- Optional variables:
-- - ENVIRONMENT (default: dev)
-- - OUTPUT_DIR (default: output)
-- - BATCH_SIZE (default: 1000)

-- Get configuration from environment
SET environment = "${ENVIRONMENT|dev}";
SET output_dir = "${OUTPUT_DIR|output}";  
SET batch_size = "${BATCH_SIZE|1000}";
SET process_date = "${date|2023-06-01}";

-- Environment-specific data loading
IF ${environment} = "prod" THEN
    SOURCE customers TYPE POSTGRES PARAMS {
        "host": "${DATABASE_HOST}",
        "user": "${DATABASE_USER}",
        "password": "${DATABASE_PASSWORD}",
        "dbname": "production"
    };
    SOURCE orders TYPE POSTGRES PARAMS {
        "host": "${DATABASE_HOST}",
        "user": "${DATABASE_USER}",
        "password": "${DATABASE_PASSWORD}",
        "dbname": "production"
    };
ELSE
    SOURCE customers TYPE CSV PARAMS {
        "path": "data/customers.csv",
        "has_header": true
    };
    SOURCE orders TYPE CSV PARAMS {
        "path": "data/orders.csv",
        "has_header": true  
    };
END IF;

LOAD customer_data FROM customers;
LOAD order_data FROM orders;

-- Process data with batch size
CREATE TABLE filtered_orders AS
SELECT * FROM order_data
WHERE order_date = '${process_date}'
LIMIT ${batch_size};

-- Create summary
CREATE TABLE daily_summary AS
SELECT 
    c.country,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_revenue
FROM customer_data c
JOIN filtered_orders o ON c.customer_id = o.customer_id
GROUP BY c.country;

-- Export with environment prefix
EXPORT SELECT * FROM daily_summary
TO "${output_dir}/${environment}_summary_${process_date}.csv"
TYPE CSV OPTIONS { "header": true };
```

Run it:
```bash
# Development with .env file values
sqlflow pipeline run ecommerce_analytics

# Override for production
sqlflow pipeline run ecommerce_analytics --var environment=prod --var date=2023-07-15

# Using different profile  
sqlflow pipeline run ecommerce_analytics --profile staging --var date=2023-07-15
```

Check your variables:
```bash
# See what variables are available
sqlflow env list

# Check .env file status
sqlflow env check

# View specific database variables
sqlflow env list --prefix DATABASE
```