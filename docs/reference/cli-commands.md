# SQLFlow CLI Commands Reference

Complete reference for all SQLFlow command-line interface commands with verified examples and usage patterns.

## Overview

SQLFlow CLI provides commands for managing data pipelines, connections, UDFs, and project initialization. All commands support global options `--verbose` and `--quiet` for output control.

```bash
sqlflow --help                    # Show all available commands
sqlflow [command] --help          # Show help for specific command
```

## Global Options

| Option | Alias | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Enable detailed output with technical information |
| `--quiet` | `-q` | Reduce output to essential information only |
| `--version` | | Show SQLFlow version |

## Project Management

### `sqlflow init`

Initialize a new SQLFlow project with sample data and working pipelines.

```bash
# Create project with sample data (recommended)
sqlflow init my_project

# Create minimal project structure
sqlflow init my_project --minimal

# Initialize and run demo pipeline immediately
sqlflow init my_project --demo
```

**Generated Structure:**
```
my_project/
├── pipelines/               # SQLFlow pipeline files
│   ├── customer_analytics.sf
│   ├── data_quality.sf
│   └── example.sf
├── profiles/                # Environment configurations
│   ├── dev.yml
│   ├── prod.yml
│   └── README.md
├── data/                    # Sample data (auto-generated)
│   ├── customers.csv        # 1,000 records
│   ├── orders.csv           # 5,000 records
│   └── products.csv         # 500 records
├── python_udfs/             # Python UDF directory
├── output/                  # Pipeline outputs
├── target/                  # Compiled plans
└── README.md
```

## Pipeline Commands

### `sqlflow pipeline list`

List all available pipelines in the current project.

```bash
sqlflow pipeline list
sqlflow pipeline list --profile prod
```

**Output Example:**
```
📋 Available Pipelines:
  customer_analytics    Customer behavior analysis with revenue metrics
  data_quality         Data validation and quality monitoring  
  example             Basic SQLFlow pipeline example

💡 Run with: sqlflow pipeline run <pipeline_name>
```

**Profile Impact:**
The `--profile` parameter affects pipeline listing only if the profile defines a custom `paths.pipelines` configuration. Most profiles use the default `pipelines/` directory, making this parameter equivalent across profiles.

### `sqlflow pipeline validate`

Validate pipeline syntax and configuration without execution.

```bash
# Validate specific pipeline
sqlflow pipeline validate customer_analytics

# Validate all pipelines
sqlflow pipeline validate

# Validate with profile (affects variable resolution)
sqlflow pipeline validate customer_analytics --profile prod

# Validate with detailed output
sqlflow pipeline validate customer_analytics --verbose

# Quiet validation check
sqlflow pipeline validate customer_analytics --quiet
```

**Success Output:**
```
✅ Pipeline 'customer_analytics' validation passed!
  📊 Found 5 operations (2 SOURCE, 2 LOAD, 1 EXPORT)
  🔗 Dependency graph validated successfully
  📝 All references resolved
```

**Error Output:**
```
❌ Pipeline validation failed with 2 error(s):

📋 Syntax Errors:
  Line 3: Unexpected token '{' - expected string literal
    SOURCE orders TYPE CSV PARAMS {
                                  ^
    💡 Check parameter formatting: {"key": "value"}

📋 Reference Errors:  
  Line 15: Table 'customer_data' not found
    LOAD customer_summary FROM customer_data;
                              ^
    💡 Available tables: customers, orders, products
    💡 Did you mean 'customers'?
```

**Profile Impact:**
The `--profile` parameter affects validation by:
- Loading profile-specific variables for variable resolution
- Using profile-specific connection configurations for connector validation
- Applying profile-specific logging and debug settings

### `sqlflow pipeline compile`

Parse and compile pipeline to execution plan without running.

```bash
# Compile specific pipeline
sqlflow pipeline compile customer_analytics

# Compile with custom output location
sqlflow pipeline compile customer_analytics --output my_plan.json

# Compile all pipelines
sqlflow pipeline compile

# Skip validation for CI/CD performance
sqlflow pipeline compile customer_analytics --skip-validation

# Compile with profile
sqlflow pipeline compile customer_analytics --profile prod
```

**With Variables:**
```bash
# JSON format
sqlflow pipeline compile customer_analytics --vars '{"date": "2023-10-25", "region": "us-east"}'

# Key=value format
sqlflow pipeline compile customer_analytics --vars 'date=2023-10-25,region=us-east'

# Complex nested variables
sqlflow pipeline compile customer_analytics --vars '{"config": {"batch_size": 1000, "debug": true}}'
```

**Compilation Output:**
```
🔄 Compiling pipeline 'customer_analytics'...
  📄 Parsing pipeline file...
  🔍 Resolving dependencies...
  📊 Building execution plan...
  💾 Saving to target/compiled/customer_analytics.json

✅ Compilation completed successfully!
  📊 Generated 5 operations
  🔗 Dependency order: [orders_source, customers_source, customer_data, analytics, export]
  📁 Plan saved to: target/compiled/customer_analytics.json
```

### `sqlflow pipeline run`

Execute a complete pipeline from source to output.

```bash
# Run pipeline with default profile
sqlflow pipeline run customer_analytics

# Run with specific profile
sqlflow pipeline run customer_analytics --profile prod

# Run with variables
sqlflow pipeline run customer_analytics --vars '{"date": "2023-10-25"}'

# Use pre-compiled plan (faster)
sqlflow pipeline run customer_analytics --from-compiled

# Quiet execution
sqlflow pipeline run customer_analytics --quiet

# Verbose execution with debug info
sqlflow pipeline run customer_analytics --verbose
```

**Automatic .env File Loading:**
SQLFlow automatically loads environment variables from a `.env` file in your project root, eliminating the need for `--vars` in most cases:

```bash
# Create .env file template
sqlflow env template

# Check .env file status  
sqlflow env check

# List environment variables
sqlflow env list
```

**Example .env file:**
```bash
# Database connections
DATABASE_URL=postgresql://user:password@localhost/mydb
POSTGRES_HOST=localhost
POSTGRES_PASSWORD=secure_password

# API credentials
SHOPIFY_TOKEN=shpat_your_token_here
API_KEY=your_api_key

# Pipeline variables
ENVIRONMENT=production
BATCH_SIZE=5000
START_DATE=2024-01-01
```

**Usage in pipelines:**
```sql
-- Variables automatically available from .env file
SOURCE orders TYPE POSTGRES PARAMS {
  "host": "${POSTGRES_HOST}",
  "password": "${POSTGRES_PASSWORD}"
};

SET batch_size = "${BATCH_SIZE|1000}";
SET environment = "${ENVIRONMENT|dev}";
```

**No --vars needed:**
```bash
# These work automatically with .env file
sqlflow pipeline run customer_analytics
sqlflow pipeline run customer_analytics --profile prod

# Override .env values if needed
sqlflow pipeline run customer_analytics --vars '{"BATCH_SIZE": "10000"}'
```

**Variable Priority (highest to lowest):**
1. CLI `--vars` parameters
2. Profile `variables` section
3. Pipeline `SET` statements  
4. `.env` file variables
5. System environment variables
6. Default values (`${VAR|default}`)

**Execution Output:**
```
🚀 Running pipeline 'customer_analytics'...

📊 Execution Plan:
  1. orders_source (SOURCE)
  2. customers_source (SOURCE)  
  3. customer_data (LOAD)
  4. analytics (LOAD)
  5. export (EXPORT)

⏳ Executing operations...
  ✅ orders_source: 5,000 rows loaded
  ✅ customers_source: 1,000 rows loaded
  ✅ customer_data: 1,000 rows processed
  ✅ analytics: 100 rows generated
  ✅ export: 100 rows exported to output/customer_summary.csv

🎉 Pipeline completed successfully!
  ⏱️  Total time: 2.3 seconds
  📊 Operations: 5/5 successful
  📁 Output: output/customer_summary.csv
```

## Connection Management

### `sqlflow connect list`

List all configured connections in the current profile.

```bash
sqlflow connect list
sqlflow connect list --profile prod
```

**Output Example:**
```
🔌 Configured Connections (dev profile):
  my_postgres     PostgreSQL database (host: localhost:5432)
  data_warehouse  PostgreSQL database (host: prod-db:5432)
  s3_bucket      S3 storage (bucket: my-data-bucket)

💡 Test with: sqlflow connect test <connection_name>
```

### `sqlflow connect test`

Test connection to verify configuration and accessibility.

```bash
# Test specific connection
sqlflow connect test my_postgres

# Test with verbose output
sqlflow connect test my_postgres --verbose

# Test with specific profile
sqlflow connect test my_postgres --profile prod
```

**Success Output:**
```
✅ Connection 'my_postgres' test successful!
  🔗 Host: localhost:5432
  📊 Database: analytics_db
  👤 User: sqlflow_user
  ⏱️  Connection time: 45ms
```

**Error Output:**
```
❌ Connection 'my_postgres' test failed!
  🚫 Error: Connection refused (host: localhost:5432)
  💡 Check if PostgreSQL is running
  💡 Verify host and port in profiles/dev.yml
```

## UDF Management

### `sqlflow udf list`

List all available Python UDFs in the project.

```bash
sqlflow udf list
sqlflow udf list --verbose
```

**Output Example:**
```
🐍 Available Python UDFs:
  calculate_metrics    Scalar UDF: Calculate customer metrics
  process_orders       Table UDF: Process order data with enrichment
  validate_data        Scalar UDF: Data validation and cleansing

📁 UDF Directory: python_udfs/
💡 View detailed info with: sqlflow udf info <name>
```

### `sqlflow udf info`

Show detailed information about a specific Python UDF.

```bash
# Show detailed UDF information
sqlflow udf info calculate_metrics

# Show info with verbose output
sqlflow udf info process_orders --verbose
```

**Output Example:**
```
🐍 UDF: calculate_metrics (scalar)

📄 File: python_udfs/metrics.py
📝 Signature: calculate_metrics(revenue: float, orders: int) -> float

📋 Description:
Calculate customer lifetime value metrics based on revenue and order count.
Returns a normalized score between 0.0 and 1.0.

📊 Parameters:
  • revenue: float - Total customer revenue
  • orders: int - Number of orders placed

✅ Validation: Passed
```

### `sqlflow udf validate`

Validate all Python UDFs in the project.

```bash
# Validate all UDFs
sqlflow udf validate

# Validate with verbose output
sqlflow udf validate --verbose
```

**Output Example:**
```
🔍 Validating Python UDFs...

✅ calculate_metrics: Valid
✅ process_orders: Valid
⚠️  validate_data: Warning - Missing type hints for some parameters

📊 Validation Summary:
  Total UDFs: 3
  Valid: 2
  Warnings: 1
  Errors: 0
```

## Environment & Configuration

### `sqlflow env list`

List environment variables available to SQLFlow.

```bash
# List all environment variables
sqlflow env list

# List variables with specific prefix
sqlflow env list --prefix SQLFLOW_

# Show actual values (security warning)
sqlflow env list --show-values

# Plain text output for scripting
sqlflow env list --plain
```

**Output Example:**
```
Environment Variables
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
┃ Name               ┃ Status             ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
│ DATABASE_URL       │ ✓ Set              │
│ POSTGRES_HOST      │ ✓ Set              │
│ POSTGRES_PASSWORD  │ ✓ Set              │
│ API_KEY            │ ✗ Empty            │
└────────────────────┴────────────────────┘

💡 Use --show-values to see actual values (be careful with sensitive data)
```

### `sqlflow env get`

Get the value of a specific environment variable.

```bash
# Get specific variable
sqlflow env get DATABASE_URL

# Get with default value
sqlflow env get API_KEY --default "not_set"
```

### `sqlflow env check`

Check if a .env file exists in the SQLFlow project and show its status.

```bash
sqlflow env check
```

**Output Example:**
```
✅ .env file found: /path/to/project/.env
📊 Contains 5 variable(s)
📁 Project root: /path/to/project

📋 Variables in .env file:
  • DATABASE_URL
  • POSTGRES_HOST
  • POSTGRES_PASSWORD
  • ENVIRONMENT
  • DEBUG
```

### `sqlflow env template`

Create a sample .env file template in the current SQLFlow project.

```bash
sqlflow env template
```

**Output:**
```
✅ Created .env template at: /path/to/project/.env
📝 Edit the file to set your environment variables
💡 Variables will be automatically available in SQLFlow pipelines using ${VARIABLE_NAME}
```

## Development & Debugging

### `sqlflow logging_status`

Show the current logging configuration and status.

```bash
sqlflow logging_status
```

**Output Example:**
```
SQLFlow Logging Status
Root level: INFO

Module levels:
  sqlflow.cli: INFO
  sqlflow.core: INFO
  sqlflow.parser: INFO
  sqlflow.planner: INFO
  sqlflow.executor: INFO
```

## Exit Codes

SQLFlow CLI uses standard exit codes for integration with CI/CD systems:

| Code | Meaning | Description |
|------|---------|-------------|
| `0` | Success | Command completed successfully |
| `1` | General Error | Command failed due to user error or configuration |
| `2` | Syntax Error | Pipeline syntax or parsing error |
| `3` | Connection Error | Database or connection failure |
| `4` | Validation Error | Pipeline validation failed |
| `5` | Execution Error | Pipeline execution failed |
| `6` | Permission Error | Insufficient permissions |
| `7` | Resource Error | Out of memory or disk space |

## Configuration Files

### Profile Configuration (`profiles/dev.yml`)

```yaml
connections:
  my_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: analytics_db
    user: sqlflow_user
    password: ${POSTGRES_PASSWORD}
    
  s3_bucket:
    type: s3
    bucket: my-data-bucket
    region: us-east-1
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}

variables:
  environment: dev
  batch_size: 1000
  debug_mode: true
```

### Project Configuration (`sqlflow.yml`)

```yaml
project:
  name: my_analytics_project
  version: "1.0.0"
  
pipelines:
  directory: pipelines/
  
udfs:
  directory: python_udfs/
  
outputs:
  directory: output/
```

## Usage Patterns

### Development Workflow

```bash
# 1. Initialize project
sqlflow init my_project
cd my_project

# 2. Validate pipelines
sqlflow pipeline validate

# 3. Test connections
sqlflow connect test my_postgres

# 4. Run pipeline
sqlflow pipeline run customer_analytics

# 5. Check outputs
ls -la output/
```

### Production Deployment

```bash
# 1. Validate all pipelines
sqlflow pipeline validate --profile prod

# 2. Pre-compile for performance
sqlflow pipeline compile --profile prod

# 3. Run from compiled plans
sqlflow pipeline run customer_analytics --profile prod --from-compiled --quiet
```

### CI/CD Integration

```bash
# Validation pipeline
sqlflow pipeline validate --quiet
if [ $? -eq 0 ]; then
  echo "✅ Pipeline validation passed"
else
  echo "❌ Pipeline validation failed"
  exit 1
fi

# Execution pipeline
sqlflow pipeline run etl_pipeline --profile prod --quiet --from-compiled
```

### Performance Optimization

```bash
# Pre-compile pipelines
sqlflow pipeline compile --skip-validation

# Use compiled plans
sqlflow pipeline run customer_analytics --from-compiled

# Quiet mode for better performance
sqlflow pipeline run customer_analytics --quiet --from-compiled
```

## Tips & Best Practices

### Command Line Efficiency

```bash
# Use aliases for common commands
alias sf='sqlflow'
alias sfr='sqlflow pipeline run'
alias sfv='sqlflow pipeline validate'

# Chain commands for workflows
sqlflow pipeline validate customer_analytics && sqlflow pipeline run customer_analytics
```

### Error Handling

```bash
# Capture exit codes
sqlflow pipeline run customer_analytics --quiet
if [ $? -eq 0 ]; then
  echo "Pipeline succeeded"
else
  echo "Pipeline failed with code $?"
  # Handle specific error codes
fi
```

### Variable Management

```bash
# Use environment variables
export SQLFLOW_DATE=$(date +%Y-%m-%d)
sqlflow pipeline run daily_report --vars "date=${SQLFLOW_DATE}"

# Use configuration files
sqlflow pipeline run customer_analytics --vars @config/prod-vars.json
```

This reference provides comprehensive coverage of all SQLFlow CLI commands with verified examples, error handling, and usage patterns for different scenarios.