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

**Options:**
- `--minimal`: Create minimal project structure without sample data
- `--demo`: Initialize project and run a demo pipeline immediately

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

**Options:**
- `--profile`, `-p`: Profile to use (default: dev)
- `--quiet`, `-q`: Reduce output to essential information only
- `--verbose`, `-v`: Enable verbose output with technical details

**Output Example:**
```
Available pipelines:
  - customer_analytics
  - data_quality
  - example
```

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

**Options:**
- `--profile`, `-p`: Profile to use (default: dev)
- `--quiet`, `-q`: Reduce output to essential information only
- `--verbose`, `-v`: Enable verbose output with technical details

**Success Output:**
```
✅ Pipeline 'customer_analytics' validation passed!
```

**Error Output:**
```
❌ Pipeline validation failed with 2 error(s):

📋 Syntax Errors:
  Line 3: Unexpected token '{' - expected string literal
    SOURCE orders TYPE CSV PARAMS {
                                  ^
    💡 Check parameter formatting: {"key": "value"}
```

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

**Options:**
- `--output`: Custom output file for the execution plan (only applies when a single pipeline is provided)
- `--vars`: Pipeline variables as JSON or key=value pairs
- `--profile`, `-p`: Profile to use (default: dev)
- `--skip-validation`: Skip validation before compilation (for CI/CD performance)
- `--quiet`, `-q`: Reduce output to essential information only
- `--verbose`, `-v`: Enable verbose output with technical details

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
📝 Compiling customer_analytics
Pipeline: pipelines/customer_analytics.sf

✅ Compilation successful!
📄 Execution plan: target/compiled/customer_analytics.json
🔢 Total operations: 5

📋 Operations by type:
  • source_definition: 3
  • load: 2
  • export: 2

🔗 Execution order:
   1. source_customers (source_definition)
   2. source_orders (source_definition)
   3. source_products (source_definition)
   4. load_customer_data (load)
   5. load_analytics (load)

💾 Plan saved to: target/compiled/customer_analytics.json
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

**Options:**
- `--vars`: Pipeline variables as JSON or key=value pairs
- `--profile`, `-p`: Profile to use (default: dev)
- `--from-compiled`: Use existing compilation in target/compiled/ instead of recompiling
- `--quiet`, `-q`: Reduce output to essential information only
- `--verbose`, `-v`: Enable verbose output with technical details

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
[SQLFlow] Using profile: dev
🚨 Running in DuckDB memory mode: results will NOT be saved after process exit.
Running pipeline: pipelines/customer_analytics.sf

📝 Compiling customer_analytics.sf
⏱️  Starting execution at 14:23:01

📥 Loaded customers (1,000 rows)
📥 Loaded orders (5,000 rows)
📥 Loaded products (500 rows)
🔄 Created customer_data (1,000 rows)
🔄 Created analytics (100 rows)
📤 Exported customer_summary.csv (100 rows)
📤 Exported top_customers.csv (20 rows)

✅ Pipeline completed successfully
⏱️  Execution completed in 2.3 seconds
```

## Connection Management

### `sqlflow connect list`

List all configured connections in the current profile.

```bash
sqlflow connect list
sqlflow connect list --profile prod
```

**Options:**
- `--profile`: Profile to use (default: dev)

**Output Example:**
```
Connections in profile 'dev':
----------------------------------------
NAME                 TYPE            STATUS
----------------------------------------
my_postgres          postgres        ✓ Ready
data_warehouse       postgres        ✓ Ready
s3_bucket           s3              ? Unknown
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

**Options:**
- `--profile`: Profile to use (default: dev)
- `--verbose`, `-v`: Show detailed connection info

**Success Output:**
```
✓ Connection to 'my_postgres' (postgres) succeeded.
```

**Verbose Output:**
```
Testing connection 'my_postgres':
Type: postgres
Parameters:
  host: localhost
  port: 5432
  database: analytics_db

Result:
✓ Connection to 'my_postgres' (postgres) succeeded.
```

**Error Output:**
```
✗ Connection to 'my_postgres' (postgres) failed: Connection refused (host: localhost:5432)
```

## UDF Management

### `sqlflow udf list`

List all available Python UDFs in the project.

```bash
sqlflow udf list
sqlflow udf list --verbose
sqlflow udf list --project-dir /path/to/project
```

**Options:**
- `--project-dir`, `-p`: Project directory
- `--verbose`, `-v`: Show detailed information
- `--plain`: Use plain text output (for testing)

**Output Example:**
```
Python UDFs
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
┃ Name               ┃ Type               ┃ Signature          ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
│ calculate_metrics  │ scalar             │ (revenue, orders)  │
│ process_orders     │ table              │ (df)               │
│ validate_data      │ scalar             │ (value)            │
└────────────────────┴────────────────────┴────────────────────┘
```

### `sqlflow udf info`

Show detailed information about a specific Python UDF.

```bash
# Show detailed UDF information
sqlflow udf info calculate_metrics

# Show info with verbose output
sqlflow udf info process_orders --verbose

# Use specific project directory
sqlflow udf info calculate_metrics --project-dir /path/to/project
```

**Options:**
- `--project-dir`, `-p`: Project directory
- `--plain`: Use plain text output (for testing)

**Output Example:**
```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ calculate_metrics (scalar)                                           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Full Name: metrics.calculate_metrics                                  │
│ File: python_udfs/metrics.py                                          │
│ Signature: calculate_metrics(revenue: float, orders: int) -> float    │
│                                                                        │
│ Description:                                                           │
│ Calculate customer lifetime value metrics based on revenue and order  │
│ count. Returns a normalized score between 0.0 and 1.0.                │
│                                                                        │
│ Parameters:                                                            │
│ ┏━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ │
│ ┃ Name    ┃ Type  ┃ Default ┃ Description                            ┃ │
│ ┡━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩ │
│ │ revenue │ float │ -       │ Total customer revenue                 │ │
│ │ orders  │ int   │ -       │ Number of orders placed               │ │
│ └─────────┴───────┴─────────┴────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

### `sqlflow udf validate`

Validate all Python UDFs in the project.

```bash
# Validate all UDFs
sqlflow udf validate

# Validate with verbose output
sqlflow udf validate --verbose

# Use specific project directory
sqlflow udf validate --project-dir /path/to/project
```

**Options:**
- `--project-dir`, `-p`: Project directory
- `--plain`: Use plain text output (for testing)

**Output Example:**
```
Validating 3 UDFs...

UDF Validation Results
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ UDF Name            ┃ Status      ┃ Warnings                                                                                                ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ calculate_metrics   │ Valid       │                                                                                                         │
│ process_orders      │ Valid       │                                                                                                         │
│ validate_data       │ Invalid     │ • Missing type hints for some parameters                                                               │
└─────────────────────┴─────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────┘

✅ All UDFs are valid!
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

**Options:**
- `--prefix`, `-p`: Filter variables by prefix (e.g., 'SQLFLOW_')
- `--plain`: Use plain text output (for scripting)
- `--show-values`: Show variable values (security warning: visible in terminal)

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

**Options:**
- `--default`, `-d`: Default value if variable is not set

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

# Use .env files for persistent variables
sqlflow env template
# Edit .env file with your variables
sqlflow pipeline run customer_analytics  # Variables loaded automatically
```

This reference provides comprehensive coverage of all SQLFlow CLI commands with verified examples, error handling, and usage patterns for different scenarios.