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
| `--version` | | Show SQLFlow version and exit |
| `--verbose` | `-v` | Enable verbose output |

## Project Management

### `sqlflow init`

Initialize a new SQLFlow project with sample data and working pipelines.

```bash
# Create project (recommended)
sqlflow init my_project

# Create in specific directory
sqlflow init my_project --directory /path/to/projects

# Create minimal project structure
sqlflow init my_project --minimal
```

**Options:**
- `--directory`, `-d`: Directory to create project in (default: project_name)
- `--minimal`: Create minimal project structure without examples

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

### `sqlflow status`

Show SQLFlow project status and configuration.

```bash
sqlflow status
```

Displays information about the current project, profiles, pipelines, and environment configuration.

### `sqlflow version`

Show SQLFlow version information.

```bash
sqlflow version
```

Shows version details including SQLFlow, Python, and dependency versions.

## Pipeline Commands

### `sqlflow pipeline list`

List all available pipelines in the current project.

```bash
sqlflow pipeline list
sqlflow pipeline list --profile prod
sqlflow pipeline list --format json
```

**Options:**
- `--profile`, `-p`: Profile to use (default: dev)
- `--format`: Output format: table or json (default: table)

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

# Validate with profile
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

Parse and compile pipeline to execution plan without running. Shows interactive selection if no pipeline specified.

```bash
# Compile specific pipeline
sqlflow pipeline compile customer_analytics

# Auto-interactive selection (no pipeline name)
sqlflow pipeline compile

# Compile with custom output directory
sqlflow pipeline compile customer_analytics --output-dir target/my_plans

# Compile with profile
sqlflow pipeline compile customer_analytics --profile prod
```

**Options:**
- `--output-dir`: Output directory for compilation results (default: target)
- `--variables`, `--vars`: Pipeline variables as JSON string
- `--profile`, `-p`: Profile to use (default: dev)

**With Variables:**
```bash
# JSON format
sqlflow pipeline compile customer_analytics --vars '{"date": "2023-10-25", "region": "us-east"}'
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

Execute a complete pipeline from source to output. Shows interactive selection if no pipeline specified.

```bash
# Run specific pipeline with default profile
sqlflow pipeline run customer_analytics

# Auto-interactive selection (no pipeline name)
sqlflow pipeline run

# Run with specific profile
sqlflow pipeline run customer_analytics --profile prod

# Run with variables
sqlflow pipeline run customer_analytics --vars '{"date": "2023-10-25"}'

# Show detailed execution summary
sqlflow pipeline run customer_analytics --summary
```

**Options:**
- `--variables`, `--vars`: Pipeline variables as JSON string
- `--profile`, `-p`: Profile to use (default: dev)
- `--summary`: Show detailed execution summary after completion

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

## Profile Management

### `sqlflow profiles list`

List all available profiles in the project.

```bash
sqlflow profiles list
sqlflow profiles list --format json
```

**Options:**
- `--project-dir`: Project directory (default: current directory)
- `--format`: Output format: table or json (default: table)
- `--quiet`, `-q`: Reduce output to essential information only

### `sqlflow profiles validate`

Validate profile configuration.

```bash
# Validate specific profile
sqlflow profiles validate dev

# Validate all profiles
sqlflow profiles validate
```

**Options:**
- `--project-dir`: Project directory (default: current directory)
- `--verbose`, `-v`: Show detailed validation information

### `sqlflow profiles show`

Show detailed profile configuration.

```bash
sqlflow profiles show dev
sqlflow profiles show dev --section connectors
```

**Options:**
- `--project-dir`: Project directory (default: current directory)
- `--section`: Show only specific section (connectors, variables, engines)

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
sqlflow udf list --format json
sqlflow udf list --plain
```

**Options:**
- `--profile`, `-p`: Profile to use (default: dev)
- `--format`: Output format: table or json (default: table)
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

# Show info with plain text output
sqlflow udf info process_orders --plain

# Use specific profile
sqlflow udf info calculate_metrics --profile prod
```

**Options:**
- `--profile`, `-p`: Profile to use (default: dev)
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

# Validate specific UDF
sqlflow udf validate my_udf_function

# Use specific profile
sqlflow udf validate --profile prod
```

**Options:**
- `--profile`, `-p`: Profile to use (default: dev)

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

## Migration & Modernization

### `sqlflow migrate to-profiles`

Convert a pipeline from inline syntax to profile-based configuration.

```bash
# Basic migration
sqlflow migrate to-profiles customer_analytics.sf

# Custom profile directory and name
sqlflow migrate to-profiles customer_analytics.sf --profile-dir profiles --profile-name production

# Preview changes without writing files
sqlflow migrate to-profiles customer_analytics.sf --dry-run

# Force overwrite existing profiles
sqlflow migrate to-profiles customer_analytics.sf --force
```

**Options:**
- `--profile-dir`: Directory to store generated profiles (default: profiles)
- `--profile-name`: Name for the generated profile (default: auto-generated)
- `--dry-run`: Preview changes without writing files
- `--force`: Overwrite existing profile files

### `sqlflow migrate extract-profiles`

Extract profiles from multiple pipeline files into a unified configuration.

```bash
# Extract from directory
sqlflow migrate extract-profiles pipelines/

# Custom output file
sqlflow migrate extract-profiles pipelines/ --output profiles/extracted.yml

# Don't merge similar configurations
sqlflow migrate extract-profiles pipelines/ --merge-similar=false

# Preview extraction
sqlflow migrate extract-profiles pipelines/ --dry-run
```

**Options:**
- `--output`: Output profile file path (default: profiles/extracted.yml)
- `--merge-similar`: Merge similar connector configurations (default: true)
- `--dry-run`: Preview extracted profiles without writing

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