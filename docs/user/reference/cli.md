# SQLFlow CLI: Command Line Interface Guide

> **Comprehensive guide for using SQLFlow's command line interface for managing data pipelines, connections, and Python UDFs.**

## Overview

The SQLFlow Command Line Interface (CLI) provides a powerful set of tools for creating, managing, and executing SQL-based data pipelines. The CLI enables you to initialize projects, compile and run pipelines, manage connections, and work with Python User-Defined Functions (UDFs).

---

## Global CLI Options

SQLFlow CLI supports the following global options that can be used with any command:

| Option      | Alias | Description                                     |
|-------------|-------|-------------------------------------------------|
| `--verbose` | `-v`  | Enable verbose output with technical details.   |
| `--quiet`   | `-q`  | Reduce output to essential information only.    |

Example:
```bash
sqlflow --verbose pipeline run example
sqlflow -q pipeline list
```

---

## Installation

SQLFlow CLI is automatically installed when you install the SQLFlow package:

```bash
pip install sqlflow-core
```

You can verify the installation by checking the version:

```bash
sqlflow --version
```

---

## Getting Started

### Initializing a New Project

Create a new SQLFlow project with working sample data and pipelines:

```bash
sqlflow init my_project
cd my_project
```

This creates a new project with the following structure:
```
my_project/
├── pipelines/
│   ├── example.sf           # Basic example pipeline
│   ├── customer_analytics.sf # Customer data analysis pipeline
│   └── data_quality.sf      # Data quality monitoring pipeline
├── profiles/                # Environment configurations
│   ├── dev.yml             # Development profile (in-memory)
│   ├── prod.yml            # Production profile (persistent)
│   └── README.md           # Profile documentation
├── data/                   # Sample data (auto-generated)
│   ├── customers.csv       # Sample customer data (1,000 records)
│   ├── orders.csv          # Sample order data (5,000 records)
│   └── products.csv        # Sample product data (500 records)
├── python_udfs/            # Python User-Defined Functions
├── output/                 # Pipeline outputs (auto-created)
├── target/                 # Compiled plans and artifacts
└── README.md              # Quick start guide
```

### Quick Start (Under 2 Minutes!)

After initialization, you can immediately run working examples:

```bash
# Run the customer analytics pipeline (works immediately!)
sqlflow pipeline run customer_analytics

# View results
cat output/customer_summary.csv
cat output/top_customers.csv
```

### Initialization Options

#### Default (With Sample Data)
```bash
sqlflow init my_project
```
Creates a project with realistic sample data and multiple working pipelines ready to run.

#### Minimal Project  
```bash
sqlflow init my_project --minimal
```
Creates only the essential directory structure without sample data - similar to current behavior.

#### Demo Mode
```bash
sqlflow init my_project --demo
```
Initializes the project and immediately runs the customer analytics pipeline to show results.

### Auto-Generated Sample Data

When you run `sqlflow init` (default mode), SQLFlow automatically creates realistic sample datasets:

**customers.csv** (1,000 records):
```csv
customer_id,name,email,country,city,signup_date,age,tier
1,Alice Johnson,alice@example.com,US,New York,2023-01-15,28,gold
2,Bob Smith,bob@example.com,UK,London,2023-02-20,34,silver
3,Maria Garcia,maria@example.com,Spain,Madrid,2023-01-20,31,bronze
...
```

**orders.csv** (5,000 records):
```csv
order_id,customer_id,product_id,quantity,price,order_date,status
1,1,101,2,29.99,2023-03-01,completed
2,1,102,1,15.99,2023-03-05,completed
3,2,103,3,45.50,2023-03-02,completed
...
```

**products.csv** (500 records):
```csv
product_id,name,category,price,stock_quantity,supplier
101,Wireless Headphones,Electronics,29.99,150,TechCorp
102,Coffee Mug,Home,15.99,200,HomeGoods
103,Running Shoes,Sports,45.50,75,SportsCorp
...
```

### Ready-to-Run Pipelines

SQLFlow creates multiple working pipelines during initialization:

#### customer_analytics.sf
```sql
-- Customer Analytics Pipeline
-- Analyzes customer behavior and creates summaries

-- Load data using DuckDB's read_csv_auto function
CREATE TABLE customers AS
SELECT * FROM read_csv_auto('data/customers.csv');

CREATE TABLE orders AS
SELECT * FROM read_csv_auto('data/orders.csv');

CREATE TABLE products AS
SELECT * FROM read_csv_auto('data/products.csv');

-- Create customer summary by country and tier
CREATE TABLE customer_summary AS
SELECT 
    c.country,
    c.tier,
    COUNT(*) as customer_count,
    AVG(c.age) as avg_age,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.price * o.quantity), 0) as total_revenue
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.country, c.tier
ORDER BY total_revenue DESC;

-- Find top customers by spending
CREATE TABLE top_customers AS
SELECT 
    c.name,
    c.email,
    c.tier,
    c.country,
    COUNT(o.order_id) as order_count,
    SUM(o.price * o.quantity) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email, c.tier, c.country
ORDER BY total_spent DESC
LIMIT 20;

-- Export results
EXPORT SELECT * FROM customer_summary 
TO 'output/customer_summary.csv' 
TYPE CSV OPTIONS { "header": true };

EXPORT SELECT * FROM top_customers 
TO 'output/top_customers.csv' 
TYPE CSV OPTIONS { "header": true };
```

#### data_quality.sf
```sql
-- Data Quality Pipeline
-- Monitors data quality and creates reports

CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');
CREATE TABLE orders AS SELECT * FROM read_csv_auto('data/orders.csv');
CREATE TABLE products AS SELECT * FROM read_csv_auto('data/products.csv');

-- Check for data quality issues
CREATE TABLE data_quality_report AS
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as missing_emails,
    COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) as missing_countries,
    COUNT(CASE WHEN age < 0 OR age > 120 THEN 1 END) as invalid_ages
FROM customers

UNION ALL

SELECT 
    'orders' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN price IS NULL OR price <= 0 THEN 1 END) as invalid_prices,
    COUNT(CASE WHEN order_date IS NULL THEN 1 END) as missing_dates,
    COUNT(CASE WHEN quantity IS NULL OR quantity <= 0 THEN 1 END) as invalid_quantities
FROM orders

UNION ALL

SELECT 
    'products' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN price IS NULL OR price <= 0 THEN 1 END) as invalid_prices,
    COUNT(CASE WHEN stock_quantity IS NULL OR stock_quantity < 0 THEN 1 END) as invalid_stock,
    COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as missing_names
FROM products;

-- Export quality report
EXPORT SELECT * FROM data_quality_report 
TO 'output/data_quality_report.csv' 
TYPE CSV OPTIONS { "header": true };
```

---

## Pipeline Management

The `pipeline` command group handles all pipeline-related operations.

### Listing Pipelines

List all available pipelines in your project:

```bash
sqlflow pipeline list
```

### Compiling a Pipeline

Compile a pipeline to validate its syntax and generate the execution plan:

```bash
sqlflow pipeline compile example
```

To save the compiled plan to a specific file:
```bash
sqlflow pipeline compile example --output target/compiled/custom_plan_name.json
```
If `--output` is not provided, the plan is saved to `target/compiled/<pipeline_name>.json` by default.
If compiling all pipelines (no specific pipeline name given), the `--output` flag is ignored, and each plan is saved to its default location.

> **Note:** Like the run command, always use just the pipeline name without path or file extension.

### Running a Pipeline

Execute a pipeline with optional variables:

```bash
sqlflow pipeline run example --profile dev --vars '{"date": "2023-10-25"}'
```

To run a pipeline from a previously compiled plan:
```bash
sqlflow pipeline run example --from-compiled
```
This will use the plan from `target/compiled/example.json` instead of recompiling.

### Pipeline Validation

Validate pipeline syntax and configuration without executing them. This helps catch errors early and ensures your pipelines are correctly configured.

#### Validate a Single Pipeline

```bash
# Validate a specific pipeline
sqlflow pipeline validate example

# Validate with detailed error information
sqlflow pipeline validate example --verbose

# Validate with minimal output
sqlflow pipeline validate example --quiet
```

#### Validate All Pipelines

```bash
# Validate all pipelines in the project
sqlflow pipeline validate

# Validate all with summary report
sqlflow pipeline validate --verbose

# Quick validation check
sqlflow pipeline validate --quiet
```

#### Clear Validation Cache

```bash
# Clear validation cache and re-validate
sqlflow pipeline validate example --clear-cache
```

#### Validation Output Examples

**Successful Validation:**
```
✅ customer_analytics
✅ example  
✅ data_quality

📊 Summary: 3 pipelines validated successfully
```

**Failed Validation:**
```
❌ Validation failed for broken_pipeline.sf

📋 Pipeline: broken_pipeline
❌ Pipeline validation failed with 3 error(s):

📋 Connector Errors:
  Line 2: Unknown connector type: MYSQL
    💡 Available connector types: CSV, POSTGRES, S3
    💡 Check the connector type spelling and case

📋 Parameter Errors:
  Line 5: Missing required parameter 'path'
    💡 Add "path": "your_file.csv" to the PARAMS
    💡 Check the CSV connector documentation

📋 Reference Errors:
  Line 8: LOAD references undefined source: 'products'
    💡 Define SOURCE 'products' before using it in LOAD
    💡 Check the source name spelling
    💡 Available sources: users, orders

📊 Summary: 3 errors found across 3 error types
```

**Multiple Errors - Enhanced Reporting:**
```
❌ broken_pipeline

📋 Pipeline validation failed with 5 error(s):

📋 Connector Errors:
  Line 2: Unknown connector type: MYSQL  
    💡 Available connector types: CSV, POSTGRES, S3
  Line 15: Unknown connector type: ORACLE
    💡 Available connector types: CSV, POSTGRES, S3

📋 Parameter Errors:
  Line 5: Missing required parameter 'path'
    💡 Add "path": "your_file.csv" to the PARAMS
  Line 10: Field 'path' does not match required pattern: .*\.csv$
    💡 CSV files should end with .csv extension

📋 Reference Errors:
  Line 20: LOAD references undefined source: 'products'
    💡 Define SOURCE 'products' before using it in LOAD
    💡 Available sources: users, orders

📊 Summary: 2 Connector Errors, 2 Parameter Errors, 1 Reference Error
```

**Mixed Results (Bulk Validation):**
```
✅ customer_analytics
✅ example
❌ broken_pipeline
✅ data_quality

📊 Validation Summary:
  Total pipelines: 4
  Passed: 3
  Failed: 1
  Total errors: 3

❌ Failed pipelines: broken_pipeline
```

#### Validation Features

- **Comprehensive Error Reporting**: Reports all validation errors at once, grouped by type (Connector, Parameter, Reference errors)
- **Type Safety**: Validates connector types and required parameters
- **Reference Checking**: Ensures SOURCE references exist in LOAD statements
- **Parameter Validation**: Checks required and optional parameters for each connector
- **File Extension Validation**: Verifies file extensions match connector types
- **Intelligent Error Grouping**: Groups errors by type and sorts by line number for easier debugging
- **Helpful Suggestions**: Provides specific suggestions for fixing errors
- **Caching**: Caches validation results for faster subsequent checks
- **Batch Processing**: Validates multiple pipelines efficiently

### Command Options

| Option      | Description                                     |
|-------------|-------------------------------------------------|
| `--profile` | Specify the connection profile to use           |
| `--vars`    | JSON string of variables to pass to the pipeline|
| `--dry-run` | Validate and compile without executing          |
| `--output`  | (compile only) Specify output file for the plan |
| `--from-compiled` | (run only) Use existing compiled plan       |
| `--skip-validation` | (compile only) Skip validation step for CI/CD performance |
| `--clear-cache` | (validate only) Clear validation cache       |
| `--verbose` | Enable detailed output with technical information |
| `--quiet`   | Reduce output to essential information only      |

### Automatic Validation Integration

Validation is automatically integrated into other pipeline commands:

#### Run Command with Validation
```bash
# Automatically validates before execution
sqlflow pipeline run customer_analytics

# If validation fails, execution is prevented:
# ❌ Validation failed - stopping execution
# Use 'sqlflow pipeline validate customer_analytics' for details
```

#### Compile Command with Validation
```bash
# Automatically validates before compilation
sqlflow pipeline compile customer_analytics

# Skip validation for CI/CD speed (advanced users only)
sqlflow pipeline compile customer_analytics --skip-validation
```

---

## Speed Comparison: SQLFlow vs Competitors

| Framework | Setup Time | First Results | Learning Curve | Sample Data |
|-----------|------------|---------------|----------------|-------------|
| **SQLFlow** | **30 seconds** | **1 minute** | **Low** - Just SQL | ✅ Auto-generated |
| dbt | 5 minutes | 15 minutes | Medium - Models + Jinja | ❌ Manual setup |
| SQLMesh | 10 minutes | 20 minutes | Medium - New concepts | ❌ Manual setup |
| Airflow | 30 minutes | 60 minutes | High - DAGs + Python | ❌ Manual setup |

### SQLFlow Speed Advantages

1. **Instant Sample Data**: Realistic datasets created automatically during init
2. **Working Examples**: Multiple pipelines ready to run immediately  
3. **Zero Configuration**: Profiles pre-configured for immediate use
4. **Pure SQL**: No new syntax or templating language to learn
5. **One Command Demo**: `sqlflow init my_project --demo`

### Typical First Experience

**SQLFlow (New Enhanced Init):**
```bash
pip install sqlflow-core     # 30 seconds
sqlflow init my_project      # 15 seconds  
cd my_project
sqlflow pipeline run customer_analytics  # 15 seconds
cat output/customer_summary.csv         # Immediate results!
# Total: Under 2 minutes to working results
```

**dbt:**
```bash
pip install dbt-core         # 1 minute
dbt init my_project          # 1 minute
# Edit profiles.yml           # 5 minutes
# Create sample data          # 5 minutes  
# Write first model           # 5 minutes
dbt run                      # 1 minute
# Total: 15+ minutes to working results
```

---

## Connection Management

The `connect`