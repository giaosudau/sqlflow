# SQLFlow Getting Started Guide

> **Get working results in under 2 minutes with SQLFlow's enhanced initialization!**

This guide shows you how to get started with SQLFlow - from installation to running your first analytics pipeline in under 2 minutes.

## Prerequisites

- Python 3.10+ or higher
- Pip (Python package installer)
- Basic knowledge of SQL

## 1. Installation

Install SQLFlow using pip:

```bash
# Install SQLFlow with everything you need for analytics
pip install sqlflow-core
```

**What's included in the basic installation:**
- DuckDB engine for fast analytical queries
- Apache Arrow for high-performance data processing  
- Pandas for data manipulation
- Rich CLI with beautiful output
- All core functionality for complete workflows

**Need database connectivity?**
```bash
# Add PostgreSQL support
pip install "sqlflow-core[postgres]"

# Add cloud storage (AWS S3 + Google Cloud)
pip install "sqlflow-core[cloud]"

# Everything included  
pip install "sqlflow-core[all]"
```

You can verify the installation was successful by checking the version:

```bash
sqlflow --version
```
You should see output similar to:
```
SQLFlow version: x.y.z
```

## 2. Create Your First SQLFlow Project (Enhanced)

SQLFlow now provides the **fastest getting started experience** of any data pipeline tool:

```bash
# Create project with realistic sample data and working pipelines
sqlflow init my_first_project
cd my_first_project
```

This creates a comprehensive project structure with **everything ready to run**:

```
my_first_project/
├── pipelines/              # SQL pipeline files (.sf)
│   ├── example.sf          # Basic example with inline data
│   ├── customer_analytics.sf # Customer behavior analysis
│   └── data_quality.sf     # Data quality monitoring
├── profiles/               # Environment configurations
│   ├── dev.yml            # Development profile (in-memory)
│   ├── prod.yml           # Production profile (persistent)
│   └── README.md          # Profile documentation
├── data/                  # Auto-generated sample data
│   ├── customers.csv      # 1,000 realistic customer records
│   ├── orders.csv         # 5,000 order transactions
│   └── products.csv       # 500 product catalog entries
├── output/                # Pipeline outputs (auto-created)
├── target/                # Compiled plans and artifacts
├── python_udfs/           # Python User-Defined Functions
└── README.md             # Quick start guide with examples
```

## 3. Immediate Results (Under 2 Minutes!)

**No configuration needed** - run analytics immediately:

```bash
# Run customer analytics (works right away!)
sqlflow pipeline run customer_analytics

# View results
cat output/customer_summary.csv
cat output/top_customers.csv
```

**That's it!** You now have working customer analytics with realistic data.

## 4. Initialization Options

### Default Mode (Recommended)
```bash
sqlflow init my_project
```
- Auto-generates realistic sample data (1,000 customers, 5,000 orders, 500 products)
- Creates multiple working pipelines ready to run
- Perfect for learning, demos, and prototyping

### Minimal Mode
```bash
sqlflow init my_project --minimal
```
- Basic project structure only
- Simple example pipeline with inline data
- No sample data generation
- Best for production projects or experienced users

### Demo Mode
```bash
sqlflow init my_project --demo
```
- Full setup + automatically runs customer analytics pipeline
- Shows immediate results
- Perfect for live demonstrations

## 5. Understanding the Auto-Generated Sample Data

SQLFlow creates realistic datasets automatically:

### Customers (1,000 records)
```csv
customer_id,name,email,country,city,signup_date,age,tier
1,Alice Johnson,alice@example.com,US,New York,2023-01-15,28,gold
2,Bob Smith,bob@example.com,UK,London,2023-02-20,34,silver
3,Maria Garcia,maria@example.com,Spain,Madrid,2023-01-20,31,bronze
...
```

### Orders (5,000 records)
```csv
order_id,customer_id,product_id,quantity,price,order_date,status
1,1,101,2,29.99,2023-03-01,completed
2,1,102,1,15.99,2023-03-05,completed
3,2,103,3,45.50,2023-03-02,completed
...
```

### Products (500 records)
```csv
product_id,name,category,price,stock_quantity,supplier
101,Wireless Headphones,Electronics,29.99,150,TechCorp
102,Coffee Mug,Home,15.99,200,HomeGoods
103,Running Shoes,Sports,45.50,75,SportsCorp
...
```

## 6. Understanding Profiles

SQLFlow uses profiles to configure environments (both are auto-created):

**dev.yml** (default):
```yaml
engines:
  duckdb:
    mode: memory        # Fast, in-memory execution
    memory_limit: 2GB   # Memory limit for DuckDB
log_level: info
```

**prod.yml** (persistent):
```yaml
engines:
  duckdb:
    mode: persistent    # Saves data to disk
    path: data/sqlflow_prod.duckdb
    memory_limit: 4GB
log_level: warning
```

## 7. Ready-to-Run Pipelines

### Basic Example (example.sf)
```sql
-- Basic Example Pipeline
-- Simple demonstration of SQLFlow capabilities

CREATE TABLE sample_data AS
SELECT * FROM VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob', 'bob@example.com'),
  (3, 'Charlie', 'charlie@example.com')
AS t(id, name, email);

CREATE TABLE processed_data AS
SELECT 
  id,
  name,
  email,
  UPPER(name) AS name_upper,
  LENGTH(name) AS name_length
FROM sample_data;

EXPORT
  SELECT * FROM processed_data
TO "output/example_results.csv"
TYPE CSV
OPTIONS { "header": true };
```

### Customer Analytics (customer_analytics.sf)
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
EXPORT
  SELECT * FROM customer_summary
TO "output/customer_summary.csv"
TYPE CSV
OPTIONS { "header": true };

EXPORT
  SELECT * FROM top_customers
TO "output/top_customers.csv"
TYPE CSV
OPTIONS { "header": true };
```

### Data Quality Monitoring (data_quality.sf)
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
EXPORT
  SELECT * FROM data_quality_report
TO "output/data_quality_report.csv"
TYPE CSV
OPTIONS { "header": true };
```

## 8. Pipeline Validation - Catch Errors Early

SQLFlow includes powerful built-in validation to catch common errors before execution, saving you time and preventing pipeline failures.

### Validate Individual Pipelines

```bash
# Validate a specific pipeline without running it
sqlflow pipeline validate customer_analytics

# Validate with detailed output
sqlflow pipeline validate customer_analytics --verbose
```

### Validate All Pipelines

```bash
# Validate all pipelines in your project
sqlflow pipeline validate

# Quick validation check (minimal output)
sqlflow pipeline validate --quiet
```

### Example: Catching Common Errors

Let's create a pipeline with intentional errors to see validation in action:

```bash
# Create a pipeline with missing required parameters
cat > pipelines/test_validation.sf << EOF
-- Pipeline with validation errors
SOURCE missing_path TYPE csv PARAMS {
    "delimiter": ","
};

SOURCE invalid_type TYPE unknown_connector PARAMS {
    "path": "data/test.csv"
};
EOF

# Validate the broken pipeline
sqlflow pipeline validate test_validation
```

You'll see helpful error messages like:
```
❌ Validation failed for test_validation.sf

📋 Pipeline: test_validation
❌ SOURCE missing_path: Missing required parameter 'path'
💡 Suggestion: Add "path": "your_file.csv" to the PARAMS

❌ SOURCE invalid_type: Unknown connector type 'unknown_connector'
💡 Suggestion: Use one of: csv, postgresql, s3, bigquery

📊 Summary: 2 errors found
```

### Automatic Validation in Commands

Validation runs automatically when you:
- **Compile**: `sqlflow pipeline compile` validates before compilation
- **Run**: `sqlflow pipeline run` validates before execution

```bash
# These commands automatically validate first
sqlflow pipeline run test_validation  # Will fail validation and stop
sqlflow pipeline compile test_validation  # Will fail validation and stop
```

### Skip Validation (Advanced Users Only)

For compilation in CI/CD environments where speed matters:

```bash
# Skip validation during compilation (not recommended for development)
sqlflow pipeline compile customer_analytics --no-validate
```

**Note**: The `run` command always validates for safety - there's no `--no-validate` option.

## 9. Run All the Examples

```bash
# Basic example (inline data)
sqlflow pipeline run example
cat output/example_results.csv

# Customer analytics (realistic data)
sqlflow pipeline run customer_analytics
cat output/customer_summary.csv
cat output/top_customers.csv

# Data quality monitoring
sqlflow pipeline run data_quality
cat output/data_quality_report.csv

# List all available pipelines
sqlflow pipeline list
```

## 10. Using Production Mode

Run with persistent storage (data saved to disk):

```bash
# Use production profile for persistent results
sqlflow pipeline run customer_analytics --profile prod

# Data is now saved to data/sqlflow_prod.duckdb
# Results persist between runs
```

## 11. Working with Variables

SQLFlow supports variable substitution for dynamic pipelines:

```bash
# Create a parameterized pipeline
cat > pipelines/parameterized_report.sf << EOF
-- Parameterized report pipeline
SET report_date = "\${date|2023-05-19}";
SET region_filter = "\${region|all}";

-- Use the auto-generated customer data
CREATE TABLE customers AS
SELECT * FROM read_csv_auto('data/customers.csv');

-- Apply region filter if specified
CREATE TABLE filtered_customers AS
SELECT * FROM customers 
WHERE 
  CASE 
    WHEN '\${region_filter}' = 'all' THEN true
    ELSE country = '\${region_filter}'
  END;

-- Create report
CREATE TABLE customer_report AS
SELECT
  COUNT(*) AS total_customers,
  '\${region_filter}' AS filtered_region,
  '\${report_date}' AS report_date
FROM filtered_customers;

-- Export the report
EXPORT
  SELECT * FROM customer_report
TO "output/customer_report_\${region_filter}_\${report_date}.csv"
TYPE CSV
OPTIONS { "header": true };
EOF

# Run with custom variables
sqlflow pipeline run parameterized_report --vars '{"region": "US", "date": "2023-05-19"}'

# Check the results
cat output/customer_report_US_2023-05-19.csv
```

## 12. Working with SQL Functions

SQLFlow supports standard SQL functions. Here's an advanced example:

```bash
# Create a pipeline with SQL string functions
cat > pipelines/advanced_features.sf << EOF
-- Advanced features pipeline using auto-generated data
CREATE TABLE customers AS
SELECT * FROM read_csv_auto('data/customers.csv');

-- Add computed features using SQL functions
CREATE TABLE customers_enhanced AS
SELECT
  customer_id,
  name,
  email,
  country,
  city,
  -- String manipulation with CONCAT (not ||)
  CONCAT(name, ' from ', city, ', ', country) AS full_description,
  -- String functions
  LENGTH(name) AS name_length,
  SUBSTR(name, 1, 1) AS name_first_letter,
  UPPER(country) AS country_upper,
  -- Date functions
  signup_date,
  EXTRACT(YEAR FROM CAST(signup_date AS DATE)) AS signup_year,
  -- Conditional logic
  CASE 
    WHEN age < 25 THEN 'Young'
    WHEN age < 50 THEN 'Middle-aged'
    ELSE 'Senior'
  END AS age_group
FROM customers;

-- Export enhanced data
EXPORT
  SELECT * FROM customers_enhanced
TO "output/customers_enhanced.csv"
TYPE CSV
OPTIONS { "header": true };
EOF

# Run the advanced pipeline
sqlflow pipeline run advanced_features
cat output/customers_enhanced.csv
```

## 13. Speed Comparison: Why SQLFlow is Fastest

| Framework | Setup Time | First Results | Sample Data | Working Examples |
|-----------|------------|---------------|-------------|------------------|
| **SQLFlow** | **30 seconds** | **1 minute** | ✅ Auto-generated | ✅ Multiple ready-to-run |
| dbt | 5 minutes | 15-20 minutes | ❌ Manual setup | ❌ Must create own |
| SQLMesh | 10 minutes | 20-30 minutes | ❌ Manual setup | ❌ Must create own |
| Airflow | 30 minutes | 30-60 minutes | ❌ Manual setup | ❌ Must create own |

**SQLFlow's advantages:**
1. **Instant Sample Data**: 1,000 customers, 5,000 orders, 500 products generated automatically
2. **Ready-to-Run Pipelines**: Three working examples included
3. **Zero Configuration**: Profiles pre-configured for immediate use
4. **Pure SQL**: No new syntax or templating language to learn

## 14. Next Steps

Now that you've experienced SQLFlow's speed, explore more advanced features:

### Customize Your Data
Replace the sample data with your own:
```bash
# Replace sample data with your CSV files
cp your_customers.csv data/customers.csv
cp your_orders.csv data/orders.csv

# Run the same analytics on your data
sqlflow pipeline run customer_analytics
```

### Create Custom Pipelines
```bash
# Create your own pipeline
cat > pipelines/my_analysis.sf << EOF
-- Your custom analysis
CREATE TABLE my_data AS
SELECT * FROM read_csv_auto('data/customers.csv');

-- Add your transformations here
CREATE TABLE my_results AS
SELECT 
  country,
  COUNT(*) as customer_count
FROM my_data
GROUP BY country
ORDER BY customer_count DESC;

EXPORT SELECT * FROM my_results 
TO 'output/my_analysis.csv' 
TYPE CSV OPTIONS { "header": true };
EOF

sqlflow pipeline run my_analysis
```

### Explore Advanced Features
- **Conditional Execution**: Use `IF/ELSE` statements in your pipelines
- **Multiple Connectors**: Connect to PostgreSQL, S3, or REST APIs
- **Complex Transformations**: Build multi-stage transformations
- **Python UDFs**: Create custom Python functions

For comprehensive examples:
```bash
# Clone the repository to access more examples
git clone https://github.com/sqlflow/sqlflow.git
cd sqlflow

# Explore the ecommerce demo
ls examples/ecommerce/
cat examples/ecommerce/README.md
```

## Troubleshooting

### Common Issues and Solutions

1. **Pipeline Validation Errors**: When validation fails, read the error messages carefully:
   ```bash
   # Always validate first when debugging
   sqlflow pipeline validate my_pipeline --verbose
   
   # Common issues and fixes:
   # - Missing required parameters: Add missing PARAMS
   # - Unknown connector types: Check supported connector list
   # - Invalid file paths: Verify file exists and path is correct
   # - Syntax errors: Check SQL syntax and SQLFlow directives
   ```

2. **Best Practice - Use Inline Data**: For learning and testing, use `VALUES` clauses instead of CSV files:
   ```sql
   CREATE TABLE test_data AS
   SELECT * FROM VALUES
     (1, 'Alice', 'alice@example.com'),
     (2, 'Bob', 'bob@example.com')
   AS t(id, name, email);
   ```

3. **String Concatenation**: Use `CONCAT()` function instead of `||`:
   ```sql
   -- Correct
   SELECT CONCAT(name, ' - ', country) AS description FROM customers;
   
   -- Avoid
   SELECT name || ' - ' || country AS description FROM customers;
   ```

4. **Variable Substitution**: Ensure proper escaping:
   ```sql
   SET region = "\${region|US}";  -- Note the quotes
   ```

5. **Memory vs Persistent Mode**: 
   - Use `dev` profile for fast testing (in-memory)
   - Use `prod` profile to save results (persistent)

6. **Output Files**: If output files aren't created, check:
   - The `output/` directory exists (auto-created in new projects)
   - You're using the correct profile mode
   - Pipeline completed successfully without errors
   - Validate the pipeline first: `sqlflow pipeline validate <pipeline_name>`

### Getting Help

- **CLI Help**: `sqlflow --help` or `sqlflow pipeline --help`
- **Pipeline Validation**: `sqlflow pipeline validate <pipeline_name>` - **Start here for any pipeline issues!**
- **Verbose Output**: `sqlflow --verbose pipeline run <pipeline_name>`
- **Validation with Details**: `sqlflow pipeline validate <pipeline_name> --verbose`

## What's Next?

You've successfully:
✅ Installed SQLFlow  
✅ Created a project with realistic sample data  
✅ Run customer analytics in under 2 minutes  
✅ Learned the basic commands and concepts  

Ready to build your own data pipelines? Check out:
- [CLI Reference](reference/cli.md) - Complete command documentation
- [Speed Comparison](reference/speed_comparison.md) - Detailed speed analysis
- [Pipeline Development Guide](guides/pipeline_development.md) - Advanced pipeline patterns
- [Python UDFs Guide](guides/python_udfs.md) - Custom function development
