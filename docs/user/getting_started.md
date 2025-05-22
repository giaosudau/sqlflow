# SQLFlow Getting Started Guide

This guide provides a comprehensive introduction to SQLFlow for new users. Follow these steps to set up your environment, create your first project, and build a complete data pipeline.

## Prerequisites

- Python 3.10+ or higher
- Pip (Python package installer)
- Basic knowledge of SQL

## 1. Installation

Install SQLFlow using pip:

```bash
pip install sqlflow-core
```

You can verify the installation was successful by checking the version:

```bash
sqlflow --version
```
You should see output similar to:
```
sqlflow, version x.y.z
```

## 2. Create Your First SQLFlow Project

Initialize a new project using the SQLFlow CLI:

```bash
sqlflow init my_first_project
cd my_first_project
```

This creates a standard project structure:

```
my_first_project/
├── pipelines/       # SQL pipeline files (.sf)
│   └── example.sf   # Auto-generated example pipeline
├── profiles/        # Environment configurations
│   └── dev.yml      # Default development profile (in-memory DuckDB)
├── models/          # Reusable SQL modules
├── macros/          # Utilities and helper functions
├── connectors/      # Connector configurations
└── tests/           # Test files
```

## 3. Understanding Profiles

SQLFlow uses profiles to configure environments:

- **dev.yml**: Default development profile that uses in-memory DuckDB (fast, but data is lost after execution)
- You can create a **production.yml** profile for persistent storage

Default dev.yml profile:
```yaml
engines:
  duckdb:
    mode: memory        # DuckDB runs in-memory (no persistence)
    memory_limit: 2GB   # Memory limit for DuckDB
log_level: info
```

Let's create a production profile for persistent storage:

```bash
mkdir -p profiles
cat > profiles/production.yml << EOF
engines:
  duckdb:
    mode: persistent
    path: target/my_project.db  # SQLFlow will use this exact path
    memory_limit: 4GB
log_level: info
EOF
```

## 4. Examine the Example Pipeline

SQLFlow generates an example pipeline in `pipelines/example.sf`. Let's look at what it contains:

```sql
-- Example SQLFlow pipeline
SET date = '${run_date|2023-10-25}';

SOURCE sample TYPE CSV PARAMS {
  "path": "data/sample_${date}.csv",
  "has_header": true
};

LOAD sample INTO raw_data;

CREATE TABLE processed_data AS
SELECT 
  *,
  UPPER(name) AS name_upper
FROM raw_data;

EXPORT
  SELECT * FROM processed_data
TO "output/processed_${date}.csv"
TYPE CSV
OPTIONS { "header": true, "delimiter": "," };
```

## 5. Create a Simple Pipeline

Let's create a new pipeline for a simple data transformation task:

```bash
mkdir -p data
mkdir -p output

# Create sample data
cat > data/users.csv << EOF
id,name,email,country,signup_date
1,Alice,alice@example.com,US,2023-01-15
2,Bob,bob@example.com,UK,2023-02-20
3,Charlie,charlie@example.com,FR,2023-01-25
4,Diana,diana@example.com,US,2023-03-10
EOF

# Create user analytics pipeline
cat > pipelines/user_analytics.sf << EOF
-- User analytics pipeline
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

-- Load data into workspace
LOAD users_table FROM users;

-- Perform transformations
CREATE TABLE user_enhanced AS
SELECT
  id,
  name,
  email,
  country,
  signup_date,
  CASE 
    WHEN country = 'US' THEN 'North America'
    WHEN country IN ('UK', 'FR') THEN 'Europe'
    ELSE 'Other'
  END AS region
FROM users_table;

-- Create summary by region
CREATE TABLE region_summary AS
SELECT
  region,
  COUNT(*) AS user_count,
  MIN(signup_date) AS earliest_signup,
  MAX(signup_date) AS latest_signup
FROM user_enhanced
GROUP BY region
ORDER BY user_count DESC;

-- Export the results
EXPORT
  SELECT * FROM user_enhanced
TO "output/enhanced_users.csv"
TYPE CSV
OPTIONS { "header": true };

EXPORT
  SELECT * FROM region_summary
TO "output/region_summary.csv"
TYPE CSV
OPTIONS { "header": true };
EOF
```

## 6. Compile and Run Your Pipeline

First, let's check the syntax and get an execution plan:

```bash
sqlflow pipeline compile user_analytics
```

This validates your pipeline and creates an execution plan file at `target/compiled/user_analytics.json`.

Now, let's run the pipeline:

```bash
# Development mode (in-memory)
sqlflow pipeline run user_analytics
```

Your results will be in the output directory:

```bash
cat output/enhanced_users.csv
cat output/region_summary.csv
```
The `output/enhanced_users.csv` will contain:
```csv
id,name,email,country,signup_date,region
1,Alice,alice@example.com,US,2023-01-15,North America
2,Bob,bob@example.com,UK,2023-02-20,Europe
3,Charlie,charlie@example.com,FR,2023-01-25,Europe
4,Diana,diana@example.com,US,2023-03-10,North America
```

The `output/region_summary.csv` will contain (order of rows with same `user_count` might vary):
```csv
region,user_count,earliest_signup,latest_signup
North America,2,2023-01-15,2023-03-10
Europe,2,2023-01-25,2023-02-20
```

## 7. Using Production Mode

Now let's run the same pipeline with the production profile, which will persist data to disk:

```bash
sqlflow pipeline run user_analytics --profile production
```

With the production profile, tables are saved to the DuckDB file specified in your profile (target/my_project.db).

## 8. Working with Variables

SQLFlow pipelines support variable substitution using the `${variable_name|default_value}` syntax:

```bash
# Create a parameterized pipeline
cat > pipelines/parameterized_report.sf << EOF
-- Parameterized report pipeline
SET report_date = "\${date|$(date +%Y-%m-%d)}";
SET region_filter = "\${region|all}";

SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users FROM users;

-- Apply region filter if specified
CREATE TABLE filtered_users AS
SELECT * FROM users 
WHERE 
  CASE 
    WHEN '${region_filter}' = 'all' THEN true
    ELSE country = '${region_filter}'
  END;

-- Create report
CREATE TABLE user_report AS
SELECT
  COUNT(*) AS total_users,
  '${region_filter}' AS filtered_region,
  '${report_date}' AS report_date
FROM filtered_users;

-- Export the report
EXPORT
  SELECT * FROM user_report
TO "output/user_report_${region_filter}_${report_date}.csv"
TYPE CSV
OPTIONS { "header": true };
EOF

# Run with custom variables
sqlflow pipeline run parameterized_report --vars '{"region": "US", "date": "2023-05-19"}'
```

## 9. Using Python UDFs

SQLFlow allows you to extend SQL capabilities with Python User-Defined Functions (UDFs). Here's how to create and use them:

```bash
# Create a Python UDFs directory
mkdir -p python_udfs

# Create a Python UDF file
cat > python_udfs/string_utils.py << EOF
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf
import pandas as pd

@python_scalar_udf
def concatenate(text1: str, text2: str) -> str:
    """Concatenates two strings with a space between them."""
    if text1 is None or text2 is None:
        return None
    return f"{text1} {text2}"

@python_table_udf
def add_name_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds name-related features to the dataframe."""
    result = df.copy()
    # Add name length
    if 'name' in result.columns:
        result['name_length'] = result['name'].str.len()
        result['name_first_letter'] = result['name'].str[0]
    return result
EOF

# Create a pipeline using UDFs
cat > pipelines/udf_pipeline.sf << EOF
-- Pipeline using Python UDFs
INCLUDE "python_udfs/string_utils.py";

SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_data FROM users;

-- Use scalar UDF
CREATE TABLE users_enhanced AS
SELECT
  id,
  name,
  email,
  PYTHON_FUNC("python_udfs.string_utils.concatenate", name, country) AS name_and_country
FROM users_data;

-- Use table UDF
CREATE TABLE users_with_features AS
SELECT * FROM PYTHON_FUNC("python_udfs.string_utils.add_name_features", users_enhanced);

-- Export results
EXPORT
  SELECT * FROM users_with_features
TO "output/users_with_features.csv"
TYPE CSV
OPTIONS { "header": true };
EOF

This pipeline uses `INCLUDE "python_udfs/string_utils.py";` at the top to make the Python functions defined in `string_utils.py` available to the SQLFlow engine.

# Run the UDF pipeline
sqlflow pipeline run udf_pipeline
```
After running, check the output:
```bash
cat output/users_with_features.csv
```
The `output/users_with_features.csv` will contain:
```csv
id,name,email,name_and_country,name_length,name_first_letter
1,Alice,alice@example.com,"Alice US",5,A
2,Bob,bob@example.com,"Bob UK",3,B
3,Charlie,charlie@example.com,"Charlie FR",7,C
4,Diana,diana@example.com,"Diana US",5,D
```

## 10. Next Steps

Now that you've built your first SQLFlow pipelines, you can explore more advanced features:

- **Conditional Execution**: Use `IF/ELSE` statements in your pipelines for dynamic processing
- **More Connectors**: Connect to PostgreSQL, S3, or REST APIs using the appropriate connectors
- **Complex Transformations**: Build multi-stage transformations with dependent tables
- **Advanced UDFs**: Create more powerful Python functions to extend your pipeline capabilities

Explore the examples directory for more comprehensive use cases:

```bash
# Clone the repository to access examples
git clone https://github.com/sqlflow/sqlflow.git
cd sqlflow

# List available examples
ls -la examples/

# Explore the ecommerce demo for a more complex implementation
cat examples/ecommerce/README.md
```

For full syntax details, refer to the [SQLFlow Syntax Reference](syntax.md) and [Python UDFs Guide](python_udfs.md).
