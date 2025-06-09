# Troubleshooting SQLFlow

**Problem**: "Something's not working and I need to fix it fast to get back to analyzing data."

**Solution**: This guide covers the most common SQLFlow issues with step-by-step solutions that actually work.

## 🎯 Quick Problem Finder

**Having trouble?** Jump directly to your issue:

- 🚀 [Installation Issues](#-installation-issues) - Can't install or SQLFlow won't start
- 🔧 [Pipeline Errors](#-pipeline-errors) - Pipelines fail to run or validate
- 📊 [Data Loading Problems](#-data-loading-problems) - Can't load your data
- 🗃️ [Database Connection Issues](#️-database-connection-issues) - Can't connect to databases
- ⚡ [Performance Problems](#-performance-problems) - SQLFlow is running slowly
- 💾 [Memory Issues](#-memory-issues) - Out of memory errors
- 🐍 [Python UDF Errors](#-python-udf-errors) - Python functions not working
- 📁 [File and Path Issues](#-file-and-path-issues) - Can't find files or paths

## 🚀 Installation Issues

### "Command not found: sqlflow"

**Problem**: After installation, `sqlflow` command doesn't work.

**Solution steps:**

1. **Verify installation:**
```bash
pip show sqlflow-core
# Should show package details
```

2. **Check Python PATH:**
```bash
# Find where sqlflow is installed
pip show -f sqlflow-core | grep Location

# Check if Python scripts are in PATH
echo $PATH | grep -o '[^:]*python[^:]*'
```

3. **Fix PATH issues:**
```bash
# Option 1: Use python -m
python -m sqlflow --version

# Option 2: Find and add to PATH
pip show -f sqlflow-core | grep "bin/sqlflow"

# Option 3: Reinstall with user flag
pip install --user sqlflow-core
```

4. **Use virtual environment (recommended):**
```bash
python -m venv sqlflow-env
source sqlflow-env/bin/activate  # Windows: sqlflow-env\Scripts\activate
pip install sqlflow-core
sqlflow --version
```

### "Permission denied" during installation

**Problem**: Installation fails with permission errors.

**Solutions:**

1. **Use virtual environment (best):**
```bash
python -m venv sqlflow-env
source sqlflow-env/bin/activate
pip install sqlflow-core
```

2. **User installation:**
```bash
pip install --user sqlflow-core
```

3. **Fix with sudo (Linux/Mac, not recommended):**
```bash
sudo pip install sqlflow-core
```

### "Python version not supported"

**Problem**: SQLFlow requires Python 3.10+

**Solutions:**

1. **Check Python version:**
```bash
python --version
python3 --version
```

2. **Install newer Python:**
```bash
# macOS with Homebrew
brew install python@3.11

# Ubuntu/Debian
sudo apt update
sudo apt install python3.11 python3.11-pip

# Windows: Download from python.org
```

3. **Use pyenv for version management:**
```bash
# Install pyenv first
pyenv install 3.11.0
pyenv global 3.11.0
pip install sqlflow-core
```

### "Failed building wheel for duckdb"

**Problem**: DuckDB dependency fails to install.

**Solutions:**

1. **Install build tools:**
```bash
# macOS
xcode-select --install

# Ubuntu/Debian
sudo apt install build-essential python3-dev

# CentOS/RHEL
sudo yum groupinstall "Development Tools"
```

2. **Use pre-built wheels:**
```bash
pip install --only-binary=duckdb sqlflow-core
```

3. **Install DuckDB separately:**
```bash
pip install duckdb
pip install sqlflow-core
```

## 🔧 Pipeline Errors

### "Pipeline validation failed"

**Problem**: `sqlflow pipeline validate` shows errors.

**Solutions:**

1. **Check syntax errors:**
```bash
# Validate with verbose output
sqlflow pipeline validate my_pipeline --verbose

# Check specific pipeline file
sqlflow pipeline validate my_pipeline.sf
```

2. **Common syntax fixes:**
```sql
-- Wrong: Missing semicolon
SOURCE users TYPE CSV PARAMS {"path": "users.csv"}
LOAD users FROM users

-- Right: Proper syntax
SOURCE users TYPE CSV PARAMS {"path": "users.csv"};
LOAD users FROM users;
```

3. **Fix missing parameters:**
```sql
-- Wrong: Missing required path
SOURCE users TYPE CSV PARAMS {};

-- Right: Include required parameters
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};
```

4. **Validate JSON in PARAMS:**
```sql
-- Wrong: Invalid JSON (trailing comma)
SOURCE users TYPE CSV PARAMS {
  "path": "users.csv",
  "has_header": true,  -- Remove this comma
};

-- Right: Valid JSON
SOURCE users TYPE CSV PARAMS {
  "path": "users.csv",
  "has_header": true
};
```

### "Table not found" errors during execution

**Problem**: Pipeline runs but fails with table not found.

**Solutions:**

1. **Check execution order:**
```bash
# List all pipelines
sqlflow pipeline list

# Check dependencies
sqlflow pipeline dependencies my_pipeline
```

2. **Run dependencies first:**
```bash
# Run data loading pipeline first
sqlflow pipeline run data_loading
sqlflow pipeline run analytics
```

3. **Use proper table references:**
```sql
-- Wrong: Referencing undefined table
CREATE TABLE analysis AS
SELECT * FROM undefined_table;

-- Right: Load table first
SOURCE users TYPE CSV PARAMS {"path": "users.csv"};
LOAD users FROM users;

CREATE TABLE analysis AS
SELECT * FROM users;
```

### "SOURCE not defined" errors

**Problem**: LOAD statement references undefined SOURCE.

**Solution:**
```sql
-- Wrong: Missing SOURCE definition
LOAD users FROM users_csv;

-- Right: Define SOURCE first
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};
LOAD users FROM users_csv;
```

### "Invalid connector type" errors

**Problem**: Using unsupported connector type.

**Solutions:**

1. **Check available connectors:**
```bash
sqlflow connectors list
```

2. **Use correct connector names:**
```sql
-- Wrong: Invalid type
SOURCE data TYPE UNKNOWN PARAMS {};

-- Right: Use supported types
SOURCE data TYPE CSV PARAMS {};        -- For CSV files
SOURCE data TYPE POSTGRESQL PARAMS {}; -- For PostgreSQL
SOURCE data TYPE S3 PARAMS {};         -- For S3
```

3. **Install connector dependencies:**
```bash
# For PostgreSQL
pip install "sqlflow-core[postgres]"

# For cloud storage
pip install "sqlflow-core[cloud]"

# For everything
pip install "sqlflow-core[all]"
```

## 📊 Data Loading Problems

### "File not found" errors

**Problem**: Can't find CSV or data files.

**Solutions:**

1. **Check file paths:**
```bash
# Verify file exists
ls -la data/users.csv

# Check current directory
pwd
ls -la
```

2. **Use absolute paths:**
```sql
-- Relative path (may not work)
SOURCE users TYPE CSV PARAMS {
  "path": "users.csv"
};

-- Absolute path (more reliable)
SOURCE users TYPE CSV PARAMS {
  "path": "/full/path/to/users.csv"
};

-- Project-relative path (recommended)
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv"
};
```

3. **Check file permissions:**
```bash
# Check if file is readable
ls -la data/users.csv

# Fix permissions if needed
chmod 644 data/users.csv
```

### "CSV parsing errors"

**Problem**: CSV files fail to load properly.

**Solutions:**

1. **Check CSV format:**
```bash
# Look at first few lines
head data/users.csv

# Check for encoding issues
file data/users.csv
```

2. **Fix CSV parameters:**
```sql
-- Handle different delimiters
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "delimiter": ";",           -- For semicolon-separated
  "has_header": true
};

-- Handle quotes and escapes
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "quote_char": "'",          -- Single quotes
  "escape_char": "\\",        -- Backslash escape
  "has_header": true
};

-- Handle encoding issues
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "encoding": "latin-1",      -- Try different encoding
  "has_header": true
};
```

3. **Skip problematic rows:**
```sql
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true,
  "skip_rows": 2,             -- Skip first 2 rows
  "ignore_errors": true,      -- Skip bad rows
  "max_errors": 10           -- Stop after 10 errors
};
```

### "Schema compatibility" errors with APPEND/UPSERT

**Problem**: Can't append or upsert due to schema mismatch.

**Solutions:**

1. **Check schema differences:**
```sql
-- Describe existing table
DESCRIBE users;

-- Compare with source
SOURCE new_users TYPE CSV PARAMS {
  "path": "data/new_users.csv",
  "has_header": true
};
DESCRIBE new_users;
```

2. **Use subset selection:**
```sql
-- Select only compatible columns
LOAD users FROM (
    SELECT user_id, name, email
    FROM new_users_csv
) MODE APPEND;
```

3. **Create compatible schema:**
```sql
-- Transform data to match schema
LOAD users FROM (
    SELECT 
        CAST(id AS INTEGER) as user_id,
        UPPER(full_name) as name,
        email_address as email,
        CURRENT_DATE as created_at
    FROM new_users_csv
) MODE APPEND;
```

### "Type conversion" errors

**Problem**: Data types don't match expectations.

**Solutions:**

1. **Explicit type casting:**
```sql
SOURCE typed_data TYPE CSV PARAMS {
  "path": "data/data.csv",
  "has_header": true,
  "column_types": {
    "date_field": "DATE",
    "number_field": "INTEGER",
    "decimal_field": "DECIMAL(10,2)"
  }
};
```

2. **Handle conversion in SQL:**
```sql
LOAD clean_data FROM (
    SELECT 
        TRY_CAST(date_string AS DATE) as date_field,
        TRY_CAST(number_string AS INTEGER) as number_field,
        COALESCE(name, 'Unknown') as name
    FROM raw_data
    WHERE TRY_CAST(date_string AS DATE) IS NOT NULL
);
```

## 🗃️ Database Connection Issues

### "Connection refused" to PostgreSQL

**Problem**: Can't connect to PostgreSQL database.

**Solutions:**

1. **Check connection details:**
```bash
# Test connection with psql
psql -h hostname -p 5432 -U username -d database

# Test network connectivity
telnet hostname 5432
```

2. **Verify profile configuration:**
```yaml
# profiles/production.yml
connections:
  prod_db:
    type: "postgresql"
    host: "correct-hostname"      # Check this
    port: 5432                    # Check this
    database: "correct-database"  # Check this
    username: "correct-username"  # Check this
    password: "${PG_PASSWORD}"    # Check environment variable
```

3. **Check environment variables:**
```bash
# Verify password is set
echo $PG_PASSWORD

# Set if missing
export PG_PASSWORD="your_password"
```

4. **Test SQLFlow connection:**
```bash
# Test connection
sqlflow connection test prod_db --profile production

# Debug with verbose output
sqlflow connection test prod_db --profile production --verbose
```

### "SSL connection required"

**Problem**: Database requires SSL but connection fails.

**Solution:**
```yaml
# profiles/production.yml
connections:
  prod_db:
    type: "postgresql"
    host: "hostname"
    port: 5432
    database: "database"
    username: "username"
    password: "${PG_PASSWORD}"
    ssl_mode: "require"           # Add SSL requirement
    ssl_cert: "/path/to/cert.pem" # If client cert needed
```

### "Too many connections" errors

**Problem**: Database rejects connections due to limits.

**Solution:**
```yaml
# profiles/production.yml
connections:
  prod_db:
    type: "postgresql"
    # ... other settings ...
    pool_settings:
      max_connections: 2          # Reduce connection pool
      min_connections: 1
      connection_timeout: 30
```

## ⚡ Performance Problems

### "Pipeline runs too slowly"

**Problem**: Pipelines take too long to execute.

**Solutions:**

1. **Use persistent mode for large data:**
```bash
# Instead of memory mode
sqlflow pipeline run analytics --profile production
```

2. **Add indexes for better performance:**
```sql
-- Add indexes on frequently joined columns
CREATE INDEX idx_customer_id ON orders(customer_id);
CREATE INDEX idx_order_date ON orders(order_date);
```

3. **Optimize queries:**
```sql
-- Instead of: Loading all data then filtering
CREATE TABLE recent_orders AS
SELECT * FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days';

-- Better: Filter during load
SOURCE recent_orders_pg TYPE POSTGRESQL PARAMS {
  "connection": "prod_db",
  "query": "SELECT * FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'"
};
```

4. **Use sampling in development:**
```sql
-- Sample large datasets during development
CREATE TABLE sample_orders AS
SELECT * FROM orders
WHERE RANDOM() < 0.1  -- 10% sample
LIMIT 10000;
```

### "Slow CSV loading"

**Problem**: Large CSV files take too long to load.

**Solutions:**

1. **Enable parallel processing:**
```sql
SOURCE large_csv TYPE CSV PARAMS {
  "path": "data/large_file.csv",
  "has_header": true,
  "parallel_load": true,        -- Enable parallel loading
  "batch_size": 10000          -- Process in batches
};
```

2. **Use columnar formats:**
```bash
# Convert CSV to Parquet for better performance
# (using external tool or Python script)
```

3. **Split large files:**
```bash
# Split large CSV into smaller files
split -l 100000 large_file.csv chunk_

# Load chunks separately
```

## 💾 Memory Issues

### "Out of memory" errors

**Problem**: SQLFlow runs out of memory with large datasets.

**Solutions:**

1. **Use persistent mode:**
```bash
# Switch from memory to disk-based processing
sqlflow pipeline run analytics --profile production
```

2. **Configure profile for large data:**
```yaml
# profiles/large_data.yml
name: "large_data"
engine:
  type: "duckdb"
  mode: "persistent"
  database_path: "data/analytics.db"
  memory_limit: "4GB"           # Set memory limit
  threads: 4                    # Limit CPU threads
```

3. **Process in chunks:**
```sql
-- Instead of processing all data at once
CREATE TABLE chunked_analysis AS
SELECT * FROM large_table
WHERE id BETWEEN {{ vars.start_id }} AND {{ vars.end_id }};
```

4. **Use streaming processing:**
```sql
SOURCE streaming_csv TYPE CSV PARAMS {
  "path": "data/huge_file.csv",
  "has_header": true,
  "streaming": true,            -- Process row by row
  "chunk_size": 1000           -- Small chunks
};
```

### "Database file too large"

**Problem**: DuckDB file grows too large.

**Solutions:**

1. **Clean up temporary tables:**
```sql
-- Drop tables you don't need
DROP TABLE IF EXISTS temp_analysis;
DROP TABLE IF EXISTS intermediate_results;
```

2. **Use VACUUM to reclaim space:**
```sql
-- Reclaim space from deleted data
VACUUM;
```

3. **Configure different database location:**
```yaml
# profiles/external_storage.yml
engine:
  type: "duckdb"
  mode: "persistent"
  database_path: "/external/storage/analytics.db"  # Use external drive
```

## 🐍 Python UDF Errors

### "Python function not found"

**Problem**: PYTHON_FUNC can't find your function.

**Solutions:**

1. **Check function file location:**
```bash
# Verify file exists
ls -la python_udfs/my_functions.py

# Check current directory
pwd
```

2. **Verify function syntax:**
```python
# python_udfs/my_functions.py
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def my_function(x: int) -> int:
    return x * 2
```

3. **Use correct function path:**
```sql
-- Correct format: module.function
SELECT PYTHON_FUNC("python_udfs.my_functions.my_function", 5);

-- Not: just function name
-- SELECT PYTHON_FUNC("my_function", 5);
```

### "Import errors" in Python UDFs

**Problem**: Python UDF can't import required modules.

**Solutions:**

1. **Install missing packages:**
```bash
# Install in same environment as SQLFlow
pip install pandas numpy scikit-learn
```

2. **Check import statements:**
```python
# python_udfs/ml_functions.py
try:
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"Missing dependency: {e}")
    raise
```

3. **Use virtual environment:**
```bash
# Ensure UDF dependencies are in same environment
source venv/bin/activate
pip install sqlflow-core pandas numpy
```

### "Type annotation errors"

**Problem**: Python UDF type hints cause errors.

**Solutions:**

1. **Use correct type annotations:**
```python
from typing import Optional
from sqlflow.udfs.decorators import python_scalar_udf
import pandas as pd

@python_scalar_udf
def process_value(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    return len(x)

@python_table_udf  
def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()
```

2. **Handle None values:**
```python
@python_scalar_udf
def safe_divide(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b
```

## 📁 File and Path Issues

### "Working directory problems"

**Problem**: SQLFlow can't find files due to path issues.

**Solutions:**

1. **Check current directory:**
```bash
# See where you are
pwd

# List files in current directory
ls -la

# Check data directory
ls -la data/
```

2. **Use absolute paths:**
```sql
-- Instead of relative paths
SOURCE users TYPE CSV PARAMS {
  "path": "/full/path/to/project/data/users.csv",
  "has_header": true
};
```

3. **Run from project root:**
```bash
# Always run SQLFlow from project directory
cd /path/to/your/project
sqlflow pipeline run analytics
```

### "Output directory doesn't exist"

**Problem**: EXPORT fails because output directory missing.

**Solutions:**

1. **Create output directory:**
```bash
mkdir -p output
mkdir -p exports
```

2. **Check export paths:**
```sql
-- Make sure directory exists
EXPORT results TO 'output/results.csv' TYPE CSV;

-- Create nested directories if needed
EXPORT results TO 'output/reports/monthly/results.csv' TYPE CSV;
```

3. **Use profile variables:**
```yaml
# profiles/dev.yml
variables:
  output_path: "output"
  
# profiles/production.yml  
variables:
  output_path: "/data/exports"
```

```sql
-- Use variable in EXPORT
EXPORT results TO '{{ vars.output_path }}/results.csv' TYPE CSV;
```

## 🔍 Getting More Help

### Enable Debug Logging

```bash
# Run with debug output
sqlflow pipeline run analytics --verbose --debug

# Check logs
ls -la logs/
cat logs/sqlflow.log
```

### Diagnostic Commands

```bash
# System information
sqlflow system info

# Check configuration
sqlflow config show

# Validate environment
sqlflow doctor

# Test specific components
sqlflow test connectors
sqlflow test udfs
```

### Community Support

**When you need more help:**

1. **Search existing issues**: [GitHub Issues](https://github.com/giaosudau/sqlflow/issues)
2. **Ask the community**: [GitHub Discussions](https://github.com/giaosudau/sqlflow/discussions)
3. **Report bugs**: Create new issue with:
   - Operating system and version
   - Python version (`python --version`)
   - SQLFlow version (`sqlflow --version`)
   - Complete error message
   - Steps to reproduce
   - Sample data (if relevant)

### Create Minimal Reproduction

When reporting issues, create a minimal example:

```bash
# Create test project
sqlflow init debug_test
cd debug_test

# Create minimal pipeline that shows the problem
echo 'SOURCE test TYPE CSV PARAMS {"path": "test.csv"};' > pipelines/test.sf

# Run and capture error
sqlflow pipeline run test 2>&1 | tee error.log
```

---

**🎯 Still Stuck?** Don't worry! The SQLFlow community is here to help. Most issues have simple solutions once you know the right approach. 