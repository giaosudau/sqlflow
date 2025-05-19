# SQLFlow: Your Data Workflow Control Plane, Defined in SQL

**SQLFlow is a SQL-native engine that empowers you to define, orchestrate, and manage your entire data workflow—from loading and transformation to export—all with the simplicity and power of SQL.**

For data analysts, engineers, and scientists who speak SQL, SQLFlow streamlines data operations by replacing tool sprawl and complex setups with a unified, SQL-centric approach. It's designed for an MVP that delivers immediate value by leveraging your existing SQL skills.

<!-- TODO: Add an engaging GIF or architectural diagram here showing SQLFlow's unified workflow -->

## Core Vision & Innovation (MVP Focus)

SQLFlow's vision is to make robust data pipelining accessible through SQL. Our MVP innovates by delivering:

*   **Unified SQL Command Center:** Go from raw data to actionable insights using a single, SQL-based DSL for loading sources, transforming data, and exporting results. No more context-switching between different tools for different stages of your pipeline.
*   **Environment Agility with Profiles:** Seamlessly manage `dev`, `prod`, and other environments. Isolate configurations for databases, engine behavior (like DuckDB's mode), and variables using simple YAML files. This is critical for reliable, reproducible data operations from day one.
*   **Flexible & Fast Execution (Powered by DuckDB):**
    *   **In-Memory Speed for Dev:** The default `dev` profile utilizes DuckDB in-memory for ultra-fast iteration and testing.
    *   **Persistent Storage for Prod:** Easily switch to `persistent` mode, saving your transformed data to a disk-based DuckDB file at a path *you* specify. This offers a pragmatic balance of performance and durability.
*   **Transparent Data Lineage:** Instantly visualize your entire SQL-defined pipeline as an interactive Directed Acyclic Graph (DAG), making dependencies and data flow clear.

## Key MVP Features

*   **Intuitive SQL-based DSL:** Define sources, loads, transformations (`CREATE TABLE AS SELECT`), and exports using familiar SQL syntax.
*   **Profile-Driven Configuration:** Manage environment-specific settings for engines (DuckDB), connections, and variables.
*   **DuckDB Integration:** Leverage DuckDB for high-performance in-memory or reliable persistent data processing.
*   **Basic Connector Support:** Start with CSV and PostgreSQL sources, with clear paths for extension.
*   **Local File & S3 Export:** Essential export capabilities for common use cases.
*   **CLI for Core Operations:** `init`, `run`, `compile`, `list` commands to manage your pipelines.
*   **Automatic DAG Visualization:** Understand your pipeline structure at a glance.

## Python UDFs: Bring Python Power to Your SQL Pipelines 🚀

> **SQLFlow lets you define and use Python User-Defined Functions (UDFs) directly in your SQL pipelines—enabling advanced, custom data transformations with the full power of Python and pandas.**

### Why SQLFlow UDFs?
- **Native Python in SQL:** Write business logic, data cleaning, or feature engineering in Python, then call it from SQL.
- **Both Scalar & Table UDFs:** Use simple row-wise functions or full DataFrame transforms.
- **Auto-discovery & Validation:** Place UDFs in `python_udfs/`, and SQLFlow will find and validate them.
- **Type Safety:** Enforced type hints and signatures for reliability.
- **Production-Ready:** UDFs are first-class citizens in your pipelines, with clear best practices and CLI support.

### Scalar UDF Example
```python
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def calculate_discount(price: float, rate: float = 0.1) -> float:
    """Calculate discount amount."""
    if price is None:
        return None
    return price * rate
```

**Use in SQL:**
```sql
SELECT
  product_id,
  price,
  PYTHON_FUNC("python_udfs.example_udf.calculate_discount", price, 0.2) AS discount
FROM products;
```

### Table UDF Example
```python
from sqlflow.udfs.decorators import python_table_udf
import pandas as pd

@python_table_udf
def add_price_metrics(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["discount_10"] = result["price"] * 0.1
    result["discount_20"] = result["price"] * 0.2
    result["final_price_10"] = result["price"] - result["discount_10"]
    result["final_price_20"] = result["price"] - result["discount_20"]
    return result
```

**Use in SQL:**
```sql
CREATE TABLE enriched_products AS
SELECT * FROM PYTHON_FUNC("python_udfs.example_udf.add_price_metrics", products);
```

### Best Practices
- **Type hints are required** for all UDF arguments and return values.
- **Table UDFs** must accept and return a `pd.DataFrame`.
- Place UDFs in the `python_udfs/` directory (or subdirectories).
- Use fully qualified names in SQL: `PYTHON_FUNC("python_udfs.module.function", ...)`.

### CLI Support for UDFs
- List UDFs: `sqlflow udf list`
- Show UDF info: `sqlflow udf info python_udfs.example_udf.calculate_discount`
- Validate UDFs: `sqlflow udf validate`

### More Advanced Usage & Troubleshooting
See the [Advanced Python UDF Guide](docs/python_udfs.md) for:
- Signature requirements
- Discovery and registration
- Performance tips
- Troubleshooting common issues
- More real-world examples ([examples/python_udfs/example_udf.py](examples/python_udfs/example_udf.py))

---

## Quick Start: Your First Unified SQLFlow Pipeline

1.  **Install SQLFlow:**
    ```bash
    pip install sqlflow
    ```

2.  **Initialize Your Project:**
    Creates `my_data_workflow/` with a default `profiles/dev.yml` (in-memory DuckDB).
    ```bash
    sqlflow init my_data_workflow
    cd my_data_workflow
    ```

3.  **Define Your Unified Pipeline (`pipelines/process_data.sf`):**
    ```sql
    -- pipelines/process_data.sf

    -- 1. DEFINE SOURCE (Loading)
    SOURCE raw_orders TYPE CSV PARAMS {
      "path": "data/orders.csv", -- Create this sample CSV file
      "has_header": true
    };

    -- 2. LOAD DATA (Staging for Transformation)
    LOAD orders_table FROM raw_orders;

    -- 3. TRANSFORM DATA (Using SQL)
    CREATE TABLE daily_sales_summary AS
    SELECT
      order_date,
      COUNT(order_id) AS num_orders,
      SUM(CAST(amount AS DECIMAL(10,2))) AS total_sales
    FROM orders_table
    GROUP BY order_date;

    -- 4. EXPORT RESULTS
    EXPORT
      SELECT * FROM daily_sales_summary
    TO "output/daily_summary_${run_id}.parquet" -- Example: use a run_id variable
    TYPE LOCAL_FILE
    OPTIONS { "format": "parquet" };
    ```
    *Remember to create a sample `data/orders.csv`! Add a `run_id` to `vars` in your profile or pass it via CLI for the export filename.*

4.  **Run Your Pipeline (Defaults to `dev` profile):**
    ```bash
    sqlflow pipeline run process_data
    ```

5.  **Explore:**
    *   **Production Run (Persistent):** Create `profiles/production.yml` (see below), then:
        `sqlflow pipeline run process_data --profile production`
    *   **Visualize DAG:** Check the `target/` directory for DAG visualizations after a run.

## Profile-Driven Configuration: Tailor Your Environments

Manage `dev`, `prod`, etc., in `profiles/`. SQLFlow uses `profiles/dev.yml` by default.

### `profiles/dev.yml` (Fast Iteration)
```yaml
engines:
  duckdb:
    mode: memory
    memory_limit: 1GB
variables:
  run_id: "dev_run"
```

### `profiles/production.yml` (Reliable Persistence)
```yaml
engines:
  duckdb:
    mode: persistent
    path: target/production_data.db # SQLFlow uses this exact path
    memory_limit: 4GB
variables:
  run_id: "prod_$(date +%Y%m%d%H%M%S)" # Example: dynamic run_id for production
  S3_BUCKET: "your-s3-data-bucket"
```

## DuckDB: The Engine Behind SQLFlow's Flexibility

*   **Memory Mode (`mode: memory`):** Ideal for dev. Fast, ephemeral. No data saved post-run.
*   **Persistent Mode (`mode: persistent`):** For prod. Data saved to disk at the `path` you set in your profile. All tables, including intermediate transforms, are persisted.

## SQLFlow Syntax Highlights (Unified Workflow)

```sql
-- Define a PostgreSQL data SOURCE
SOURCE customers_db TYPE POSTGRES PARAMS {
  "connection_string": "${DB_CONN_VAR}",
  "query": "SELECT id, name, signup_date FROM active_users"
};

-- LOAD data into an SQLFlow table
LOAD latest_customers FROM customers_db;

-- TRANSFORM data using familiar SQL
CREATE TABLE customer_cohorts AS
SELECT
  STRFTIME(signup_date, '%Y-%m') AS cohort_month,
  COUNT(DISTINCT id) AS new_customers
FROM latest_customers
GROUP BY cohort_month;

-- EXPORT results to S3
EXPORT
  SELECT * FROM customer_cohorts
TO "s3://${S3_BUCKET}/reports/customer_cohorts/"
TYPE S3
OPTIONS {"format": "parquet"};
```

Use variables (`${VAR_NAME}`) from profiles or CLI (`--vars '{"VAR_NAME": "value"}'`).

## Core Use Cases (MVP)

*   **SQL-Centric ETL/ELT:** For analysts and engineers who prefer SQL to manage the full data lifecycle from simple sources (CSVs, database queries) to transformed outputs.
*   **Rapid Prototyping of Data Pipelines:** Quickly build and test data transformation logic with minimal setup and easy environment switching.
*   **Automating Reporting Feeds:** Prepare and export datasets for BI tools or downstream systems using a clear, SQL-defined process.

## Configuring Logging

SQLFlow provides flexible logging configuration to help you debug and monitor your data pipelines:

### Log Levels

Control verbosity with these log levels (from most to least verbose):
- `debug`: Detailed debugging information 
- `info`: General operational information (default)
- `warning`: Warning messages
- `error`: Error messages
- `critical`: Critical issues

### Configuration Methods

SQLFlow offers multiple ways to configure logging:

#### 1. Environment Variable

Set the `SQLFLOW_LOG_LEVEL` environment variable:

```bash
# On Linux/macOS
export SQLFLOW_LOG_LEVEL=debug

# On Windows
set SQLFLOW_LOG_LEVEL=debug
```

#### 2. Command Line Option

Use the `--log-level` option when running SQLFlow:

```bash
sqlflow pipeline run my_pipeline --log-level debug
```

#### 3. Profile Configuration

Add logging settings to your profile YAML files:

```yaml
# profiles/dev.yml
log_level: debug
module_log_levels:
  sqlflow.core.engines: info
  sqlflow.connectors: debug
  sqlflow.udfs: debug
```

This allows you to set different log levels for specific modules.

#### 4. Programmatic Configuration

For Python scripts that use SQLFlow as a library:

```python
from sqlflow.logging import configure_logging

# Set global log level
configure_logging(log_level="debug")

# Or configure with more specific settings
configure_logging(config={
    "log_level": "info",
    "module_log_levels": {
        "sqlflow.core.engines": "debug",
        "sqlflow.connectors": "warning"
    }
})
```

### Recommended Settings for Common Scenarios

- **Development/Debugging**: Use `debug` log level to see detailed execution information
- **Production**: Use `info` or `warning` to reduce log volume
- **Troubleshooting UDFs**: Set module-specific level for `sqlflow.udfs` to `debug`
- **Connector Issues**: Set module-specific level for `sqlflow.connectors` to `debug`

### Log Output Format

SQLFlow logs include timestamp, logger name, level and message:

```
2023-05-14 15:23:45,789 - sqlflow.core.engines.duckdb_engine - INFO - Table users registered successfully
```

This standardized format makes it easier to filter and analyze logs.

## SQL Features & Best Practices

### Proper SQL Formatting

SQLFlow ensures your SQL queries are formatted correctly for execution by:

1. **Table and Column References**: Maintaining proper dot notation without extra spaces
```sql
-- Table aliases with column references
SELECT 
  u.id,           -- Not "u . id" 
  u.name, 
  o.order_date
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.status = 'completed';
```

2. **Function Calls**: Ensuring no spaces between function names and parentheses
```sql
-- Aggregate functions properly formatted
SELECT
  region,
  COUNT(DISTINCT user_id) as unique_users,  -- Not "COUNT ( DISTINCT user_id )"
  SUM(amount) as total_sales,               -- Not "SUM ( amount )"
  AVG(price) as average_price               -- Not "AVG ( price )"
FROM sales
GROUP BY region;
```

These formatting rules ensure your SQL queries are valid across different database engines.

### Variable Substitution

Use variables in your SQL with the `${variable_name}` syntax:

```sql
-- Filter data based on a profile variable
CREATE TABLE regional_sales AS
SELECT * FROM sales
WHERE region = '${target_region}';

-- Use variables in export paths
EXPORT 
  SELECT * FROM daily_sales
TO "output/${run_date}_sales_report.csv"
TYPE CSV
OPTIONS { "header": true };
```

Variables can be defined in your profile configuration or passed at runtime.

**Default Value Rules:**
- You can specify a default value for a variable: `${var|default}`
- **If the default value contains spaces, it must be quoted** (single or double quotes):
  - ✅ `${region|"us east"}` (valid)
  - ✅ `${region|'us east'}` (valid)
  - ❌ `${region|us east}` (**invalid**, will cause a validation error)
- Unquoted default values with spaces are not allowed and will cause a pipeline validation error.

## Vision & Next Steps (Beyond MVP)

Our MVP focuses on delivering a solid SQL-native workflow foundation. The vision is to expand:
*   **Connector Ecosystem:** Broader support for diverse data sources and destinations.
*   **Advanced Orchestration:** Scheduling, incremental processing, and richer dependency management.
*   **Data Quality & Testing:** Integrated mechanisms to ensure data reliability.

We aim to keep SQLFlow lean, intuitive, and powerful for SQL practitioners.

## Join Us & Shape SQLFlow

SQLFlow is young and driven by community. As we build upon this MVP:

*   ⭐ **Star us on GitHub!**
*   💡 **Share Feedback & Ideas:** Open an issue for feature requests or improvements.
*   🐞 **Report Bugs:** Help us stabilize and refine the MVP.
*   (Future) **Contribute:** We'll be formalizing a `CONTRIBUTING.md` as the project matures, outlining how to contribute effectively.

## Documentation

(Coming Soon) Detailed documentation will be available as features are solidified.

## License

SQLFlow is released under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for details. This license allows for broad use and contribution while providing a framework for governance and future development.

## FAQ

**Q: How is SQLFlow different from dbt?**
A: While both leverage SQL, SQLFlow aims to provide a more self-contained, lightweight engine for the *entire* load-transform-export workflow, especially for use cases where a simpler, SQL-native orchestration is preferred. dbt excels at complex in-warehouse transformations. SQLFlow integrates the "T" with "E" and "L" in a more direct, SQL-defined manner for its supported sources/sinks.

**Q: DuckDB configuration (memory/persistent, path)?**
A: In your profile YAML files (`profiles/*.yml`) under the `engines.duckdb` key. SQLFlow uses the exact `path` specified for persistent DuckDB files.

**Q: Switching DuckDB modes?**
A: Edit `mode` (and `path` for persistent) in the active profile. Run with `--profile your_profile`.

**Q: Adding new environments (e.g., `staging`)?**
A: Create `profiles/staging.yml`. Configure as needed. Run with `--profile staging`.

**Q: Are intermediate tables saved in persistent mode?**
A: Yes. All tables from `CREATE TABLE ... AS SELECT ...` are saved in the DuckDB file, aiding debugging.
