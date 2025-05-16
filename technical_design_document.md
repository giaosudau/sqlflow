# SQLFlow Technical Design Document

## Executive Summary

SQLFlow is an end-to-end, SQL-centric data transformation framework that empowers data analysts and engineers at small and medium enterprises (SMEs) to rapidly translate business requirements into production-grade pipelines. It extends standard SQL with pipeline directives (e.g., SOURCE, LOAD, EXPORT, ENGINE, INCLUDE), integrates seamlessly with Python via zero-copy Apache Arrow transfers, and embraces a modular, plugin-based architecture for both ingestion and export. Designed for familiarity (dbt-style CLI, directory structure) and extensibility (custom connectors, Python UDFs), SQLFlow sits uniquely at the intersection of ELT tools (dbt, Airbyte), orchestration engines (Airflow, Prefect), and hybrid SQL+Python platforms—delivering full control over data movement, transformation, and delivery without sacrificing simplicity or performance.

## 1. Goals & Principles

### 1.1 Goals

1. **Natural SQL-Based Pipelines**  
   Allow users to define all aspects of a data pipeline—ingest, transform, export—using an extended SQL dialect.

2. **Universal Connectivity**  
   Support any data source or destination via pluggable connectors (SaaS APIs, file systems, databases, HTTP endpoints).

3. **Powerful Transformations**  
   Combine DuckDB's high-performance SQL engine with pandas-based Python UDFs for complex logic.

4. **Modular Extensibility**  
   Enable easy development of new connectors, engines, and pipeline directives through a clear plugin interface.

5. **Analyst & Engineer Productivity**  
   Offer a familiar CLI, clear project layout, parameterization, and visualization to minimize onboarding time and maximize throughput.

### 1.2 Key Principles

- **SQL-First**: Leverage users' existing SQL skills; minimize context switching.  
- **Simplicity**: Clear, consistent syntax; avoid hidden "magic."  
- **Pythonic**: Follow Python community conventions in APIs and code.  
- **Modularity**: Decouple parser, compiler, runtime, connectors, and CLI into interchangeable components.  
- **Performance**: Optimize by pushing down filters, parallel execution, Arrow-based zero-copy transfers, and cost-based engine selection.

## 2. Positioning & Differentiators

### 2.1 Market Landscape

- **dbt / sqlmesh**: Focused on in-warehouse transformations; no native ingestion or export.  
- **Airbyte / Meltano**: Provide connectors but limited to ELT; lack deep SQL-first transformation capabilities.  
- **Airflow / Prefect**: Orchestration-first; users build pipelines in Python; lack SQL DSL.  
- **NiFi / StreamSets**: Flow-based GUIs; heavyweight for SMEs; not code-centric.  

### 2.2 Unique Value of SQLFlow

1. **End-to-End in SQL**: Ingest, transform, export—all declared in SQL.  
2. **Hybrid Engine Support**: Native DuckDB for SQL, pandas for advanced logic—auto-selected or user-controlled.  
3. **Connector Symmetry**: SOURCE and EXPORT are both plugin ecosystems, enabling any future destination.  
4. **Arrow-Backed UDFs**: Zero-copy data exchange for high throughput between SQL and Python.  
5. **Lightweight & Local-First**: Single CLI binary, no container-per-connector, but easily scales to Celery, Kubernetes, or Airflow.

## 3. Project Structure & CLI

### 3.1 Directory Layout

```
my_project/
├── sqlflow.yml           # Global config: engines, variables, profiles
├── pipelines/            # Declarative pipeline scripts (.sf)
│   ├── daily_sales.sf
│   └── weekly_report.sf
├── models/               # Reusable SQL modules (.sf)
│   ├── dims.sf
│   └── marts.sf
├── macros/               # Python & SQL utilities
│   ├── helpers.py
│   └── date_utils.sf
├── connectors/           # Built-in & custom connector plugins
└── tests/                # Pipeline and unit tests
```

### 3.2 CLI Commands

```bash
# Core commands
sqlflow init [project_name]                                # Scaffold a new project
sqlflow --version                                          # Show version and exit

# Pipeline commands (grouped under 'pipeline' subcommand)
sqlflow pipeline compile [pipeline_name] [--output file]   # Parse & validate pipeline(s), output execution plan
sqlflow pipeline run [pipeline_name] [--vars '{"key": "value"}']  # Execute a pipeline end-to-end
sqlflow pipeline list                                      # List available pipelines in the project

# Future commands (post-MVP)
sqlflow describe [resource]                                # Show details (table, connector)
sqlflow viz [pipeline_name]                                # Render DAG visualization
sqlflow connect list                                       # List available connectors
sqlflow connect test [connector]                           # Validate connector configuration
sqlflow docs generate                                      # Generate HTML/Markdown docs
```

Note: All pipeline-related commands are organized under the `pipeline` subcommand for clarity and future extensibility. Pipeline names are referenced without the `.sf` extension.

## 4. DSL Extensions

### 4.1 Core Directives

- **SOURCE**  
  ```sql
  SOURCE <name> TYPE <connector_type> PARAMS { …JSON… };
  ```
- **LOAD**  
  ```sql
  LOAD <table_name> FROM <source_name>;
  ```
- **INCLUDE**  
  ```sql
  INCLUDE "path/to/file.sf" [AS <alias>];
  ```
- **ENGINE**  
  ```sql
  ENGINE <engine_name> OPTIONS { … };
  ```
- **EXPORT**  
  ```sql
  EXPORT
    SELECT … FROM …
  TO "<destination_uri>"
  TYPE <connector_type>
  OPTIONS { …JSON… };
  ```
- **PYTHON_FUNC**  
  ```sql
  CREATE TABLE … AS
  SELECT PYTHON_FUNC("<module>.<func>", <args…>) AS …
  FROM …;
  ```

### 4.2 Advanced Flow Controls

- **SET** (variables)  
  ```sql
  SET variable_name = "value";
  SET date = "${run_date|2023-10-25}";  -- With default value
  ```
- **IF/ELSE** (conditional)  
- **FOR EACH** (iteration)  
- **DEPENDS_ON** (cross-pipeline dependencies)  

### 4.3 Variable Substitution

SQLFlow supports variable substitution in strings and JSON objects using the `${var}` syntax:

- Variables can be set with the `SET` directive
- Variables can have default values: `${var|default}`
- Variables are passed during execution: `--vars '{"var":"value"}'`
- Variables can be used in paths, queries, and connection parameters
- JSON objects support variable substitution with proper validation

Example:
```sql
SET run_date = "${date|2023-10-25}";

SOURCE sales TYPE CSV PARAMS {
  "path": "data/sales_${run_date}.csv",
  "has_header": true
};

EXPORT SELECT * FROM data 
TO "s3://bucket/${run_date}/data.csv"
TYPE S3
OPTIONS {
  "content_type": "text/csv",
  "acl": "private"
};
```

## 5. Architecture

### 5.1 Phased Workflow

1. **Parsing**  
   - Extend DuckDB's parser to recognize SQLFlow directives.  
2. **Compilation**  
   - Build an AST → dependency graph (DAG of operations).  
3. **Optimization**  
   - Cost-based engine selection, predicate pushdown, parallelization.  
4. **Execution**  
   - Task executor runs DAG, supports resumption, status tracking.  

### 5.2 SQLEngine Interface

```python
class SQLEngine(ABC):
    def execute_query(self, query: str): …
    def create_temp_table(self, name: str, data): …
    def register_arrow(self, table_name: str, arrow_table: pa.Table): …
    def register_python_func(self, name: str, func): …
    def supports_feature(self, feature: str) -> bool: …
```

## 6. Connectors

### 6.1 Connector Plugin Model

```python
class Connector(ABC):
    def configure(self, params: dict): …
    def test_connection(self) -> ConnectionTestResult: …
    def discover(self) -> List[str]: …
    def get_schema(self, object_name: str) -> Schema: …
    def read(self, object_name: str, …) -> Iterator[DataChunk]: …
    def write(self, object_name: str, chunk: DataChunk) -> None: …
```

### 6.2 Sample CSV Connector

```python
class CSVConnector(Connector):
    def configure(self, params):
        self.path = params["path"]
        self.delimiter = params.get("delimiter", ",")
    def test_connection(self):
        open(self.path).close()
        return ConnectionTestResult(success=True)
    def discover(self): return [os.path.splitext(os.path.basename(self.path))[0]]
    def get_schema(self, obj): …  # infer from header + sample rows
    def read(self, obj, columns=None, filters=None): …  # stream DataChunk batches
    def write(self, obj, chunk): …  # append to CSV
```

## 7. Python Function Integration

### 7.1 Registration Decorator

```python
# macros/helpers.py
from sqlflow import python_func
import pandas as pd

@python_func
def calculate_ltv(df: pd.DataFrame, discount_rate: float = 0.1) -> pd.DataFrame:
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    grouped = df.groupby('customer_id')['amount'].sum().reset_index()
    grouped['ltv'] = grouped['amount'] * (1 + discount_rate)
    return grouped[['customer_id', 'ltv']]
```

### 7.2 Invocation in SQL

```sql
INCLUDE "macros/helpers.py";

CREATE TABLE customer_ltv AS
SELECT
  customer_id,
  PYTHON_FUNC("helpers.calculate_ltv", raw_sales, 0.08) AS ltv
FROM raw_sales;
```

## 8. DAG Execution

1. Build DAG from all pipeline statements.  
2. Validate acyclic and completeness.  
3. Execute with thread-pool or distributed executor (Celery).  
4. Track status, allow resume from failure.  
5. Visualize via CLI or Airflow DAG export.

## 9. UX & Onboarding

- Quick Start:  
  1. pip install sqlflow  
  2. sqlflow init my_project  
  3. Write simple .sf with SOURCE/LOAD/EXPORT  
  4. sqlflow pipeline run example  
- Tutorials: "From Business Ask to Pipeline" guides.  
- Cheat Sheets: Core directives, variable syntax, connector patterns.  
- Deployment Recipes: Local, Airflow operator, Kubernetes+Celery, Serverless.

### 9.1 Ecommerce Demo

The ecommerce demonstration provides a comprehensive example of SQLFlow capabilities:

- **Complete Docker Infrastructure**: Ready-to-run environment with Postgres, MockServer for API simulation, and SQLFlow container
- **Multiple Pipeline Examples**: From simple CSV processing to complex database-to-S3 flows
- **Shell Scripts**: Easy initialization in both Bash and Fish shells
- **Testing Tools**: Scripts for connector testing, data generation, and debugging
- **Sample Data**: Pre-configured with realistic ecommerce datasets
- **Documentation**: QUICKSTART.md and comprehensive README.md

## 10. Roadmap

| Phase                 | Timeline      | Deliverables                                        |
|-----------------------|---------------|-----------------------------------------------------|
| MVP                   | 2–3 months    | Core DSL, DuckDB, file connectors, basic CLI        |
| Production-Ready      | 2–3 months    | Python UDFs, DB/API connectors, visualization, resumption |
| Enterprise & Scaling  | 3–4 months    | Distributed execution, monitoring, web UI           |

## 11. Appendix

### 11.1 Example Pipeline

```sql
SET date = '${run_date|2025-05-14}';

SOURCE sales TYPE CSV PARAMS {
  "path": "data/sales_${date}.csv",
  "has_header": true
};

SOURCE users TYPE POSTGRES PARAMS {
  "connection": "${DB_CONN}",
  "table": "users"
};

LOAD sales INTO raw_sales;
LOAD users INTO raw_users;

CREATE TABLE sales_summary AS
SELECT
  s.date,
  u.country,
  SUM(s.amount) AS total_sales,
  PYTHON_FUNC("helpers.calculate_ltv", s, 0.08) AS avg_ltv
FROM raw_sales s
JOIN raw_users u ON s.user_id = u.id
WHERE s.date = ${date}
GROUP BY s.date, u.country;

EXPORT
  SELECT * FROM sales_summary
TO "s3://reports/sales_summary_${date}.parquet"
TYPE S3
OPTIONS { "compression": "snappy", "format": "parquet" };
```

