# SQLFlow vs Airflow

> **MVP Status**: This comparison is part of SQLFlow's MVP documentation and will be expanded as SQLFlow matures.

This document compares SQLFlow with [Apache Airflow](https://airflow.apache.org/), a popular workflow orchestration platform.

## Overview

**Apache Airflow** is a platform to programmatically author, schedule, and monitor workflows. It uses directed acyclic graphs (DAGs) to define workflows as code, typically written in Python. Airflow is designed to handle complex dependencies and scheduling for a wide variety of tasks, not just data processing.

**SQLFlow** is an end-to-end data pipeline platform that covers ingestion, transformation, and export in a unified SQL-based workflow. It includes its own embedded engine (DuckDB) and is specifically focused on data processing pipelines.

## Key Differences

| Feature | SQLFlow | Airflow |
|---------|---------|---------|
| **Core Language** | SQL with directives | Python |
| **Primary Use Case** | Data pipelines | General workflow orchestration |
| **Execution Engine** | Built-in DuckDB engine | Pluggable executors (no built-in engine) |
| **DAG Definition** | Implicit from SQL dependencies | Explicit in Python code |
| **Task Types** | SQL-focused (SOURCE, LOAD, CREATE, EXPORT) | General purpose (can run any code) |
| **Scheduling** | Basic scheduling (MVP phase) | Advanced scheduling with cron syntax |
| **Monitoring** | Basic logging (MVP phase) | Comprehensive UI and monitoring |
| **Deployment** | Lightweight, embeddable | Requires dedicated infrastructure |
| **Learning Curve** | Low (for SQL users) | Moderate to steep |
| **Extensibility** | Python UDFs | Operators, sensors, hooks |

## When to Choose SQLFlow

Consider SQLFlow when:

- Your primary workflow is **data transformation**
- You and your team are more comfortable with **SQL than Python**
- You want a **lightweight solution** without complex infrastructure
- You need **end-to-end data pipelines** in a single tool
- You're working with **smaller to medium datasets**
- You want **automatic dependency tracking** without explicit definitions
- You need **fast development cycles** with minimal setup

## When to Choose Airflow

Consider Airflow when:

- You need to orchestrate **diverse types of tasks** (not just data)
- You require **complex scheduling** patterns
- You need a **mature monitoring** and alerting system
- You're working with **large-scale distributed processing**
- You want to leverage a **large ecosystem** of integrations
- You're comfortable with **Python-based workflow definitions**
- You need to **integrate with many different systems** and services

## Feature Comparison

### Workflow Definition

**SQLFlow**:
```sql
-- pipeline.sf
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users;

CREATE TABLE user_metrics AS
SELECT
  country,
  COUNT(*) AS user_count
FROM users_table
GROUP BY country;

EXPORT
  SELECT * FROM user_metrics
TO "output/metrics.csv"
TYPE CSV
OPTIONS { "header": true };
```

**Airflow**:
```python
# dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG(
    'user_metrics_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

download_data = BashOperator(
    task_id='download_data',
    bash_command='curl -o /tmp/users.csv https://example.com/data/users.csv',
    dag=dag
)

process_data = BashOperator(
    task_id='process_data',
    bash_command='python /scripts/process_users.py',
    dag=dag
)

export_metrics = BashOperator(
    task_id='export_metrics',
    bash_command='python /scripts/export_metrics.py',
    dag=dag
)

download_data >> process_data >> export_metrics
```

### Dependency Management

**SQLFlow** automatically detects dependencies based on table references in SQL queries, creating a directed acyclic graph (DAG) without requiring explicit references.

**Airflow** requires explicit dependency definitions using operators like `>>` or `set_upstream()`/`set_downstream()`.

### Execution Model

**SQLFlow** uses DuckDB as its primary execution engine, running SQL operations in-memory or with persistent storage.

**Airflow** uses a pluggable executor model that can run tasks on different types of workers (LocalExecutor, CeleryExecutor, KubernetesExecutor, etc.), but doesn't provide an execution engine itself.

### Scheduling

**SQLFlow** has basic scheduling capabilities (MVP phase).

**Airflow** has comprehensive scheduling with:
- Cron-based schedules
- Data-driven triggers
- Backfilling capabilities
- Catchup mode for missed runs

## Integration Possibilities

SQLFlow and Airflow can be complementary in some scenarios:

- Use **Airflow** for overall workflow orchestration
- Use **SQLFlow** for specific data processing steps within an Airflow DAG
- Trigger **SQLFlow** pipelines from Airflow using the BashOperator

Example of using SQLFlow within an Airflow DAG:
```python
run_sqlflow_pipeline = BashOperator(
    task_id='run_sqlflow_pipeline',
    bash_command='sqlflow pipeline run user_metrics --profile production --vars \'{"date": "{{ ds }}"}\''
)
```

## Conclusion

SQLFlow and Airflow serve different but complementary needs in the data ecosystem. SQLFlow provides a simpler, SQL-focused approach specifically for data pipelines, while Airflow offers a more general-purpose orchestration platform with extensive features for complex workflows.

For teams primarily working with data and comfortable with SQL, SQLFlow offers a more straightforward approach with less overhead. For organizations that need to orchestrate diverse workloads beyond just data processing, Airflow provides the flexibility and robustness required for complex enterprise scenarios. 