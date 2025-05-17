# SQLFlow Ecommerce Demo - Quick Start Guide

This guide will help you quickly set up and run the SQLFlow ecommerce demo, showcasing key features through practical pipelines.

> **Important: Profile-Driven Configuration**
> All environment and engine configurations for this demo (and SQLFlow projects in general) are managed via **profiles** located in the `profiles/` directory.
> - Use `--profile dev` for local development. This typically uses an **in-memory DuckDB** (fast, no data saved to disk).
> - Use `--profile production` for simulated production runs. This typically uses a **persistent DuckDB** (data saved to `target/ecommerce_demo_prod.db` as configured in `profiles/production.yml`, using this exact path). SQLFlow will use the exact `path` specified in the profile for persistent DuckDB files.
> Edit the appropriate profile YAML file (e.g., `profiles/dev.yml`, `profiles/production.yml`) to change connector settings, engine parameters (like DuckDB mode or path), or define variables.
> Also, remember that the `target/run/<pipeline_name>/` directory containing artifacts for a specific pipeline run is cleared and recreated each time `sqlflow pipeline run` is executed.

## Prerequisites

- Docker and Docker Compose v2 installed
- Git repository of SQLFlow cloned

## 1. Build SQLFlow (If Needed)

This step is separated out so you can update the SQLFlow package independently. The `start-demo.sh` script will check if this step has been completed and guide you through it if needed.

```bash
# Navigate to the SQLFlow repository root
cd /path/to/sqlflow

# Set up Python environment and build the package
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel build
pip install -e .
python -m build --wheel
```

## 2. Start the Demo Environment

```bash
# Navigate to the ecommerce demo directory
cd /path/to/sqlflow/demos/ecommerce_demo

# Make the scripts executable if needed
chmod +x start-demo.sh init-demo.sh

# Build and start all services
./start-demo.sh
```

The script will:
1. Check for a pre-built SQLFlow wheel and guide you to build one if not found
2. Copy the wheel to the demo directory
3. Build a Docker image for the demo (if needed)
4. Start all required services with Docker Compose

## 3. Initialize and Run Demo Pipelines

```bash
./init-demo.sh
```

This will:
1. Verify all services are running
2. Test connections to PostgreSQL, MinIO, and MockServer
3. Run the example pipelines, demonstrating various SQLFlow features
4. Show you how to access and explore the results

## 4. Available Pipelines

The demo includes several example pipelines located in the `pipelines/` directory. These are designed to showcase different features and use cases of SQLFlow.

### Showcase Pipelines:

These pipelines demonstrate core features and advanced capabilities.

1.  **`showcase_01_basic_csv_processing.sf`**
    *   **Description**: Demonstrates core SQLFlow features using CSV files for input and output. Covers data loading, SQL transformations, and exporting results.
    *   **Run**: `docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/showcase_01_basic_csv_processing.sf`

2.  **`showcase_02_multi_connector_integration.sf`**
    *   **Description**: Highlights SQLFlow's ability to integrate with multiple systems. Uses PostgreSQL for input, CSV for comparison, and exports to S3, PostgreSQL, and a REST API.
    *   **Run**: `docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/showcase_02_multi_connector_integration.sf --vars '{"date": "2023-10-25", "API_TOKEN": "your_api_token"}'`

3.  **`showcase_03_conditional_execution.sf`**
    *   **Description**: Illustrates conditional logic within SQLFlow pipelines, allowing different processing paths based on variables (e.g., environment, region).
    *   **Run**: `docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/showcase_03_conditional_execution.sf --vars '{"environment": "production", "region": "us-east"}'`

4.  **`showcase_04_advanced_analytics.sf`**
    *   **Description**: Presents advanced SQL analytics capabilities, including time series analysis, cohort analysis, RFM segmentation, and market basket analysis.
    *   **Run**: `docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/showcase_04_advanced_analytics.sf`

### Additional Examples:

1.  **`example_01_daily_sales_report.sf`**
    *   **Description**: A practical example of a daily sales reporting pipeline. Sources data from PostgreSQL, performs enrichments and aggregations, and exports to S3 and a REST API. Includes debug steps.
    *   **Run**: `docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/example_01_daily_sales_report.sf --vars '{"date": "2023-10-25", "API_TOKEN": "your_api_token"}'`

2.  **`example_02_csv_only_processing.sf`**
    *   **Description**: A self-contained example demonstrating a complete data pipeline using only CSV files for input and output.
    *   **Run**: `docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/example_02_csv_only_processing.sf`

## 5. Exploring Results

### PostgreSQL Data

```bash
docker compose exec postgres psql -U sqlflow -d ecommerce -c "SELECT * FROM sales LIMIT 5;"
```

### MinIO Results

1. Open the MinIO Console at http://localhost:9001 (login with minioadmin/minioadmin)
2. Navigate to the `analytics` bucket to see the exported files

### REST API Notifications

```bash
curl -X PUT "http://localhost:1080/mockserver/retrieve?type=REQUESTS&format=JSON" | jq
```

## 6. Development Within the Container

```bash
# Open a shell in the SQLFlow container
docker compose exec sqlflow bash

# Run commands inside the container
cd /app/sqlflow/demos/ecommerce_demo
sqlflow pipeline list
```

## 7. Cleaning Up

```bash
# Stop and remove all containers, networks, and volumes
docker compose down -v
```

# Configuration

All environment and engine configuration for the SQLFlow ecommerce demo is managed via **profiles** in the `profiles/` directory. This allows you to easily switch between different setups, for example, an in-memory DuckDB for development and a persistent DuckDB for production scenarios.

### `profiles/dev.yml` (Development)
This profile is configured for local development and testing.
```yaml
# Example: profiles/dev.yml
engines:
  duckdb:
    mode: memory      # DuckDB runs in-memory, no data is saved to disk.
    memory_limit: 4GB # Example memory limit for dev
variables:
  DATE: '${env.DATE:2023-10-25}' # Example: use environment variable DATE or default
  API_TOKEN: '${env.API_TOKEN:dev-dummy-token}'
connectors:
  postgres:
    type: POSTGRES
    params:
      host: postgres # Service name in docker-compose
      port: 5432
      dbname: ecommerce
      user: sqlflow
      password: sqlflow123
  s3:
    type: S3
    params:
      endpoint_url: http://minio:9000 # Service name in docker-compose
      region: us-east-1
      access_key: minioadmin
      secret_key: minioadmin
      bucket: analytics
  rest:
    type: REST
    params:
      base_url: http://mockserver:1080 # Service name in docker-compose
      auth_method: bearer
      auth_params:
        token: '${API_TOKEN}'
```

### `profiles/production.yml` (Production-like)
This profile simulates a production setup, notably by using a persistent DuckDB.
```yaml
# Example: profiles/production.yml
engines:
  duckdb:
    mode: persistent  # DuckDB saves data to the specified file. SQLFlow uses this exact path.
    path: target/ecommerce_demo_prod.db # All tables, including transforms, are saved here.
    memory_limit: 8GB # Example memory limit for prod
variables:
  DATE: '${env.DATE:$(date +%Y-%m-%d)}' # Example: use environment variable DATE or default to today
  API_TOKEN: '${env.API_TOKEN:prod-secret-token}'
connectors:
  # Production connector configurations would typically point to real services
  postgres:
    type: POSTGRES
    params:
      host: postgres # In a real prod, this would be your production DB host
      port: 5432
      dbname: ecommerce
      user: sqlflow
      password: sqlflow123
  s3:
    type: S3
    params:
      endpoint_url: http://minio:9000 # In a real prod, this would be your production S3 endpoint
      region: us-east-1
      access_key: minioadmin
      secret_key: minioadmin
      bucket: analytics-prod # Example: different bucket for production
  rest:
    type: REST
    params:
      base_url: http://mockserver:1080 # In a real prod, this would be your production API
      auth_method: bearer
      auth_params:
        token: '${API_TOKEN}'
```

To use a specific profile, simply add the `--profile <profile_name>` flag to your `sqlflow pipeline run` command. If omitted, SQLFlow will default to using the `dev` profile if `profiles/dev.yml` exists.
