# SQLFlow Ecommerce Demo - Quick Start Guide

This guide will help you quickly start the SQLFlow ecommerce demo environment.

> **Important: Profile-Driven Configuration**
> All environment and engine configurations for this demo (and SQLFlow projects in general) are managed via **profiles** located in the `profiles/` directory.
> - Use `--profile dev` for local development. This typically uses an **in-memory DuckDB** (fast, no data saved to disk).
> - Use `--profile production` for simulated production runs. This typically uses a **persistent DuckDB** (data saved to `target/ecommerce_demo_prod.db` as configured in `profiles/production.yml`).
> Edit the appropriate profile YAML file (e.g., `profiles/dev.yml`, `profiles/production.yml`) to change connector settings, engine parameters (like DuckDB mode or path), or define variables.

## Prerequisites

- Docker and Docker Compose installed
- Git repository of SQLFlow cloned

## Steps to Start the Demo

### 1. Start the Docker services

```bash
# Clone the repository if you haven't already
git clone https://github.com/your-org/sqlflow.git
cd sqlflow/demos/ecommerce_demo

# Make the script executable if needed
chmod +x start-demo.sh

# Build and start all services
./start-demo.sh
```

### 2. Initialize and test the demo environment

```bash
./init-demo.sh
```

Alternative for Fish shell users:
```fish
./init-demo.fish
```

### 3. Access the services

- **PostgreSQL:** localhost:5432 (User: sqlflow, Password: sqlflow123, DB: ecommerce)
- **MinIO Console:** http://localhost:9001 (User: minioadmin, Password: minioadmin)
- **MockServer:** http://localhost:1080

### 4. Run the SQLFlow pipeline manually

Remember to select the appropriate profile for your needs. For example, to run with development settings (in-memory DuckDB):
```bash
export DATE=$(date '+%Y-%m-%d')
export API_TOKEN=demo-token # Example token

cd /path/to/sqlflow/demos/ecommerce_demo
sqlflow pipeline run pipelines/daily_sales_report_docker.sf --vars "{\"date\": \"$DATE\", \"API_TOKEN\": \"$API_TOKEN\"}" --profile dev
```

To run with production settings (persistent DuckDB, data saved to `target/ecommerce_demo_prod.db`):
```bash
sqlflow pipeline run pipelines/daily_sales_report_docker.sf --vars "{\"date\": \"$DATE\", \"API_TOKEN\": \"$API_TOKEN\"}" --profile production
```

Alternative for Fish shell users (development profile example):
```fish
set -x DATE (date '+%Y-%m-%d')
set -x API_TOKEN demo-token # Example token

cd /path/to/sqlflow/demos/ecommerce_demo
sqlflow pipeline run pipelines/daily_sales_report_docker.sf --vars "{\"date\": \"$DATE\", \"API_TOKEN\": \"$API_TOKEN\"}" --profile dev
```

### 5. Access the SQLFlow container for development

```bash
docker-compose exec sqlflow bash
```

## What's included in the demo?

1. **PostgreSQL database with sample data:**
   - Customers
   - Products
   - Sales transactions

2. **MinIO S3-compatible storage:**
   - Used to demonstrate S3 export connector

3. **MockServer for HTTP endpoints:**
   - Used to demonstrate REST export connector

4. **SQLFlow demo container:**
   - Pre-configured to work with all services

## Demo workflow

The sample pipeline demonstrates:

1. Reading data from PostgreSQL tables
2. Transforming data with SQL operations
3. Exporting results to S3 (MinIO) in Parquet format
4. Sending notifications to a REST API endpoint (MockServer)

## Extending the Demo

1. Add new connectors or change engine settings by editing the appropriate profile YAML in the `profiles/` directory (e.g., `profiles/dev.yml`, `profiles/production.yml`).
2. Create new pipeline files in the `pipelines` directory
3. Add new data sources by updating the PostgreSQL initialization scripts

## Troubleshooting

1. **Services not starting:**
   ```bash
   docker-compose logs
   ```

2. **Pipeline execution errors:**
   Check the SQLFlow logs during pipeline execution

3. **Resetting the environment:**
   ```bash
   docker-compose down -v
   docker-compose up -d
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
    mode: persistent  # DuckDB saves data to the specified file.
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
