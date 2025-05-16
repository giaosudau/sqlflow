# SQLFlow Ecommerce Demo - Quick Start Guide

This guide will help you quickly start the SQLFlow ecommerce demo environment.

> **Note:** All environment and engine configuration is now managed via **profiles** in the `profiles/` directory. Use `--profile dev` for local development (in-memory DuckDB), and `--profile production` for production (persistent DuckDB, data saved to disk). Edit the appropriate profile YAML to change connectors, engine settings, or variables.

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

```bash
export DATE=$(date '+%Y-%m-%d')
export API_TOKEN=demo-token

cd /path/to/sqlflow/demos/ecommerce_demo
sqlflow pipeline run pipelines/daily_sales_report_docker.sf --vars "{\"date\": \"$DATE\", \"API_TOKEN\": \"$API_TOKEN\"}" --profile dev
```

Alternative for Fish shell users:
```fish
set -x DATE (date '+%Y-%m-%d')
set -x API_TOKEN demo-token

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

All environment and engine configuration is now managed via **profiles** in the `profiles/` directory. For example:

- `profiles/dev.yml` for local development (in-memory DuckDB, fast, ephemeral)
- `profiles/production.yml` for production (persistent DuckDB, data saved to disk)

To add new connectors or change engine settings, edit the appropriate profile YAML file.
