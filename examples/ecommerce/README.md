# SQLFlow Ecommerce Demo

This demo showcases SQLFlow's capabilities to handle real-world data pipeline scenarios, providing a comprehensive example of SQL-based data transformation pipelines.

## Key Features Demonstrated

### 1. Multiple Connector Types
- **PostgreSQL Connector** - Read from and write to databases
- **S3 Connector** - Export data to S3-compatible storage (MinIO)
- **REST API Connector** - Send data to RESTful API endpoints

### 2. Complete Data Pipeline Process
- Data ingestion from multiple sources
- Transformation with SQL
- Export to multiple destinations
- Conditional logic

### 3. Variable Substitution
- Dynamic parameterization with dates, tokens, and other variables
- Default values for variables

## Demo Structure

```
demos/ecommerce_demo/
├── data/                  # Sample data files
├── pipelines/             # SQLFlow pipeline definitions
├── profiles/              # Configuration profiles
├── init-scripts/          # Initialization scripts for services
├── mock-config/           # Configuration for mock services
├── docker-compose.yml     # Docker Compose configuration
├── Dockerfile             # Docker image for SQLFlow
├── init-demo.sh           # Demo initialization script
├── start-demo.sh          # Demo startup script
└── build-sqlflow-package.sh  # Script to build SQLFlow package
```

## Quick Start

### 1. Build the SQLFlow Package (Optional)

This step is optional but recommended if you want to use the latest version of SQLFlow:

```bash
./build-sqlflow-package.sh
```

This builds the SQLFlow package from source and places it in the `dist/` directory. The package will be used by the Docker image.

### 2. Start the Demo Environment

```bash
./start-demo.sh
```

This script:
- Checks all prerequisites
- Builds and starts Docker containers for:
  - SQLFlow demo
  - PostgreSQL database
  - MinIO (S3-compatible storage)
  - Mock REST API server

### 3. Run Demo Pipelines

```bash
./init-demo.sh
```

This script:
- Tests all service connections
- Initializes databases and buckets
- Runs sample pipelines

### 4. Clean Up

```bash
docker compose down
```

## Demo Pipelines

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

## Customizing the Demo

### Using Variables

All pipelines support variable substitution:

```bash
docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/simple_test.sf --vars '{"date": "2023-11-01"}' --profile production
```

### Development vs. Production Mode

The demo supports multiple configuration profiles:

- **Development**: `--profile dev` - Uses in-memory database for fast testing
- **Production**: `--profile production` - Uses persistent storage

## Troubleshooting

If you encounter issues with the demo:

1. Check container status: `docker compose ps`
2. View container logs: `docker compose logs sqlflow`
3. Access container shell: `docker compose exec sqlflow bash`

## Advanced Usage

### Exploring the Database

Connect to PostgreSQL:

```bash
docker compose exec postgres psql -U sqlflow -d ecommerce
```

### Exploring MinIO

Access the MinIO console at http://localhost:9001 (User: minioadmin, Password: minioadmin).

### Testing the Mock API Server

Access the MockServer UI at http://localhost:1080/mockserver/dashboard.
