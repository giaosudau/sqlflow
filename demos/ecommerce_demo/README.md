# SQLFlow Ecommerce Demo

This demo showcases SQLFlow's capabilities to handle real-world data pipeline scenarios, particularly focusing on various connector types and export functionalities.

## Featured Connectors

### 1. PostgreSQL Connector
- **Type:** Source & Export
- **Capabilities:**
  - Reading data from PostgreSQL tables
  - Exporting data to PostgreSQL tables with upsert/append modes
  - Connection pooling for better performance
  - Schema inference from database tables

### 2. S3 Connector
- **Type:** Export
- **Capabilities:**
  - Exporting data to S3-compatible storage
  - Support for various file formats (CSV, Parquet, JSON)
  - Compression options (gzip, snappy)
  - Multi-part uploads for large datasets
  - Custom file naming templates

### 3. REST API Connector
- **Type:** Export
- **Capabilities:**
  - Exporting data to REST API endpoints
  - Support for various authentication methods (Basic, Bearer, API Key, OAuth)
  - Customizable headers and request parameters
  - Retry logic for better reliability
  - Batching of records for better performance

## Services Included

This Docker Compose setup includes:

1. **PostgreSQL** - For database operations
   - Host: `postgres`
   - Port: `5432`
   - Database: `ecommerce`
   - Username: `sqlflow`
   - Password: `sqlflow123`

2. **MinIO** - S3-compatible storage service
   - API Endpoint: `http://localhost:9000`
   - Console: `http://localhost:9001`
   - Access Key: `minioadmin`
   - Secret Key: `minioadmin`
   - Bucket: `analytics`

3. **MockServer** - Mock HTTP API endpoints
   - Endpoint: `http://localhost:1080`
   - Configured endpoint: `/notifications` (POST)

## Running the Demo

### 1. Start the environment

```bash
cd /path/to/sqlflow/demos/ecommerce_demo
docker-compose up -d
```

### 2. Verify all services are running

```bash
docker-compose ps
```

### 3. Run the SQLFlow pipeline

```bash
sqlflow pipeline run daily_sales_report_docker --vars '{"date": "2023-10-26", "API_TOKEN": "demo-token"}'
```

### 4. Check the results

#### PostgreSQL Data

Connect to the PostgreSQL database to see the source data:

```bash
docker-compose exec postgres psql -U sqlflow -d ecommerce -c "SELECT * FROM sales;"
```

#### MinIO Results

1. Open the MinIO Console at http://localhost:9001 (login with minioadmin/minioadmin)
2. Navigate to the `analytics` bucket to see the exported Parquet files

#### REST API Notifications

View the received notifications:

```bash
curl -X PUT "http://localhost:1080/mockserver/retrieve?type=REQUESTS&format=JSON" | jq
```

## Modifying the Demo

### Updating the SQL Pipeline

Edit the `pipelines/daily_sales_report_docker.sf` file to modify the pipeline logic.

### Adding New Data

You can add new data by:

1. Modifying the PostgreSQL initialization scripts in `init-scripts/postgres/`
2. Rebuilding the environment with `docker-compose down -v && docker-compose up -d`

### Testing Different Connectors

The demo includes examples of:
- PostgreSQL source connector
- S3 export connector
- REST API export connector

You can modify these configurations in the pipeline file to test with different parameters.

## Business Use Cases Demonstrated

This demo showcases how SQLFlow can be used for real-world ecommerce analytics:

### 1. Sales Performance Analysis
- Calculate daily revenue and order counts
- Track sales by product category
- Analyze regional performance differences

### 2. Customer Insights
- Calculate customer lifetime value metrics
- Identify high-value customers
- Track customer purchase frequency

### 3. Inventory and Product Management
- Identify best-selling products
- Analyze product category performance
- Support inventory forecasting through sales data

### 4. Financial Reporting
- Generate data for revenue reports
- Calculate critical KPIs like Average Order Value (AOV)
- Track growth metrics day-over-day

### 5. Executive Dashboards
- Prepare visualization-ready data for BI tools
- Support business decision making with aggregated metrics
- Enable drill-down analysis from high-level KPIs

To explore these business cases, run the different pipeline examples:
- `daily_sales_report_docker.sf` - Comprehensive sales analytics
- `simple_test.sf` - Basic sales summary
- `visualization_demo.sf` - Data preparation for dashboards

## Developing with SQLFlow

This demo setup is not only for showcasing SQLFlow capabilities but also serves as a development environment for:

1. Testing connector implementations
2. Validating pipeline execution flows
3. Debugging integration between different services

When developing new features for SQLFlow, you can use this environment to validate your changes in a realistic scenario.

## Troubleshooting

### Database Connection Issues

If the pipeline fails to connect to PostgreSQL:

```bash
docker-compose logs postgres
```

### MinIO Export Failures

Check MinIO logs:

```bash
docker-compose logs minio
```

### REST API Problems

Examine MockServer logs:

```bash
docker-compose logs mockserver
```

## Cleaning Up

To stop and remove all containers, networks, and volumes:

```bash
docker-compose down -v
```
