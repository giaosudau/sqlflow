# SQLFlow Connector Usage Guide

This guide explains how to use data connectors in SQLFlow, including both source connectors for data ingestion and export connectors for data delivery.

## Table of Contents

1. [Introduction to Connectors](#introduction-to-connectors)
2. [Available Connectors](#available-connectors)
3. [Source Connectors](#source-connectors)
4. [Export Connectors](#export-connectors)
5. [Configuration Parameters](#configuration-parameters)
6. [Examples](#examples)
7. [Bidirectional Connectors](#bidirectional-connectors)
8. [Advanced Usage](#advanced-usage)

## Introduction to Connectors

Connectors in SQLFlow provide standardized interfaces for reading data from various sources and writing data to different destinations. They enable seamless data movement between systems while maintaining a consistent SQL-based interface.

### Key Concepts

- **Source Connectors**: Used for reading data from external systems
- **Export Connectors**: Used for writing data to external systems
- **Bidirectional Connectors**: Support both reading and writing operations
- **Data Chunks**: Data is transferred in chunks for efficiency and memory management
- **Connection Management**: Handles authentication, connection pooling, and state tracking

## Available Connectors

SQLFlow provides the following built-in connectors:

| Connector Type | Source | Export | Description |
|---------------|--------|--------|-------------|
| CSV           | ✅     | ✅     | Read/write CSV files |
| PostgreSQL    | ✅     | ✅     | Connect to PostgreSQL databases |
| REST          | ✅     | ✅     | Interact with REST APIs |
| S3            | ✅     | ✅     | Store/retrieve data from S3-compatible storage |
| Parquet       | ✅     | ✅     | Read/write Parquet files |
| Google Sheets | ✅     | ✅     | Read/write Google Sheets data |

## Source Connectors

Source connectors are used to read data from external systems. They are defined using the `SOURCE` statement in SQLFlow.

### Basic Syntax

```sql
SOURCE source_name TYPE connector_type PARAMS {
  "param1": "value1",
  "param2": "value2",
  ...
};

LOAD table_name FROM source_name;
```

### Example: CSV Source Connector

```sql
SOURCE sales_data TYPE CSV PARAMS {
  "path": "data/sales_2023.csv",
  "has_header": true
};

LOAD sales_table FROM sales_data;
```

### Example: PostgreSQL Source Connector

```sql
SOURCE customer_data TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "dbname": "ecommerce",
  "user": "sqlflow",
  "password": "password123",
  "table": "customers" 
};

LOAD customers FROM customer_data;
```

### Example: Google Sheets Source Connector

```sql
SOURCE monthly_budget TYPE GOOGLE_SHEETS PARAMS {
  "credentials_file": "path/to/service-account-key.json",
  "spreadsheet_id": "1a2b3c4d5e6f7g8h9i0j",
  "sheet_name": "Budget2023",
  "range": "A1:F100",
  "has_header": true
};

LOAD budget_data FROM monthly_budget;
```

## Export Connectors

Export connectors write data to external systems. They are used with the `EXPORT` statement.

### Basic Syntax

```sql
EXPORT select_statement
TO "destination_path_or_uri"
TYPE connector_type
OPTIONS {
  "option1": "value1",
  "option2": "value2",
  ...
};
```

### Example: S3 Export Connector

```sql
EXPORT 
  SELECT * FROM daily_sales_summary 
TO "s3://analytics/reports/sales_summary_${date}.parquet"
TYPE S3
OPTIONS { 
  "format": "parquet",
  "compression": "snappy",
  "endpoint_url": "http://localhost:9000",
  "region": "us-east-1",
  "access_key": "access_key_here",
  "secret_key": "secret_key_here"
};
```

### Example: REST Export Connector

```sql
EXPORT
  SELECT 
    'daily_report' AS report_type,
    COUNT(*) AS total_records,
    SUM(amount) AS total_amount
  FROM sales_data
TO "https://api.example.com/reports"
TYPE REST
OPTIONS {
  "method": "POST",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "Bearer ${API_TOKEN}"
  }
};
```

### Example: Google Sheets Export Connector

```sql
EXPORT
  SELECT 
    department,
    SUM(salary) AS total_salary,
    AVG(salary) AS average_salary,
    COUNT(*) AS employee_count
  FROM employees
  GROUP BY department
TO "Department Salary Report"
TYPE GOOGLE_SHEETS
OPTIONS {
  "credentials_file": "path/to/service-account-key.json",
  "spreadsheet_id": "1a2b3c4d5e6f7g8h9i0j",
  "sheet_name": "SalaryReport",
  "has_header": true
};
```

## Configuration Parameters

Each connector type requires specific configuration parameters. Here are the common parameters for each connector:

### CSV Connector

| Parameter | Required | Description |
|-----------|----------|-------------|
| path      | Yes      | File path (relative or absolute) |
| has_header| No       | Whether the file has a header row (default: true) |
| delimiter | No       | Column delimiter (default: ",") |
| encoding  | No       | File encoding (default: "utf-8") |

### PostgreSQL Connector

| Parameter | Required | Description |
|-----------|----------|-------------|
| host      | Yes      | Database host |
| port      | Yes      | Database port |
| dbname    | Yes      | Database name |
| user      | Yes      | Database username |
| password  | Yes      | Database password |
| table     | Yes      | Table name for source connector |
| schema    | No       | Database schema (default: "public") |

### S3 Connector

| Parameter    | Required | Description |
|--------------|----------|-------------|
| access_key   | Yes      | AWS access key |
| secret_key   | Yes      | AWS secret key |
| region       | Yes      | AWS region |
| bucket       | Yes      | S3 bucket name |
| endpoint_url | No       | Custom endpoint for S3-compatible storage |
| format       | No       | File format (parquet, csv, json) for export |
| compression  | No       | Compression type (snappy, gzip, etc.) |

### Google Sheets Connector

| Parameter        | Required | Description |
|------------------|----------|-------------|
| credentials_file | Yes      | Path to service account credentials JSON file |
| spreadsheet_id   | Yes      | ID of the Google Spreadsheet |
| sheet_name       | No       | Name of the sheet (default: "Sheet1") |
| range            | No       | Cell range (e.g., "A1:D100") |
| has_header       | No       | Whether the sheet has headers (default: true) |

## Examples

### Complete Pipeline Example

```sql
-- Define data sources
SOURCE sales TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "dbname": "ecommerce",
  "user": "sqlflow",
  "password": "sqlflow123",
  "table": "sales"
};

SOURCE customers TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "dbname": "ecommerce",
  "user": "sqlflow",
  "password": "sqlflow123",
  "table": "customers"
};

-- Load data
LOAD raw_sales FROM sales;
LOAD customer_data FROM customers;

-- Transform data
CREATE TABLE sales_summary AS
SELECT 
  s.order_id,
  s.customer_id,
  c.name AS customer_name,
  s.order_date,
  SUM(s.quantity * s.price) AS total_amount
FROM raw_sales s
JOIN customer_data c ON s.customer_id = c.id
GROUP BY s.order_id, s.customer_id, c.name, s.order_date;

-- Export to Google Sheets
EXPORT 
  SELECT * FROM sales_summary
TO "Sales Summary"
TYPE GOOGLE_SHEETS
OPTIONS {
  "credentials_file": "path/to/credentials.json",
  "spreadsheet_id": "your-spreadsheet-id-here",
  "sheet_name": "SalesSummary"
};

-- Export to S3 storage
EXPORT 
  SELECT * FROM sales_summary
TO "s3://analytics/sales_summary.parquet"
TYPE S3
OPTIONS {
  "format": "parquet",
  "access_key": "${S3_ACCESS_KEY}",
  "secret_key": "${S3_SECRET_KEY}",
  "region": "us-east-1"
};
```

### Using Environment Variables

You can use environment variables in your configuration by using `${VAR_NAME}` syntax:

```sql
SOURCE pg_data TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "dbname": "ecommerce",
  "user": "${DB_USER}",
  "password": "${DB_PASSWORD}",
  "table": "sales"
};
```

## Bidirectional Connectors

SQLFlow provides a specialized `BidirectionalConnector` base class for connectors that support both reading and writing operations. These connectors are registered using the `@register_bidirectional_connector` decorator, which automatically registers them as both source and export connectors.

### Benefits of Bidirectional Connectors

- **Simplified Implementation**: Inheriting from a single base class instead of multiple inheritance
- **Clear Intent**: Makes it explicit that the connector supports both reading and writing
- **Improved Type Safety**: Better IDE support and clearer error messages
- **Runtime Validation**: Checks at registration time that both `read()` and `write()` methods are properly implemented
- **Streamlined Registry**: One registration call instead of two
- **Backward Compatibility**: Automatically registered in both source and export registries

### Technical Implementation

The bidirectional connector architecture consists of:

1. **ConnectorType Enum**: Defines `SOURCE`, `EXPORT`, and `BIDIRECTIONAL` types for clear semantics
2. **BidirectionalConnector Base Class**: Inherits from both `Connector` and `ExportConnector`
3. **Registration Decorator**: `@register_bidirectional_connector` provides runtime validation
4. **Registry Management**: Connectors are accessible through dedicated getter methods

Example of a connector registration:

```python
from sqlflow.connectors.base import BidirectionalConnector
from sqlflow.connectors.registry import register_bidirectional_connector

@register_bidirectional_connector("MY_CONNECTOR")
class MyConnector(BidirectionalConnector):
    def __init__(self):
        super().__init__()
        # The connector_type is automatically set to ConnectorType.BIDIRECTIONAL
        
    # Must implement methods from both source and export interfaces:
    # configure, test_connection, discover, get_schema, read, write
```

### Google Sheets as a Bidirectional Connector

The Google Sheets connector is implemented as a bidirectional connector. It allows you to:

1. **Read data from Google Sheets** - Use it as a source connector to import data from spreadsheets into your SQLFlow pipeline
2. **Write data to Google Sheets** - Use it as an export connector to send processed data to Google Sheets

Here's an example of a complete pipeline that reads data from one Google Sheet, processes it, and writes the results to another sheet:

```sql
-- Read data from a Google Sheet
SOURCE product_data TYPE GOOGLE_SHEETS PARAMS {
  "credentials_file": "path/to/service-account-key.json",
  "spreadsheet_id": "1a2b3c4d5e6f7g8h9i0j",
  "sheet_name": "RawProducts",
  "has_header": true
};

-- Load the data
LOAD raw_products FROM product_data;

-- Transform the data
CREATE TABLE product_summary AS
SELECT 
  category,
  COUNT(*) AS product_count,
  AVG(price) AS average_price,
  SUM(inventory) AS total_inventory
FROM raw_products
GROUP BY category;

-- Export the transformed data to another Google Sheet
EXPORT
  SELECT * FROM product_summary
TO "Product Summary"
TYPE GOOGLE_SHEETS
OPTIONS {
  "credentials_file": "path/to/service-account-key.json",
  "spreadsheet_id": "0z9y8x7w6v5u4t3s2r1q",
  "sheet_name": "CategorySummary"
};
```

### Other Bidirectional Connectors

In addition to Google Sheets, the following connectors also support bidirectional operations:

- **PostgreSQL** - Read data from tables and write results back to the database
- **S3** - Read from files in S3 buckets and write results to new files
- **REST API** - Pull data from API endpoints and push results to other endpoints

Using bidirectional connectors enables efficient data synchronization between different systems without requiring multiple specialized connectors.

### Testing Bidirectional Connectors

SQLFlow provides a `TestBidirectionalConnectorMixin` that can be used as a base class for testing any connector that inherits from `BidirectionalConnector`. This mixin ensures that:

1. The connector has the correct connector type
2. The connector implements both source and export interfaces
3. The connector can perform round-trip operations (write then read)

The testing mixin verifies all contract requirements are satisfied, ensuring reliable behavior across all bidirectional connectors.

## Advanced Usage

### Working with Profiles

SQLFlow supports profiles for managing environment-specific configurations. Create a profile file in the `profiles/` directory:

```yaml
# profiles/dev.yaml
variables:
  DB_HOST: localhost
  DB_PORT: 5432
  DB_USER: dev_user
  DB_PASSWORD: dev_password
```

Then run your pipeline with the profile:

```bash
sqlflow run pipeline --profile dev
```

### Error Handling

Connectors provide detailed error information when failures occur. You can configure logging to capture these errors:

```bash
sqlflow run pipeline --log-level debug
```

### Connection Testing

You can test connections before running a pipeline:

```bash
sqlflow connect test my_postgres_conn
```

Where `my_postgres_conn` is defined in your profile or configuration.

### Listing Available Connectors

List all registered connectors:

```bash
sqlflow connect list
```

---

For more information, refer to the [SQLFlow documentation](https://sqlflow.dev/docs) or contact the support team. 