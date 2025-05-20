# SQLFlow Connector Management Best Practices

This guide outlines best practices for managing data connectors in SQLFlow pipelines across different environments. Following these patterns will improve maintainability, security, and environment portability of your pipelines.

## The Problem with Duplicated Connector Definitions

A common issue in data pipeline development is the hardcoding of connection details directly in pipeline files. This creates several challenges:

- **Credential Management**: Sensitive information like passwords and API keys get stored in pipeline code
- **Environment Switching**: Different environments (dev, test, prod) require manual changes to connection details
- **Duplication**: The same connection information gets repeated across multiple pipelines
- **Security Risk**: Hardcoded credentials in files are more likely to be committed to source control

## The SQLFlow Profile System

SQLFlow solves these problems with its profile system, which separates connection configuration from pipeline logic:

- **Profiles**: YAML files in the `profiles/` directory define environment-specific settings
- **Connector Definitions**: Each profile contains a `connectors` section with named connection configurations
- **Pipeline References**: Pipelines reference these named connectors instead of defining connections inline

## Two Syntax Options for Defining Sources

SQLFlow supports two syntax options for defining sources:

### 1. Direct Source Definition (Not Recommended for Production)

```sql
-- Directly defines connector details in the pipeline
SOURCE postgres_sales TYPE POSTGRES PARAMS {
  "host": "postgres",
  "port": 5432,
  "dbname": "ecommerce",
  "user": "sqlflow",
  "password": "sqlflow123",
  "table": "sales"
};
```

### 2. Profile-Based Source Definition (Recommended)

```sql
-- References a connector defined in the profile
SOURCE postgres_sales FROM "postgres" OPTIONS { "table": "sales" };
```

## Best Practice Recommendations

### 1. Define Base Connectors in Profiles

Create properly structured profile files for each environment:

```yaml
# profiles/dev.yml
connectors:
  postgres:
    type: POSTGRES
    params:
      host: "localhost"
      port: 5432
      dbname: "ecommerce"
      user: "dev_user"
      password: "dev_password"
  s3:
    type: S3
    params:
      endpoint_url: "http://localhost:9000"
      region: "us-east-1"
      access_key: "minioadmin"
      secret_key: "minioadmin"
      bucket: "analytics-dev"
```

### 2. Reference Profile Connectors in Pipelines

Use the `FROM` syntax in your pipelines to reference connectors defined in profiles:

```sql
-- Reference PostgreSQL data sources from profile
SOURCE postgres_sales FROM "postgres" OPTIONS { "table": "sales" };
SOURCE postgres_customers FROM "postgres" OPTIONS { "table": "customers" };
SOURCE postgres_products FROM "postgres" OPTIONS { "table": "products" };
```

### 3. Add Source-Specific Options with OPTIONS

The `OPTIONS` clause allows you to specify source-specific parameters without duplicating the connection details:

```sql
-- Only specify the table name, connection details come from profile
SOURCE postgres_sales FROM "postgres" OPTIONS { 
  "table": "sales",
  "schema": "public",
  "limit": 10000
};
```

### 4. Use Environment Variables in Profiles

For sensitive information, use environment variables in your profile definitions:

```yaml
connectors:
  postgres:
    type: POSTGRES
    params:
      host: "${DB_HOST}"
      port: ${DB_PORT}
      dbname: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
```

### 5. Running with the Right Profile

Always specify which profile to use when running your pipelines:

```bash
sqlflow pipeline run my_pipeline.sf --profile production
```

## Benefits of This Approach

- **Separation of Concerns**: Pipeline code focuses on data transformations, profiles handle connections
- **Environment Portability**: The same pipeline works across environments by changing only the profile
- **Credential Management**: Sensitive information stays out of pipeline files
- **Reusability**: Multiple pipelines can reference the same connector without duplication
- **Maintainability**: Connection changes only need to be made in one place

## Real-World Example

Here's a complete example demonstrating the recommended pattern:

**Profile (profiles/dev.yml)**:
```yaml
connectors:
  postgres:
    type: POSTGRES
    params:
      host: "localhost"
      port: 5432
      dbname: "ecommerce"
      user: "dev_user"
      password: "dev_password"
  s3:
    type: S3
    params:
      endpoint_url: "http://localhost:9000"
      access_key: "minioadmin"
      secret_key: "minioadmin"
      bucket: "analytics-dev"
```

**Pipeline (pipelines/analytics.sf)**:
```sql
-- Define data sources referencing connectors from profile
SOURCE sales_data FROM "postgres" OPTIONS { "table": "sales" };
SOURCE customer_data FROM "postgres" OPTIONS { "table": "customers" };

-- Define output destinations
SOURCE s3_export FROM "s3" OPTIONS { "path": "analytics/reports/" };

-- Load data
LOAD sales FROM sales_data;
LOAD customers FROM customer_data;

-- Transform data with SQL
CREATE TABLE sales_summary AS
SELECT
  date_trunc('month', order_date) AS month,
  COUNT(*) AS order_count,
  SUM(amount) AS total_sales
FROM sales
GROUP BY 1
ORDER BY 1;

-- Export results
EXPORT SELECT * FROM sales_summary
TO "${s3_export.path}sales_summary_${date}.csv"
TYPE CSV OPTIONS { "header": true };
```

By following these practices, your SQLFlow pipelines will be more maintainable, secure, and portable across different environments. 