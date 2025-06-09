# PostgreSQL Source Connector

The PostgreSQL Source connector reads data from tables or views in a PostgreSQL database. It uses the `psycopg2` driver via SQLAlchemy and pandas for efficient data extraction.

## ✅ Features

- **Direct Connection**: Connects directly to your PostgreSQL database.
- **Table or Query**: Reads data from an entire table or from the result of a custom SQL query.
- **SQLAlchemy Powered**: Leverages SQLAlchemy for robust connection management.

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"postgres"`. | ✅ | `"postgres"` |
| `uri` | `string` | The SQLAlchemy connection URI for your PostgreSQL database. | ✅ | `"postgresql://user:pass@host:5432/dbname"`|

### Authentication
Credentials and connection details are provided directly in the `uri`. It's highly recommended to use environment variables or other secrets management tools to avoid exposing credentials in plain text.

```sql
-- Using a variable for the connection string
SOURCE my_db_users TYPE POSTGRES PARAMS {
    "uri": "${DB_CONNECTION_STRING}"
};
```

## 💡 How to Read Data

The PostgreSQL source is unique because you specify what to read in the `LOAD` step of your pipeline, not in the `SOURCE` definition itself. The `LOAD` step passes options to the connector's `read` method.

### Reading an Entire Table
To read all data from a table named `public.customers`, you would define a generic source and then specify the table in the `LOAD` step's options.

```sql
-- Define the connection
SOURCE postgres_db TYPE POSTGRES PARAMS { "uri": "${DB_URI}" };

-- Load the 'customers' table from the 'public' schema
LOAD customers_table FROM postgres_db OPTIONS {
  "table": "customers",
  "schema": "public"
};
```

### Reading from a Custom Query
To read data using a custom SQL query, provide a `query` option in the `LOAD` step. This is useful for pre-filtering, joining, or transforming data within the database before it enters the SQLFlow engine.

```sql
-- Define the connection
SOURCE postgres_db TYPE POSTGRES PARAMS { "uri": "${DB_URI}" };

-- Load only active customers from the US using a query
LOAD active_us_customers FROM postgres_db OPTIONS {
  "query": "SELECT * FROM customers WHERE country = 'US' AND status = 'active'"
};
```
> **Note**: The `query` provided in the `OPTIONS` block is executed directly on the PostgreSQL database.

## 📈 Incremental Loading

This connector **does not** have built-in incremental loading logic. To perform incremental loads, you must provide a custom SQL query in the `query` option that filters for new data based on a watermark or timestamp column.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported (Manual via query) 