# PostgreSQL Destination Connector

The PostgreSQL Destination connector writes data from a pandas DataFrame into a table in a PostgreSQL database. It uses SQLAlchemy and pandas for efficient and reliable data insertion.

## ✅ Features

- **Direct Connection**: Connects directly to your PostgreSQL database to write data.
- **Flexible Write Modes**: Supports "replace", "append", and "fail" modes for handling existing tables.
- **SQLAlchemy Powered**: Leverages SQLAlchemy for robust connection management and writing.

## 📋 Configuration

The core configuration for the destination is the connection `uri`, which should be defined in a `SOURCE` or `DESTINATION` block in a profile or pipeline file. However, since this connector is used in an `EXPORT` step, the most important parameters are provided in the `OPTIONS` block of the `EXPORT` statement.

### Primary Configuration
| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `uri` | `string` | The SQLAlchemy connection URI for your PostgreSQL database. This should be configured in a profile. | ✅ | `"postgresql://user:pass@host:5432/dbname"`|

### EXPORT Options
| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `table_name` | `string` | The name of the target table to write data into. | ✅ | `"quarterly_summary"`|
| `schema` | `string` | The schema of the target table. | `public` (default) | `"reporting"` |
| `if_exists` | `string` | How to behave if the table already exists. Options: `fail`, `replace`, `append`. | `fail` (default) | `"replace"` |

## 💡 Example

This example exports the `final_report` table to a PostgreSQL database. It writes to the `sales_reports` table in the `analytics` schema, replacing the table if it already exists.

First, you would typically define your database connection in a profile:
```yaml
# profiles/dev.yml
...
# You can define a dummy source or a destination block to hold the URI
sources:
  my_postgres_db:
    type: "postgres"
    uri: "${DB_CONNECTION_STRING}"
```

Then, use it in an `EXPORT` statement:
```sql
EXPORT
  SELECT * FROM final_report
TO "postgres://my_postgres_db/analytics/sales_reports" -- URI here is for context
TYPE POSTGRES
OPTIONS {
  "table_name": "sales_reports",
  "schema": "analytics",
  "if_exists": "replace"
};
```
> **Note**: The `TO` URI in the `EXPORT` statement is primarily for descriptive purposes. The actual connection details are pulled from the connector's configuration, which is loaded based on the `TYPE`. The critical parameters for the write operation are `table_name`, `schema`, and `if_exists` within the `OPTIONS` block.

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported 