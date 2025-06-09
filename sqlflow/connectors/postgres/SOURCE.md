# PostgreSQL Source

The PostgreSQL Source connector reads data from a PostgreSQL database. For a general overview, see the [main README](./README.md).

## Configuration

The connection to PostgreSQL is configured directly in the source definition within your `profiles.yml` file.

### Connection Parameters

The following parameters are based on the standard `psycopg2` connection string.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | `string` | ✅ | Must be `"postgres"`. |
| `host` | `string` | ✅ | The database server host. |
| `port` | `integer`| ✅ | The database server port. |
| `user` | `string` | ✅ | The username to connect with. |
| `password`| `string` | ✅ | The password for the user. |
| `dbname` | `string` | ✅ | The name of the database to connect to. |

### Example Profile Configuration
```yaml
# profiles/dev.yml
sources:
  my_postgres_db:
    type: postgres
    host: "localhost"
    port: 5432
    user: "db_user"
    password: "db_password"
    dbname: "analytics"
```

## Reading Data

You can read data either by specifying a table name or by providing a full SQL query. These are provided as options in your `READ` or `FROM` statement.

### Read Options

| Option | Type | Description |
|--------|------|-------------|
| `table_name` | `string` | The name of the table to read from. |
| `schema` | `string` | The schema of the table (defaults to `"public"`). |
| `query` | `string` | A full SQL query to execute for retrieving data. |

If `query` is provided, it takes precedence over `table_name`.

### Usage Examples

#### Reading from a Table
This reads the entire `users` table from the `public` schema.

```sql
-- pipelines/pipeline.sql
READ 'my_postgres_db' OPTIONS (
  table_name = 'users'
);
```

#### Reading from a Table in a Different Schema

```sql
-- pipelines/pipeline.sql
READ 'my_postgres_db' OPTIONS (
  table_name = 'raw_events',
  schema = 'staging'
);
```

#### Reading with a Custom Query

```sql
-- pipelines/pipeline.sql
READ 'my_postgres_db' OPTIONS (
  query = 'SELECT id, name, email FROM users WHERE active = TRUE'
);
```

## Features

- **Direct Table Read**: Easily read an entire table.
- **Custom SQL Queries**: Use the full power of SQL to select, join, and filter data on the database side.
- **Schema Support**: Specify tables from any schema.
- **Connection Re-use**: The underlying `sqlalchemy` engine efficiently manages connections.

## Limitations
- This connector does not currently have built-in support for incremental loading. You must implement incremental logic within your SQL queries if needed. 