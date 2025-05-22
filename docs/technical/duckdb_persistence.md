# DuckDB Persistence in SQLFlow

This document explains how data persistence works in SQLFlow using DuckDB as the execution engine.

## Overview

SQLFlow uses DuckDB as its execution engine, which supports two modes of operation:

1. **Memory Mode** - Faster execution, but data is lost when SQLFlow exits
2. **Persistent Mode** - Data is saved to disk and persists between SQLFlow runs

## Configuration

DuckDB persistence is configured in your profile YAML files (found in the `profiles/` directory).

### Memory Mode (Default)

```yaml
engines:
  duckdb:
    mode: memory
    memory_limit: 2GB
```

### Persistent Mode

```yaml
engines:
  duckdb:
    mode: persistent
    path: data/sqlflow.duckdb  # Required for persistent mode
    memory_limit: 4GB
```

### Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `mode` | Either `memory` or `persistent` | Yes | `memory` |
| `path` | Path to database file (absolute or relative to project dir) | Only for persistent mode | None |
| `memory_limit` | Memory limit for DuckDB | No | `2GB` |

## How Persistence Works

When configured in persistent mode, SQLFlow:

1. Creates a DuckDB database file at the specified path
2. Manages transactions for all operations
3. Executes `CHECKPOINT` commands after transactions to ensure data durability
4. Verifies persistence to detect any issues

## Transaction Management

All SQL operations in SQLFlow are automatically wrapped in transactions:

1. A transaction is started before executing SQL
2. SQL commands are executed
3. The transaction is committed after successful execution
4. A checkpoint is executed to ensure data is written to disk
5. On error, the transaction is rolled back

## Table Registration and Dependencies

### Table Registration

The LocalExecutor maintains tables in two ways:

1. **In-memory representations**: Tables are stored in memory as PyArrow tables or pandas DataFrames
2. **Persistent tables**: When in persistent mode, tables are also stored in the DuckDB database file

For tables to be accessible in SQL queries, they must be registered with the DuckDB engine using the `_register_tables_with_engine()` method, which:

1. Iterates through all tables in memory
2. Registers each table with the DuckDB connection
3. Makes tables available for subsequent SQL operations

This table registration happens:
- After loading data with the `LOAD` operation
- Before executing SQL in `TRANSFORM` operations
- When manually calling `_register_tables_with_engine()`

### Dependency Management

The LocalExecutor uses a dependency system to ensure operations are executed in the correct order:

1. Each operation may specify which operations it depends on
2. The executor ensures dependent operations are completed successfully before executing an operation
3. For operations that don't specify dependencies, the execution order is determined by their position in the pipeline

It's important to ensure proper dependencies, especially in tests, to prevent issues like "Table does not exist" errors when operations execute out of order.

## Best Practices

### When to Use Memory Mode

- Development and testing
- Processing small datasets
- Quick experiments
- Cases where persistence is not required

### When to Use Persistent Mode

- Production environments
- Large datasets that need to be processed in stages
- Cases where data needs to survive program restarts
- Sharing data between multiple SQLFlow runs

### File Paths

- Use absolute paths for production environments to avoid confusion
- For relative paths, they are relative to the project directory
- Create a dedicated data directory for your database files

### Backup Considerations

- DuckDB database files can be backed up like any other file
- Consider creating periodic backups of your database file
- Use the `-wal` and `-shm` files along with the main database file

### Connection Management

- Always close connections when you're done with them
- Only one connection can be open to a database file with a given configuration
- Use `read_only=True` when connecting to inspect data to avoid conflicts
- When testing with persistent databases, be careful with file handles and locks

## Troubleshooting

### Common Issues

1. **Permissions Problems**

   ```
   Error: Permission denied when accessing database file
   ```

   - Ensure the user running SQLFlow has write access to the directory and file

2. **Path Not Found**

   ```
   Error: Directory for database file does not exist
   ```

   - Ensure the directory exists or use a different path

3. **Data Not Persisting**

   - Check that mode is set to `persistent` in your profile
   - Verify the path is correctly specified
   - Check for disk space issues

4. **Table Does Not Exist**

   ```
   Error: Table with name X does not exist
   ```

   - Ensure the table was created in a previous step
   - Check that dependencies between steps are properly configured
   - Verify that `_register_tables_with_engine()` was called after table creation
   - Check for errors in previous steps that might have prevented table creation

5. **Connection Conflicts**

   ```
   Connection Error: Can't open a connection to same database file with a different configuration
   ```

   - Close existing connections before opening new ones
   - Use `read_only=True` when just inspecting data
   - Force garbage collection with `import gc; gc.collect()` to release handles
   - Add a small delay after closing connections

### Verification

You can verify persistence is working correctly by:

1. Running a query that creates and populates a table
2. Stopping SQLFlow
3. Starting SQLFlow again
4. Running a query against the same table - data should still be there

## Internal Architecture

SQLFlow's persistence implementation consists of:

1. **TransactionManager** - Manages database transactions with proper durability guarantees
2. **DuckDBEngine** - Configures and interfaces with DuckDB
3. **LocalExecutor** - Uses the DuckDB engine for SQL execution

The TransactionManager ensures:
- Proper begin/commit/rollback operations
- Automatic checkpoints after commits
- Verification of persistence

## Advanced Configuration

### WAL Mode

DuckDB uses Write-Ahead Logging (WAL) for durability. SQLFlow configures optimal settings automatically:

```sql
PRAGMA wal_autocheckpoint=1000
PRAGMA journal_mode=WAL
PRAGMA synchronous=FULL
```

These settings balance performance and durability. 