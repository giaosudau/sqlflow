# SQLFlow Testing Documentation

This directory contains documentation related to testing practices in SQLFlow.

## Testing Guides

- [Integration Testing Guide](integration_testing.md) - Best practices for writing integration tests, handling table registration, dependency management, and DuckDB connections.

## Additional Testing Resources

- [Testing and Backward Compatibility](../technical/testing_and_compatibility.md) - How SQLFlow maintains backward compatibility and migration strategies for tests.
- [DuckDB Persistence](../technical/duckdb_persistence.md) - Technical details about DuckDB persistence, including table registration and connection management.

## Key Testing Concepts

### Dependency Management

Proper dependency management is crucial in SQLFlow integration tests to ensure operations execute in the correct order. Without explicit dependencies, tests may fail with errors like "Table does not exist" when operations execute out of order.

### Table Registration

For tables to be accessible in SQL queries, they must be registered with the DuckDB engine. The LocalExecutor provides a `_register_tables_with_engine()` method that makes tables available for SQL operations.

### Connection Management

When testing with DuckDB, proper connection management is essential to avoid issues with file locking and resource leaks. Always close connections when done and handle file cleanup carefully. 