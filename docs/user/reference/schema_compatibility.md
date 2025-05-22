# Schema Compatibility in SQLFlow

This guide explains how SQLFlow validates schema compatibility between source and target tables when using different load modes.

## Overview

When working with load operations, SQLFlow performs schema compatibility validation to ensure that the source and target schemas are compatible. This validation is especially important for APPEND and MERGE operations, which require schema consistency to function correctly.

## Schema Validation for Different Load Modes

SQLFlow performs different levels of schema validation depending on the load mode:

### REPLACE Mode

In REPLACE mode, schema validation is not necessary since the target table will be completely replaced by the source data, adopting its schema.

```sql
LOAD target_table
FROM source_table
MODE REPLACE
```

### APPEND Mode

In APPEND mode, SQLFlow validates that:

1. All columns in the source table exist in the target table
2. The data types of corresponding columns are compatible

```sql
LOAD target_table
FROM source_table
MODE APPEND
```

### MERGE Mode

In MERGE mode, SQLFlow performs the same schema compatibility checks as APPEND mode, plus:

1. Validates that the specified merge keys exist in both tables
2. Ensures merge keys have compatible types

```sql
LOAD target_table
FROM source_table
MODE MERGE
MERGE_KEYS (id)
```

## Type Compatibility

SQLFlow uses a flexible type compatibility system that normalizes similar types for comparison. For example:

- `VARCHAR`, `TEXT`, `CHAR`, and `STRING` are considered compatible
- `INTEGER`, `INT`, `BIGINT`, and `SMALLINT` are considered compatible
- `FLOAT`, `DOUBLE`, `DECIMAL`, and `NUMERIC` are considered compatible
- `BOOLEAN` and `BOOL` are considered compatible
- `DATE` is only compatible with other `DATE` types
- `TIME` is only compatible with other `TIME` types
- `TIMESTAMP` is only compatible with other `TIMESTAMP` types

## Schema Validation Behavior

When schema validation fails, SQLFlow will:

1. Halt the execution of the current operation or pipeline
2. Return a descriptive error message indicating the schema incompatibility
3. Provide details about which column and data type caused the issue

## Best Practices

1. **Plan Your Schemas Carefully**: Design your schemas with evolution in mind, especially for tables that will be targets of APPEND or MERGE operations.

2. **Use REPLACE for Schema Changes**: If you need to change the schema of a table, use REPLACE mode rather than trying to APPEND or MERGE with incompatible schemas.

3. **Type Selection**: Use the most appropriate and specific data types for your columns to minimize compatibility issues.

4. **Testing**: Test schema compatibility in a development environment before running operations in production.

5. **Subset Selection**: When using APPEND or MERGE modes, you can select a subset of columns from the source table that matches the target schema:

   ```sql
   LOAD target_table 
   FROM (SELECT id, name, email FROM source_table)
   MODE APPEND
   ```

## Error Messages

When schema validation fails, you'll receive an error message like:

```
Column 'email' in source does not exist in target table 'users_table'
```

or

```
Column 'active' has incompatible types: source=INTEGER, target=BOOLEAN
```

These messages help you identify and resolve the schema incompatibility issues.

## Advanced Schema Management

For more advanced schema management, including schema evolution and versioning, see the [Schema Management](schema_management.md) documentation. 