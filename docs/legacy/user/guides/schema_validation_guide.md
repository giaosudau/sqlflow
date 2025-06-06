# Schema Validation in SQLFlow: A Practical Guide

Schema validation is a critical feature in SQLFlow that ensures data integrity when working with different load modes. This guide provides practical examples and best practices for using schema validation in your SQLFlow pipelines.

## Understanding Schema Validation

When using `APPEND` or `MERGE` modes in SQLFlow, the system automatically validates that:

1. All columns in the source exist in the target table
2. Data types are compatible between source and target
3. For `MERGE` operations, merge keys exist in both tables

## Practical Examples

### Working with Compatible Schemas

When source and target schemas are compatible, operations proceed smoothly:

```sql
-- Create a users table
LOAD users
FROM (
    SELECT 1 AS user_id, 'John' AS name, 'john@example.com' AS email
)
MODE REPLACE;

-- Append data with a compatible schema
LOAD users
FROM (
    SELECT 2 AS user_id, 'Jane' AS name, 'jane@example.com' AS email
)
MODE APPEND;
```

### Handling Schema Incompatibilities

When schemas are incompatible, SQLFlow provides clear error messages:

```sql
-- Create a users table
LOAD users
FROM (
    SELECT 1 AS user_id, 'John' AS name, 'john@example.com' AS email
)
MODE REPLACE;

-- This will fail because 'status' column doesn't exist in the target
LOAD users
FROM (
    SELECT 3 AS user_id, 'Alice' AS name, 'alice@example.com' AS email, 'active' AS status
)
MODE APPEND;
```

Error message: `Column 'status' in source does not exist in target table 'users'`

### Subset Selection for Compatibility

You can select a subset of columns to ensure compatibility:

```sql
-- Select only compatible columns
LOAD users
FROM (
    SELECT user_id, name, email FROM users_with_more_columns
)
MODE APPEND;
```

## Best Practices

1. **Design for Evolution**: When creating tables that will be targets for `APPEND` or `MERGE` operations, design schemas with future evolution in mind.

2. **Type Compatibility**: SQLFlow normalizes similar types (e.g., VARCHAR and TEXT), but it's best to use consistent types.

3. **Handling Schema Changes**: Use `REPLACE` mode when you need to change a table's schema, rather than trying to `APPEND` or `MERGE` with incompatible schemas.

4. **Testing**: Test schema compatibility in development before running in production.

5. **Error Handling**: Set up proper error handling in your pipelines for schema validation failures.

## Type Compatibility Reference

SQLFlow groups similar types together for compatibility checks:

| Type Group | Compatible Types |
|------------|------------------|
| String     | VARCHAR, TEXT, CHAR, STRING |
| Integer    | INTEGER, INT, BIGINT, SMALLINT |
| Float      | FLOAT, DOUBLE, DECIMAL, NUMERIC |
| Boolean    | BOOLEAN, BOOL |
| Date       | DATE |
| Time       | TIME |
| Timestamp  | TIMESTAMP |

## Conclusion

Schema validation is a powerful feature that helps ensure data integrity in your SQLFlow pipelines. By understanding how it works and following best practices, you can build robust data pipelines that handle schema changes gracefully.

For a more detailed reference, see the [Schema Compatibility Reference](../reference/schema_compatibility.md) documentation. 