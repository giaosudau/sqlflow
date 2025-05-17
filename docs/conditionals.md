# Conditional Execution in SQLFlow

SQLFlow supports conditional execution through SQL-native IF-THEN-ELSE syntax, allowing you to create dynamic data pipelines that can adapt to different environments, scenarios, or data conditions.

## Syntax

SQLFlow conditional blocks follow a SQL-inspired syntax:

```sql
IF condition THEN
  -- SQL statements for when condition is true
[ELSE IF another_condition THEN
  -- SQL statements for when another_condition is true]
[ELSE
  -- SQL statements for when no conditions are true]
END IF;
```

## Variable References

Conditions can reference variables using the `${variable_name}` syntax. You can also provide default values for variables that aren't set using the pipe syntax: `${variable_name|default_value}`.

Examples:
- `${env}` - References the "env" variable
- `${debug|false}` - References the "debug" variable, using "false" as the default if not set
- `${region|us-east-1}` - References the "region" variable with "us-east-1" as the default

## Operators

The following operators are supported in conditions:

### Comparison Operators

- `==` - Equal to
- `!=` - Not equal to
- `<` - Less than
- `<=` - Less than or equal to
- `>` - Greater than
- `>=` - Greater than or equal to

### Logical Operators

- `and` - Logical AND
- `or` - Logical OR
- `not` - Logical NOT

## Examples

### Basic Environment Selection

```sql
IF ${env} == 'production' THEN
    CREATE TABLE users AS SELECT * FROM prod_users;
ELSE
    CREATE TABLE users AS SELECT * FROM dev_users;
END IF;
```

### Multiple Conditions with ELSE IF

```sql
IF ${env} == 'production' THEN
    CREATE TABLE users AS SELECT * FROM prod_users;
ELSE IF ${env} == 'staging' THEN
    CREATE TABLE users AS SELECT * FROM staging_users;
ELSE
    CREATE TABLE users AS SELECT * FROM dev_users;
END IF;
```

### Nested Conditional Blocks

```sql
IF ${env} == 'production' THEN
    IF ${region} == 'us-east-1' THEN
        CREATE TABLE users AS SELECT * FROM prod_east_users;
    ELSE
        CREATE TABLE users AS SELECT * FROM prod_west_users;
    END IF;
ELSE
    CREATE TABLE users AS SELECT * FROM dev_users;
END IF;
```

### Combining with Other SQLFlow Features

```sql
IF ${env} == 'production' THEN
    LOAD users FROM SOURCE my_pg_source;
    
    CREATE TABLE filtered_users AS
    SELECT * FROM users
    WHERE active = true;
ELSE
    LOAD users FROM SOURCE my_csv_source;
    
    CREATE TABLE filtered_users AS
    SELECT * FROM users
    WHERE active = true
    AND created_date > '2023-01-01';
END IF;

EXPORT TO postgres_destination
FROM filtered_users
WITH (destination_table = 'filtered_users');
```

## Implementation Details

Conditional execution in SQLFlow is implemented using compile-time evaluation. The conditions are evaluated during the planning phase, before execution, based on variable values provided to the pipeline. Only the active branch (the first branch with a true condition, or the else branch if no conditions are true) is included in the final execution plan.

This approach eliminates runtime overhead for conditional evaluation and ensures that the execution plan is optimized and deterministic.

## Limitations

- Conditions can only reference variables, not computed values or SQL query results
- Conditions are evaluated once at planning time, not at runtime
- Nested conditions can reference the same variables as their parent conditions

## Best Practices

1. **Use Clear Variable Names**: Choose descriptive names for variables to make conditions easier to understand
2. **Provide Default Values**: Always provide sensible defaults for optional variables using the `${var|default}` syntax
3. **Keep Conditions Simple**: Avoid overly complex conditions with many nested logical operators
4. **Test All Branches**: Ensure each conditional branch is tested by varying your variable values 