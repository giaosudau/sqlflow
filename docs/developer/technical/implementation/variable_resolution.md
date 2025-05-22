# Variable Resolution in SQLFlow

SQLFlow provides a powerful variable system that allows you to parameterize your pipelines. This document explains how variables work, their priority order, and best practices for using them effectively.

## Variable Syntax

You can use variables in your SQLFlow pipelines using the following syntax:

```sql
-- Simple variable reference
SELECT * FROM data WHERE region = '${region}'

-- Variable with default value
SELECT * FROM data WHERE region = '${region|us-east}'
```

Variables can be used in:
- SQL queries
- Export destinations
- Source and connector configurations
- Conditional statements

## Variable Sources

Variables can come from multiple sources, and SQLFlow applies a specific priority order when resolving them:

1. **CLI Variables** (highest priority)
   - Specified when running a pipeline with the `--vars` option
   - Example: `sqlflow run my_pipeline --vars="region=eu-west,env=prod"`

2. **Profile Variables** (medium priority)
   - Defined in your profile configuration files
   - Example in `profiles/dev.yaml`:
     ```yaml
     variables:
       region: us-east
       env: dev
     ```

3. **SET Variables** (lowest priority)
   - Defined within the pipeline using SET statements
   - Example:
     ```sql
     SET region = 'us-west';
     SET env = 'dev';
     ```

4. **Default Values** (fallback only)
   - Specified in the variable reference using the pipe syntax
   - Only used when the variable is not defined in any other source
   - Example: `${region|us-east}`

## Priority Resolution

When a variable appears in multiple sources, SQLFlow resolves it using the priority order above. For example:

- If `region` is defined in CLI arguments, the profile, and with SET, the CLI value will be used.
- If `region` is defined in the profile and with SET, the profile value will be used.
- If `region` is only defined with SET, that value will be used.
- If `region` is not defined anywhere but has a default value in the reference (e.g., `${region|us-east}`), the default value will be used.
- If `region` is not defined anywhere and has no default, it remains unresolved (which may cause errors).

## Examples

### Basic Variable Usage

```sql
-- Define variables with SET statements
SET output_dir = 'data/output';
SET region = 'us-east';

-- Use variables in SQL queries
CREATE TABLE filtered_data AS
SELECT * FROM raw_data
WHERE region = '${region}';

-- Use variables in export destinations
EXPORT filtered_data TO CSV
'${output_dir}/${region}/filtered_data.csv';
```

### Using Default Values

```sql
-- Use a default value if the variable is not set elsewhere
EXPORT filtered_data TO CSV
'${output_dir|default_output}/${region|us-east}/filtered_data.csv';
```

### Conditional Logic with Variables

```sql
-- Conditionally execute SQL based on environment
IF ${env} = 'prod' THEN
  -- Production-specific logic
  CREATE TABLE prod_filtered_data AS
  SELECT * FROM raw_data WHERE quality = 'high';
ELSE
  -- Non-production logic
  CREATE TABLE prod_filtered_data AS
  SELECT * FROM raw_data;
END IF;
```

## Best Practices

1. **Use CLI variables for environment-specific settings**
   - Values that change between development, staging, and production

2. **Use profile variables for user preferences**
   - Settings specific to a developer or team

3. **Use SET variables for pipeline-specific defaults**
   - Values that are specific to the pipeline but can be overridden

4. **Always provide defaults for optional variables**
   - Use `${var|default}` syntax for variables that might not be set

5. **Document required variables**
   - Make it clear which variables must be provided when running a pipeline

## Troubleshooting

If you're having issues with variables not being resolved correctly:

1. Check the priority order (CLI > Profile > SET > Defaults)
2. Verify your variable names match exactly (they are case-sensitive)
3. Check your SQLFlow logs for warnings about unresolved variables
4. Use the `--verbose` flag when running pipelines to see more details about variable resolution 