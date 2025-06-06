# Working with Variables in SQLFlow

> **MVP Status**: This guide is part of SQLFlow's MVP documentation. The feature is fully implemented, but this guide may be expanded with more examples in future updates.

Variables in SQLFlow provide a powerful way to make your pipelines dynamic and reusable across different environments and scenarios. This guide explains how to define, use, and pass variables in your SQLFlow pipelines.

## Defining Variables

Variables are defined using the `SET` directive:

```sql
SET date = "2023-10-25";
SET region = "us-east";
```

## Variable Substitution

Once defined, variables can be referenced within strings using the `${variable_name}` syntax:

```sql
EXPORT 
  SELECT * FROM sales_summary
TO "output/sales_summary_${date}.csv"
TYPE CSV
OPTIONS { "header": true };
```

## Default Values

You can provide default values for variables that might not be set using the pipe (`|`) operator:

```sql
SET output_path = "${output_dir|output}/result.csv";
```

In this example, if `output_dir` is not defined, the value "output" will be used.

## Passing Variables via CLI

Variables can be passed when running a pipeline using the `--vars` option:

```bash
sqlflow pipeline run sales_report --vars '{"date": "2023-11-01", "region": "eu-west"}'
```

## Environment-Specific Variables

Profile files can define environment-specific variables:

```yaml
# profiles/production.yml
variables:
  data_source: "production_db"
  s3_bucket: "company-production-data"
  
# profiles/dev.yml
variables:
  data_source: "dev_db"
  s3_bucket: "company-dev-data"
```

Then reference them in your pipeline:

```sql
SOURCE orders TYPE POSTGRES PARAMS {
  "connection_string": "postgresql://user:pass@${data_source}/orders"
};
```

## Best Practices

1. **Use consistent naming**: Choose a consistent naming convention for variables.
2. **Provide default values**: Always provide sensible defaults where possible.
3. **Document variables**: Comment which variables your pipeline expects.
4. **Pass sensitive values**: Don't hardcode credentials; pass them as variables.

## Example: Parameterized Pipeline

```sql
-- Define expected variables with defaults
SET processing_date = "${date|2023-01-01}";
SET region = "${region|global}";
SET output_format = "${format|csv}";

-- Source data with filters
SOURCE orders TYPE CSV PARAMS {
  "path": "data/orders_${processing_date}.csv",
  "has_header": true
};

LOAD orders_data FROM orders;

-- Filter by region if specified
CREATE TABLE filtered_orders AS
SELECT * FROM orders_data
WHERE
  CASE 
    WHEN '${region}' = 'global' THEN true
    ELSE region = '${region}'
  END;

-- Export with dynamic format
EXPORT
  SELECT * FROM filtered_orders
TO "output/orders_${region}_${processing_date}.${output_format}"
TYPE ${output_format}
OPTIONS { "header": true };
```

## Variable Scope

Variables are globally scoped within a pipeline. When you define a variable, it's available throughout the entire pipeline. However, the value of a variable can be overridden by redefining it later in the pipeline.

## Related Resources

- [Getting Started Guide](../getting_started.md)
- [Syntax Reference](../reference/syntax.md)
- [Working with Profiles](profiles.md)
- [Conditional Execution](conditionals.md) 