# SQLFlow CLI: Command Line Interface Guide

> **Comprehensive guide for using SQLFlow's command line interface for managing data pipelines, connections, and Python UDFs.**

## Overview

The SQLFlow Command Line Interface (CLI) provides a powerful set of tools for creating, managing, and executing SQL-based data pipelines. The CLI enables you to initialize projects, compile and run pipelines, manage connections, and work with Python User-Defined Functions (UDFs).

---

## Installation

SQLFlow CLI is automatically installed when you install the SQLFlow package:

```bash
pip install sqlflow
```

You can verify the installation by checking the version:

```bash
sqlflow --version
```

---

## Getting Started

### Initializing a New Project

Create a new SQLFlow project with the required directory structure:

```bash
sqlflow init my_project
cd my_project
```

This creates a new project with the following structure:
```
my_project/
├── pipelines/
│   └── example.sf    # Example pipeline file
├── profiles/         # For connection profiles
├── data/             # Data directory (auto-created)
├── python_udfs/      # For Python UDFs
└── output/           # For pipeline outputs
```

---

## Pipeline Management

The `pipeline` command group handles all pipeline-related operations.

### Listing Pipelines

List all available pipelines in your project:

```bash
sqlflow pipeline list
```

### Compiling a Pipeline

Compile a pipeline to validate its syntax and generate the execution plan:

```bash
sqlflow pipeline compile example
```

### Running a Pipeline

Execute a pipeline with optional variables:

```bash
sqlflow pipeline run example --profile dev --vars '{"date": "2023-10-25"}'
```

### Pipeline Validation

Validate a pipeline without executing it:

```bash
sqlflow pipeline validate example
```

### Command Options

| Option      | Description                                     |
|-------------|-------------------------------------------------|
| `--profile` | Specify the connection profile to use           |
| `--vars`    | JSON string of variables to pass to the pipeline|
| `--dry-run` | Validate and compile without executing          |

---

## Connection Management

The `connect` command group manages connection profiles for external data sources.

### Listing Connections

List all available connection profiles:

```bash
sqlflow connect list
```

### Testing a Connection

Test if a connection profile is properly configured:

```bash
sqlflow connect test dev
```

---

## User-Defined Functions (UDFs)

The `udf` command group helps you discover, validate, and get information about Python UDFs.

### Listing UDFs

List all available Python UDFs:

```bash
sqlflow udf list
```

### UDF Information

Get detailed information about a specific UDF:

```bash
sqlflow udf info python_udfs.text_utils.capitalize_words
```

### Validating UDFs

Validate all Python UDFs to ensure they meet the requirements:

```bash
sqlflow udf validate
```

---

## Variables in Pipelines

SQLFlow supports variable substitution in pipelines with default values:

```sql
-- Example of variable usage in a pipeline
SET date = '${run_date|2023-10-25}';

SOURCE sample TYPE CSV PARAMS {
  "path": "data/sample_${date}.csv",
  "has_header": true
};
```

You can provide variables when running a pipeline:

```bash
sqlflow pipeline run example --vars '{"run_date": "2023-11-01"}'
```

---

## Best Practices

1. **Use Profiles**: Create different profiles (dev, test, prod) for connection settings.
   
2. **Version Control**: Keep your pipeline files under version control.

3. **Parameterize Pipelines**: Use variables to make pipelines reusable.

4. **Organize UDFs**: Keep related UDFs in separate modules within the `python_udfs` directory.

5. **Test Before Production**: Use the `--dry-run` option to validate pipelines before executing them in production.

---

## Troubleshooting

### Common Issues

1. **Pipeline Not Found**: Ensure you're in the correct project directory and the pipeline file exists.

2. **Connection Errors**: Verify your connection profile settings and test the connection.

3. **UDF Discovery Issues**: Check UDF imports and ensure they follow required signatures.

4. **Variable Substitution Errors**: Validate variable formats in pipelines (`${var_name|default_value}`).

---

## Advanced Usage

### Environment Variables

You can use environment variables in your connection profiles:

```yaml
# profiles/dev.yml
default:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  user: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  database: analytics
```

### Scheduling Pipeline Runs

Use cron or other scheduling tools to run pipelines periodically:

```bash
# Run daily at midnight
0 0 * * * cd /path/to/project && sqlflow pipeline run daily_processing --profile prod
```

---

## Reference

For more detailed information on SQLFlow syntax and features, refer to:

- [SQLFlow Syntax Reference](./syntax.md)
- [Python UDFs Guide](./python_udfs.md)
- [Connection Profiles Guide](./connector_usage_guide.md)

---

For further questions or to contribute improvements, see the main [README](../README.md) or open an issue.
