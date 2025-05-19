# SQLFlow Logging Configuration Guide

Effective logging is essential for debugging issues, monitoring performance, and understanding the behavior of your data pipelines. SQLFlow provides flexible logging options that can be configured to match your needs in both development and production environments.

## Log Levels

SQLFlow supports the standard Python logging levels, from most to least verbose:

| Level | Description |
|-------|-------------|
| `DEBUG` | Detailed information, typically useful for diagnosing problems |
| `INFO` | Confirmation that things are working as expected (default) |
| `WARNING` | Indication that something unexpected happened, but the process can continue |
| `ERROR` | Due to a more serious problem, the process couldn't perform a specific function |
| `CRITICAL` | A serious error indicating the program may be unable to continue running |

## Configuration Methods

There are several ways to configure logging in SQLFlow:

### 1. Environment Variable

Set the `SQLFLOW_LOG_LEVEL` environment variable:

```bash
# In fish shell
set -x SQLFLOW_LOG_LEVEL debug

# For the current session only
export SQLFLOW_LOG_LEVEL=debug
```

### 2. Command Line Option

Use the `--log-level` option when running a SQLFlow command:

```bash
sqlflow pipeline run my_pipeline --log-level debug
```

The command-line option takes precedence over the environment variable.

### 3. Profile Configuration

Add logging settings to your profile YAML files for more granular control:

```yaml
# profiles/dev.yml
log_level: debug
module_log_levels:
  sqlflow.core.engines: info
  sqlflow.connectors: debug
  sqlflow.udfs: debug
```

This approach allows you to:
- Set a global log level for all SQLFlow components
- Configure specific log levels for individual modules
- Maintain different logging configurations across environments (dev, staging, prod)

### 4. Programmatic Configuration

When using SQLFlow as a library in Python scripts:

```python
from sqlflow.logging import configure_logging

# Simple configuration
configure_logging(log_level="debug")

# Advanced configuration
configure_logging(config={
    "log_level": "info",
    "module_log_levels": {
        "sqlflow.core.engines": "debug",
        "sqlflow.connectors": "warning"
    },
    "log_file": "sqlflow.log"
})
```

## Module-Specific Logging

SQLFlow is organized into several modules, each handling different aspects of pipeline execution. You can set specific log levels for each module:

| Module | What It Logs |
|--------|-------------|
| `sqlflow.core.engines` | SQL engine operations, query execution, performance metrics |
| `sqlflow.connectors` | Data source and export operations, connection details |
| `sqlflow.udfs` | Python UDF registration, execution, and errors |
| `sqlflow.parser` | SQL parsing, directive processing, syntax validation |
| `sqlflow.core.planner` | DAG construction, dependency resolution |
| `sqlflow.core.executor` | Task execution, state transitions |

## Logging to Files

To save logs to a file, you can configure a file handler:

```yaml
# profiles/dev.yml
log_level: info
log_file: "logs/sqlflow.log"
log_format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

Or via the API:

```python
configure_logging(log_file="logs/sqlflow.log")
```

## Recommended Configurations

### Development

```yaml
log_level: debug
module_log_levels:
  sqlflow.core.engines: debug
  sqlflow.connectors: debug
  sqlflow.udfs: debug
```

### Debugging Specific Issues

#### For UDF Problems:
```yaml
log_level: info
module_log_levels:
  sqlflow.udfs: debug
```

#### For Connector Issues:
```yaml
log_level: info
module_log_levels:
  sqlflow.connectors: debug
```

#### For Performance Analysis:
```yaml
log_level: info
module_log_levels:
  sqlflow.core.engines: debug
  sqlflow.core.executor: debug
```

### Production

```yaml
log_level: warning
log_file: "logs/sqlflow.log"
```

## Log Output Format

SQLFlow logs include timestamp, logger name, level and message:

```
2025-05-14 15:23:45,789 - sqlflow.core.engines.duckdb_engine - INFO - Table users registered successfully
```

This standardized format makes it easier to filter and analyze logs.

## Advanced: JSON Logging

For integration with log aggregation tools, SQLFlow supports JSON-formatted logs:

```yaml
# profiles/production.yml
log_level: info
log_format: json
log_file: "logs/sqlflow.json"
```

Example JSON log:
```json
{
  "timestamp": "2025-05-14T15:23:45.789Z",
  "level": "INFO",
  "logger": "sqlflow.core.engines.duckdb_engine",
  "message": "Table users registered successfully",
  "pipeline": "customer_analytics",
  "execution_id": "runs-2025-05-14-001"
}
```

## Integrating with Monitoring Tools

SQLFlow logs can be integrated with popular logging and monitoring tools:

### ELK Stack
Configure JSON logging and use Filebeat to ship logs to Elasticsearch.

### Datadog
Use the Datadog agent to collect logs from your SQLFlow log files.

### CloudWatch
For AWS environments, use the CloudWatch agent to stream logs.

## Troubleshooting Common Issues

### Missing Logs

If logs aren't appearing:
1. Check that the log level isn't set too restrictive
2. Verify file permissions if logging to a file
3. Ensure the logging directory exists

### Too Many Logs

If you're getting overwhelmed with log volume:
1. Increase the minimum log level (e.g., from `debug` to `info`)
2. Use module-specific logging to focus on relevant components
3. Add filter rules to exclude noisy but unimportant messages

## Best Practices

1. **Use different logging configurations for different environments**
   - Verbose in development
   - Minimal in production (unless debugging an issue)

2. **Log to files in production environments**
   - Enables post-mortem analysis
   - Doesn't clutter terminal output

3. **Use module-specific logging when debugging**
   - Reduces noise
   - Focuses on relevant information

4. **Rotate log files in production**
   - Prevents disk space issues
   - Maintains log history

5. **Include execution ID in logs**
   - Makes it easier to trace a specific pipeline run
   - Essential for concurrent executions
