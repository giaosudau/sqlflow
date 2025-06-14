# SQLFlow Variable Substitution Guide

Complete guide to variable substitution in SQLFlow pipelines.

## Quick Start

### Basic Usage

```sql
-- pipeline.sql
SELECT * FROM ${table_name} WHERE created_date > '${start_date}';
```

```bash
# Run with variables
sqlflow run pipeline.sql --vars table_name=users start_date=2025-01-01
```

### Variable Priority

Variables are resolved in this order (highest to lowest):
1. **CLI Variables** - `--vars key=value`
2. **Profile Variables** - From profile configuration
3. **SET Variables** - From SET statements
4. **Environment Variables** - From system environment

## Variable Syntax

### Basic Substitution

```sql
SELECT * FROM ${table_name};
SELECT '${environment}' as env_name;
SELECT ${column_list} FROM ${schema}.${table_name};
```

### Default Values

```sql
-- Variable with default
SELECT * FROM ${table_name|users} WHERE status = '${status|active}';

-- Quoted defaults
SELECT * FROM ${schema|"public"} WHERE region = '${region|"us-east"}';
```

## Configuration

### Profile Variables

```yaml
# profiles/dev.yml
variables:
  environment: development
  schema: public
  batch_size: 1000

connections:
  target:
    type: duckdb
    path: dev.db
```

### Environment Variables

```bash
export DATABASE_URL=postgres://localhost:5432/mydb
export ENVIRONMENT=production
```

```sql
-- Automatic environment variable access
CONNECT TO ${DATABASE_URL};
SELECT * FROM logs WHERE env = '${ENVIRONMENT}';
```

## Programming Interface

### New Unified System (Default)

```python
from sqlflow.core.variables.manager import VariableConfig, VariableManager

# Create configuration
config = VariableConfig(
    cli_variables={"table": "users"},
    profile_variables={"schema": "public"}
)

# Use manager
manager = VariableManager(config)
result = manager.substitute("SELECT * FROM ${schema}.${table}")
validation = manager.validate("SELECT * FROM ${missing_var}")
```

### Migration from Legacy

```python
# OLD WAY
from sqlflow.core.variable_substitution import VariableSubstitutionEngine
engine = VariableSubstitutionEngine({"table": "users"})

# NEW WAY (now default)
from sqlflow.core.variables.manager import VariableConfig, VariableManager
config = VariableConfig(cli_variables={"table": "users"})
manager = VariableManager(config)
```

## Feature Control

```bash
# Use new system (default)
sqlflow run pipeline.sql --vars key=value

# Force old system for compatibility
export SQLFLOW_USE_NEW_VARIABLES=false
sqlflow run pipeline.sql --vars key=value
```

## Best Practices

1. **Use meaningful variable names**
2. **Provide sensible defaults**
3. **Validate variables before execution**
4. **Use environment variables for secrets**
5. **Document required variables**

## Examples

### Dynamic Pipeline

```sql
-- Conditional table selection
SELECT * FROM ${table_prefix}${environment}_events
WHERE region = '${target_region|us-west-2}';
```

### Template Pipeline

```sql
-- Reusable template
SOURCE ${source_name} FROM ${connector_type}
PARAMS (path: '${source_path}', format: '${file_format|csv}');

LOAD FROM ${source_name} TO ${target_table}
MODE ${load_mode|append};
```

## Testing

```python
def test_variable_substitution():
    config = VariableConfig(cli_variables={"table": "test_users"})
    manager = VariableManager(config)
    
    result = manager.substitute("SELECT * FROM ${table}")
    assert "test_users" in result
```

For more examples, see the `examples/` directory in the SQLFlow repository.

## Related Documentation

- **Architecture Deep Dive**: [Variable Substitution Architecture](variable-substitution-architecture.md)
- **Migration Guide**: [Variable Parsing Migration](../migration/variable-parsing-migration.md)
- **Building Connectors**: [Building Connectors Guide](building-connectors.md) 