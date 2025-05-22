# SQLFlow Table UDF Dependency Resolution

## Problem and Solution

### Problem
We encountered an issue where table UDFs in SQLFlow weren't properly recognizing dependencies 
on their input tables. When using a pattern like:

```sql
CREATE TABLE transformed_data AS
SELECT * FROM PYTHON_FUNC("module.function", input_table);
```

The planner wasn't detecting that `transformed_data` depended on `input_table`, causing execution
order issues where the UDF would run before its input data was loaded.

### Root Cause
The dependency detection in `_extract_referenced_tables` method only handled standard SQL patterns:
- `FROM table`
- `JOIN table`

It didn't handle the UDF-specific pattern where a table name is passed as an argument to `PYTHON_FUNC`.

### Solution
We extended the `_extract_referenced_tables` method in `ExecutionPlanBuilder` to also detect
table references in UDF calls with a new regex pattern:

```python
# Handle table UDF pattern: PYTHON_FUNC("module.function", table_name)
udf_table_matches = re.finditer(
    r"python_func\s*\(\s*['\"][\w\.]+['\"]\s*,\s*([a-zA-Z0-9_]+)", sql_lower
)
for match in udf_table_matches:
    table_name = match.group(1).strip()
    if table_name and table_name not in tables:
        tables.append(table_name)
```

This ensures the planner correctly identifies and orders operations that depend on table UDFs.

## Best Practices for Table UDFs

When using table UDFs in SQLFlow:

1. Make sure source tables are loaded before UDF calls
2. Use explicit naming in UDF calls to avoid ambiguity 
3. For complex transformations, consider breaking into multiple steps
4. Validate pipeline execution plans to ensure correct dependencies

## Example 

Correct table UDF usage:

```sql
-- First load the raw data
LOAD raw_data FROM source_data;

-- Then use the table UDF
CREATE TABLE processed_data AS
SELECT * FROM PYTHON_FUNC("my_module.process_data", raw_data);

-- Further transform or export the result
CREATE TABLE summary AS
SELECT column, COUNT(*) FROM processed_data GROUP BY column;
```
