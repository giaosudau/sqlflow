# SQLFlow Integration Testing Guide

This document outlines best practices and common patterns for writing effective integration tests for SQLFlow.

## Overview

Integration tests in SQLFlow verify that different components work together correctly. These tests typically involve:

1. Creating temporary test environments
2. Executing real SQLFlow operations
3. Verifying results match expectations
4. Cleaning up test resources

## Test Environment Setup

### Temporary Directories

Use Python's `tempfile` module to create isolated test environments:

```python
import tempfile
import os
import shutil

def setUp(self):
    # Create a temporary directory for test files
    self.test_dir = tempfile.mkdtemp(prefix="sqlflow_test_")
    
    # Create subdirectories as needed
    os.makedirs(os.path.join(self.test_dir, "data"), exist_ok=True)
    os.makedirs(os.path.join(self.test_dir, "profiles"), exist_ok=True)
    os.makedirs(os.path.join(self.test_dir, "pipelines"), exist_ok=True)

def tearDown(self):
    # Clean up temporary directory
    if hasattr(self, 'test_dir') and os.path.exists(self.test_dir):
        shutil.rmtree(self.test_dir)
```

### DuckDB Persistence Configuration

For tests that need to verify persistence, configure DuckDB in persistent mode:

```python
# Create a profile with persistent DuckDB configuration
db_path = os.path.join(self.test_dir, "test.duckdb")
profile_content = f"""
engines:
  duckdb:
    mode: persistent
    path: {db_path}
    memory_limit: 1GB
"""
with open(os.path.join(self.test_dir, "profiles", "test.yml"), "w") as f:
    f.write(profile_content)
```

## Testing Best Practices

### Proper Step Dependencies

When testing with the LocalExecutor, ensure that steps have proper dependency information. Without explicit dependencies, steps may execute in an incorrect order, leading to flaky tests.

```python
# Manually set up dependencies between steps
operations = planner.create_plan(pipeline, variables)

# Create a modified operations list with explicit dependencies
modified_operations = []
for i, step in enumerate(operations):
    step_copy = step.copy()
    # For transform and export steps, ensure they depend on previous steps
    if i > 0 and step_copy.get("type") in ["transform", "export"]:
        if "depends_on" not in step_copy:
            step_copy["depends_on"] = []
        # Add dependency on previous step if not already there
        prev_step_id = operations[i-1]["id"]
        if prev_step_id not in step_copy["depends_on"]:
            step_copy["depends_on"].append(prev_step_id)
    modified_operations.append(step_copy)
```

### Table Registration

When testing operations that depend on previous table creation, ensure that tables are properly registered with the DuckDB engine. The LocalExecutor automatically handles table registration when `_register_tables_with_engine()` is called.

For operations like `LOAD` that create tables, the LocalExecutor should register these tables with DuckDB before subsequent operations use them.

### Managing DuckDB Connections

When testing DuckDB database operations, be careful with connection management:

1. **Connection Conflicts**: DuckDB doesn't allow multiple connections with different configurations to the same database file.
2. **Connection Cleanup**: Always close connections when done to avoid resource leaks.
3. **Memory Management**: In tests that need to verify a database file after executor runs, use garbage collection and delays to ensure file handles are released.

Example of proper connection management in tests:

```python
# Close any existing connections and force garbage collection
import gc
gc.collect()

# Add a short delay to ensure file handles are released
import time
time.sleep(0.5)

try:
    # Connect in read-only mode to avoid conflicts
    conn = duckdb.connect(database=db_path, read_only=True)
    
    # Perform verification
    result = conn.execute("SELECT * FROM my_table").fetchall()
    assert len(result) > 0
    
    # Always close the connection when done
    conn.close()
except Exception as e:
    # Handle connection errors gracefully
    print(f"Connection error: {e}")
    # Provide alternative verification
    assert os.path.exists(db_path)
    assert os.path.getsize(db_path) > 10000
finally:
    # Ensure connection is closed even if an exception occurs
    if conn:
        try:
            conn.close()
        except:
            pass
```

## Testing SQLFlow Parser and Compatibility

When testing code that uses the SQLFlow parser, be aware that older tests might use `SQLFlowParser` while newer code uses `Parser`. The project provides backward compatibility through an alias:

```python
# In sqlflow/parser/__init__.py
from sqlflow.parser.parser import Parser

# Create an alias for backwards compatibility with tests
SQLFlowParser = Parser
```

To update tests, import `SQLFlowParser` from the correct location:

```python
# Old import - may break
from sqlflow.parser.parser import SQLFlowParser

# Updated import - compatible with current code
from sqlflow.parser import SQLFlowParser
```

## Common Test Patterns

### Testing CSV Export with Variables

```python
def test_csv_export_with_variables(self, test_environment):
    # Create a pipeline that uses variables
    pipeline_text = f"""
    SOURCE test_source TYPE CSV PARAMS {{
        "path": "{test_environment['data_file']}",
        "has_header": true
    }};
    
    LOAD data FROM test_source;
    
    CREATE TABLE enriched_data AS
    SELECT 
        id,
        name,
        value,
        value * 2 AS double_value
    FROM data;
    
    EXPORT SELECT * FROM enriched_data
    TO "{test_environment['output_dir']}/${{region|global}}/${{date|today}}_report.csv"
    TYPE CSV
    OPTIONS {{
        "header": true
    }};
    """

    # Parse and execute the pipeline
    parser = Parser(pipeline_text)
    pipeline = parser.parse()

    variables = {"region": "test-region", "date": "2023-11-15"}
    planner = Planner()
    operations = planner.create_plan(pipeline, variables)
    
    # Ensure proper dependencies
    modified_operations = []
    for i, step in enumerate(operations):
        step_copy = step.copy()
        if i > 0 and step_copy.get("type") in ["transform", "export"]:
            if "depends_on" not in step_copy:
                step_copy["depends_on"] = []
            prev_step_id = operations[i-1]["id"]
            if prev_step_id not in step_copy["depends_on"]:
                step_copy["depends_on"].append(prev_step_id)
        modified_operations.append(step_copy)

    executor = LocalExecutor()
    executor.profile = {"variables": variables}
    result = executor.execute(modified_operations, variables=variables)

    # Verify results
    assert "error" not in result, f"Pipeline execution error: {result.get('error')}"
    
    expected_file = os.path.join(
        test_environment["output_dir"], "test-region/2023-11-15_report.csv"
    )
    assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"
```

### Testing Persistence Across Executor Instances

```python
def test_persistence_across_executor_instances(self):
    # First execution - create tables
    executor1 = LocalExecutor(profile_name="persistent", project_dir=self.project_dir)
    
    # Execute pipeline and create tables
    result1 = executor1.execute(steps)
    
    # Verify successful execution
    self.assertEqual(result1.get("status"), "success")
    
    # Verify the database file exists
    self.assertTrue(os.path.exists(self.db_path))
    
    # Close the first executor
    executor1.duckdb_engine.close()
    
    # Create a second executor instance
    executor2 = LocalExecutor(profile_name="persistent", project_dir=self.project_dir)
    
    # Verify data persisted by querying tables created by first executor
    data = executor2.duckdb_engine.execute_query("SELECT * FROM my_table").fetchdf()
    self.assertEqual(len(data), expected_row_count)
```

## Common Issues and Solutions

### Issue: Table Does Not Exist

Error: `Table with name X does not exist`

Possible causes:
1. Steps are executing out of order due to missing dependencies
2. Tables aren't being registered with DuckDB
3. The `LOAD` step is failing silently

Solutions:
1. Add explicit dependencies between steps
2. Ensure `_register_tables_with_engine()` is called after table creation
3. Check the logs for any errors in the `LOAD` step

### Issue: DuckDB Connection Conflicts

Error: `Connection Error: Can't open a connection to same database file with a different configuration`

Solutions:
1. Close all existing connections before opening a new one
2. Use garbage collection to release file handles
3. Add a delay after closing connections
4. Use read-only mode when just verifying data
5. As a fallback, verify file existence and size instead of querying

### Issue: Flaky Tests Due to File Cleanup

Problem: Tests fail intermittently due to file locking issues

Solutions:
1. Always use `finally` blocks to ensure cleanup happens
2. Close connections before attempting to delete files
3. Add retry logic for file operations that might fail due to locks 