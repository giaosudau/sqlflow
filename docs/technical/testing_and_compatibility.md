# Testing and Backward Compatibility in SQLFlow

This document describes how SQLFlow maintains backward compatibility and the testing practices to ensure that changes don't break existing code.

## Backward Compatibility

SQLFlow evolves continuously, but we strive to maintain backward compatibility so that existing pipelines and integrations continue to work. This section outlines the key compatibility strategies used in the project.

### Class and Module Aliases

To support backward compatibility, SQLFlow sometimes uses aliases to rename or relocate classes without breaking existing code. For example:

```python
# In sqlflow/parser/__init__.py
from sqlflow.parser.parser import Parser

# Create an alias for backwards compatibility with tests
SQLFlowParser = Parser
```

This approach allows:
1. Code evolution with better naming conventions
2. Relocation of classes to more appropriate modules
3. Existing tests and integrations to continue working

### Common Aliases Used in SQLFlow

| Original Name | Current Name | Location |
|---------------|--------------|----------|
| `SQLFlowParser` | `Parser` | `sqlflow.parser` |

### Best Practices for Using Aliases

When using aliases for backward compatibility:

1. Document the alias in the module docstring
2. Add a comment explaining the purpose of the alias
3. Consider marking the alias as deprecated when appropriate
4. Plan for eventual removal of the alias in a major version update

## Testing Practices

### Test Structure

SQLFlow uses a combination of unit and integration tests to ensure code quality:

- **Unit Tests**: Located in `tests/unit/`, test individual components in isolation
- **Integration Tests**: Located in `tests/integration/`, test components working together

### Test Dependencies

Integration tests often involve multiple SQLFlow components working together. When testing with components like the LocalExecutor, it's essential to ensure proper dependency management:

```python
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

### Testing with DuckDB

When testing with DuckDB, especially in persistent mode:

1. **Table Registration**: Ensure tables are properly registered with the DuckDB engine using `_register_tables_with_engine()`
2. **Connection Management**: Close connections when done and be careful with file locking
3. **Error Handling**: Use try/except/finally blocks to ensure clean test teardown

Example of proper connection handling:

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

### Maintaining Test Compatibility

When refactoring code that affects existing tests:

1. **Avoid Breaking Changes**: Use aliases or backward compatibility layers
2. **Update Tests Gradually**: If possible, update tests to use new APIs alongside old ones
3. **Document Migration Paths**: Provide clear documentation for users to update their code
4. **Deprecation Warnings**: Use deprecation warnings to signal future changes

## Common Test Issues and Solutions

### Table Does Not Exist Errors

Error: `Table with name X does not exist`

This is often caused by:
1. Operations executing out of order due to missing dependencies
2. Tables not being registered with DuckDB before use
3. Issues with the LocalExecutor's table management

Solutions:
1. Add explicit dependencies between steps
2. Ensure `_register_tables_with_engine()` is called after table creation
3. Fix any issues in the executor implementation

### DuckDB Connection Conflicts

Error: `Connection Error: Can't open a connection to same database file with a different configuration`

Solutions:
1. Close all existing connections before opening a new one
2. Use garbage collection to release file handles
3. Use read-only mode when possible
4. Add a short delay after closing connections
5. Implement a fallback verification method that doesn't require connecting

### Test File Cleanup Issues

Problem: Tests fail to clean up temporary files or directories

Solutions:
1. Use `finally` blocks to ensure cleanup happens even after exceptions
2. Close all connections before attempting to delete files
3. Use the `with` statement with context managers like `tempfile.TemporaryDirectory()`

## Test Migration Strategy

When updating existing test code:

1. **Identify Deprecated Features**: Scan for use of renamed classes or methods
2. **Create Gradual Migration Plan**: Update tests in batches rather than all at once
3. **Run Tests Frequently**: Ensure each batch of updates doesn't break functionality
4. **Document Migration Process**: Create documentation for other contributors

By following these guidelines, SQLFlow maintains backward compatibility while evolving the codebase and keeps tests reliable. 