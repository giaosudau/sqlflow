# SQLFlow Planner and UDF System: Technical Deep Dive

⚠️ **Important Note**: This document describes the technical architecture for table UDF SQL integration. However, due to DuckDB Python API limitations, table UDFs cannot currently be called directly in SQL FROM clauses. The dependency detection and planning logic described here is implemented but not functional for table UDFs in production. Scalar UDFs work as described.

**Current Status:**
- ✅ **Scalar UDFs**: Fully functional as described in this document
- ❌ **Table UDFs in SQL**: Architecture implemented but not functional due to DuckDB limitations
- ✅ **Table UDFs (Programmatic)**: Work when called directly from Python
- ✅ **External Processing**: Recommended alternative for complex transformations

For current working approaches, see `docs/user/reference/python_udfs.md`.

---

## 1. Introduction

This document provides an in-depth technical explanation of the SQLFlow planner and User-Defined Function (UDF) systems. It covers how the planner builds execution plans, how it detects dependencies between operations, and how Python UDFs (both scalar and table UDFs) are integrated into the execution flow.

The document targets SQLFlow developers who need to enhance, debug, or extend these systems. It aims to serve as a foundational reference for future development work.

## 2. Planner Architecture

### 2.1 Core Components

The SQLFlow planner is responsible for converting a validated SQLFlow DAG (Directed Acyclic Graph) into a linear, JSON-serialized execution plan. Its key components include:

- **ExecutionPlanBuilder**: Builds an execution plan from a validated SQLFlow DAG
- **OperationPlanner**: Interfaces with the plan builder and handles error management
- **DependencyResolver**: Resolves dependencies between pipeline steps

The planner resides in `/sqlflow/core/planner.py` and works closely with the dependency resolver in `/sqlflow/core/dependencies.py`.

### 2.2 Execution Plan Building Process

The execution plan is built through the following sequence:

1. **Table-to-Step Mapping**: Creating a map of table names to the steps that produce them
2. **Dependency Analysis**: Identifying which steps depend on which other steps
3. **Cycle Detection**: Ensuring no circular dependencies exist
4. **Execution Order Resolution**: Determining the final execution order
5. **Step ID Generation**: Creating unique IDs for all steps
6. **Plan Serialization**: Creating the final JSON plan structure

```python
# Core planning sequence
def build_plan(self, pipeline: Pipeline, variables: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Build an execution plan for a pipeline."""
    # Initialize state
    self.dependency_resolver = DependencyResolver()
    self.step_id_map = {}
    self.step_dependencies = {}
    
    # Build a mapping of table names to steps that produce them
    table_to_step = self._build_table_to_step_mapping(pipeline)
    
    # Build dependency graph
    self._build_dependency_graph(pipeline)
    
    # Detect and report cycles
    cycles = self._detect_cycles(self.dependency_resolver)
    if cycles:
        error_msg = self._format_cycle_error(cycles)
        raise PlanningError(error_msg)
        
    # Get special step types
    source_steps, load_steps = self._get_sources_and_loads(pipeline)
    
    # Add load dependencies
    self._add_load_dependencies(source_steps, load_steps)
    
    # Generate unique IDs for each step
    self._generate_step_ids(pipeline)
    
    # Resolve execution order based on dependencies
    execution_order = self._resolve_execution_order()
    
    # Build final execution steps
    return self._build_execution_steps(pipeline, execution_order)
```

## 3. Dependency Detection

### 3.1 Table Reference Extraction

One of the most critical components of the planner is the dependency detection system, which determines the proper execution order. The core method for this is `_extract_referenced_tables`:

```python
def _extract_referenced_tables(self, sql_query: str) -> List[str]:
    sql_lower = sql_query.lower()
    tables = []
    
    # Handle standard SQL FROM clauses
    from_matches = re.finditer(
        r"from\s+([a-zA-Z0-9_]+(?:\s*,\s*[a-zA-Z0-9_]+)*)", sql_lower
    )
    for match in from_matches:
        table_list = match.group(1).split(",")
        for table in table_list:
            table_name = table.strip()
            if table_name and table_name not in tables:
                tables.append(table_name)
    
    # Handle standard SQL JOINs
    join_matches = re.finditer(r"join\s+([a-zA-Z0-9_]+)", sql_lower)
    for match in join_matches:
        table_name = match.group(1).strip()
        if table_name and table_name not in tables:
            tables.append(table_name)
            
    # Handle table UDF pattern: PYTHON_FUNC("module.function", table_name)
    # Note: This pattern is detected but doesn't work in practice due to DuckDB limitations
    udf_table_matches = re.finditer(
        r"python_func\s*\(\s*['\"][\w\.]+['\"]\s*,\s*([a-zA-Z0-9_]+)", sql_lower
    )
    for match in udf_table_matches:
        table_name = match.group(1).strip()
        if table_name and table_name not in tables:
            tables.append(table_name)
            
    return tables
```

This method uses regular expressions to identify:
1. Tables in `FROM` clauses
2. Tables in `JOIN` operations
3. Tables referenced as parameters to Python table UDFs (architecture present but non-functional)

### 3.2 Building the Dependency Graph

After extracting table references, the planner builds a dependency graph:

```python
def _build_dependency_graph(self, pipeline: Pipeline) -> None:
    table_to_step = self._build_table_to_step_mapping(pipeline)
    for step in pipeline.steps:
        if isinstance(step, SQLBlockStep):
            self._analyze_sql_dependencies(step, table_to_step)
        elif isinstance(step, ExportStep):
            self._analyze_export_dependencies(step, table_to_step)

def _analyze_sql_dependencies(
    self, step: SQLBlockStep, table_to_step: Dict[str, PipelineStep]
) -> None:
    sql_query = step.sql_query.lower()
    self._find_table_references(step, sql_query, table_to_step)
```

The `_find_table_references` method checks which tables are used in a query, and determines the dependencies between steps:

```python
def _find_table_references(
    self, step: PipelineStep, sql_query: str, table_to_step: Dict[str, PipelineStep]
) -> None:
    referenced_tables = self._extract_referenced_tables(sql_query)
    undefined_tables = []
    for table_name in referenced_tables:
        if table_name in table_to_step:
            table_step = table_to_step.get(table_name)
            if table_step and table_step != step:
                self._add_dependency(step, table_step)
        else:
            undefined_tables.append(table_name)
    if undefined_tables and hasattr(step, "line_number"):
        logger.warning(
            f"Step at line {step.line_number} references tables that might not be defined: {', '.join(undefined_tables)}"
        )
```

When a dependency is found, it's added to the dependency resolver:

```python
def _add_dependency(
    self, dependent_step: PipelineStep, dependency_step: PipelineStep
) -> None:
    dependent_id = str(id(dependent_step))
    dependency_id = str(id(dependency_step))
    self.dependency_resolver.add_dependency(dependent_id, dependency_id)
```

## 4. Python UDF System

### 4.1 UDF Types and Decorators

SQLFlow supports two types of Python UDFs:

1. **Scalar UDFs**: Process data row-by-row
2. **Table UDFs**: Process entire DataFrames at once

These are defined using decorators in `/sqlflow/udfs/decorators.py`:

#### Scalar UDF Decorator

```python
def python_scalar_udf(
    func: Optional[FuncType] = None,
    *,
    name: Optional[str] = None,
) -> Callable:
    """Decorator to mark a function as a SQLFlow scalar UDF."""
    
    # Define the decorator
    def decorator(f: FuncType) -> FuncType:
        # Validate function signature
        sig = inspect.signature(f)
        
        # Function wrapper
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Call the function and return result
            return f(*args, **kwargs)
            
        # Mark as a SQLFlow UDF with metadata
        wrapper._is_sqlflow_udf = True
        wrapper._udf_type = "scalar"
        wrapper._udf_name = name or f.__name__
        wrapper._signature = str(sig)
        
        return cast(FuncType, wrapper)
        
    # Handle case where decorator is used with or without arguments
    if func is None:
        return decorator
    return decorator(func)
```

#### Table UDF Decorator

```python
def python_table_udf(
    func: Optional[FuncType] = None,
    *,
    name: Optional[str] = None,
    required_columns: Optional[List[str]] = None,
    output_schema: Optional[Dict[str, str]] = None,
    infer: bool = False,
) -> Callable:
    """Decorator to mark a function as a SQLFlow table UDF."""
    
    # Define the decorator
    def decorator(f: FuncType) -> FuncType:
        # Validate function signature
        sig = inspect.signature(f)
        params = list(sig.parameters.values())
        _validate_table_udf_signature(f, params)
        
        # Validate schema configuration
        if output_schema is None and not infer:
            raise ValueError(
                f"Table UDF {f.__name__} must specify output_schema or set infer=True"
            )
        
        # Function wrapper
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Validate DataFrame input
            if not args:
                raise ValueError(f"Table UDF {f.__name__} requires a DataFrame argument")
                
            df = args[0]
            _validate_table_udf_input(f, df, required_columns)
            
            # Call the function
            result = f(df, **kwargs)
            
            # Infer schema if needed (first execution only)
            nonlocal output_schema
            if output_schema is None and infer:
                output_schema = _infer_output_schema(result)
                wrapper._output_schema = output_schema
                
            # Validate the output DataFrame
            _validate_table_udf_output(f, result, output_schema)
            
            return result
            
        # Mark as a SQLFlow UDF with metadata
        wrapper._is_sqlflow_udf = True
        wrapper._udf_type = "table"
        wrapper._udf_name = name or f.__name__
        wrapper._required_columns = required_columns
        wrapper._signature = str(sig)
        wrapper._param_info = _create_param_info(sig.parameters)
        wrapper._output_schema = output_schema
        wrapper._infer_schema = infer
        
        return cast(FuncType, wrapper)
        
    # Handle case where decorator is used with or without arguments
    if func is None:
        return decorator
    return decorator(func)
```

### 4.2 UDF Discovery and Registration

The `PythonUDFManager` in `/sqlflow/udfs/manager.py` is responsible for:

1. Discovering UDFs in Python modules
2. Extracting UDF metadata
3. Registering UDFs with the execution engine
4. Parsing SQL queries to identify UDF references

```python
class PythonUDFManager:
    """Manages Python UDFs for SQLFlow."""
    
    def __init__(self, project_dir: str = "."):
        """Initialize a PythonUDFManager."""
        self.project_dir = os.path.abspath(project_dir)
        self.udfs = {}
        self.udf_info = {}
        self.discovery_errors = {}
        
    def discover_udfs(self) -> Dict[str, Callable]:
        """Discover UDFs in Python modules within the project directory."""
        # Implementation discovers Python modules with UDF decorators
        # ...
        
    def register_udfs_with_engine(self, engine: Any, udf_names: Optional[List[str]] = None):
        """Register UDFs with a SQLFlow engine."""
        # Implementation registers UDFs with the engine
        # ...
        
    def extract_udf_references(self, sql: str) -> Set[str]:
        """Extract UDF references from a SQL query."""
        # Implementation extracts UDF references from SQL using regex
        # ...
```

## 5. Table UDF Execution

### 5.1 Table UDF Execution Flow

Table UDFs have a special execution pattern, particularly for the direct table UDF pattern, which is executed by the `LocalExecutor`:

```python
def _execute_transform(self, step: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a transform step to create a table from a SQL query."""
    sql = step["query"]
    table_name = step["name"]
    
    # Check if this is a direct table UDF call
    if self._is_direct_table_udf_call(sql):
        udf_name, args = self._extract_table_udf_info(sql)
        if udf_name and args:
            # Extract UDF info
            udf_match = (udf_name, args)
            print(f"DEBUG: Detected direct table UDF call pattern")
            return self._execute_direct_table_udf(udf_match, table_name, step["id"])
    
    # If not a direct UDF call, proceed with regular SQL execution
    return self._execute_sql_transform(sql, table_name, step["id"])
```

The direct table UDF execution happens in `_execute_direct_table_udf`:

```python
def _execute_direct_table_udf(self, match_obj, table_name: str, step_id: str) -> Dict[str, Any]:
    """Execute a table UDF directly without going through SQL."""
    # Extract the UDF name and arguments
    udf_name = match_obj[0]
    udf_args = match_obj[1].strip()
    
    try:
        # Fetch the UDF from the manager
        udf_func = self.udf_manager.get_udf(udf_name)
        if not udf_func:
            error_msg = f"UDF {udf_name} not found"
            return {"status": "failed", "error": error_msg}
        
        # Verify this is a table UDF
        udf_type = getattr(udf_func, "_udf_type", "unknown")
        if udf_type != "table":
            error_msg = f"Expected table UDF but got {udf_type} UDF: {udf_name}"
            return {"status": "failed", "error": error_msg}
        
        # Get the input data
        input_data = self._get_input_data_for_direct_udf(udf_args)
        if isinstance(input_data, dict) and input_data.get("status") == "failed":
            return input_data
        
        # Execute the UDF directly
        result_df = udf_func(input_data)
        
        # Store the result
        self.table_data[table_name] = DataChunk(result_df)
        self.step_table_map[step_id] = table_name
        if step_id not in self.step_output_mapping:
            self.step_output_mapping[step_id] = []
        self.step_output_mapping[step_id].append(table_name)
        
        return {"status": "success"}
    except Exception as e:
        error_msg = f"Error executing table UDF {udf_name} directly: {str(e)}"
        return {"status": "failed", "error": error_msg}
```

### 5.2 Input Data Retrieval

Table UDFs need to access input data. The method `_get_input_data_for_direct_udf` handles this:

```python
def _get_input_data_for_direct_udf(self, udf_args: str) -> Union[pd.DataFrame, Dict[str, Any]]:
    """Get input data for a directly executed table UDF."""
    # Check if arg is a SQL query
    if udf_args.strip().upper().startswith("SELECT "):
        try:
            result = self.duckdb_engine.execute_query(udf_args).fetchdf()
            return result
        except Exception as e:
            error_msg = f"Error executing SQL subquery for UDF: {str(e)}"
            return {"status": "failed", "error": error_msg}
    
    # Otherwise, assume it's a table name
    table_name = udf_args.strip().strip("'\"")  # Remove quotes if present
    
    if table_name in self.table_data:
        result = self.table_data[table_name].pandas_df
        return result
    
    # Table not found, check for partial matches
    for name in self.table_data.keys():
        if table_name.lower() in name.lower():
            result = self.table_data[name].pandas_df
            return result
    
    # No table found
    error_msg = f"Table not found for UDF input: {table_name}"
    return {"status": "failed", "error": error_msg}
```

## 6. The Recent Dependency Resolution Fix

### 6.1 Problem Description

The recent fix addressed a critical issue where table UDFs weren't properly recognizing dependencies on their input tables. In the pattern:

```sql
CREATE TABLE transformed_data AS
SELECT * FROM PYTHON_FUNC("module.function", input_table);
```

The planner wasn't detecting that `transformed_data` depended on `input_table`. This caused execution order issues where the UDF would run before its input data was loaded.

### 6.2 Root Cause Analysis

The dependency detection in `_extract_referenced_tables` method only handled standard SQL patterns:
- `FROM table`
- `JOIN table`

It didn't handle the UDF-specific pattern where a table name is passed as an argument to `PYTHON_FUNC`.

### 6.3 Solution

The fix extended the `_extract_referenced_tables` method to detect table references within UDF calls:

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

### 6.4 Edge Cases and Limitations

The current regex pattern works well for the standard UDF usage pattern where a table name is passed directly as an argument. However, there are some edge cases to be aware of:

1. **Complex SQL Subqueries**: The pattern doesn't handle cases where a SQL subquery is passed to the UDF instead of a table name:
   ```sql
   SELECT * FROM PYTHON_FUNC("module.function", (SELECT * FROM table WHERE condition))
   ```
   In these cases, the dependency needs to be extracted from the subquery, which is handled separately.

2. **Multiple Table Arguments**: Some UDFs might accept multiple tables:
   ```sql
   SELECT * FROM PYTHON_FUNC("module.join_tables", table1, table2)
   ```
   The current regex only extracts the first table argument.

3. **Dynamic Table Names**: Table names constructed through string interpolation are not detected:
   ```sql
   SELECT * FROM PYTHON_FUNC("module.function", ${table_prefix}_sales)
   ```

### 6.5 Testing Dependency Resolution

To verify that dependencies are correctly resolved, examine the execution plan. For a pipeline like:

```sql
LOAD raw_sales FROM sales_source;
CREATE TABLE sales_outliers AS SELECT * FROM PYTHON_FUNC("analytics.detect_outliers", raw_sales);
```

The execution plan should show that `transform_sales_outliers` depends on `load_raw_sales`:

```json
[
  {
    "id": "load_raw_sales",
    "type": "load",
    "source": "sales_source",
    "name": "raw_sales"
  },
  {
    "id": "transform_sales_outliers",
    "type": "transform",
    "query": "SELECT * FROM PYTHON_FUNC(\"analytics.detect_outliers\", raw_sales)",
    "name": "sales_outliers",
    "depends_on": ["load_raw_sales"]
  }
]
```

If this dependency is missing, it could indicate a problem with the table reference extraction.

### 6.6 Future Enhancements

Future improvements to the dependency resolution system could include:

1. **Enhanced Pattern Matching**: Supporting more complex UDF argument patterns
2. **AST-Based Parsing**: Moving from regex to proper SQL AST parsing for more robust extraction
3. **Schema Validation**: Adding validation for table schema compatibility with UDF requirements
4. **Explicit Dependency Declaration**: Allowing explicit dependency declarations in the pipeline syntax

## 7. Performance Considerations

- **Execution Plan Caching**: Reusing execution plans for unchanged queries
- **Incremental Execution**: Supporting partial re-execution for appended data
- **Resource Management**: Monitoring and adapting to available system resources

## 8. Extensibility Points

### 8.1 Planner Enhancements

1. **Enhanced Dependency Detection**: Improve regex patterns for more complex SQL cases
2. **Parallel Execution Planning**: Add hints for operations that can run in parallel
3. **Resource-Based Planning**: Consider memory and CPU requirements in execution order
4. **Execution Statistics**: Collect and utilize runtime statistics for optimization

#### 8.1.1 Planner Optimization Techniques

The planner could be enhanced with several optimization techniques:

1. **Operation Fusion**: Combining multiple operations that can be executed together, reducing intermediate data materializations:
   ```python
   def _fuse_compatible_operations(self, steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
       """Fuse compatible operations to reduce materialization."""
       # Implementation to identify and combine compatible operations
       # ...
   ```

2. **Partitioning Awareness**: Leveraging data partitioning for better execution planning:
   ```python
   def _analyze_partitioning_opportunities(self, step: Dict[str, Any]) -> Dict[str, Any]:
       """Analyze if a step can benefit from partitioning."""
       # Implementation to annotate steps with partitioning information
       # ...
   ```

3. **Cost-Based Optimization**: Using statistics to estimate operation costs:
   ```python
   def _estimate_operation_cost(self, step: Dict[str, Any], stats: Dict[str, Any]) -> float:
       """Estimate the computational cost of an operation."""
       # Implementation to calculate operation costs
       # ...
   ```

4. **Memory Management Planning**: Adding memory requirement estimates to avoid OOM errors:
   ```python
   def _estimate_memory_requirements(self, step: Dict[str, Any]) -> int:
       """Estimate memory requirements for an operation."""
       # Implementation to calculate memory needs
       # ...
   ```

### 8.2 UDF System Enhancements

1. **Additional UDF Types**: Support for window functions, aggregations, etc.
2. **Performance Optimizations**: Arrow-based data transfer for large datasets
3. **Improved Schema Inference**: More accurate type detection and conversion
4. **Dependency Tracking**: Track UDF dependencies on other UDFs
5. **UDF Versioning**: Support for versioned UDFs to handle schema changes
