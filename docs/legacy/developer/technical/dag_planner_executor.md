# SQLFlow DAG, Planner & Executor Technical Documentation

This document provides a detailed explanation of the DAG (Directed Acyclic Graph), Planner, and Executor components in SQLFlow, including their relationships, implementation details, and key technical decisions.

## 1. Overview

SQLFlow's execution architecture consists of three main components that work together to transform SQL scripts into executable pipelines:

1. **DAG (Directed Acyclic Graph)**: Represents the dependency relationships between operations in a SQLFlow script.
2. **Planner**: Converts the validated DAG into a linear, JSON-serialized execution plan.
3. **Executor**: Executes the plan items, potentially in parallel, with support for resuming from failures.

## 2. Component Relationships

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Parser    │────▶│  Compiler   │────▶│   Planner   │────▶│  Executor   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                                       │
                           ▼                                       ▼
                    ┌─────────────┐                        ┌─────────────┐
                    │     DAG     │                        │State Backend│
                    └─────────────┘                        └─────────────┘
```

### 2.1 Flow of Execution

1. The **Parser** processes SQLFlow scripts (.sf files) and generates an Abstract Syntax Tree (AST).
2. The **Compiler** transforms the AST into a DAG, validating dependencies and detecting cycles.
3. The **Planner** converts the validated DAG into a linear execution plan with explicit dependencies.
4. The **Executor** processes the execution plan, potentially in parallel, tracking task states and handling failures.

## 3. DAG (Directed Acyclic Graph)

The DAG represents the dependency relationships between operations in a SQLFlow script. It ensures that operations are executed in the correct order.

### 3.1 Implementation

The DAG is implemented in the `DependencyResolver` class, which:

1. Builds a graph of dependencies between blocks in the SQLFlow script
2. Validates that the graph is acyclic (no circular dependencies)
3. Resolves the execution order using topological sorting

### 3.2 Cycle Detection

Cycle detection is implemented using Kahn's algorithm (in-degree method):

1. Compute in-degrees for all nodes in the graph
2. Iteratively remove nodes with zero in-degree
3. If any nodes remain after this process, a cycle exists

```python
def _find_cycle(self, graph):
    """Find a cycle in the graph if one exists.
    
    Args:
        graph: Dictionary mapping node IDs to lists of dependent node IDs
        
    Returns:
        List of node IDs forming a cycle, or None if no cycle exists
    """
    # Create a copy of the graph to avoid modifying the original
    temp_graph = {node: list(deps) for node, deps in graph.items()}
    
    # Calculate in-degrees for all nodes
    in_degree = {node: 0 for node in temp_graph}
    for node in temp_graph:
        for dep in temp_graph[node]:
            in_degree[dep] = in_degree.get(dep, 0) + 1
    
    # Find nodes with zero in-degree
    queue = [node for node, degree in in_degree.items() if degree == 0]
    
    # Remove nodes with zero in-degree
    while queue:
        node = queue.pop(0)
        for dep in temp_graph[node]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)
    
    # If any nodes remain, there's a cycle
    remaining = [node for node, degree in in_degree.items() if degree > 0]
    if not remaining:
        return None
    
    # Find a cycle in the remaining nodes
    cycle = []
    start = remaining[0]
    cycle.append(start)
    current = next(iter(temp_graph[start]), None)
    
    while current and current != start and current not in cycle:
        cycle.append(current)
        current = next(iter(temp_graph[current]), None)
    
    if current:
        cycle.append(current)
    
    return cycle
```

## 4. Planner

The Planner converts a validated DAG into a linear, JSON-serialized execution plan that can be consumed by an executor.

### 4.1 Implementation

The Planner is implemented in the `OperationPlanner` class, which:

1. Takes a validated DAG as input
2. Performs a topological sort to determine execution order
3. Maps each DAG node to an `ExecutionStep` object
4. Serializes the execution plan to JSON

### 4.2 Execution Plan Format

The execution plan is a JSON array of `ExecutionStep` objects with the following structure:

```json
{
  "id": "transform_sales",
  "type": "transform",
  "query": "CREATE TABLE sales_summary AS SELECT ...",
  "depends_on": ["load_sales"],
  "metadata": { "engine_preference": "duckdb" }
}
```

### 4.3 Mapping Rules

- `LOAD` blocks map to steps with `type: "load"`, including `source_connector_type` and configuration in `query`.
- `TRANSFORM` blocks map to steps with `type: "transform"`, with SQL string in `query`.
- `EXPORT` blocks map to steps with `type: "export"`, with `source_table`, `source_connector_type`, and export configuration in `query`.

## 5. Executor

The Executor processes the execution plan, potentially in parallel, tracking task states and handling failures.

### 5.1 Thread-pool Task Executor

The `ThreadPoolTaskExecutor` executes pipeline steps concurrently using Python's `concurrent.futures.ThreadPoolExecutor`. It:

1. Tracks task states and dependencies
2. Executes eligible tasks in parallel
3. Updates dependent tasks when a task completes
4. Handles task failures and retries
5. Supports resuming from failures

```python
def execute(
    self,
    plan: List[Dict[str, Any]],
    dependency_resolver: Optional[DependencyResolver] = None,
    resume: bool = False,
) -> Dict[str, Any]:
    """Execute a pipeline plan concurrently.
    
    Args:
        plan: List of operations to execute
        dependency_resolver: Optional DependencyResolver to cross-check execution order
        resume: Whether to resume from a previous execution
        
    Returns:
        Dict containing execution results
    """
    self.results = {}
    self.dependency_resolver = dependency_resolver
    self.original_plan = plan.copy()
    
    self._check_execution_order(plan, dependency_resolver)
    self._initialize_execution_state(plan, resume)
    
    with ConcurrentThreadPoolExecutor(max_workers=self.max_workers) as executor:
        return self._execute_with_thread_pool(executor, plan)
```

### 5.2 Task State Management

Tasks can be in one of five states:

1. **PENDING**: Task is waiting for dependencies to be satisfied
2. **ELIGIBLE**: All dependencies are satisfied, task is ready to run
3. **RUNNING**: Task is currently executing
4. **SUCCESS**: Task completed successfully
5. **FAILED**: Task failed after all retry attempts

```python
class TaskState(str, Enum):
    """Task execution states."""
    
    PENDING = "PENDING"
    ELIGIBLE = "ELIGIBLE"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
```

### 5.3 Resume-from-failure Logic

The `ThreadPoolTaskExecutor` supports resuming from failures by:

1. Persisting execution state after each task
2. Tracking failed tasks and their dependencies
3. Only re-running tasks that are not in the `SUCCESS` state
4. Supporting configurable retries and retry delays

```python
def resume(self, run_id: Optional[str] = None) -> Dict[str, Any]:
    """Resume execution from the last failure.
    
    Args:
        run_id: Optional run ID to resume from. If not provided,
               uses the current run_id.
               
    Returns:
        Dict containing execution results
    """
    if run_id is not None:
        self.run_id = run_id
        
    if self.state_backend is not None:
        return self._resume_from_state_backend()
        
    if not self.can_resume():
        return {"status": "nothing_to_resume"}
        
    failed_step = self.failed_step
    if failed_step is None:
        return {"status": "nothing_to_resume"}
    
    self.failed_step = None
    plan = self._get_plan_for_resume()
    
    try:
        self._execute_failed_step(failed_step)
        
        with ConcurrentThreadPoolExecutor(max_workers=self.max_workers) as executor:
            result = self._execute_with_thread_pool(executor, plan)
            if result is not None:
                self.results.update(result)
    except Exception as e:
        self.failed_step = failed_step
        self.results["error"] = str(e)
        self.results["failed_step"] = failed_step["id"]
        self._update_task_state(failed_step["id"], TaskState.FAILED, str(e))
        return self.results
        
    return self.results
```

### 5.4 State Backend

The `DuckDBStateBackend` persists execution state in a DuckDB database, allowing for resumption across process restarts:

1. Stores the execution plan
2. Tracks task statuses
3. Records run metadata
4. Supports loading state for resumption

```python
class DuckDBStateBackend:
    """DuckDB-based state backend for SQLFlow executors."""
    
    def __init__(self, db_path: str = ":memory:"):
        """Initialize a DuckDBStateBackend.
        
        Args:
            db_path: Path to DuckDB database file. Defaults to in-memory.
        """
        self.db_path = db_path
        self.conn = None
        self._initialize_db()
        
    def _initialize_db(self):
        """Initialize the database schema."""
        self.conn = duckdb.connect(self.db_path)
        
        # Create runs table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id VARCHAR PRIMARY KEY,
                status VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                metadata JSON
            )
        """)
        
        # Create plans table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS plans (
                run_id VARCHAR PRIMARY KEY,
                plan JSON,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
        """)
        
        # Create task_statuses table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS task_statuses (
                run_id VARCHAR,
                task_id VARCHAR,
                status JSON,
                PRIMARY KEY (run_id, task_id),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
        """)
```

## 6. Comparison with Industry Standards

### 6.1 Airflow

Airflow uses a similar DAG-based approach but with some key differences:

| Feature | SQLFlow | Airflow |
|---------|---------|---------|
| **DAG Definition** | SQL-based with directives | Python code with operators |
| **Task States** | PENDING, ELIGIBLE, RUNNING, SUCCESS, FAILED | NONE, SCHEDULED, QUEUED, RUNNING, SUCCESS, FAILED, etc. |
| **Execution Model** | Thread-pool or distributed | Distributed with workers |
| **Resumption** | Built-in with state backend | XCom and external storage |
| **Concurrency Control** | Dependency-based | Pool-based and dependency-based |

### 6.2 Dagster

Dagster has a more data-centric approach:

| Feature | SQLFlow | Dagster |
|---------|---------|---------|
| **Core Abstraction** | Operations with dependencies | Assets with dependencies |
| **Task States** | PENDING, ELIGIBLE, RUNNING, SUCCESS, FAILED | STARTED, SUCCESS, FAILURE, etc. |
| **Execution Model** | Thread-pool or distributed | Distributed with executors |
| **Resumption** | Built-in with state backend | Run storage and event log |
| **Concurrency Control** | Dependency-based | Resource-based and dependency-based |

## 7. Key Technical Decisions

### 7.1 Thread-pool vs. Process-pool

We chose a thread-pool executor over a process-pool executor for several reasons:

1. **Shared Memory**: Threads share memory, making it easier to share data between tasks
2. **Lower Overhead**: Threads have lower creation and context-switching overhead
3. **Simpler Communication**: No need for serialization/deserialization between processes
4. **Resource Efficiency**: Better for I/O-bound tasks, which are common in data pipelines

### 7.2 Kahn's Algorithm for Cycle Detection

We selected Kahn's algorithm (in-degree method) for detecting cycles because:

1. **Efficiency**: O(V + E) time complexity, where V is the number of vertices and E is the number of edges
2. **Simplicity**: Straightforward implementation with clear logic
3. **Dual Purpose**: Also produces a valid topological sort for execution order
4. **Early Detection**: Can detect cycles before attempting execution

### 7.3 DuckDB for State Backend

We chose DuckDB as the state backend for several reasons:

1. **Embedded**: No need for a separate database server
2. **Performance**: Fast for analytical queries and JSON handling
3. **SQL Interface**: Familiar query language for debugging and analysis
4. **Lightweight**: Small footprint and easy deployment
5. **Compatibility**: Works well with SQLFlow's SQL-centric approach

### 7.4 JSON for Execution Plan Format

We chose JSON for the execution plan format because:

1. **Human-Readable**: Easy to debug and understand
2. **Interoperability**: Compatible with many tools and languages
3. **Flexibility**: Supports nested structures and metadata
4. **Serialization**: Easy to serialize and deserialize

## 8. Examples

### 8.1 Simple Pipeline Example

Consider a simple SQLFlow script:

```sql
SOURCE sales TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

SOURCE users TYPE POSTGRES PARAMS {
  "connection": "${DB_CONN}",
  "table": "users"
};

LOAD sales INTO raw_sales;
LOAD users INTO raw_users;

CREATE TABLE sales_summary AS
SELECT
  s.date,
  u.country,
  SUM(s.amount) AS total_sales
FROM raw_sales s
JOIN raw_users u ON s.user_id = u.id
GROUP BY s.date, u.country;

EXPORT
  SELECT * FROM sales_summary
TO "s3://reports/sales_summary.parquet"
TYPE S3
OPTIONS { "compression": "snappy", "format": "parquet" };
```

### 8.2 Resulting DAG

The DAG for this pipeline would look like:

```
┌─────────────┐     ┌─────────────┐
│ SOURCE sales│     │ SOURCE users│
└─────────────┘     └─────────────┘
       │                   │
       ▼                   ▼
┌─────────────┐     ┌─────────────┐
│ LOAD sales  │     │ LOAD users  │
└─────────────┘     └─────────────┘
       │                   │
       └───────┬───────────┘
               ▼
       ┌─────────────┐
       │CREATE TABLE │
       │sales_summary│
       └─────────────┘
               │
               ▼
       ┌─────────────┐
       │   EXPORT    │
       └─────────────┘
```

### 8.3 Execution Plan

The execution plan for this pipeline would be:

```json
[
  {
    "id": "source_sales",
    "type": "source",
    "query": {
      "path": "data/sales.csv",
      "has_header": true
    },
    "depends_on": []
  },
  {
    "id": "source_users",
    "type": "source",
    "query": {
      "connection": "${DB_CONN}",
      "table": "users"
    },
    "depends_on": []
  },
  {
    "id": "load_sales",
    "type": "load",
    "source_connector_type": "csv",
    "query": {
      "source": "sales",
      "destination": "raw_sales"
    },
    "depends_on": ["source_sales"]
  },
  {
    "id": "load_users",
    "type": "load",
    "source_connector_type": "postgres",
    "query": {
      "source": "users",
      "destination": "raw_users"
    },
    "depends_on": ["source_users"]
  },
  {
    "id": "transform_sales_summary",
    "type": "transform",
    "query": "CREATE TABLE sales_summary AS SELECT s.date, u.country, SUM(s.amount) AS total_sales FROM raw_sales s JOIN raw_users u ON s.user_id = u.id GROUP BY s.date, u.country",
    "depends_on": ["load_sales", "load_users"]
  },
  {
    "id": "export_sales_summary",
    "type": "export",
    "source_table": "sales_summary",
    "source_connector_type": "s3",
    "query": {
      "destination_uri": "s3://reports/sales_summary.parquet",
      "compression": "snappy",
      "format": "parquet"
    },
    "depends_on": ["transform_sales_summary"]
  }
]
```

### 8.4 Execution with Failures

If the `load_users` step fails, the executor would:

1. Mark `load_users` as FAILED
2. Store the failure information in the state backend
3. Not execute `transform_sales_summary` or `export_sales_summary` since they depend on `load_users`

When resuming, the executor would:

1. Load the execution state from the state backend
2. Retry the `load_users` step
3. If successful, continue with `transform_sales_summary` and `export_sales_summary`
4. If it fails again, repeat the process until the maximum number of retries is reached

## 9. Conclusion

The DAG, Planner, and Executor components form the core of SQLFlow's execution architecture. They work together to transform SQL scripts into executable pipelines, with support for dependency management, parallel execution, and resumption from failures.

The design choices made in these components reflect SQLFlow's principles of simplicity, modularity, and SQL-first approach, while also drawing inspiration from industry standards like Airflow and Dagster.
