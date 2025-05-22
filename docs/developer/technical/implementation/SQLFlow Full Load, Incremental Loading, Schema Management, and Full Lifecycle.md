# SQLFlow Full Load, Incremental Loading, Schema Management, and Full Lifecycle

## Target Audience: Product Manager, Software Architect

This comprehensive guide outlines SQLFlow's approach to data loading, schema management, and full data lifecycle support. It incorporates insights from industry architects across the data tooling ecosystem and focuses on delivering a cohesive solution for SMEs.

---

## Document 1: Enhanced Load Controls (MODE Parameter)

**Target Audience:** Product Manager, Software Architect  
**Project Phase:** MVP Implementation (Finalization)  
**Relevant SQLFlow Files/Docs:**
- `technical_design_document.md` (Section 4.1 LOAD directive)
- `README.md` (SQLFlow Syntax Highlights)
- `sqlflow/core/engines/` (Execution logic)
- `sqlflow/parser/` (DSL parsing)
- `sqlflow/connectors/` (Especially `write` methods if applicable)

### 1.1. For the Product Manager

**Feature Name:** Unified Data Loading Control with `MODE`

**Product Description:**
SQLFlow empowers users to precisely control how data is loaded into their target tables using a simple and intuitive `MODE` parameter within the `LOAD` statement (or as part of `CREATE TABLE AS SELECT` if it implies a load). This feature supports common data warehousing patterns like complete replacement, appending new data, or merging/upserting records. SQLFlow ensures these operations are reliable by providing robust validation and clear, actionable error messages, guiding users to quick resolutions.

**Supported Modes (MVP):**
- **`REPLACE` (Default):** Completely overwrites the target table with new data. If the table doesn't exist, it's created. If it exists, its content is replaced.
- **`APPEND`:** Adds new records to an existing table without altering existing data. SQLFlow will verify schema compatibility and warn or error if schemas mismatch significantly.
- **`MERGE` (Basic):** Updates existing records and inserts new ones based on user-specified key column(s). For MVP, this will require clear definition of merge keys and will validate their presence.

**MERGE Behavior Details:**
- Records matching on key(s) will be updated with values from the source
- Records in source not matching target will be inserted
- Records in target not present in source will remain unchanged
- Future enhancements will support deletion logic and custom match conditions

**User Benefits (SME Focus):**
- **Simplicity & Familiarity:** Uses SQL-like concepts that are easy for analysts to understand and implement, reducing the learning curve.
- **Data Integrity:** Prevents accidental data loss or corruption through clear operational modes and validation.
- **Reduced Errors:** Proactive validation (e.g., checking if a table exists for `APPEND`, merge key presence) and informative error messages reduce debugging time.
- **Predictable Outcomes:** Users can confidently predict the result of their load operations, building trust in the platform.

**Roadmap Beyond MVP:**
- Advanced materialization strategies similar to dbt (incremental with custom predicates)
- SCD Type 1/2 patterns as built-in templates
- Warehouse-specific optimization parameters

**MVP Focus & Success Criteria:**
- `REPLACE` and `APPEND` modes are fully robust and well-tested.
- `MERGE` mode is implemented with clear syntax for specifying unique/join keys; robust validation for key presence and basic conflict scenarios.
- Comprehensive validation for all modes (e.g., schema checks for `APPEND`, table existence, key constraints for `MERGE`).
- Error messages are user-friendly, providing context (pipeline name, line number, table, issue).

### 1.2. For the Software Architect

**Technical Decisions & Direction:**

**Integration Point:**
- The `MODE` logic will be primarily handled by the SQL generation layer within the `SQLEngine` (e.g., `DuckDBEngine`). 
- The parser identifies the `MODE`, the compiler/planner annotates the operation, and the engine generates the appropriate DDL/DML.
- This approach maintains separation between parsing logic and execution logic.

**SQL Generation Strategies:**
- **`REPLACE`:** 
  - In DuckDB: Will use `CREATE OR REPLACE TABLE AS SELECT...`
  - Alternative pattern: `DROP TABLE IF EXISTS ...; CREATE TABLE AS ...`
  - Performance consideration: Single statement is preferred where supported
  
- **`APPEND`:** 
  - Translates to `INSERT INTO ... SELECT ...` 
  - Schema compatibility checks are performed before execution by comparing column names, types, and nullability
  - Edge case handling: Source columns not in target are checked for nullable status

- **`MERGE`:** 
  - For DuckDB: Will use `INSERT INTO ... ON CONFLICT DO UPDATE SET...`
  - For other engines: Implement with transaction-wrapped staging table pattern
  - Required parameters: Primary key column(s) for matching records
  - Implementation note: Will execute in a transaction for atomicity

**Validation Workflow:**
1. **Parser/Compiler Phase:** 
   - Validate `MODE` syntax, presence of required parameters (e.g., merge keys for `MERGE`)
   - Verify referential integrity of specified tables and columns
   - Check for semantic correctness of operation

2. **Pre-Execution (Engine):**
   - For `APPEND`: Check if the target table exists. Perform schema compatibility check between source data and target table.
   - For `MERGE`: Ensure specified merge keys exist in both source and target and have compatible types.
   - Validate that specified mode is compatible with the target system's capabilities

**Error Handling:**
- Centralize error reporting in the `SQLEngine` executor
- Implement distinct error classes for different categories of failures
- Errors should halt the specific task and propagate clearly to the user
- Include detailed context in error messages: pipeline, statement, `MODE` used, table(s) involved, specific error (e.g., "Schema mismatch for APPEND: Column 'X' has type Y in source, Z in target")

**Warehouse-Specific Considerations:**
- Design SQL generation to be engine-aware, with optimized patterns for each supported backend
- Include hooks for future warehouse-specific parameters (e.g., distribution keys, clustering)
- Document expected behavior differences between engines where unavoidable

**Alignment with Existing Code:**
- The `LOAD` directive parsing (ref: `technical_design_document.md` 4.1) needs to be extended to capture the `MODE` and any associated parameters (like merge keys).
- The `SQLEngine` interface and its implementations (`sqlflow/core/engines/`) will need to generate different SQL based on the `MODE`.
- Connectors' `write` methods (`sqlflow/connectors/base.py`, `postgres_export_connector.py`, etc.) are generally about delivering a `DataChunk`. The `MODE` logic is more about *how* that data is integrated into the target persistent table by the *engine*, rather than the connector itself, unless the connector writes directly to a system that has its own mode concepts (e.g., an API endpoint). For database targets, the engine generates the SQL.

---

## Document 2: Core Connector Framework & Initial Implementations

**Target Audience:** Product Manager, Software Architect  
**Project Phase:** MVP Implementation  
**Relevant SQLFlow Files/Docs:**
- `technical_design_document.md` (Section 6. Connectors, Section 5.1 SQLEngine Interface)
- `README.md` (Key MVP Features, SQLFlow Syntax Highlights)
- `sqlflow/connectors/base.py` (Connector ABC)
- `sqlflow/connectors/registry.py` (Connector discovery)
- `sqlflow/connectors/connector_engine.py` (Orchestration)
- `sqlflow/connectors/csv_connector.py`, `s3_connector.py`, `postgres_connector.py`, `parquet_connector.py`

### 2.1. For the Product Manager

**Feature Name:** SQLFlow Connectors: Your Data, Seamlessly Connected

**Product Description:**
SQLFlow simplifies accessing and moving data with its robust Connector Framework. Users can easily ingest data from various sources and export transformed data to desired destinations using intuitive `SOURCE` and `EXPORT` commands directly within their SQLFlow pipelines. For MVP, SQLFlow will provide built-in connectors for common SME needs: PostgreSQL databases and S3 (for CSV and Parquet files). A key capability is schema inference, where SQLFlow automatically attempts to determine the structure of your source data, reducing manual configuration and potential errors.

**Key Capabilities (MVP):**
- **`SOURCE` Connectors:**
  - PostgreSQL: Read data using SQL queries or table references
  - S3: Read CSV and Parquet files with robust error handling
- **`EXPORT` Connectors:**
  - S3: Write data as CSV and Parquet files with configurable options
  - (Implicit) DuckDB tables: Results of transformations are inherently available in DuckDB
- **Schema Inference:** Automatically determines column names, types, and nullability from sources
- **Simple Configuration:** Connector parameters defined within the `PARAMS` block of `SOURCE`/`EXPORT` directives

**Implementation Strategy:**
- Focus on quality over quantity for connectors
- Robust error handling and clear validation messages
- Thorough testing with edge cases (empty files, large datasets, problematic data)

**User Benefits (SME Focus):**
- **Reduced Complexity:** Eliminates the need for custom scripts or separate EL tools for basic ingestion and export tasks.
- **Time Savings:** Schema inference and straightforward configuration speed up pipeline development.
- **Unified Workflow:** Manage data sourcing, transformation, and export within the same SQL-centric environment.
- **"Batteries Included":** Essential connectors for common data storage systems used by SMEs are available out-of-the-box.

**Connector Roadmap:**
- MVP: PostgreSQL, S3 (CSV/Parquet) 
- Near-term: MySQL, Snowflake, BigQuery
- Mid-term: 1-2 key SaaS connectors (based on user feedback)
- Long-term: Connector SDK for community contributions

**MVP Focus & Success Criteria:**
- Reliable data reading from PostgreSQL (specified query) and S3 (CSV, Parquet).
- Reliable data writing to S3 (CSV, Parquet).
- Basic schema inference works for S3 CSV/Parquet (column names, common data types) and for PostgreSQL query results.
- Clear error messages for connection issues or misconfigurations.
- `test_connection` method implemented for each connector.

### 2.2. For the Software Architect

**Technical Decisions & Direction:**

**Connector Interface (`sqlflow/connectors/base.py`):**
- Solidify the `Connector` abstract base class with these key methods:
  ```python
  class Connector(ABC):
      @abstractmethod
      def configure(self, params: dict, profile_variables: dict) -> None:
          """Initialize with pipeline parameters and profile variables."""
          pass
          
      @abstractmethod
      def test_connection(self) -> ConnectionTestResult:
          """Validate connectivity to the source/destination."""
          pass
          
      @abstractmethod
      def get_schema(self, object_name: str) -> Schema:
          """Return the schema of a source object (table, file, etc)."""
          pass
          
      @abstractmethod
      def read(self, object_name: str, columns: Optional[List[str]] = None,
               filters: Optional[Any] = None, limit: Optional[int] = None) -> Iterator[DataChunk]:
          """Stream data from the source."""
          pass
          
      @abstractmethod
      def write(self, object_name: str, data_iterator: Iterator[DataChunk],
                mode: str = 'REPLACE') -> None:
          """Write data to the destination."""
          pass
  ```

**Error Handling & Retries:**
- Implement retryable operations for network-related failures
- Use exponential backoff for transient errors
- Clear error categorization (auth, network, resource, permission, etc.)
- Detailed logging with sufficient context for debugging

**Performance Considerations:**
- Batch size tuning for optimal throughput
- Stream processing to avoid memory bottlenecks
- Consider chunking for large files
- Monitor and report throughput metrics

**Schema Fetching/Inference:**
- **PostgreSQL:** 
  - Primary: Query `INFORMATION_SCHEMA.COLUMNS` for tables
  - For queries: Analyze result set metadata from a sample execution
  - Cache schema information where appropriate

- **S3 (CSV/Parquet):**
  - CSV: Use DuckDB's `read_csv_auto(..., sample_size=-1)` for inference
  - Parquet: Extract schema directly from file metadata
  - Handle problematic cases:
    - Mixed types in CSV columns
    - Nullable/non-nullable determination
    - Date/time format detection

**Connector Orchestration (`sqlflow/connectors/connector_engine.py`):**
- Manage connector lifecycle: instantiation, configuration, execution
- Handle resource cleanup properly
- Implement connection pooling where appropriate
- Monitor and limit concurrent connections

**Registry (`sqlflow/connectors/registry.py`):**
- Plugin architecture for connector discovery
- Clear versioning for connectors
- Dependency management for connector-specific libraries

**Parameterization:**
- Environment variable support for credentials 
- Secret handling through profiles
- Support for file-based configuration
- Parameter validation before connection attempts

**Alignment with Existing Code:**
- The current connector files (`csv_connector.py`, `s3_connector.py`, `postgres_connector.py`, `parquet_connector.py`) provide a strong foundation. Review and align their `get_schema` and `read`/`write` methods with the refined `Connector` interface.
- `s3_connector.py` is quite comprehensive; ensure its schema inference logic is robust.
- `postgres_placeholder.py` should be fully replaced by/merged into `postgres_connector.py`.

**Testing Strategy:**
- Unit tests for each connector method
- Integration tests with containerized services
- Edge case testing (empty data, schema changes, large datasets)
- Performance benchmarks for read/write operations

---

## Document 3: Schema Management & Evolution

**Target Audience:** Product Manager, Software Architect  
**Project Phase:** MVP Implementation  
**Relevant SQLFlow Files/Docs:**
- `sqlflow/connectors/base.py` (especially `get_schema` method)
- `sqlflow/connectors/connector_engine.py` (Orchestration point for checks)
- `sqlflow/core/compiler.py` or `sqlflow/core/planner.py` (Where to integrate the check)

### 3.1. For the Product Manager

**Feature Name:** Schema Management: Control & Confidence in Your Data Structure

**Product Description:**
SQLFlow provides comprehensive schema management capabilities that go beyond simple schema drift detection. The system helps maintain data integrity by monitoring source schemas, providing options for handling changes, and ensuring transformations work correctly as data evolves. This approach prevents unexpected pipeline failures, data corruption, and silent errors, empowering users to address schema evolution thoughtfully and confidently.

**Key Capabilities (MVP+):**
- **Schema Monitoring:** Tracks source schemas over time
- **Change Detection:** Identifies added/removed columns, type changes, and nullable status changes
- **Response Options:**
  - **Alert Mode (Default):** Halts the task and alerts the user
  - **Permissive Mode:** Proceeds with compatible changes (e.g., new nullable columns)
  - **Schema Declaration:** Optional explicit schema definitions to override inference
- **Change History:** Maintains a record of schema changes for auditing

**User Workflows:**
1. **First Run:** SQLFlow captures source schema
2. **Schema Change Detected:** 
   - Alert shown with detailed comparison
   - User evaluates impact on transformations
3. **User Action Options:**
   - Update pipeline to accommodate changes
   - Accept new schema (via CLI command or UI)
   - Define explicit schema to override source

**User Benefits (SME Focus):**
- **Prevents Data Disasters:** Catches schema changes that could lead to incorrect data transformations or load failures.
- **Saves Debugging Time:** Eliminates hours spent troubleshooting cryptic errors caused by unexpected source schema modifications.
- **Increases Data Trust:** Builds confidence in pipeline reliability and the accuracy of the resulting data.
- **Simplifies Maintenance:** Makes pipeline maintenance easier by explicitly flagging external changes that require attention.
- **Enables Evolution:** Provides clear paths to adapt to changing data structures.

**Compatibility Matrix:**
| Change Type | Alert Mode | Permissive Mode |
|-------------|------------|-----------------|
| New Column (nullable) | ⚠️ Alert | ✓ Accept |
| New Column (non-nullable) | ⚠️ Alert | ⚠️ Alert |
| Removed Column | ⚠️ Alert | ⚠️ Alert |
| Type Change (compatible) | ⚠️ Alert | ✓ Accept |
| Type Change (incompatible) | ⚠️ Alert | ⚠️ Alert |
| Nullable → Non-nullable | ⚠️ Alert | ⚠️ Alert |
| Non-nullable → Nullable | ⚠️ Alert | ✓ Accept |

**MVP Focus & Success Criteria:**
- Schema drift detection works reliably for PostgreSQL and S3 (CSV/Parquet) sources.
- Detection covers added/removed columns and data type changes.
- Alerts are clear, specific, and halt the problematic task, not the entire pipeline if other tasks are independent.
- Schema metadata is stored efficiently and can be examined by the user.
- Simple CLI command for accepting schema changes.

### 3.2. For the Software Architect

**Technical Decisions & Direction:**

**Schema Representation:**
- Define a canonical schema format as a structured object:
  ```python
  @dataclass
  class ColumnSchema:
      name: str
      data_type: str
      is_nullable: bool
      position: int
      description: Optional[str] = None
      
  @dataclass
  class Schema:
      columns: List[ColumnSchema]
      source_fingerprint: str  # Hash of schema for quick comparison
      captured_at: datetime
      
      def compare(self, other: 'Schema') -> SchemaComparisonResult:
          """Compare two schemas and identify differences"""
  ```

**Storage Strategy:**
- **MVP:** Store schemas as JSON files in project's `target/` directory
  - Location: `target/metadata/schemas/<pipeline_file_hash>/<source_name>.json`
  - Format: Serialized `Schema` object
- **Future:** Consider a SQLite database within the project for more robust metadata:
  ```
  target/metadata/sqlflow.db
  ```

**Metadata Evolution Plan:**
- Start with file-based storage for simplicity
- Design for migration to embedded database
- Eventually support shared metadata repositories for teams

**Detection Logic:**
1. **Initial Run / Schema Capture:** 
   - On first successful run, capture schema via connector's `get_schema()`
   - Serialize and store the schema
   
2. **Subsequent Runs:**
   - Prior to execution, fetch current schema
   - Compare with stored schema using `Schema.compare()`
   - Generate a structured difference report
   - Based on configuration, either:
     - Raise exception (Alert mode)
     - Proceed if changes are compatible (Permissive mode)
     - Log warning for all changes

**Schema Comparison Algorithm:**
```python
def compare_schemas(current: Schema, stored: Schema) -> SchemaComparisonResult:
    """
    Compare schemas and return structured differences
    """
    result = SchemaComparisonResult()
    
    # Index columns by name for efficient lookup
    current_cols = {col.name: col for col in current.columns}
    stored_cols = {col.name: col for col in stored.columns}
    
    # Find added columns
    for name, col in current_cols.items():
        if name not in stored_cols:
            result.added_columns.append(col)
    
    # Find removed columns
    for name, col in stored_cols.items():
        if name not in current_cols:
            result.removed_columns.append(col)
    
    # Find modified columns
    for name, current_col in current_cols.items():
        if name in stored_cols:
            stored_col = stored_cols[name]
            if current_col.data_type != stored_col.data_type:
                result.type_changes.append((stored_col, current_col))
            if current_col.is_nullable != stored_col.is_nullable:
                result.nullable_changes.append((stored_col, current_col))
            if current_col.position != stored_col.position:
                result.position_changes.append((stored_col, current_col))
    
    return result
```

**Integration Points:**
- Schema drift check runs in `ConnectorEngine` before executing `LOAD`
- Configuration controlled via:
  - `LOAD` directive parameters
  - Profile settings
  - CLI flags

**User Commands:**
- `sqlflow schema show <pipeline> <source>` - Display current schema
- `sqlflow schema history <pipeline> <source>` - Show schema change history
- `sqlflow schema accept <pipeline> <source>` - Accept current schema as new baseline
- `sqlflow schema compare <pipeline> <source>` - Compare current vs. stored without running pipeline

**Future Extensions:**
- Schema enforcement (reject data that doesn't conform)
- Schema evolution migration generation
- Data validation rules based on schema

**Alignment with Existing Code:**
- `Connector.get_schema()` is fundamental. Ensure its output is standardized.
- The `ConnectorEngine` is the natural place to orchestrate schema fetching and comparison.
- Error handling mechanisms need to be able to surface these specific schema drift alerts clearly.
- This is a new layer of functionality, but leverages existing connector capabilities.

---

## Document 4: Execution Model & Scalability

**Target Audience:** Product Manager, Software Architect  
**Project Phase:** MVP and Beyond  
**Relevant SQLFlow Files/Docs:**
- `technical_design_document.md` (Section 8. DAG Execution)
- `sqlflow/core/executors.py`
- `sqlflow/core/planner.py`

### 4.1. For the Product Manager

**Feature Name:** SQLFlow Execution: Reliable, Scalable Pipeline Processing

**Product Description:**
SQLFlow features a robust execution model that reliably runs data pipelines from source to destination. The system handles dependencies between operations, manages resource allocation, provides status tracking, and enables both local execution for development and distributed execution for production workloads. This flexible approach allows SMEs to start simple and scale as their needs grow.

**Key Capabilities (MVP+):**
- **Dependency Management:** Automatically determines operation order based on data dependencies
- **Parallel Execution:** Runs independent operations concurrently for optimal performance
- **Status Tracking:** Provides clear visibility into pipeline progress and completion status
- **Failure Handling:** Manages errors gracefully with clear reporting and partial retry options
- **Execution Modes:**
  - **Local:** Single-process execution (MVP)
  - **Distributed:** Multi-node execution via Celery (Future)
  - **Orchestrator Integration:** Airflow, Prefect, Dagster export (Future)

**Execution Workflow:**
1. **Compilation:** SQLFlow parses pipeline, builds DAG of operations
2. **Validation:** Validates all operations, connections, and references
3. **Execution Planning:** Determines execution order and parallelization
4. **Execution:** Processes operations with dependency management
5. **Monitoring:** Reports progress and captures metrics
6. **Completion/Error Handling:** Finalizes all resources, reports success/failure

**User Benefits (SME Focus):**
- **Simplified Operations:** No need to manage complex orchestration systems initially
- **Efficient Resource Use:** Parallel execution maximizes throughput
- **Reliability:** Consistent handling of errors and dependencies
- **Visibility:** Clear insights into pipeline status and issues
- **Growth Path:** Ability to scale from laptop to production cluster

**Integration Options (Beyond MVP):**
- **Native Scheduling:** Built-in scheduler for time-based triggering
- **Airflow Provider:** Export SQLFlow pipelines as Airflow DAGs
- **Kubernetes Deployment:** Run on container orchestration
- **Event Triggers:** Execute based on S3 notifications, webhooks, etc.

**MVP Focus & Success Criteria:**
- Local execution with thread-pool parallelism works reliably
- Dependencies are correctly tracked and enforced
- Error handling gracefully manages failures
- Users can see execution progress and status
- Core metrics (execution time, records processed) are captured

### 4.2. For the Software Architect

**Technical Decisions & Direction:**

**DAG Construction:**
- Build from AST generated by parser
- Nodes represent atomic operations (source, load, transform, export)
- Edges represent data dependencies
- Optimize graph by combining operations where possible
- Validate DAG for cycles and completeness

**Concurrency Model:**
- **MVP:** ThreadPoolExecutor for parallel task execution
  - Default to `min(32, os.cpu_count() * 5)` threads
  - Configurable via profile
- **Future:** 
  - Celery-based distributed execution
  - Support for process-based parallelism

**Task States:**
```python
class TaskState(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
```

**Execution Algorithm:**
```python
def execute_dag(dag: DAG):
    """Execute a DAG of operations"""
    # Initialize task states
    task_states = {task: TaskState.PENDING for task in dag.tasks}
    results = {}
    
    # Track dependencies
    remaining_dependencies = {
        task: set(dag.get_dependencies(task)) 
        for task in dag.tasks
    }
    
    # Process until no tasks remain or all are in terminal state
    while any(state == TaskState.PENDING for state in task_states.values()):
        # Find ready tasks (all dependencies satisfied)
        ready_tasks = [
            task for task, deps in remaining_dependencies.items()
            if not deps and task_states[task] == TaskState.PENDING
        ]
        
        if not ready_tasks:
            # Check for deadlock or all complete
            if any(state == TaskState.PENDING for state in task_states.values()):
                raise ExecutionError("Deadlock detected in execution plan")
            break
            
        # Submit ready tasks to executor
        futures = {}
        for task in ready_tasks:
            task_states[task] = TaskState.RUNNING
            futures[executor.submit(execute_task, task, results)] = task
            
        # Wait for completions
        for future in as_completed(futures):
            task = futures[future]
            try:
                results[task] = future.result()
                task_states[task] = TaskState.SUCCEEDED
                
                # Update dependency tracking for dependent tasks
                for dependent in dag.get_dependents(task):
                    remaining_dependencies[dependent].remove(task)
                    
            except Exception as e:
                task_states[task] = TaskState.FAILED
                # Handle failure based on policy
                if dag.failure_policy == FailurePolicy.FAIL_FAST:
                    raise ExecutionError(f"Task {task} failed: {e}")
                elif dag.failure_policy == FailurePolicy.CONTINUE:
                    # Mark dependent tasks as skipped
                    for dependent in dag.get_all_dependents(task):
                        task_states[dependent] = TaskState.SKIPPED
                        
    return ExecutionResult(task_states, results)
```

**Resource Management:**
- Control memory usage by limiting concurrent transformations
- Use streaming execution where possible
- Implement backpressure mechanisms
- Monitor and enforce resource limits

**Failure Handling:**
- Configurable policies:
  - `FAIL_FAST`: Stop entire pipeline on first error
  - `CONTINUE`: Skip dependent tasks but continue independents
  - `RETRY`: Attempt N retries before failing (future)
- Detailed error reporting with context
- Transaction management for database operations

**Status Tracking:**
- Real-time status updates in console
- JSON-formatted execution log
- Execution history in `target/logs/`
- Web dashboard for monitoring (future)

**Scaling Strategy:**
1. **Local Development:** ThreadPool on developer machine
2. **Small Production:** Process-based parallelism on single server
3. **Medium Scale:** Celery workers on multiple servers
4. **Large Scale:** Kubernetes-based distributed execution

**Metrics Collection:**
- Execution time per task
- Records processed
- Memory used
- I/O operations
- External service calls

**Orchestrator Integration:**
- Export SQLFlow pipelines as Airflow DAGs
- Support Prefect and Dagster task definitions
- Enable webhook triggers for CI/CD integration

**Alignment with Existing Code:**
- The `DAG` construction logic mentioned in `technical_design_document.md` Section 8 forms the foundation
- The existing executor in `sqlflow/core/executors.py` needs extension for more robust parallel execution
- Error handling and status reporting mechanisms should be standardized across components

---

## Document 5: Data Warehouse Integration

**Target Audience:** Product Manager, Software Architect  
**Project Phase:** Future Roadmap (Post-MVP)  
**Relevant SQLFlow Files/Docs:**
- `technical_design_document.md` (Section 5.2 SQLEngine Interface)
- `sqlflow/core/engines/` (Engine implementations)

### 5.1. For the Product Manager

**Feature Name:** SQLFlow Warehouse Integration: Native Performance, Familiar Interface

**Product Vision:**
SQLFlow will seamlessly integrate with leading data warehouses (Snowflake, BigQuery, Redshift, Databricks), allowing users to leverage their power and scale while maintaining SQLFlow's intuitive SQL-first interface. This integration preserves SQLFlow's unified approach to data pipelines while taking advantage of warehouse-specific optimizations, materialization strategies, and performance features.

**Key Capabilities (Future):**
- **Native Engine Support:** Direct execution in Snowflake, BigQuery, Redshift, and Databricks
- **Optimization Push-down:** Leverage warehouse-specific features for maximum performance
- **Materialization Controls:** Fine-grained control over clustering, partitioning, and distribution
- **Hybrid Execution:** Mix local processing with warehouse execution where appropriate
- **Cost Management:** Intelligent decisions to minimize warehouse compute costs

**Warehouse-Specific Features:**
- **Snowflake:** Leverage clustering keys, materialized views, zero-copy clones
- **BigQuery:** Utilize partitioning, clustering, and authorized views
- **Redshift:** Optimize with distribution styles, sort keys, and compression
- **Databricks:** Take advantage of Delta Lake features, Z-ordering, and Photon

**User Benefits (SME Focus):**
- **Leverage Investments:** Utilize existing data warehouse capabilities
- **Performance:** Get warehouse-native speed for large datasets
- **Familiarity:** Maintain the SQLFlow interface you know
- **Cost Control:** Optimize warehouse usage intelligently
- **Future-Proof:** Easily switch between warehouses as needs change

**Implementation Strategy:**
1. DuckDB as initial engine (MVP)
2. Add warehouse connectors for reading/writing (Near-term)
3. Implement direct SQL execution in warehouses (Mid-term)
4. Add warehouse-specific optimizations (Long-term)

**Success Criteria (Future):**
- SQLFlow pipelines execute natively in supported warehouses
- Performance matches or exceeds hand-written warehouse-specific SQL
- Users can control warehouse-specific features through SQLFlow
- Migration between warehouses requires minimal changes

### 5.2. For the Software Architect

**Technical Decisions & Direction:**

**SQLEngine Interface Extensions:**
```python
class SQLEngine(ABC):
    # Existing methods...
    
    @abstractmethod
    def get_optimization_hints(self) -> Dict[str, List[str]]:
        """Return optimization hints supported by this engine"""
        pass
        
    @abstractmethod
    def apply_optimization(self, query: str, hints: Dict[str, Any]) -> str:
        """Apply engine-specific optimizations to the query"""
        pass
        
    @abstractmethod
    def supports_feature(self, feature: str) -> bool:
        """Check if engine supports specific feature"""
        pass
        
    @abstractmethod
    def get_cost_estimate(self, query: str) -> Dict[str, Any]:
        """Estimate cost of executing this query"""
        pass
```

**SQL Generation Strategy:**
- Build abstract syntax tree (AST) representation of queries
- Apply engine-specific transformations to the AST
- Generate optimized SQL for the target engine
- Include engine-specific hints and directives

**Warehouse-Specific Engine Implementations:**

**Snowflake Engine:**
- Connection via native API or SQLAlchemy
- Support for clustering keys, materialized views
- Warehouse sizing and auto-suspend configuration
- Leverage COPY commands for bulk loading

**BigQuery Engine:**
- Connection via Google Cloud API
- Utilize partitioning and clustering
- Support for authorized views
- Cost controls and query optimization

**Redshift Engine:**
- Connection via PostgreSQL protocol
- Distribution style optimization
- Sort key configuration
- COPY from S3 integration

**Databricks Engine:**
- Connection via JDBC or Spark API
- Delta Lake table features
- Z-ordering support
- Integration with Photon engine

**Optimization Pushdown:**
- Predicate pushdown to source connectors
- Join optimization based on engine capabilities
- Aggregation pushdown where beneficial
- Sort/limit pushdown

**Cross-Engine Execution:**
- Define criteria for engine selection
- Support data movement between engines
- Cost-based optimizer for engine selection
- Cache intermediate results when beneficial

**Feature Detection & Compatibility:**
- Runtime feature detection for engines
- Graceful fallbacks for unsupported features
- Clear error messages for incompatible operations
- Configuration options to force specific engines

**SQL Dialect Management:**
- Abstract common SQL operations
- Dialect-specific implementations
- Automated testing across dialects
- Validation before execution

**Alignment with Future Direction:**
- The `SQLEngine` interface in `technical_design_document.md` Section 5.2 provides the foundation
- The goal is to make SQLFlow warehouse-aware while maintaining its SQL-first approach
- This extends beyond the MVP but should inform early architecture decisions
- DuckDB remains the primary engine for MVP, with warehouse support as a future path

---

## Implementation Timeline

This implementation timeline outlines the phased approach for delivering SQLFlow's data loading, schema management, and full lifecycle capabilities. The plan incorporates feedback from industry architects, SME data analysts, software engineers, product managers, and data platform advocates.

### Phase 1: Core Loading Capabilities (Weeks 1-4)

**Focus: Foundational Loading Mechanisms**
- **Enhanced Load Controls (`MODE` Parameter)**
  - Implement `REPLACE` mode (default) with comprehensive validation
  - Add `APPEND` mode with schema compatibility checking
  - Develop basic `MERGE` functionality with key-based matching
  - Create robust error handling with context-rich messages

**Development Strategy:**
- Enhance parser and AST to support `MODE` parameter syntax
- Implement SQL generation strategies for each mode in DuckDB engine
- Develop validation workflows for all modes
- Build comprehensive test suite for all loading patterns
- Focus on user experience with clear, actionable error messages

**Success Metrics:**
- All three modes function correctly with various data scenarios
- Users can confidently control how data is loaded
- Error messages clearly explain validation failures
- Test coverage exceeds 90% for loading features

### Phase 2: Connector Framework & Schema Management (Weeks 5-8)

**Focus: Data Access & Schema Intelligence**
- **Core Connector Framework**
  - Finalize connector interface with consistent behavior
  - Implement robust PostgreSQL connector
  - Build S3 connector for CSV/Parquet files
  - Develop connector discovery and registration system

- **Schema Drift Detection & Alerting**
  - Implement schema representation and storage
  - Build comparison logic for detecting changes
  - Create alerting mechanism for schema drift
  - Add CLI commands for schema management

**Development Strategy:**
- Build on existing connector code ensuring consistency
- Focus on error handling and retry mechanisms
- Implement metadata storage for schemas
- Design clear user workflows for schema evolution

**Success Metrics:**
- Connectors reliably access data from PostgreSQL and S3
- Schema drift is accurately detected and reported
- Users can understand and act on schema change alerts
- Command-line interface makes schema management intuitive

### Phase 3: Execution Model & Observability (Weeks 9-12)

**Focus: Reliability & Visibility**
- **Execution Model Enhancements**
  - Implement DAG-based dependency management
  - Build concurrent execution with ThreadPoolExecutor
  - Develop failure handling policies
  - Add execution status tracking and reporting

- **Observability Foundations**
  - Add detailed metrics collection
  - Implement logging with contextual information
  - Create execution history tracking
  - Develop basic visualization of execution plans

**Development Strategy:**
- Enhance existing executor implementations
- Design for future extensibility to distributed execution
- Focus on reliability and error recovery
- Build foundations for advanced observability

**Success Metrics:**
- Pipelines execute reliably with proper dependency management
- Concurrent execution improves performance
- Failed tasks provide clear context for troubleshooting
- Users can understand pipeline execution through visualizations

### Phase 4: Refinement & Extended Features (Beyond MVP)

**Focus: User Experience & Advanced Capabilities**
- **User Experience Refinements**
  - Enhance documentation with real-world examples
  - Add templates for common loading patterns
  - Improve CLI with more informative output
  - Incorporate early user feedback

- **Advanced Features**
  - Data warehouse integration with native execution
  - Additional connector types based on user demand
  - Enhanced schema evolution capabilities
  - Integration with external orchestrators

**Development Strategy:**
- Gather and incorporate user feedback
- Prioritize features based on user needs
- Maintain SQL-first philosophy while extending capabilities
- Build toward the full data lifecycle vision

**Success Metrics:**
- User adoption and positive feedback
- Reduction in pipeline development time
- Increased data reliability in production
- Seamless integration with existing data infrastructure

## Alignment with SQLFlow Architecture

The implementation of these features builds upon SQLFlow's core architectural principles:

1. **SQL-First Approach:** All features maintain SQL as the primary paradigm while extending functionality
2. **Modularity:** Clear separation between parsing, planning, and execution components
3. **Flexibility:** Support for different execution environments via profiles
4. **Simplicity:** Intuitive syntax and clear error messages focused on user experience
5. **Performance:** Efficient data handling with Apache Arrow and parallel execution

## Risk Mitigation Strategies

1. **Connector Complexity:**
   - Start with a focused set of well-implemented connectors (PostgreSQL, S3)
   - Establish clear patterns for connector development
   - Implement thorough testing with edge cases and error conditions

2. **Schema Evolution Challenges:**
   - Begin with conservative "halt and alert" approach
   - Add permissive mode for non-breaking changes based on clear compatibility rules
   - Provide explicit user controls for schema acceptance

3. **Execution Reliability:**
   - Implement comprehensive logging for execution steps
   - Design for graceful failure handling with clear error contexts
   - Ensure resource cleanup even during failures

4. **Scaling Limitations:**
   - Document expected performance characteristics
   - Design with future distributed execution in mind
   - Provide clear guidance on workload sizing for different deployment models

By following this phased approach, SQLFlow will deliver incrementally increasing value while maintaining a focus on reliability, user experience, and alignment with the overall architectural vision.

This document provides a comprehensive view of SQLFlow's approach to data loading, schema management, execution, and data warehouse integration. It is structured to serve both product managers (with feature descriptions, benefits, and roadmaps) and software architects (with technical details, code snippets, and implementation guidance).

Each section can be referenced independently, but together they form a cohesive strategy for delivering a robust, user-friendly data pipeline platform specifically designed for the needs of SMEs.