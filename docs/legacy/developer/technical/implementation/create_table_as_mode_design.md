# SQLFlow CREATE TABLE AS MODE: Technical Design Document

## Executive Summary

This document outlines the technical design for implementing MODE parameter support in CREATE TABLE AS statements within SQLFlow. This feature extends SQLFlow's transformation capabilities by bringing the same proven MODE functionality from LOAD statements to CREATE TABLE AS statements, enabling incremental transformations, automated watermarking, and efficient data pipeline operations while maintaining SQLFlow's SQL-centric philosophy.

The implementation leverages existing LOAD mode infrastructure (REPLACE, APPEND, UPSERT) and extends it to transformation operations, providing users with consistent syntax patterns and robust incremental processing capabilities for complex SQL transformations.

## 1. Motivation & Goals

### 1.1 Business Need

Current SQLFlow CREATE TABLE AS statements only support full replacement semantics:
```sql
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders, SUM(amount) as revenue
FROM orders
GROUP BY user_id;
```

This approach forces full table reconstruction on every pipeline run, leading to:
- **Performance inefficiencies** for large transformations with small incremental updates
- **Resource waste** when reprocessing unchanged historical data  
- **Increased pipeline runtime** proportional to total data volume, not change volume
- **Inconsistent user experience** compared to LOAD statement capabilities

### 1.2 Goals

1. **Consistent User Experience**: Extend proven LOAD MODE syntax to CREATE TABLE AS
2. **Performance Optimization**: Enable incremental transformations for better resource utilization
3. **Infrastructure Reuse**: Leverage existing LOAD mode implementations and testing
4. **Progressive Enhancement**: Maintain backward compatibility while adding advanced capabilities
5. **SQL-Native Design**: Keep transformations declarative and SQL-centric

### 1.3 Success Criteria

- Users can apply REPLACE, INCREMENTAL, and UPSERT modes to CREATE TABLE AS statements
- Incremental transformations automatically manage watermarks and change detection
- Performance improves significantly for large datasets with incremental updates
- Existing CREATE TABLE AS statements continue to work without modification
- Syntax remains intuitive and follows established LOAD patterns

## 2. Technical Architecture

### 2.1 Component Overview

The implementation extends three core SQLFlow layers following the established architecture:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Parser & AST   │────▶│   Planner/DAG    │────▶│    Executor     │
│  Extensions     │     │   Extensions     │     │   Extensions    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ SQLBlockStep    │     │ Execution Step   │     │ LOAD Mode       │
│ + mode fields   │     │ + mode metadata  │     │ Handlers Reuse  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

### 2.2 Design Principles

1. **Reuse over Reinvention**: Leverage existing LOAD mode infrastructure
2. **Consistent Syntax**: Follow established LOAD MODE patterns exactly
3. **Backward Compatibility**: Existing CREATE TABLE AS statements work unchanged
4. **Performance First**: Optimize for incremental processing scenarios
5. **Extensible Foundation**: Design for future enhancements (partitioning, quality checks)

## 3. Feature Specification

### 3.1 Syntax Extensions

#### Basic Syntax
```sql
CREATE TABLE table_name AS 
SELECT ... 
[MODE mode_type [BY column_name] [UPSERT_KEYS (key1, key2, ...)]]
```

#### Supported Modes

**REPLACE Mode (Default)**
```sql
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders
FROM transactions
GROUP BY user_id
MODE REPLACE;

-- Equivalent to current behavior (backward compatible)
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders  
FROM transactions
GROUP BY user_id;
```

**INCREMENTAL Mode**
```sql
-- Auto-detect incremental column
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders, MAX(updated_at) as last_updated
FROM transactions  
GROUP BY user_id
MODE INCREMENTAL;

-- Explicit incremental column
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders, MAX(updated_at) as last_updated
FROM transactions
WHERE updated_at > '2024-01-01'
GROUP BY user_id  
MODE INCREMENTAL BY last_updated;
```

**INCREMENTAL with UPSERT_KEYS**
```sql
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders, SUM(amount) as revenue, MAX(updated_at) as last_updated
FROM transactions
WHERE updated_at > ${last_run_time}
GROUP BY user_id
MODE INCREMENTAL BY last_updated UPSERT_KEYS (user_id);
```

### 3.2 Behavioral Semantics

#### REPLACE Mode
- **First Run**: `CREATE TABLE table_name AS SELECT ...`
- **Subsequent Runs**: `CREATE OR REPLACE TABLE table_name AS SELECT ...`
- **Use Cases**: Full refreshes, small datasets, development environments

#### INCREMENTAL Mode (Append-Only)
- **First Run**: `CREATE TABLE table_name AS SELECT ...`
- **Subsequent Runs**: `INSERT INTO table_name SELECT ... WHERE incremental_column > last_watermark`
- **Watermark Management**: Automatic tracking of maximum incremental column value
- **Use Cases**: Event logs, time-series data, append-only datasets

#### INCREMENTAL Mode with UPSERT_KEYS
- **First Run**: `CREATE TABLE table_name AS SELECT ...`
- **Subsequent Runs**: Execute UPSERT operation using existing LOAD UPSERT logic
- **Behavior**: Update existing records + insert new records based on upsert keys
- **Use Cases**: Dimension tables, aggregated metrics, slowly changing dimensions

### 3.3 Watermark Management

Incremental modes automatically manage watermarks using SQLFlow's existing infrastructure:

```sql
-- Pipeline run 1: Creates table + stores watermark
CREATE TABLE daily_metrics AS
SELECT date, user_id, SUM(amount) as revenue
FROM transactions  
WHERE date >= '2024-01-01'
GROUP BY date, user_id
MODE INCREMENTAL BY date;

-- Pipeline run 2: Automatically uses watermark
-- Translates to: WHERE date > '2024-01-31' (last processed date)
CREATE TABLE daily_metrics AS
SELECT date, user_id, SUM(amount) as revenue
FROM transactions
WHERE date >= '2024-01-01'  -- Original filter preserved
GROUP BY date, user_id
MODE INCREMENTAL BY date;
```

## 4. Implementation Design

### 4.1 Parser Layer Extensions

#### AST Changes
```python
# sqlflow/parser/ast.py
@dataclass  
class SQLBlockStep(PipelineStep):
    table_name: str
    sql_query: str
    # NEW FIELDS
    mode: Optional[str] = None                    # "REPLACE", "INCREMENTAL"
    incremental_by: Optional[str] = None          # Column name for incremental processing
    upsert_keys: Optional[List[str]] = None        # Upsert keys for INCREMENTAL+UPSERT
    line_number: Optional[int] = None

    def validate(self) -> List[str]:
        """Enhanced validation for mode-specific logic."""
        errors = []
        if not self.table_name:
            errors.append("CREATE TABLE AS requires a table name")
        if not self.sql_query:
            errors.append("CREATE TABLE AS requires a SQL query")
            
        # Mode-specific validation
        if self.mode == "INCREMENTAL":
            if self.upsert_keys and not self.incremental_by:
                errors.append("INCREMENTAL with UPSERT_KEYS requires BY <column>")
                
        return errors
```

#### Lexer Token Extensions
```python
# sqlflow/parser/lexer.py - Add new tokens
class TokenType(Enum):
    # ... existing tokens ...
    MODE = auto()            # Already exists
    INCREMENTAL = auto()     # NEW
    BY = auto()             # NEW
    UPSERT_KEYS = auto()     # Already exists
```

#### Parser Method Extensions
```python
# sqlflow/parser/parser.py
def _parse_sql_block_statement(self) -> SQLBlockStep:
    """Parse CREATE TABLE AS statement with optional MODE clause."""
    create_token = self._consume(TokenType.CREATE, "Expected 'CREATE'")
    self._consume(TokenType.TABLE, "Expected 'TABLE' after 'CREATE'")
    table_name_token = self._consume(TokenType.IDENTIFIER, "Expected table name")
    self._consume(TokenType.AS, "Expected 'AS' after table name")
    
    # Parse SQL query tokens
    sql_query = self._parse_sql_query_tokens()
    
    # NEW: Parse optional MODE clause
    mode = None
    incremental_by = None  
    upsert_keys = []
    
    if self._check(TokenType.MODE):
        mode, incremental_by, upsert_keys = self._parse_create_table_mode()
    
    self._consume(TokenType.SEMICOLON, "Expected ';' after statement")
    
    return SQLBlockStep(
        table_name=table_name_token.value,
        sql_query=sql_query,
        mode=mode,
        incremental_by=incremental_by,
        upsert_keys=upsert_keys,
        line_number=create_token.line,
    )

def _parse_create_table_mode(self) -> tuple[str, Optional[str], List[str]]:
    """Parse MODE clause for CREATE TABLE AS statements."""
    self._advance()  # Consume MODE token
    
    if self._check(TokenType.REPLACE):
        self._advance()
        return "REPLACE", None, []
        
    elif self._check(TokenType.INCREMENTAL):
        self._advance()
        incremental_by = None
        upsert_keys = []
        
        # Optional BY clause
        if self._check(TokenType.BY):
            self._advance()
            column_token = self._consume(TokenType.IDENTIFIER, "Expected column name after BY")
            incremental_by = column_token.value
        
        # Optional UPSERT_KEYS clause
        if self._check(TokenType.UPSERT_KEYS):
            upsert_keys = self._parse_upsert_keys()  # Reuse existing method
            
        return "INCREMENTAL", incremental_by, upsert_keys
    else:
        token = self._peek()
        raise ParserError(
            "Expected 'REPLACE' or 'INCREMENTAL' after 'MODE'",
            token.line, token.column
        )
```

### 4.2 Planning Layer Extensions

#### Execution Step Enhancement
```python
# sqlflow/core/planner.py
def _build_sql_block_step(
    self, step: SQLBlockStep, step_id: str, depends_on: List[str]
) -> Dict[str, Any]:
    """Build execution step for SQL block with MODE support."""
    return {
        "id": step_id,
        "type": "transform",
        "name": step.table_name,
        "query": step.sql_query,
        "depends_on": depends_on,
        # NEW: Mode metadata
        "mode": step.mode,
        "incremental_by": step.incremental_by,
        "upsert_keys": step.upsert_keys or [],
    }
```

### 4.3 Execution Layer Extensions

#### SQL Generator Enhancement
```python
# sqlflow/core/sql_generator.py
def _generate_transform_sql(
    self, operation: Dict[str, Any], context: Dict[str, Any]
) -> str:
    """Generate SQL for transformation with MODE support."""
    table_name = operation.get("name", "")
    base_query = operation.get("query", "")
    mode = operation.get("mode")
    incremental_by = operation.get("incremental_by")
    upsert_keys = operation.get("upsert_keys", [])
    
    # Default behavior (backward compatible)
    if not mode or mode == "REPLACE":
        return f"CREATE OR REPLACE TABLE {table_name} AS\n{base_query};"
    
    # Incremental processing
    elif mode == "INCREMENTAL":
        return self._generate_incremental_transform_sql(
            table_name, base_query, incremental_by, upsert_keys, context
        )
    
    else:
        logger.warning(f"Unknown mode: {mode}, using REPLACE")
        return f"CREATE OR REPLACE TABLE {table_name} AS\n{base_query};"

def _generate_incremental_transform_sql(
    self, table_name: str, base_query: str, incremental_by: Optional[str],
    upsert_keys: List[str], context: Dict[str, Any]
) -> str:
    """Generate SQL for incremental CREATE TABLE AS."""
    
    table_exists = context.get("table_exists", False)
    
    if not table_exists:
        # First run: Create table normally
        return f"CREATE TABLE {table_name} AS\n{base_query};"
    
    if upsert_keys:
        # Incremental with updates: Use UPSERT strategy via temp table
        return self._generate_incremental_upsert_sql(
            table_name, base_query, incremental_by, upsert_keys, context
        )
    else:
        # Incremental append: Use INSERT strategy
        return self._generate_incremental_append_sql(
            table_name, base_query, incremental_by, context
        )

def _generate_incremental_upsert_sql(
    self, table_name: str, base_query: str, incremental_by: Optional[str],
    upsert_keys: List[str], context: Dict[str, Any]
) -> str:
    """Generate UPSERT-based incremental SQL by leveraging LOAD infrastructure."""
    
    # Create temporary view with query results
    temp_view = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
    
    # Apply incremental filter if specified
    filtered_query = self._apply_incremental_filter(
        base_query, incremental_by, context
    )
    
    # Reuse existing LOAD UPSERT SQL generation
    sql_generator = SQLGenerator()
    upsert_sql = sql_generator.generate_upsert_sql(
        table_name=table_name,
        source_name=temp_view,
        upsert_keys=upsert_keys,
        source_schema=context.get("source_schema", {})
    )
    
    return f"""
-- Create temporary view with incremental query results
CREATE TEMPORARY VIEW {temp_view} AS
{filtered_query};

{upsert_sql}

-- Cleanup
DROP VIEW {temp_view};
""".strip()
```

#### LocalExecutor Enhancement
```python
# sqlflow/core/executors/local_executor.py
def _execute_transform(self, step: Dict[str, Any]) -> Dict[str, Any]:
    """Execute transform step with MODE support."""
    try:
        table_name = step.get("name", "")
        mode = step.get("mode")
        
        # Gather execution context
        context = self._build_transform_context(step)
        
        # Generate MODE-aware SQL
        sql_generator = SQLGenerator()
        full_sql = sql_generator.generate_operation_sql(step, context)
        
        # Execute the generated SQL
        self.duckdb_engine.execute_query(full_sql)
        
        # Update watermarks for incremental modes
        if mode == "INCREMENTAL":
            self._update_incremental_watermark(step, context)
        
        logger.info(f"Transform completed: {table_name} (mode: {mode or 'REPLACE'})")
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"Transform failed: {e}")
        return {"status": "error", "message": str(e)}

def _build_transform_context(self, step: Dict[str, Any]) -> Dict[str, Any]:
    """Build execution context for transform operations."""
    table_name = step.get("name", "")
    mode = step.get("mode")
    incremental_by = step.get("incremental_by")
    
    context = {
        "table_exists": False,
        "source_schema": {},
        "last_watermark": None,
    }
    
    if mode == "INCREMENTAL" and self.duckdb_engine:
        # Check if target table exists
        context["table_exists"] = self.duckdb_engine.table_exists(table_name)
        
        # Get last watermark for incremental processing
        if incremental_by:
            context["last_watermark"] = self._get_watermark(table_name, incremental_by)
    
    return context
```

## 5. Infrastructure Reuse Strategy

### 5.1 LOAD Mode Handler Integration

The implementation maximizes reuse of existing LOAD mode infrastructure:

```python
# sqlflow/core/engines/duckdb/transform/handlers.py - NEW MODULE
class CreateTableModeHandler:
    """Handles CREATE TABLE AS with different modes by leveraging LOAD infrastructure."""
    
    def __init__(self, engine: "DuckDBEngine"):
        self.engine = engine
        self.load_factory = LoadModeHandlerFactory()  # Reuse existing factory
    
    def execute_incremental_upsert(
        self, table_name: str, query: str, upsert_keys: List[str]
    ) -> str:
        """Execute incremental CREATE TABLE AS with UPSERT semantics."""
        
        # Create temporary view with query results
        temp_view = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_view_sql = f"CREATE TEMPORARY VIEW {temp_view} AS {query};"
        
        # Create synthetic LoadStep for existing UPSERT handler
        load_step = LoadStep(
            table_name=table_name,
            source_name=temp_view, 
            mode="UPSERT",
            upsert_keys=upsert_keys
        )
        
        # Use existing UPSERT handler
        upsert_handler = self.load_factory.create("UPSERT", self.engine)
        upsert_sql = upsert_handler.generate_sql(load_step)
        
        return f"{create_view_sql}\n{upsert_sql}"
```

### 5.2 Watermark Management Reuse

Incremental modes leverage existing watermark infrastructure from LOAD operations:

```python
# Reuse existing WatermarkManager
def _update_incremental_watermark(self, step: Dict[str, Any], context: Dict[str, Any]):
    """Update watermark after successful incremental transform."""
    table_name = step.get("name", "")
    incremental_by = step.get("incremental_by")
    
    if not incremental_by:
        return
    
    # Use existing watermark manager
    watermark_manager = WatermarkManager(self.state_backend)
    
    # Get max value of incremental column from result
    max_value_query = f"SELECT MAX({incremental_by}) FROM {table_name}"
    result = self.duckdb_engine.execute_query(max_value_query)
    max_value = result.fetchone()[0]
    
    if max_value:
        watermark_manager.update_watermark(
            self.pipeline_name, table_name, max_value
        )
```

## 6. Testing Strategy

### 6.1 Unit Testing

#### Parser Tests
```python
# tests/unit/parser/test_create_table_mode.py
def test_create_table_with_replace_mode():
    """Test CREATE TABLE AS with explicit REPLACE mode."""
    parser = Parser("""
        CREATE TABLE users AS 
        SELECT * FROM source_users 
        MODE REPLACE;
    """)
    pipeline = parser.parse()
    
    assert len(pipeline.steps) == 1
    step = pipeline.steps[0]
    assert isinstance(step, SQLBlockStep)
    assert step.mode == "REPLACE"
    assert step.incremental_by is None
    assert step.upsert_keys == []

def test_create_table_with_incremental_mode():
    """Test CREATE TABLE AS with INCREMENTAL mode."""
    parser = Parser("""
        CREATE TABLE user_metrics AS
        SELECT user_id, COUNT(*) as orders
        FROM transactions
        GROUP BY user_id
        MODE INCREMENTAL BY last_updated UPSERT_KEYS (user_id);
    """)
    pipeline = parser.parse()
    
    step = pipeline.steps[0]
    assert step.mode == "INCREMENTAL" 
    assert step.incremental_by == "last_updated"
    assert step.upsert_keys == ["user_id"]
```

#### SQL Generation Tests
```python
# tests/unit/core/test_transform_sql_generation.py
def test_incremental_upsert_sql_generation():
    """Test SQL generation for incremental CREATE TABLE AS with UPSERT."""
    operation = {
        "name": "user_metrics",
        "query": "SELECT user_id, COUNT(*) FROM transactions GROUP BY user_id",
        "mode": "INCREMENTAL",
        "incremental_by": "updated_at",
        "upsert_keys": ["user_id"]
    }
    
    context = {
        "table_exists": True,
        "last_watermark": "2024-01-01",
        "source_schema": {"user_id": "INTEGER", "count": "INTEGER"}
    }
    
    generator = SQLGenerator()
    sql = generator._generate_transform_sql(operation, context)
    
    assert "CREATE TEMPORARY VIEW temp_" in sql
    assert "UPSERT INTO user_metrics" in sql
    assert "UPDATE SET" in sql
    assert "INSERT" in sql
```

### 6.2 Integration Testing

#### End-to-End Pipeline Tests
```python
# tests/integration/test_create_table_mode_integration.py
def test_incremental_transform_pipeline():
    """Test complete incremental transform pipeline execution."""
    
    # Setup initial data
    executor = LocalExecutor()
    executor.duckdb_engine.execute_query("""
        CREATE TABLE transactions AS
        SELECT 1 as user_id, 100 as amount, '2024-01-01' as updated_at
        UNION
        SELECT 2 as user_id, 200 as amount, '2024-01-01' as updated_at
    """)
    
    # First run: Create metrics table
    pipeline_content = """
        CREATE TABLE user_metrics AS
        SELECT user_id, SUM(amount) as total_amount, MAX(updated_at) as last_updated
        FROM transactions
        GROUP BY user_id
        MODE INCREMENTAL BY last_updated UPSERT_KEYS (user_id);
    """
    
    result1 = executor.execute_pipeline_content(pipeline_content)
    assert result1["status"] == "success"
    
    # Verify initial data
    metrics = executor.duckdb_engine.execute_query("SELECT * FROM user_metrics").fetchdf()
    assert len(metrics) == 2
    
    # Add incremental data
    executor.duckdb_engine.execute_query("""
        INSERT INTO transactions VALUES 
        (1, 150, '2024-01-02'),  -- Update existing user
        (3, 300, '2024-01-02')   -- New user
    """)
    
    # Second run: Process incremental changes
    result2 = executor.execute_pipeline_content(pipeline_content)
    assert result2["status"] == "success"
    
    # Verify incremental processing
    metrics = executor.duckdb_engine.execute_query("SELECT * FROM user_metrics ORDER BY user_id").fetchdf()
    assert len(metrics) == 3  # Should have 3 users now
    assert metrics[metrics["user_id"] == 1]["total_amount"].iloc[0] == 250  # Updated
    assert metrics[metrics["user_id"] == 3]["total_amount"].iloc[0] == 300  # New
```

### 6.3 Performance Testing

```python
# tests/performance/test_incremental_performance.py
def test_incremental_vs_full_refresh_performance():
    """Compare performance of incremental vs full refresh for large datasets."""
    
    # Setup large dataset (1M records)
    setup_large_dataset(executor, num_records=1_000_000)
    
    # Measure full refresh time
    start_time = time.time()
    execute_full_refresh_pipeline(executor)
    full_refresh_time = time.time() - start_time
    
    # Add small incremental change (1K records)
    add_incremental_data(executor, num_records=1_000)
    
    # Measure incremental processing time
    start_time = time.time()
    execute_incremental_pipeline(executor)
    incremental_time = time.time() - start_time
    
    # Incremental should be significantly faster
    assert incremental_time < full_refresh_time * 0.1  # At least 10x faster
```

## 7. Migration & Compatibility

### 7.1 Backward Compatibility

**Existing pipelines work unchanged:**
```sql
-- This continues to work exactly as before
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders
FROM transactions
GROUP BY user_id;
```

**Explicit migration path:**
```sql
-- Step 1: Add MODE REPLACE (equivalent behavior)
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders
FROM transactions
GROUP BY user_id
MODE REPLACE;

-- Step 2: Migrate to incremental when ready
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders, MAX(updated_at) as last_updated
FROM transactions
WHERE updated_at > '2024-01-01'  -- Add appropriate filter
GROUP BY user_id
MODE INCREMENTAL BY last_updated UPSERT_KEYS (user_id);
```

### 7.2 Error Handling & Validation

The implementation provides clear error messages for common mistakes:

```sql
-- Error: INCREMENTAL with UPSERT_KEYS requires BY clause
CREATE TABLE metrics AS SELECT * FROM source
MODE INCREMENTAL UPSERT_KEYS (id);  -- ❌ Missing BY column

-- Error: BY column must exist in SELECT clause  
CREATE TABLE metrics AS SELECT user_id, COUNT(*) FROM source
MODE INCREMENTAL BY updated_at;  -- ❌ updated_at not in SELECT

-- Error: UPSERT_KEYS must exist in SELECT clause
CREATE TABLE metrics AS SELECT COUNT(*) FROM source  
MODE INCREMENTAL BY date UPSERT_KEYS (user_id);  -- ❌ user_id not in SELECT
```

## 8. Documentation & Examples

### 8.1 User Documentation Updates

#### Reference Documentation
Update `docs/user/reference/sql_syntax.md`:

```markdown
## CREATE TABLE AS Statement

### Syntax
```sql
CREATE TABLE table_name AS
SELECT ...
[MODE mode_type [BY column_name] [UPSERT_KEYS (key1, key2, ...)]]
```

### Modes

**REPLACE (Default)**
- Replaces the entire table on each run
- Best for: Small datasets, development environments, full refreshes

**INCREMENTAL**  
- Processes only new/changed data based on watermark column
- Best for: Large datasets, time-series data, event logs

**INCREMENTAL with UPSERT_KEYS**
- Updates existing records and inserts new ones
- Best for: Dimension tables, aggregated metrics, slowly changing data
```

#### Tutorial Documentation
Create `docs/user/tutorials/incremental_transformations.md`:

```markdown
# Building Incremental Transformations

This tutorial shows how to build efficient data transformations that process only changed data.

## Basic Incremental Processing

Start with append-only incremental processing for event data:

```sql
CREATE TABLE daily_metrics AS
SELECT 
    DATE(order_date) as date,
    COUNT(*) as order_count,
    SUM(amount) as revenue
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY DATE(order_date)
MODE INCREMENTAL BY date;
```

## Advanced: Updates with UPSERT_KEYS

For data that can change, use UPSERT_KEYS to handle updates:

```sql
CREATE TABLE customer_lifetime_value AS
SELECT 
    customer_id,
    SUM(amount) as total_spent,
    COUNT(*) as order_count,
    MAX(order_date) as last_order_date
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id
MODE INCREMENTAL BY last_order_date UPSERT_KEYS (customer_id);
```
```

### 8.2 Example Pipelines

Create `examples/incremental_transformations/` with practical examples:

```sql
-- examples/incremental_transformations/time_series_analytics.sf
-- Time-series analytics with incremental processing

SOURCE events TYPE CSV PARAMS {
    "file_path": "data/events_${date}.csv",
    "has_header": true
};

LOAD events_raw FROM events MODE APPEND;

-- Incremental daily aggregation
CREATE TABLE daily_user_metrics AS
SELECT 
    DATE(event_timestamp) as date,
    user_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT session_id) as session_count,
    MAX(event_timestamp) as last_event_time
FROM events_raw
WHERE DATE(event_timestamp) >= '2024-01-01'
GROUP BY DATE(event_timestamp), user_id  
MODE INCREMENTAL BY date;

-- Incremental user lifetime metrics (with updates)
CREATE TABLE user_lifetime_metrics AS
SELECT 
    user_id,
    COUNT(*) as total_events,
    COUNT(DISTINCT DATE(event_timestamp)) as active_days,
    MIN(event_timestamp) as first_seen,
    MAX(event_timestamp) as last_seen
FROM events_raw
WHERE event_timestamp >= '2024-01-01'
GROUP BY user_id
MODE INCREMENTAL BY last_seen UPSERT_KEYS (user_id);
```

## 9. Future Enhancements

### 9.1 Phase 2: Advanced Features

**Partitioning Support**
```sql
CREATE TABLE sales_metrics AS
SELECT region, product_id, SUM(amount) as revenue
FROM sales
GROUP BY region, product_id
MODE INCREMENTAL BY updated_at 
     PARTITION BY region;
```

**Quality Checks**
```sql
CREATE TABLE user_metrics AS
SELECT user_id, COUNT(*) as orders
FROM transactions  
GROUP BY user_id
MODE INCREMENTAL BY updated_at
     QUALITY CHECK (
         COUNT(*) > 1000,
         AVG(orders) > 5
     );
```

**Scheduling Integration**
```sql
CREATE TABLE daily_reports AS
SELECT * FROM complex_metrics
MODE INCREMENTAL BY date
     SCHEDULE DAILY AT '06:00';
```

### 9.2 Phase 3: Performance Optimizations

- **Automatic indexing** on upsert keys and incremental columns
- **Parallel processing** for large incremental operations  
- **Compression optimization** for incremental tables
- **Cost-based optimization** for incremental vs full refresh decisions

## 10. Rollout Plan

### Week 1: Foundation
- [ ] Extend SQLBlockStep AST with mode fields
- [ ] Add new lexer tokens (INCREMENTAL, BY)
- [ ] Implement parser extensions
- [ ] Unit tests for parsing logic

### Week 2: Planning Integration  
- [ ] Extend planner to pass mode metadata
- [ ] Update execution step structure
- [ ] Integration tests with planning layer

### Week 3: Execution Implementation
- [ ] Extend SQL generator with mode support
- [ ] Update LocalExecutor for mode handling
- [ ] Integrate with existing LOAD mode handlers
- [ ] Watermark management integration

### Week 4: Testing & Documentation
- [ ] Comprehensive integration testing
- [ ] Performance benchmarking
- [ ] User documentation updates
- [ ] Example pipeline creation

### Week 5: Polish & Release
- [ ] Error handling improvements
- [ ] CLI help text updates  
- [ ] Release notes preparation
- [ ] Community feedback incorporation

## 11. Success Metrics

### Technical Metrics
- **Performance**: 10x+ improvement for incremental scenarios with <5% data changes
- **Compatibility**: 100% backward compatibility for existing CREATE TABLE AS statements
- **Code Reuse**: >90% reuse of existing LOAD mode infrastructure
- **Test Coverage**: >95% coverage for new functionality

### User Experience Metrics  
- **Adoption**: Usage of MODE parameter in 50%+ of new CREATE TABLE AS statements
- **Error Rate**: <1% syntax errors in MODE usage after documentation
- **Performance Satisfaction**: User-reported performance improvements in incremental scenarios

This design provides a robust, performant, and user-friendly extension to SQLFlow's transformation capabilities while maintaining the simplicity and SQL-centric approach that makes SQLFlow unique in the data pipeline ecosystem. 