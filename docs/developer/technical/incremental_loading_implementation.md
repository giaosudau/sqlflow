# SQLFlow Incremental Loading: Technical Implementation Guide

## Executive Summary

This document outlines the technical approach for implementing incremental loading capabilities in SQLFlow, designed to optimize pipeline performance and resource utilization when processing data that changes over time. The implementation follows a phased approach focusing on immediate value delivery while maintaining SQLFlow's SQL-native philosophy.

## 1. Architecture Overview

### 1.1 Core Components

The incremental loading functionality will interact with several existing SQLFlow components:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Parser & AST   │────▶│ Connectors Layer │────▶│  SQLFlow Engine │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   DSL Syntax    │     │    State Mgmt    │     │  CRUD Operations│
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

### 1.2 Phased Implementation

The implementation will follow three phases, each building upon the previous:

1. **Phase 1**: Enhanced Load Controls (MODE options)
2. **Phase 2**: Source-Level Incremental Loading (INCREMENTAL BY)
3. **Phase 3**: User-Guided Incremental Transformations

## 2. Phase 1: Enhanced Load Controls

### 2.1 Parser Extensions

Extend the parser to support the following syntax:

```sql
LOAD <table_name> FROM <source_name> [MODE <mode>] [ON <key_columns>];
```

Where:
- `<mode>` can be one of: `REPLACE` (default), `APPEND`, or `MERGE`
- `<key_columns>` is required when `mode` is `MERGE` and is a comma-separated list of column names

### 2.2 LoadStep Class Extension

```python
@dataclass
class LoadStep(PipelineStep):
    """Represents a LOAD directive with incremental capabilities."""
    
    table_name: str
    source_name: str
    mode: Optional[str] = "REPLACE"  # Default to REPLACE
    merge_keys: Optional[List[str]] = None
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the LOAD directive with new mode options."""
        errors = []
        # Existing validation logic
        if not self.table_name:
            errors.append("LOAD directive requires a table name")
        if not self.source_name:
            errors.append("LOAD directive requires a source name")
            
        # New validation logic
        if self.mode and self.mode not in ["REPLACE", "APPEND", "MERGE"]:
            errors.append(f"Invalid LOAD mode: {self.mode}. Must be one of: REPLACE, APPEND, MERGE")
        if self.mode == "MERGE" and not self.merge_keys:
            errors.append("MERGE mode requires ON <key_columns> to be specified")
        
        return errors
```

### 2.3 Parser Implementation

Extend the `_parse_load_statement()` method in the Parser class:

```python
def _parse_load_statement(self) -> LoadStep:
    """Parse a LOAD statement with optional MODE parameter."""
    # Existing implementation for basic LOAD statement
    # ...
    
    # Check for MODE keyword
    mode = "REPLACE"  # Default mode
    merge_keys = None
    
    if self._match(TokenType.MODE):
        mode_token = self._consume(TokenType.IDENTIFIER, "Expected mode type after MODE")
        mode = mode_token.value.upper()
        
        # Handle MERGE mode with key columns
        if mode == "MERGE":
            self._consume(TokenType.ON, "Expected ON after MERGE mode")
            
            # Parse comma-separated list of key columns
            key_columns = []
            while True:
                key_token = self._consume(TokenType.IDENTIFIER, "Expected key column identifier")
                key_columns.append(key_token.value)
                
                if not self._check(TokenType.COMMA):
                    break
                    
                self._consume(TokenType.COMMA, "Expected comma between key columns")
            
            merge_keys = key_columns
    
    self._consume(TokenType.SEMICOLON, "Expected ';' after LOAD statement")
    
    return LoadStep(
        table_name=table_name_token.value,
        source_name=source_name_token.value,
        mode=mode,
        merge_keys=merge_keys,
        line_number=load_token.line,
    )
```

### 2.4 Connector Interface Updates

Extend the `write()` method in the `BidirectionalConnector` and `ExportConnector` classes to support the different modes:

```python
@abstractmethod
def write(
    self, 
    object_name: str, 
    data_chunk: DataChunk, 
    mode: str = "append",
    merge_keys: Optional[List[str]] = None
) -> None:
    """Write data to the destination.
    
    Args:
        object_name: Name of the object to write to
        data_chunk: Data to write
        mode: Write mode (append, replace, merge)
        merge_keys: Key columns to use for merging (required when mode="merge")
        
    Raises:
        ConnectorError: If writing fails
    """
```

### 2.5 SQL Generation

Expand the `_generate_load_sql()` method in `SQLGenerator`:

```python
def _generate_load_sql(
    self, operation: Dict[str, Any], context: Dict[str, Any]
) -> str:
    """Generate SQL for a load operation with different modes.
    
    Args:
        operation: Load operation definition
        context: Execution context
        
    Returns:
        SQL for the load operation
    """
    query = operation.get("query", {})
    source_name = query.get("source_name", "")
    table_name = query.get("table_name", "")
    mode = query.get("mode", "REPLACE").upper()
    merge_keys = query.get("merge_keys", [])
    
    if mode == "REPLACE":
        return f"""-- Load operation (REPLACE mode)
CREATE OR REPLACE TABLE {table_name} AS
SELECT * FROM {source_name};"""
    
    elif mode == "APPEND":
        return f"""-- Load operation (APPEND mode)
INSERT INTO {table_name}
SELECT * FROM {source_name};"""
    
    elif mode == "MERGE":
        # Generate list of key columns for ON clause
        key_conditions = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        # Generate list of columns for UPDATE clause
        source_schema = self._get_schema(context, source_name)
        columns = [c for c in source_schema.keys() if c not in merge_keys]
        updates = ", ".join([f"target.{c} = source.{c}" for c in columns])
        
        # Generate column list for INSERT clause
        column_list = ", ".join(source_schema.keys())
        source_columns = ", ".join([f"source.{c}" for c in source_schema.keys()])
        
        return f"""-- Load operation (MERGE mode)
MERGE INTO {table_name} target
USING {source_name} source
ON {key_conditions}
WHEN MATCHED THEN
  UPDATE SET {updates}
WHEN NOT MATCHED THEN
  INSERT ({column_list})
  VALUES ({source_columns});"""
```

### 2.6 Execution Metrics

Enhance the `LocalExecutor` to capture and report operation metrics:

```python
def _execute_load_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a load operation and capture metrics.
    
    Args:
        operation: Load operation definition
        
    Returns:
        Dictionary with execution metrics
    """
    start_time = time.time()
    
    # Get source and target tables
    source_name = operation["query"]["source_name"]
    table_name = operation["query"]["table_name"]
    mode = operation.get("mode", "REPLACE")
    
    # Get row counts before operation
    source_count = self._get_row_count(source_name)
    target_before_count = 0
    if mode in ["APPEND", "MERGE"] and self.duckdb_engine.table_exists(table_name):
        target_before_count = self._get_row_count(table_name)
    
    # Execute the operation
    result = super()._execute_load_operation(operation)
    
    # Calculate metrics
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    
    target_after_count = self._get_row_count(table_name)
    
    metrics = {
        "source_records": source_count,
        "execution_time_seconds": execution_time
    }
    
    if mode == "REPLACE":
        metrics["records_written"] = target_after_count
    elif mode == "APPEND":
        metrics["records_written"] = target_after_count - target_before_count
    elif mode == "MERGE":
        # For merge, we'd need more detailed tracking in the SQL to get updated vs inserted
        metrics["total_records_affected"] = target_after_count - target_before_count
    
    # Store metrics in operation result
    result["metrics"] = metrics
    
    return result
```

### 2.7 User Feedback

Enhance the CLI output to show metrics for incremental operations:

```python
def _format_load_metrics(metrics: Dict[str, Any], mode: str) -> str:
    """Format load operation metrics for display.
    
    Args:
        metrics: Metrics from the load operation
        mode: The mode used (REPLACE, APPEND, MERGE)
        
    Returns:
        Formatted string with metrics
    """
    output = []
    
    if mode == "REPLACE":
        output.append(f"✓ Replaced table with {metrics['records_written']} records")
    elif mode == "APPEND":
        output.append(f"✓ Appended {metrics['records_written']} records")
    elif mode == "MERGE":
        output.append(f"✓ Merged {metrics['source_records']} records")
        output.append(f"  → {metrics.get('records_updated', 'unknown')} records updated")
        output.append(f"  → {metrics.get('records_inserted', 'unknown')} records inserted")
    
    output.append(f"  → Completed in {metrics['execution_time_seconds']}s")
    
    return "\n".join(output)
```

## 3. Phase 2: Source-Level Incremental Loading

### 3.1 State Management System

Create a `WatermarkManager` class to track and manage watermarks:

```python
class WatermarkManager:
    """Manages watermarks for incremental source loading."""
    
    def __init__(self, state_backend: DuckDBStateBackend):
        """Initialize the watermark manager.
        
        Args:
            state_backend: The state backend for persistence
        """
        self.state_backend = state_backend
    
    def get_watermark(self, pipeline_name: str, source_name: str) -> Optional[Any]:
        """Get the current watermark for a source.
        
        Args:
            pipeline_name: Name of the pipeline
            source_name: Name of the source
            
        Returns:
            The current watermark value or None if not set
        """
        # Implementation using state_backend
    
    def update_watermark(self, pipeline_name: str, source_name: str, value: Any) -> None:
        """Update the watermark for a source.
        
        Args:
            pipeline_name: Name of the pipeline
            source_name: Name of the source
            value: New watermark value
        """
        # Implementation using state_backend
```

### 3.2 Extend Source Definition

Update the `SourceDefinitionStep` to support incremental loading:

```python
@dataclass
class SourceDefinitionStep(PipelineStep):
    """Represents a SOURCE directive in the pipeline with incremental support."""
    
    name: str
    connector_type: str
    params: Dict[str, Any]
    incremental_column: Optional[str] = None
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the SOURCE directive with incremental loading."""
        errors = []
        # Existing validation logic
        # ...
        
        # Validate incremental configuration if specified
        if self.incremental_column and not isinstance(self.incremental_column, str):
            errors.append("INCREMENTAL BY requires a column name")
        
        return errors
```

### 3.3 Parser Extensions for Incremental Source

Extend the `_parse_source_statement()` method:

```python
def _parse_source_statement(self) -> SourceDefinitionStep:
    """Parse a SOURCE statement with optional INCREMENTAL BY parameter."""
    # Existing implementation for basic SOURCE statement
    # ...
    
    # Check for INCREMENTAL BY clause
    incremental_column = None
    if self._match(TokenType.INCREMENTAL):
        self._consume(TokenType.BY, "Expected 'BY' after 'INCREMENTAL'")
        column_token = self._consume(
            TokenType.IDENTIFIER, "Expected column name after 'INCREMENTAL BY'"
        )
        incremental_column = column_token.value
    
    self._consume(TokenType.SEMICOLON, "Expected ';' after SOURCE statement")
    
    return SourceDefinitionStep(
        name=name_token.value,
        connector_type=type_token.value,
        params=params,
        incremental_column=incremental_column,
        line_number=source_token.line,
    )
```

### 3.4 Connector Interface for Incremental Loading

Extend the `Connector` base class to support incremental loading:

```python
@abstractmethod
def read_incremental(
    self,
    object_name: str,
    column: str,
    last_value: Any,
    columns: Optional[List[str]] = None,
    batch_size: int = 10000
) -> Iterator[DataChunk]:
    """Read data incrementally based on a watermark column.
    
    Args:
        object_name: Name of the object to read
        column: Name of the incremental column
        last_value: Last processed value of the incremental column
        columns: Optional list of columns to read
        batch_size: Number of rows per batch
        
    Returns:
        Iterator yielding DataChunk objects with new/changed data
        
    Raises:
        ConnectorError: If reading fails
    """
```

### 3.5 Planner Integration for Incremental Sources

Extend the `_plan_source_operation()` method in the `Planner` class:

```python
def _plan_source_operation(self, source_step: SourceDefinitionStep) -> Dict[str, Any]:
    """Plan a source operation with incremental support.
    
    Args:
        source_step: The source step with potential incremental configuration
        
    Returns:
        Planned operation dictionary
    """
    operation = {
        "id": f"source_{source_step.name}",
        "type": "source_definition",
        "source_name": source_step.name,
        "source_connector_type": source_step.connector_type,
        "query": source_step.params,
    }
    
    # Add incremental configuration if specified
    if source_step.incremental_column:
        operation["incremental"] = {
            "column": source_step.incremental_column,
        }
    
    return operation
```

### 3.6 Execution Logic for Incremental Sources

Extend the `LocalExecutor` to handle incremental sources:

```python
def _execute_source_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a source operation with incremental support.
    
    Args:
        operation: The source operation to execute
        
    Returns:
        Result of the execution
    """
    start_time = time.time()
    
    # Get source configuration
    source_name = operation["source_name"]
    connector_type = operation["source_connector_type"]
    query = operation["query"]
    
    # Check if this is an incremental source
    incremental_config = operation.get("incremental")
    if incremental_config:
        # Get watermark manager
        watermark_manager = WatermarkManager(self.state_backend)
        
        # Get current watermark
        last_value = watermark_manager.get_watermark(
            self.pipeline_name, source_name
        )
        
        # Get connector and read incrementally
        connector = self._get_connector(connector_type)
        connector.configure(query)
        
        if last_value is not None:
            # Read incrementally
            data_chunks = connector.read_incremental(
                query.get("table", ""),
                incremental_config["column"],
                last_value
            )
        else:
            # First run, read all data
            data_chunks = connector.read(query.get("table", ""))
        
        # Get the max value of the incremental column to update watermark
        if data_chunks:
            # Combine all chunks to find max value
            # This is simplified; in practice, you'd track this while processing chunks
            all_data = pd.concat([chunk.to_pandas() for chunk in data_chunks])
            if not all_data.empty:
                max_value = all_data[incremental_config["column"]].max()
                
                # Update watermark
                watermark_manager.update_watermark(
                    self.pipeline_name, source_name, max_value
                )
    else:
        # Regular non-incremental source processing
        # Existing implementation
        # ...
    
    end_time = time.time()
    
    return {
        "status": "success",
        "execution_time": end_time - start_time
    }
```

## 4. Phase 3: User-Guided Incremental Transformations

### 4.1 Extend SQL Block for Incremental Transformations

Update the `SQLBlockStep` class:

```python
@dataclass
class SQLBlockStep(PipelineStep):
    """Represents a CREATE TABLE AS SQL block with incremental support."""
    
    table_name: str
    sql_query: str
    mode: Optional[str] = None  # Mode like INCREMENTAL_MERGE
    merge_keys: Optional[List[str]] = None  # Keys for incremental merge
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the SQL block with incremental settings."""
        errors = []
        
        if not self.table_name:
            errors.append("SQL block requires a table name")
        if not self.sql_query:
            errors.append("SQL block requires a query")
            
        # Validate incremental mode settings
        if self.mode == "INCREMENTAL_MERGE" and not self.merge_keys:
            errors.append("INCREMENTAL_MERGE mode requires ON <key_columns>")
        
        return errors
```

### 4.2 Parser Extension for SQL Block

Extend the `_parse_sql_block_statement()` method:

```python
def _parse_sql_block_statement(self) -> SQLBlockStep:
    """Parse a CREATE TABLE AS statement with optional MODE parameter."""
    # Existing implementation for CREATE TABLE AS
    # ...
    
    # Check for MODE keyword
    mode = None
    merge_keys = None
    
    if self._match(TokenType.MODE):
        mode_token = self._consume(TokenType.IDENTIFIER, "Expected mode type after MODE")
        mode = mode_token.value.upper()
        
        # Handle INCREMENTAL_MERGE mode with key columns
        if mode == "INCREMENTAL_MERGE":
            self._consume(TokenType.ON, "Expected ON after INCREMENTAL_MERGE mode")
            
            # Parse comma-separated list of key columns
            key_columns = []
            while True:
                key_token = self._consume(TokenType.IDENTIFIER, "Expected key column identifier")
                key_columns.append(key_token.value)
                
                if not self._check(TokenType.COMMA):
                    break
                    
                self._consume(TokenType.COMMA, "Expected comma between key columns")
            
            merge_keys = key_columns
    
    # Continue with the AS part of CREATE TABLE AS
    # ...
    
    return SQLBlockStep(
        table_name=table_name_token.value,
        sql_query=sql_query,
        mode=mode,
        merge_keys=merge_keys,
        line_number=create_token.line,
    )
```

### 4.3 SQL Generation for Incremental Transformation

Extend the SQL generator for incremental transformations:

```python
def _generate_transform_sql(
    self, operation: Dict[str, Any], context: Dict[str, Any]
) -> str:
    """Generate SQL for a transform operation with incremental support.
    
    Args:
        operation: Transform operation definition
        context: Execution context
        
    Returns:
        SQL for the transform operation
    """
    table_name = operation.get("table_name", "")
    query = operation.get("query", "")
    mode = operation.get("mode")
    merge_keys = operation.get("merge_keys", [])
    
    if not mode or mode == "REPLACE":
        return f"""-- Transform operation (REPLACE mode)
CREATE OR REPLACE TABLE {table_name} AS
{query}"""
    
    elif mode == "INCREMENTAL_MERGE":
        # For INCREMENTAL_MERGE, we need to:
        # 1. Create a temporary table with the query results
        # 2. Merge the temporary table into the target table
        
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        
        # Generate list of key columns for ON clause
        key_conditions = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        # SQL to create temp table
        temp_sql = f"""-- Create temporary table with query results
CREATE TEMPORARY TABLE {temp_table} AS
{query};"""
        
        # Get schema of temp table
        column_info = self._get_schema(context, temp_table)
        columns = list(column_info.keys())
        
        # Generate update clause (all columns except merge keys)
        update_columns = [c for c in columns if c not in merge_keys]
        updates = ", ".join([f"target.{c} = source.{c}" for c in update_columns])
        
        # Generate insert clause
        column_list = ", ".join(columns)
        source_columns = ", ".join([f"source.{c}" for c in columns])
        
        # SQL to check if target table exists
        check_sql = f"""-- Check if target table exists
CREATE TABLE IF NOT EXISTS {table_name} AS
SELECT * FROM {temp_table} LIMIT 0;"""
        
        # SQL to merge data
        merge_sql = f"""-- Merge data incrementally
MERGE INTO {table_name} target
USING {temp_table} source
ON {key_conditions}
WHEN MATCHED THEN
  UPDATE SET {updates}
WHEN NOT MATCHED THEN
  INSERT ({column_list})
  VALUES ({source_columns});

-- Clean up temporary table
DROP TABLE {temp_table};"""
        
        return f"{temp_sql}\n\n{check_sql}\n\n{merge_sql}"
```

## 5. CLI Enhancements for Watermark Management

### 5.1 Watermark CLI Commands

Add new subcommands to manage watermarks:

```python
@pipeline_app.command("show-watermarks")
def show_watermarks(
    pipeline_name: Optional[str] = typer.Argument(
        None, help="Show watermarks for specific pipeline (all if omitted)"
    ),
    source_name: Optional[str] = typer.Option(
        None, help="Filter by specific source name"
    ),
):
    """Show current watermark values for incremental sources."""
    # Implementation to retrieve and display watermarks
```

```python
@pipeline_app.command("reset-watermark")
def reset_watermark(
    pipeline_name: str = typer.Argument(..., help="Pipeline name"),
    source_name: str = typer.Argument(..., help="Source name"),
    confirm: bool = typer.Option(
        False, "--confirm", help="Confirm watermark reset without prompt"
    ),
):
    """Reset a watermark for an incremental source (forces full reload next run)."""
    # Implementation to reset watermark with confirmation
```

```python
@pipeline_app.command("set-watermark")
def set_watermark(
    pipeline_name: str = typer.Argument(..., help="Pipeline name"),
    source_name: str = typer.Argument(..., help="Source name"),
    value: str = typer.Argument(..., help="New watermark value"),
):
    """Manually set a watermark value for an incremental source."""
    # Implementation to set watermark value
```

## 6. Testing Strategy

### 6.1 Unit Tests

Following the TDD approach in our coding rules:

1. Create unit tests for the parser extensions:
   - Test parsing LOAD with different MODE options
   - Test parsing SOURCE with INCREMENTAL BY
   - Test parsing CREATE TABLE with MODE INCREMENTAL_MERGE

2. Create unit tests for the `WatermarkManager`:
   - Test storing and retrieving watermarks
   - Test updating watermarks

3. Create unit tests for SQL generation:
   - Test SQL generation for different load modes
   - Test SQL generation for incremental merge transformations

### 6.2 Integration Tests

1. End-to-end tests with sample datasets:
   - Test incremental loading with real data
   - Verify watermark tracking across pipeline runs
   - Confirm merge behavior works correctly

2. Golden fixtures for expected outputs and states after incremental runs

### 6.3 Performance Testing

1. Compare execution times for full vs. incremental loads with various data volumes
2. Measure overhead of watermark tracking
3. Evaluate memory usage during incremental operations

## 7. Implementation Checklist

- [ ] Phase 1: Enhanced Load Controls
  - [ ] Parser extensions for LOAD ... MODE syntax
  - [ ] Connector interface updates
  - [ ] SQL generation for different load modes
  - [ ] Operation metrics and user feedback

- [ ] Phase 2: Source-Level Incremental Loading
  - [ ] Watermark management system
  - [ ] Parser extensions for SOURCE ... INCREMENTAL BY
  - [ ] Connector interface for incremental reading
  - [ ] Execution logic for incremental sources

- [ ] Phase 3: User-Guided Incremental Transformations
  - [ ] Parser extensions for CREATE TABLE ... MODE
  - [ ] SQL generation for incremental transformations
  
- [ ] CLI Enhancements
  - [ ] Watermark management commands
  - [ ] Improved feedback and diagnostics

## 8. Recommendations

1. **Start with Load Modes**: Implement Phase 1 first as it provides immediate value and requires fewer architectural changes.

2. **Test with Real Data Patterns**: Test with realistic data change patterns (inserts, updates, deletions) to ensure robust behavior.

3. **Focus on Error Handling**: Make error messages clear and actionable, especially for merge conflicts and schema drift.

4. **Provide Migration Path**: Document how users can migrate existing pipelines to use incremental features.

5. **Performance Monitoring**: Add instrumentation to track performance improvements of incremental vs. full loads.

6. **Documentation and Examples**: Create comprehensive documentation with examples for each incremental pattern.

7. **Incremental by Default**: Where safe, automatically suggest incremental patterns to users based on source schema analysis.

## 9. Conclusion

The incremental loading feature will significantly improve SQLFlow's performance and resource utilization for recurring pipelines. By implementing it in phases, we can deliver value quickly while building toward more sophisticated capabilities.

Following SQLFlow's SQL-native philosophy, the implementation adds incremental capabilities in an intuitive way that will feel natural to SQL users. The focus on clear diagnostics and error messages will help users adopt and troubleshoot these features effectively.
