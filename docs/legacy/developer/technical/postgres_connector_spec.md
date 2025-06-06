# Enhanced PostgreSQL Connector Specification

**Document Version:** 1.1  
**Date:** January 2025  
**Task:** 2.2 - Enhanced PostgreSQL Connector  
**Status:** Completed - Backward Compatibility Implemented

---

## Executive Summary

This specification defines the enhanced PostgreSQL connector for SQLFlow, implementing industry-standard parameters with **full backward compatibility**, incremental loading capabilities, and comprehensive monitoring features. The connector supports both legacy parameter names (`dbname`, `user`) and new industry-standard names (`database`, `username`) for seamless migration from existing configurations.

## Goals

1. **Backward Compatibility**: Support existing `dbname`/`user` parameters seamlessly
2. **Industry-Standard Parameters**: Support new `database`/`username` parameters for Airbyte/Fivetran compatibility  
3. **Parameter Precedence**: New parameter names take precedence when both are provided
4. **Incremental Loading**: Built-in support for cursor-based incremental reading
5. **Enhanced Monitoring**: Comprehensive health checks and performance metrics
6. **Connection Pooling**: Efficient connection management for production use
7. **Schema Awareness**: Support for multiple schemas and custom queries
8. **Error Resilience**: Robust error handling and clear error messages

## Key Features Implemented

### 1. Backward Compatible Parameter Support

**Legacy Configuration (Continues to Work):**
```sql
SOURCE postgres_orders TYPE POSTGRES PARAMS {
  "host": "${DB_HOST}",
  "port": 5432,
  "dbname": "ecommerce",           -- Legacy parameter name (supported)
  "user": "${DB_USER}",            -- Legacy parameter name (supported)
  "password": "${DB_PASSWORD}",
  "sync_mode": "incremental",
  "cursor_field": "updated_at",
  "primary_key": ["order_id"],
  "table": "orders"
};
```

**Industry-Standard Configuration (Recommended):**
```sql
SOURCE postgres_orders TYPE POSTGRES PARAMS {
  "host": "${DB_HOST}",
  "port": 5432,
  "database": "ecommerce",         -- Industry-standard name (Airbyte/Fivetran compatible)
  "username": "${DB_USER}",        -- Industry-standard name (Airbyte/Fivetran compatible)  
  "password": "${DB_PASSWORD}",
  "schema": "public",              -- Schema support
  "sync_mode": "incremental",      -- Airbyte/Fivetran standard
  "cursor_field": "updated_at",    -- Incremental loading field
  "primary_key": ["order_id"],     -- Primary key specification
  "table": "orders",               -- Target table
  "sslmode": "prefer",             -- SSL configuration
  "batch_size": 10000,             -- Performance tuning
  "timeout_seconds": 300,          -- Connection timeout
  "max_retries": 3                -- Retry configuration
};
```

**Mixed Configuration (New Parameters Take Precedence):**
```sql
SOURCE postgres_orders TYPE POSTGRES PARAMS {
  "host": "${DB_HOST}",
  "dbname": "old_database",        -- Legacy name
  "database": "new_database",      -- Industry-standard name (WINS)
  "user": "old_user",              -- Legacy name  
  "username": "new_user",          -- Industry-standard name (WINS)
  "password": "${DB_PASSWORD}"
};
-- Result: Connects to "new_database" as "new_user"
```

### 2. Connection Management

**Connection Pooling:**
- Configurable min/max connections (default: 1-5)
- Automatic connection pool creation and management
- SSL support with configurable SSL modes
- Application name tagging for monitoring

**Parameters:**
- `min_connections`: Minimum pool size (default: 1)
- `max_connections`: Maximum pool size (default: 5)
- `connect_timeout`: Connection timeout in seconds (default: 10)
- `application_name`: Application identifier (default: "sqlflow")
- `sslmode`: SSL mode (default: "prefer")

### 3. Incremental Loading

**Cursor-Based Incremental Reading:**
```python
# Automatic incremental query generation
# Original table: SELECT * FROM orders
# Incremental: SELECT * FROM orders WHERE updated_at > '2024-01-15 10:30:00' ORDER BY updated_at
```

**Features:**
- Automatic WHERE clause generation with cursor filtering
- Support for custom queries with incremental filtering
- Consistent ordering for reliable incremental loading
- Cursor value extraction from data chunks
- Fallback to full refresh if incremental fails

### 4. Health Monitoring

**Comprehensive Health Checks:**
```python
{
    "status": "healthy",
    "connected": True,
    "response_time_ms": 45.2,
    "last_check": "2025-01-28T10:30:00Z",
    "database_size_bytes": 1024000000,
    "table_count": 25,
    "postgresql_version": "PostgreSQL 14.5 on x86_64-pc-linux-gnu",
    "schema": "public",
    "capabilities": {
        "incremental": True,
        "schema_discovery": True,
        "batch_reading": True,
        "custom_queries": True
    }
}
```

**Performance Metrics:**
- Connection establishment time
- Query execution time
- Throughput metrics (rows/second)
- Connection pool status
- Schema-level statistics

### 5. Schema Discovery

**Enhanced Discovery:**
- Schema-aware table discovery
- Configurable schema selection
- Detailed column information with PostgreSQL data types
- Automatic Arrow schema conversion
- Error handling for missing tables/schemas

**Supported PostgreSQL Types:**
- Integer types: `integer`, `bigint`, `smallint` → `pa.int64()`
- Numeric types: `real`, `double precision`, `numeric` → `pa.float64()`
- Boolean: `boolean` → `pa.bool_()`
- Date/Time: `date`, `timestamp` → `pa.date32()`, `pa.timestamp("ns")`
- Text: All other types → `pa.string()`

## Implementation Details

### 1. Backward Compatible Parameter Validation

```python
class PostgresParameterValidator(ParameterValidator):
    def _get_required_params(self) -> List[str]:
        return ["host"]  # Only host is truly required
    
    def _get_optional_params(self) -> Dict[str, Any]:
        return {
            "port": 5432,
            "schema": "public",
            "sync_mode": "full_refresh",
            "connect_timeout": 10,
            "application_name": "sqlflow",
            "min_connections": 1,
            "max_connections": 5,
            "sslmode": "prefer",
            # Support both old and new parameter names
            "database": None,  # New industry-standard name
            "dbname": None,    # Old parameter name (backward compatibility)
            "username": None,  # New industry-standard name
            "user": None,      # Old parameter name (backward compatibility)
            "password": "",    # Required but can be empty for some auth methods
        }

    def validate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters with backward compatibility support."""
        validated = super().validate(params)
        
        # Handle database name - new 'database' takes precedence over old 'dbname'
        database = validated.get("database") or validated.get("dbname")
        if not database:
            raise ParameterError("Either 'database' or 'dbname' parameter is required")
        validated["database"] = database
        
        # Handle username - new 'username' takes precedence over old 'user'
        username = validated.get("username") or validated.get("user")
        if not username:
            raise ParameterError("Either 'username' or 'user' parameter is required")
        validated["username"] = username
        
        # Ensure integer parameters are converted properly
        for int_param in ["port", "connect_timeout", "min_connections", "max_connections"]:
            if int_param in validated and validated[int_param] is not None:
                try:
                    validated[int_param] = int(validated[int_param])
                except (ValueError, TypeError):
                    raise ParameterError(f"Parameter '{int_param}' must be an integer")
        
        return validated
```

### 2. Incremental Loading Implementation

**Query Building:**
```python
# Base query from table
base_query = f'SELECT {column_str} FROM "{schema}"."{table}"'

# Add incremental filter
if cursor_value is not None:
    base_query += f' WHERE "{cursor_field}" > %s'
    params.append(cursor_value)

# Add ordering for consistency
base_query += f' ORDER BY "{cursor_field}"'
```

**Custom Query Support:**
```python
# Custom query with incremental filtering
if self.params.query:
    base_query = f"SELECT {column_str} FROM ({self.params.query}) as subquery"
    # Add WHERE clause for incremental
    if cursor_value is not None:
        base_query += f' WHERE "{cursor_field}" > %s'
```

### 3. Connection Pool Management

**Initialization:**
```python
self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=self.params.min_connections,
    maxconn=self.params.max_connections,
    host=self.params.host,
    port=self.params.port,
    dbname=self.params.database,  # psycopg2 uses dbname
    user=self.params.username,    # psycopg2 uses user
    password=self.params.password,
    connect_timeout=self.params.connect_timeout,
    application_name=self.params.application_name,
    sslmode=self.params.sslmode,
)
```

**Usage Pattern:**
```python
conn = self.connection_pool.getconn()
try:
    # Use connection
    cursor = conn.cursor()
    # ... execute queries ...
finally:
    self.connection_pool.putconn(conn)
```

## Usage Examples

### 1. Basic Configuration

```sql
SOURCE customers TYPE POSTGRES PARAMS {
  "host": "localhost",
  "database": "ecommerce",
  "username": "readonly_user",
  "password": "${DB_PASSWORD}",
  "table": "customers"
};

LOAD customer_data FROM customers;
```

### 2. Incremental Loading

```sql
SOURCE orders TYPE POSTGRES PARAMS {
  "host": "${DB_HOST}",
  "database": "sales",
  "username": "${DB_USER}",
  "password": "${DB_PASSWORD}",
  "schema": "public",
  "table": "orders",
  "sync_mode": "incremental",
  "cursor_field": "updated_at",
  "primary_key": ["order_id"]
};

LOAD orders_incremental FROM orders MODE APPEND;
```

### 3. Custom Query with Schema

```sql
SOURCE analytics_data TYPE POSTGRES PARAMS {
  "host": "analytics.company.com",
  "database": "warehouse",
  "username": "analytics_user",
  "password": "${ANALYTICS_PASSWORD}",
  "schema": "analytics",
  "query": "SELECT o.*, c.segment FROM orders o JOIN customers c ON o.customer_id = c.id",
  "sync_mode": "incremental",
  "cursor_field": "o.updated_at"
};

LOAD enriched_orders FROM analytics_data MODE REPLACE;
```

### 4. Production Configuration

```sql
SOURCE production_orders TYPE POSTGRES PARAMS {
  "host": "prod-db.company.com",
  "port": 5432,
  "database": "production",
  "username": "etl_user",
  "password": "${PROD_DB_PASSWORD}",
  "schema": "orders",
  "table": "order_items",
  "sync_mode": "incremental",
  "cursor_field": "modified_at",
  "primary_key": ["order_id", "item_id"],
  "sslmode": "require",
  "min_connections": 2,
  "max_connections": 10,
  "batch_size": 5000,
  "timeout_seconds": 600,
  "connect_timeout": 30
};
```

## Error Handling

### 1. Configuration Errors

**Missing Required Parameters:**
```
ParameterError: Missing required parameters: {'database', 'username'}
```

**Invalid Incremental Configuration:**
```
IncrementalError: cursor_field is required for incremental sync_mode
```

### 2. Connection Errors

**Connection Failed:**
```
ConnectorError: Failed to create connection pool: could not connect to server: Connection refused
```

**Authentication Failed:**
```
ConnectorError: FATAL: password authentication failed for user "wrong_user"
```

### 3. Query Errors

**Table Not Found:**
```
ConnectorError: Table not found: invalid_table in schema public
```

**Invalid Cursor Field:**
```
IncrementalError: Cursor field 'invalid_field' not found in data chunk columns: ['id', 'name', 'created_at']
```

## Performance Considerations

### 1. Connection Pooling

- **Default Pool Size**: 1-5 connections suitable for most use cases
- **High-Volume**: Increase max_connections to 10-20 for high-throughput scenarios
- **Development**: Use min_connections=1, max_connections=2 for development

### 2. Batch Size Optimization

- **Default**: 10,000 rows per batch (good balance of memory and performance)
- **Large Tables**: Reduce to 5,000 for wide tables with many columns
- **High Memory**: Increase to 50,000 for narrow tables with sufficient memory

### 3. Incremental Loading

- **Cursor Field Selection**: Use indexed timestamp columns for best performance
- **Query Optimization**: Ensure WHERE clause on cursor field uses appropriate indexes
- **Data Types**: Timestamp and integer cursor fields perform better than strings

## Testing Strategy

### 1. Unit Tests

- Parameter validation with valid/invalid configurations
- Connection pool creation and management
- Incremental query building
- Cursor value extraction
- Error handling scenarios

### 2. Integration Tests

- Real PostgreSQL connection testing
- Incremental loading with sample data
- Health monitoring validation
- Performance metrics collection
- Schema discovery across multiple schemas

### 3. Performance Tests

- Connection pool performance under load
- Large dataset incremental loading
- Query execution time benchmarks
- Memory usage optimization validation

## Migration from Standard Parameters

### No Migration Required - Full Backward Compatibility

**Existing Configurations Continue to Work:**
```sql
-- Existing SQLFlow configurations (no changes needed)
SOURCE postgres_data TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "dbname": "ecommerce",    -- OLD: Still works perfectly
  "user": "username",       -- OLD: Still works perfectly  
  "password": "pass"
};
```

### Recommended Migration Path (Optional)

**From Legacy SQLFlow to Industry-Standard:**
```sql
-- BEFORE (legacy - still works)
SOURCE old_postgres TYPE POSTGRES PARAMS {
  "host": "localhost",
  "dbname": "ecommerce",    -- Legacy parameter
  "user": "username",       -- Legacy parameter
  "password": "pass"
};

-- AFTER (recommended - industry standard)
SOURCE new_postgres TYPE POSTGRES PARAMS {
  "host": "localhost",
  "database": "ecommerce",  -- Industry-standard (Airbyte/Fivetran compatible)
  "username": "username",   -- Industry-standard (Airbyte/Fivetran compatible)
  "password": "pass",
  "sync_mode": "incremental",  -- New capabilities
  "cursor_field": "updated_at",
  "schema": "public"
};
```

### From Airbyte Configuration (Direct Migration)

```yaml
# Airbyte postgres.yaml
host: localhost
port: 5432
database: ecommerce
username: user
password: pass
sync_mode: incremental
cursor_field: updated_at
```

```sql
-- Direct SQLFlow equivalent (1:1 parameter mapping)
SOURCE postgres_data TYPE POSTGRES PARAMS {
  "host": "localhost",
  "port": 5432,
  "database": "ecommerce",      -- Same parameter name ✅
  "username": "user",           -- Same parameter name ✅
  "password": "pass",
  "sync_mode": "incremental",   -- Same parameter name ✅
  "cursor_field": "updated_at"  -- Same parameter name ✅
};
```

### Parameter Precedence Rules

When both old and new parameter names are provided:

```sql
SOURCE mixed_params TYPE POSTGRES PARAMS {
  "host": "localhost",
  "dbname": "legacy_db",        -- Legacy name
  "database": "standard_db",    -- Industry-standard name
  "user": "legacy_user",        -- Legacy name
  "username": "standard_user",  -- Industry-standard name
  "password": "pass"
};

-- RESULT: Connects to "standard_db" as "standard_user"
-- Industry-standard parameters always take precedence
```

## Next Steps

1. **Task 2.3**: Enhanced S3 Connector with similar industry-standard patterns
2. **Task 2.4**: Resilience Patterns - implement retry logic, circuit breakers
3. **Task 2.5**: Phase 2 Demo Integration - showcase all enhanced connectors

## Conclusion

The Enhanced PostgreSQL Connector successfully implements industry-standard parameters, incremental loading, and comprehensive monitoring while maintaining backward compatibility. It provides a solid foundation for other database connectors and demonstrates SQLFlow's commitment to industry-standard conventions and operational excellence. 