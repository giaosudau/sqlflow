# Building SQLFlow Connectors

## Overview

SQLFlow's connector system provides a standardized interface for integrating external data sources and destinations. This guide covers the architecture, implementation patterns, and best practices for building production-ready connectors.

**Connector Philosophy**
- **Industry standards first**: Compatible with Airbyte/Fivetran patterns
- **Automatic state management**: Built-in incremental loading and watermark tracking
- **Resilience by design**: Automatic retry, circuit breaker, and rate limiting
- **Performance optimization**: Batching, connection pooling, and caching

## 🏗️ Connector Architecture

### Base Connector Interface

All connectors implement a standardized interface defined in `sqlflow/connectors/base.py`:

```python
from sqlflow.connectors.base import Connector, register_connector
from sqlflow.connectors.data_chunk import DataChunk

@register_connector("MYAPI")  # Auto-registration with SQLFlow
class MyAPIConnector(Connector):
    def configure(self, params: Dict[str, Any]) -> None:
        """Configure connector with validated parameters."""
        validated_params = self.validate_params(params)
        self.api_key = validated_params["api_key"]
        self.base_url = validated_params["base_url"]
        
    def test_connection(self) -> ConnectionTestResult:
        """Test connectivity and return result."""
        # Implementation here
        
    def read(self, object_name: str, **kwargs) -> Iterator[DataChunk]:
        """Read data and yield DataChunk objects."""
        # Implementation here
        
    def read_incremental(self, object_name: str, cursor_field: str, 
                        cursor_value: Optional[Any] = None, **kwargs) -> Iterator[DataChunk]:
        """Read data incrementally using cursor-based filtering."""
        # Implementation here - state management handled automatically
```

### Connector Types

SQLFlow supports three connector types:

1. **Source Connectors**: Read data from external systems
2. **Export Connectors**: Write data to external systems  
3. **Bidirectional Connectors**: Both read and write capabilities

## 📋 Implementation Guide

### Step 1: Choose Your Connector Type

Based on your data flow requirements:

```python
# Source-only connector
from sqlflow.connectors.base import Connector

# Export-only connector  
from sqlflow.connectors.base import ExportConnector

# Bidirectional connector
from sqlflow.connectors.base import BidirectionalConnector
```

### Step 2: Implement Core Methods

**Required Methods:**
- `configure(params)`: Parameter validation and setup
- `test_connection()`: Connection health check
- `read()` or `write()`: Core data operations

**Optional Methods:**
- `read_incremental()`: For incremental loading support
- `discover()`: List available objects/tables
- `get_schema()`: Schema introspection

### Step 3: Parameter Validation

Use the built-in parameter validation framework:

```python
def configure(self, params: Dict[str, Any]) -> None:
    """Configure with industry-standard parameter validation."""
    validated_params = self.validate_params(params)
    
    # Connection parameters (industry standard)
    self.host = validated_params["host"]
    self.database = validated_params.get("database")
    self.username = validated_params["username"]
    self.password = validated_params["password"]
    
    # Incremental loading parameters
    self.sync_mode = validated_params.get("sync_mode", "full_refresh")
    self.cursor_field = validated_params.get("cursor_field")
    self.primary_key = validated_params.get("primary_key", [])
    
    # Performance parameters (with defaults)
    self.batch_size = validated_params.get("batch_size", 10000)
    self.timeout_seconds = validated_params.get("timeout_seconds", 300)
```

### Step 4: Add Incremental Support

For incremental loading, implement the `read_incremental` method:

```python
def read_incremental(self, object_name: str, cursor_field: str, 
                    cursor_value: Optional[Any] = None, **kwargs) -> Iterator[DataChunk]:
    """Read data incrementally with automatic watermark management."""
    
    # Build query with WHERE clause for incremental filtering
    if cursor_value is not None:
        query = f"SELECT * FROM {object_name} WHERE {cursor_field} > %s"
        params = [cursor_value]
    else:
        # First run - load all data
        query = f"SELECT * FROM {object_name}"
        params = []
    
    # Execute and yield data chunks
    for chunk in self._execute_query_chunks(query, params):
        yield chunk

def supports_incremental(self) -> bool:
    """Indicate that this connector supports incremental loading."""
    return True
```

State management and watermark tracking are handled automatically by SQLFlow's execution engine.

### Step 5: Testing and Validation

Implement comprehensive testing:

```python
def test_connection(self) -> ConnectionTestResult:
    """Test connection with detailed error reporting."""
    try:
        # Test basic connectivity
        self._test_connection()
        
        # Test read capability
        self._test_read_capability()
        
        return ConnectionTestResult(success=True, message="Connection successful")
        
    except Exception as e:
        return ConnectionTestResult(success=False, message=str(e))
```

## 🔧 Advanced Features

### Resilience Patterns

SQLFlow provides automatic resilience features:

```python
from sqlflow.connectors.resilience import resilient_operation

class MyConnector(Connector):
    @resilient_operation()  # Automatic retry, circuit breaker, rate limiting
    def read(self, object_name: str, **kwargs):
        # Your connector logic gets automatic failure recovery
        pass
```

### Performance Optimization

Built-in optimizations include:

- **Batch Processing**: Configurable batch sizes for memory efficiency
- **Connection Pooling**: Automatic connection reuse
- **Query Optimization**: Smart query construction for incremental loads
- **Caching**: Metadata and schema caching

### Schema Handling

Implement schema discovery and validation:

```python
def get_schema(self, object_name: str) -> Schema:
    """Get schema information for an object."""
    # Query metadata tables or API
    columns = self._get_column_info(object_name)
    arrow_schema = self._build_arrow_schema(columns)
    return Schema(arrow_schema)

def discover(self) -> List[str]:
    """Discover available objects/tables."""
    return self._list_available_objects()
```

## 📊 Real-World Examples

### Database Connector Pattern

```python
@register_connector("MYSQL")
class MySQLConnector(Connector):
    def configure(self, params):
        validated_params = self.validate_params(params)
        self.connection_string = self._build_connection_string(validated_params)
        
    def read_incremental(self, object_name, cursor_field, cursor_value=None, **kwargs):
        query = f"SELECT * FROM {object_name}"
        if cursor_value:
            query += f" WHERE {cursor_field} > %s ORDER BY {cursor_field}"
            
        # Execute in batches and yield DataChunk objects
        for chunk_df in self._execute_chunked_query(query, [cursor_value]):
            yield DataChunk(chunk_df)
```

### API Connector Pattern

```python
@register_connector("RESTAPI")
class RestAPIConnector(Connector):
    def configure(self, params):
        self.base_url = params["base_url"]
        self.api_key = params["api_key"]
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        
    def read_incremental(self, object_name, cursor_field, cursor_value=None, **kwargs):
        params = {"limit": self.batch_size}
        if cursor_value:
            params["since"] = cursor_value
            
        response = requests.get(f"{self.base_url}/{object_name}", 
                              headers=self.headers, params=params)
        
        data = response.json()
        df = pd.DataFrame(data)
        yield DataChunk(df)
```

### File-based Connector Pattern

```python
@register_connector("S3")  
class S3Connector(Connector):
    def read_incremental(self, object_name, cursor_field, cursor_value=None, **kwargs):
        # List files based on timestamp filtering
        files = self._list_files_since(cursor_value)
        
        for file_path in files:
            df = self._read_file(file_path)
            yield DataChunk(df)
```

## 🧪 Testing Your Connector

### Unit Testing

```python
import pytest
from sqlflow.connectors.your_connector import YourConnector

class TestYourConnector:
    def test_configuration(self):
        connector = YourConnector()
        params = {"host": "localhost", "database": "test"}
        connector.configure(params)
        assert connector.state == ConnectorState.CONFIGURED
    
    def test_connection(self):
        connector = YourConnector()
        connector.configure(self.test_params)
        result = connector.test_connection()
        assert result.success
        
    def test_incremental_reading(self):
        # Test incremental loading behavior
        pass
```

### Integration Testing

Use the connector interface demo as a reference:

```bash
# Run the connector interface demo
cd examples/connector_interface_demo
./run_demo.sh
```

This demonstrates:
- Parameter validation framework
- Health monitoring capabilities  
- Incremental loading interface
- Performance metrics collection

## 📈 State Management Integration

SQLFlow automatically handles state management for incremental loading. See **[State Management](state-management.md)** for detailed information on:

- Watermark management and persistence
- Incremental loading patterns
- Error handling and recovery
- Performance optimization

Your connector only needs to implement `read_incremental()` - SQLFlow handles the rest.

## 🔍 Debugging and Monitoring

### Connection Health

Use built-in health monitoring:

```python
def check_health(self) -> Dict[str, Any]:
    """Enhanced health check with metrics."""
    health_info = super().check_health()
    
    # Add connector-specific metrics
    health_info.update({
        "api_rate_limit_remaining": self._get_rate_limit(),
        "connection_pool_size": self._get_pool_size(),
        "last_successful_request": self._get_last_success_time()
    })
    
    return health_info
```

### Performance Metrics

Monitor connector performance:

```python
def get_performance_metrics(self) -> Dict[str, Any]:
    """Get connector performance metrics."""
    return {
        "average_request_time": self._get_avg_request_time(),
        "requests_per_minute": self._get_request_rate(),
        "error_rate": self._get_error_rate(),
        "data_throughput_mbps": self._get_throughput()
    }
```

## 🚀 Deployment and Best Practices

### Configuration Management

Use environment variables for sensitive data:

```python
def configure(self, params):
    # Support environment variable substitution
    self.api_key = params.get("api_key") or os.getenv("API_KEY")
    self.password = params.get("password") or os.getenv("DB_PASSWORD")
```

### Error Handling

Implement comprehensive error handling:

```python
from sqlflow.core.errors import ConnectorError

def read(self, object_name, **kwargs):
    try:
        # Connector logic here
        pass
    except requests.exceptions.Timeout:
        raise ConnectorError(self.connector_type, "Request timeout - check network connectivity")
    except requests.exceptions.ConnectionError:
        raise ConnectorError(self.connector_type, "Connection failed - check host and port")
    except Exception as e:
        raise ConnectorError(self.connector_type, f"Unexpected error: {str(e)}")
```

### Security Best Practices

- Use secure credential storage
- Implement proper authentication
- Validate all input parameters
- Use parameterized queries to prevent injection
- Follow principle of least privilege

## 📚 Learning Resources

### Example Implementations

Study existing connectors for patterns:

- **PostgreSQL Connector**: `sqlflow/connectors/postgres_connector.py`
- **CSV Connector**: `sqlflow/connectors/csv_connector.py`
- **S3 Connector**: `sqlflow/connectors/s3_connector.py`
- **Shopify Connector**: `sqlflow/connectors/shopify_connector.py`

### Interface Demo

The connector interface demo provides hands-on examples:

```bash
cd examples/connector_interface_demo
./run_demo.sh
```

This includes:
- Parameter validation examples
- Health monitoring setup
- Incremental loading patterns
- Performance optimization techniques

### Documentation References

- **[State Management](state-management.md)**: Incremental loading and watermarks
- **[Architecture Overview](architecture-overview.md)**: System design principles
- **[Shopify Connector Guide](../user/guides/shopify_connector.md)**: Complete implementation example

---

**Ready to build your connector?** Start with the interface demo examples and follow the implementation patterns above. The SQLFlow community is here to help with questions and code reviews. 